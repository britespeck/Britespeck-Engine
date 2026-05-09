use anyhow::{Context, Result};
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use rsa::{
    pkcs1::DecodeRsaPrivateKey,
    pkcs8::DecodePrivateKey,
    pss::SigningKey,
    signature::{RandomizedSigner, SignatureEncoding},
    RsaPrivateKey,
};
use serde_json::{json, Value};
use sha2::Sha256;
use sqlx::PgPool;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message},
};
use tracing::{error, info, warn};

const WS_URL: &str = "wss://api.elections.kalshi.com/trade-api/ws/v2";

fn load_key(pem: &str) -> Result<RsaPrivateKey> {
    RsaPrivateKey::from_pkcs8_pem(pem)
        .or_else(|_| RsaPrivateKey::from_pkcs1_pem(pem))
        .context("parse Kalshi PEM (PKCS#8 or PKCS#1)")
}

fn sign_pss(key: &RsaPrivateKey, msg: &str) -> Result<String> {
    let signing_key = SigningKey::<Sha256>::new(key.clone());
    let mut rng = rand::thread_rng();
    let sig = signing_key.sign_with_rng(&mut rng, msg.as_bytes());
    Ok(B64.encode(sig.to_bytes()))
}

/// Connects to Kalshi's authenticated WS feed and streams orderbook + trade
/// data into Postgres. Caller is responsible for restart-on-error.
pub async fn run_kalshi_ws_loop(pool: PgPool, tickers: Vec<String>) -> Result<()> {
    if tickers.is_empty() {
        warn!("kalshi_ws: empty ticker list, nothing to subscribe to");
        return Ok(());
    }

    let key_id = std::env::var("KALSHI_API_KEY_ID").context("KALSHI_API_KEY_ID")?;
    let pem    = std::env::var("KALSHI_PRIVATE_KEY").context("KALSHI_PRIVATE_KEY")?;
    let key    = load_key(&pem)?;

    let ts     = Utc::now().timestamp_millis().to_string();
    let method = "GET";
    let path   = "/trade-api/ws/v2";
    let sig    = sign_pss(&key, &format!("{ts}{method}{path}"))?;

    let mut req = WS_URL.into_client_request()?;
    req.headers_mut().insert("KALSHI-ACCESS-KEY",       key_id.parse()?);
    req.headers_mut().insert("KALSHI-ACCESS-TIMESTAMP", ts.parse()?);
    req.headers_mut().insert("KALSHI-ACCESS-SIGNATURE", sig.parse()?);

    let (mut ws, _) = connect_async(req).await.context("Kalshi WS connect")?;
    info!("✅ Kalshi WS connected ({} tickers)", tickers.len());

    // Subscribe in batches of 50 to avoid oversized frames
    let mut cmd_id = 1u64;
    for chunk in tickers.chunks(50) {
        let sub = json!({
            "id": cmd_id,
            "cmd": "subscribe",
            "params": {
                "channels": ["orderbook_delta", "trade"],
                "market_tickers": chunk
            }
        });
        ws.send(Message::Text(sub.to_string())).await?;
        cmd_id += 1;
    }

    while let Some(msg) = ws.next().await {
        match msg {
            Ok(Message::Text(txt)) => {
                if let Ok(v) = serde_json::from_str::<Value>(&txt) {
                    if let Err(e) = handle_msg(&pool, &v).await {
                        warn!("kalshi msg handle err: {e}");
                    }
                }
            }
            Ok(Message::Ping(p)) => { let _ = ws.send(Message::Pong(p)).await; }
            Ok(Message::Close(_)) => { warn!("Kalshi WS closed"); break; }
            Err(e) => { error!("Kalshi WS err: {e}"); break; }
            _ => {}
        }
    }
    Ok(())
}

async fn handle_msg(pool: &PgPool, v: &Value) -> Result<()> {
    let Some(ch) = v.get("type").and_then(|x| x.as_str()) else { return Ok(()) };
    let msg = v.get("msg").unwrap_or(&Value::Null);

    match ch {
        "orderbook_snapshot" | "orderbook_delta" => {
            let ticker = msg.get("market_ticker").and_then(|x| x.as_str()).unwrap_or("");
            if ticker.is_empty() { return Ok(()); }
            let event_id = format!("kalshi:{ticker}");
            let now = Utc::now();

            // yes side = bids, no side = asks (Kalshi prices are cents → 0..1)
            let mut tx = pool.begin().await?;
            sqlx::query("DELETE FROM orderbook_snapshots WHERE event_id = $1")
                .bind(&event_id).execute(&mut *tx).await?;

            for (side_in, side_out) in [("yes", "bid"), ("no", "ask")] {
                if let Some(levels) = msg.get(side_in).and_then(|x| x.as_array()) {
                    for (i, lvl) in levels.iter().take(10).enumerate() {
                        let price = lvl.get(0).and_then(|x| x.as_f64()).unwrap_or(0.0) / 100.0;
                        let size  = lvl.get(1).and_then(|x| x.as_f64()).unwrap_or(0.0);
                        sqlx::query(
                            "INSERT INTO orderbook_snapshots
                             (event_id, platform, token_id, side, price, size, level, captured_at)
                             VALUES ($1,'Kalshi',$2,$3,$4,$5,$6,$7)"
                        )
                        .bind(&event_id).bind(ticker).bind(side_out)
                        .bind(price).bind(size).bind(i as i32).bind(now)
                        .execute(&mut *tx).await?;
                    }
                }
            }
            tx.commit().await?;

            // ── Real-time price from orderbook best bid ────────────
            // Extract best yes bid price and update prediction_events
            // This fires on every orderbook update — much more frequent than trades
            if let Some(yes_levels) = msg.get("yes").and_then(|x| x.as_array()) {
                if let Some(best_bid) = yes_levels.first() {
                    let bid_price = best_bid.get(0)
                        .and_then(|x| x.as_f64())
                        .map(|p| p / 100.0)
                        .unwrap_or(0.0);

                    if bid_price > 0.0 && bid_price < 1.0 {
                        sqlx::query(
                            "UPDATE public.prediction_events
                             SET odds = $1, updated_at = NOW()
                             WHERE external_id = $2
                               AND status = 'active'"
                        )
                        .bind(bid_price)
                        .bind(&event_id)
                        .execute(pool)
                        .await
                        .ok();

                        // Broadcast to SSE clients instantly
                        crate::live_prices::publish_price_update(
                            &event_id, &event_id, "Kalshi", bid_price
                        );
                    }
                }
            }
        }
        "trade" => {
            let ticker = msg.get("market_ticker").and_then(|x| x.as_str()).unwrap_or("");
            if ticker.is_empty() { return Ok(()); }
            let event_id = format!("kalshi:{ticker}");
            let price = msg.get("yes_price").and_then(|x| x.as_f64()).unwrap_or(0.0) / 100.0;
            let size  = msg.get("count").and_then(|x| x.as_f64()).unwrap_or(0.0);
            let side  = msg.get("taker_side").and_then(|x| x.as_str()); // "yes"|"no"
            let trade_side = match side {
                Some("yes") => "buy",
                Some("no")  => "sell",
                _ => "buy",
            };
            let ts_secs = msg.get("ts").and_then(|x| x.as_i64()).unwrap_or(0);
            let ts = chrono::DateTime::from_timestamp(ts_secs, 0).unwrap_or_else(Utc::now);

            // ── Real-time price update ─────────────────────────────
            // Update prediction_events.odds immediately on every trade
            // so Britespeck shows live prices matching Kalshi's interface
            if price > 0.0 && price < 1.0 {
                sqlx::query(
                    "UPDATE public.prediction_events
                     SET odds = $1, updated_at = NOW()
                     WHERE external_id = $2
                       AND status = 'active'"
                )
                .bind(price)
                .bind(&event_id)
                .execute(pool)
                .await
                .ok(); // Non-fatal — don't fail the whole handler on miss

                // Broadcast to SSE clients instantly
                crate::live_prices::publish_price_update(
                    &event_id, &event_id, "Kalshi", price
                );
            }

            sqlx::query(
                "INSERT INTO trades_tape (event_id, platform, token_id, side, price, size, trade_timestamp)
                 VALUES ($1,'Kalshi',$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING"
            )
            .bind(&event_id).bind(ticker).bind(trade_side)
            .bind(price).bind(size).bind(ts)
            .execute(pool).await?;
        }
        _ => {}
    }
    Ok(())
}