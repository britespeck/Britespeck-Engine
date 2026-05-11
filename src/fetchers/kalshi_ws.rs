//! Kalshi WebSocket real-time price feed.
//!
//! Auth: RSA-PSS signature in HTTP upgrade headers (NOT a message)
//! Pattern from taetaehoho/poly-kalshi-arb kalshi.rs

use anyhow::{Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use futures_util::{SinkExt, StreamExt};
use pkcs1::DecodeRsaPrivateKey;
use rsa::{
    pss::SigningKey,
    sha2::Sha256,
    signature::{RandomizedSigner, SignatureEncoding},
    RsaPrivateKey,
};
use serde_json::Value;
use sqlx::PgPool;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{http::Request, Message},
};
use tracing::{info, warn};

use crate::live_prices::publish_price_update;

const KALSHI_WS_URL: &str = "wss://api.elections.kalshi.com/trade-api/ws/v2";

/// Strip -Y or -N suffix from market ticker to get event ticker
fn market_to_event_ticker(ticker: &str) -> &str {
    if ticker.ends_with("-Y") || ticker.ends_with("-N") {
        &ticker[..ticker.len() - 2]
    } else {
        ticker
    }
}

/// Parse price from Kalshi's March 2026 fixed-point format
/// New format: "0.6500" (string) — NO division needed
fn parse_kalshi_price(value: &Value) -> f64 {
    if let Some(s) = value.as_str() {
        s.parse::<f64>().unwrap_or(0.0)
    } else if let Some(n) = value.as_f64() {
        if n > 1.0 { n / 100.0 } else { n }
    } else {
        0.0
    }
}

/// Sign message with RSA-PSS using the Kalshi private key
fn sign_kalshi(private_key: &RsaPrivateKey, message: &str) -> Result<String> {
    let signing_key = SigningKey::<Sha256>::new(private_key.clone());
    let signature = signing_key.sign_with_rng(&mut rand::thread_rng(), message.as_bytes());
    Ok(BASE64.encode(signature.to_bytes()))
}

pub async fn run_kalshi_ws_loop(pool: PgPool, tickers: Vec<String>) -> Result<()> {
    // Load credentials — same env vars as the REST fetcher
    let api_key_id = std::env::var("KALSHI_API_KEY_ID")
        .context("KALSHI_API_KEY_ID not set")?;

    let key_path = std::env::var("KALSHI_PRIVATE_KEY_PATH")
        .or_else(|_| std::env::var("KALSHI_PRIVATE_KEY_FILE"))
        .unwrap_or_else(|_| "kalshi_private_key.txt".to_string());

    let pem = std::fs::read_to_string(&key_path)
        .with_context(|| format!("Failed to read Kalshi key from {}", key_path))?;

    let private_key = RsaPrivateKey::from_pkcs1_pem(pem.trim())
        .context("Failed to parse Kalshi RSA private key")?;

    info!("🔌 Kalshi WS connecting to {}", KALSHI_WS_URL);

    // Build RSA signature for the HTTP upgrade request
    // Format: "{timestamp_ms}GET/trade-api/ws/v2"
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .to_string();

    let msg_to_sign = format!("{}GET/trade-api/ws/v2", timestamp_ms);
    let signature = sign_kalshi(&private_key, &msg_to_sign)?;

    // Build the WebSocket upgrade request with auth headers
    // This is how Kalshi WS v2 authenticates — in the HTTP headers, not a message
    let request = Request::builder()
        .uri(KALSHI_WS_URL)
        .header("KALSHI-ACCESS-KEY", &api_key_id)
        .header("KALSHI-ACCESS-SIGNATURE", &signature)
        .header("KALSHI-ACCESS-TIMESTAMP", &timestamp_ms)
        .header("Host", "api.elections.kalshi.com")
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Version", "13")
        .header(
            "Sec-WebSocket-Key",
            tokio_tungstenite::tungstenite::handshake::client::generate_key(),
        )
        .body(())?;

    let (ws_stream, _) = connect_async(request)
        .await
        .context("Failed to connect to Kalshi WS")?;

    info!("✅ Kalshi WS authenticated and connected");

    let (mut write, mut read) = ws_stream.split();

    // Subscribe — use provided tickers or all if empty
    let sub_msg = serde_json::json!({
        "id": 1,
        "cmd": "subscribe",
        "params": {
            "channels": ["orderbook_delta", "ticker", "trade"],
            "market_tickers": tickers
        }
    });

    write.send(Message::Text(sub_msg.to_string())).await?;
    info!("📡 Kalshi WS subscribed to {} tickers", if tickers.is_empty() { "ALL".to_string() } else { tickers.len().to_string() });

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if let Ok(v) = serde_json::from_str::<Value>(&text) {
                    if let Err(e) = handle_msg(&pool, &v).await {
                        warn!("kalshi msg handle err: {}", e);
                    }
                }
            }
            Ok(Message::Ping(data)) => {
                write.send(Message::Pong(data)).await.ok();
            }
            Ok(Message::Close(_)) => {
                warn!("Kalshi WS closed by server");
                break;
            }
            Err(e) => {
                warn!("Kalshi WS error: {}", e);
                break;
            }
            _ => {}
        }
    }

    Ok(())
}

async fn handle_msg(pool: &PgPool, v: &Value) -> Result<()> {
    let msg_type = v.get("type").and_then(|x| x.as_str()).unwrap_or("");
    let msg = v.get("msg").unwrap_or(v);

    match msg_type {
        "ticker" => {
            let price = msg.get("yes_bid_dollars")
                .or_else(|| msg.get("price_dollars"))
                .or_else(|| msg.get("yes_price_dollars"))
                .map(parse_kalshi_price)
                .unwrap_or(0.0);

            let ticker = msg.get("market_ticker")
                .or_else(|| msg.get("ticker"))
                .and_then(|x| x.as_str())
                .unwrap_or("");

            if ticker.is_empty() || price < 0.01 || price > 0.99 { return Ok(()); }

            let event_id = format!("kalshi:{}", market_to_event_ticker(ticker));

            let r = sqlx::query(
                "UPDATE public.prediction_events
                 SET odds = $1, updated_at = NOW()
                 WHERE external_id = $2 AND status = 'active'"
            )
            .bind(price)
            .bind(&event_id)
            .execute(pool)
            .await?;

            if r.rows_affected() > 0 {
                publish_price_update(&event_id, &event_id, "Kalshi", price);
            }
        }

        "trade" => {
            let price = msg.get("yes_price_dollars")
                .or_else(|| msg.get("price_dollars"))
                .or_else(|| msg.get("yes_price"))
                .map(parse_kalshi_price)
                .unwrap_or(0.0);

            let ticker = msg.get("market_ticker")
                .and_then(|x| x.as_str())
                .unwrap_or("");

            if ticker.is_empty() || price < 0.01 || price > 0.99 { return Ok(()); }

            let event_id = format!("kalshi:{}", market_to_event_ticker(ticker));

            sqlx::query(
                "UPDATE public.prediction_events
                 SET odds = $1, updated_at = NOW()
                 WHERE external_id = $2 AND status = 'active'"
            )
            .bind(price)
            .bind(&event_id)
            .execute(pool)
            .await?;

            publish_price_update(&event_id, &event_id, "Kalshi", price);

            let taker_side = msg.get("taker_side").and_then(|x| x.as_str()).unwrap_or("yes");
            let size = msg.get("count").and_then(|x| x.as_f64()).unwrap_or(0.0);

            sqlx::query(
                "INSERT INTO trades_tape
                 (event_id, platform, market_id, side, taker_side, price, size, trade_timestamp)
                 VALUES ($1, 'Kalshi', $2, $3, $4, $5, $6, NOW())
                 ON CONFLICT DO NOTHING"
            )
            .bind(&event_id).bind(ticker)
            .bind(taker_side).bind(taker_side)
            .bind(price).bind(size)
            .execute(pool).await.ok();
        }

        "orderbook_delta" | "orderbook_snapshot" => {
            let ticker = msg.get("market_ticker")
                .and_then(|x| x.as_str())
                .unwrap_or("");

            if ticker.is_empty() { return Ok(()); }

            let event_id = format!("kalshi:{}", market_to_event_ticker(ticker));

            let yes_levels = msg.get("yes_dollars_fp")
                .or_else(|| msg.get("yes"))
                .and_then(|x| x.as_array());

            if let Some(levels) = yes_levels {
                if let Some(best) = levels.first() {
                    let price = best.get(0).map(parse_kalshi_price).unwrap_or(0.0);
                    if price > 0.01 && price < 0.99 {
                        sqlx::query(
                            "UPDATE public.prediction_events
                             SET odds = $1, updated_at = NOW()
                             WHERE external_id = $2 AND status = 'active'"
                        )
                        .bind(price).bind(&event_id)
                        .execute(pool).await?;

                        publish_price_update(&event_id, &event_id, "Kalshi", price);
                    }
                }
            }
        }

        "subscribed" | "ok" | "subscriptions" => {
            info!("Kalshi WS: {}", msg_type);
        }

        "error" => {
            warn!("Kalshi WS error msg: {:?}", msg);
        }

        _ => {}
    }

    Ok(())
}