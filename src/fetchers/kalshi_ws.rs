//! Kalshi authenticated WebSocket client.
//! Streams orderbook_snapshot/delta + ticker_v2 fills for the top markets.

use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use rsa::{pkcs1::DecodeRsaPrivateKey, pkcs8::DecodePrivateKey,
          pss::SigningKey, signature::{RandomizedSigner, SignatureEncoding},
          RsaPrivateKey};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::Sha256;
use sqlx::PgPool;
use std::collections::HashMap;
use std::env;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_tungstenite::{connect_async, tungstenite::{client::IntoClientRequest, Message}};
use uuid::Uuid;

const WS_URL: &str = "wss://api.elections.kalshi.com/trade-api/ws/v2";
const WATCHLIST_SIZE: i64 = 100;
const WATCHLIST_REFRESH_SECS: u64 = 300;
const RECONNECT_BACKOFF_SECS: u64 = 5;

#[derive(Debug, Clone)]
struct WatchedMarket { event_id: String, ticker: String }

#[derive(Debug, Default, Clone)]
struct LocalBook {
    // price (cents 1-99) → contracts
    yes_bids: HashMap<i64, f64>,
    yes_asks: HashMap<i64, f64>,
}

#[derive(Debug, Deserialize)]
struct OrderbookSnapshotMsg {
    market_ticker: String,
    #[serde(default)]
    yes: Vec<[i64; 2]>,
    #[serde(default)]
    no:  Vec<[i64; 2]>,
}

#[derive(Debug, Deserialize)]
struct OrderbookDeltaMsg {
    market_ticker: String,
    price: i64,
    delta: i64,
    side: String,             // "yes" | "no"
}

#[derive(Debug, Deserialize)]
struct TickerV2Msg {
    market_ticker: String,
    #[serde(default)]
    price: Option<i64>,       // cents
    #[serde(default)]
    yes_bid: Option<i64>,
    #[serde(default)]
    yes_ask: Option<i64>,
    #[serde(default)]
    volume_delta: Option<i64>,
    #[serde(default)]
    last_side: Option<String>,
}

pub async fn run_kalshi_ws_loop(pool: PgPool) {
    let key_id = match env::var("KALSHI_API_KEY_ID").ok().filter(|s| !s.trim().is_empty()) {
        Some(v) => v,
        None => {
            tracing::warn!("⚠️  KALSHI_API_KEY_ID not set — Kalshi WS loop disabled");
            return;
        }
    };
    let pem = match env::var("KALSHI_API_PRIVATE_KEY").ok().filter(|s| !s.trim().is_empty()) {
        Some(v) => v,
        None => {
            tracing::warn!("⚠️  KALSHI_API_PRIVATE_KEY not set — Kalshi WS loop disabled");
            return;
        }
    };

    let private_key = match parse_rsa_key(&pem) {
        Ok(k) => k,
        Err(e) => {
            tracing::error!("Kalshi private key parse failed: {} — disabling loop", e);
            return;
        }
    };

    tracing::info!("📡 Starting Kalshi authenticated WS loop");

    loop {
        let watchlist = match load_watchlist(&pool).await {
            Ok(w) if !w.is_empty() => w,
            Ok(_) => {
                tracing::warn!("Kalshi watchlist empty — sleeping 30s");
                tokio::time::sleep(Duration::from_secs(30)).await;
                continue;
            }
            Err(e) => {
                tracing::error!("Kalshi watchlist load failed: {}", e);
                tokio::time::sleep(Duration::from_secs(30)).await;
                continue;
            }
        };

        tracing::info!("📡 Kalshi WS connecting with {} markets", watchlist.len());

        if let Err(e) = run_session(&pool, &watchlist, &key_id, &private_key).await {
            tracing::warn!("Kalshi WS session ended: {} — reconnecting in {}s", e, RECONNECT_BACKOFF_SECS);
        }
        tokio::time::sleep(Duration::from_secs(RECONNECT_BACKOFF_SECS)).await;
    }
}

fn parse_rsa_key(pem: &str) -> anyhow::Result<RsaPrivateKey> {
    // Accept either PKCS#8 or PKCS#1
    if let Ok(k) = RsaPrivateKey::from_pkcs8_pem(pem) { return Ok(k); }
    if let Ok(k) = RsaPrivateKey::from_pkcs1_pem(pem) { return Ok(k); }
    anyhow::bail!("RSA key is neither PKCS#8 nor PKCS#1 PEM")
}

fn sign_request(key: &RsaPrivateKey, ts_ms: &str, method: &str, path: &str) -> anyhow::Result<String> {
    // Kalshi: msg = ts + method + path
    let msg = format!("{}{}{}", ts_ms, method, path);
    let signing_key: SigningKey<Sha256> = SigningKey::<Sha256>::new(key.clone());
    let mut rng = rand::thread_rng();
    let sig = signing_key.sign_with_rng(&mut rng, msg.as_bytes());
    Ok(B64.encode(sig.to_bytes()))
}

async fn load_watchlist(pool: &PgPool) -> anyhow::Result<Vec<WatchedMarket>> {
    // external_id is "kalshi:<TICKER>" — strip prefix for Kalshi API
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT external_id
         FROM public.prediction_events
         WHERE platform = 'Kalshi'
           AND external_id IS NOT NULL
           AND COALESCE(status, 'open') NOT IN ('closed','resolved','expired')
         ORDER BY volume_24h DESC NULLS LAST
         LIMIT $1"
    )
    .bind(WATCHLIST_SIZE)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter()
        .filter_map(|(ext,)| {
            let ticker = ext.strip_prefix("kalshi:").unwrap_or(&ext).to_string();
            if ticker.is_empty() { None } else { Some(WatchedMarket { event_id: ext, ticker }) }
        })
        .collect())
}

async fn run_session(
    pool: &PgPool,
    watchlist: &[WatchedMarket],
    key_id: &str,
    private_key: &RsaPrivateKey,
) -> anyhow::Result<()> {
    // Build signed handshake headers
    let ts_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis().to_string();
    let path = "/trade-api/ws/v2";
    let signature = sign_request(private_key, &ts_ms, "GET", path)?;

    let mut req = WS_URL.into_client_request()?;
    let h = req.headers_mut();
    h.insert("KALSHI-ACCESS-KEY",       key_id.parse()?);
    h.insert("KALSHI-ACCESS-SIGNATURE", signature.parse()?);
    h.insert("KALSHI-ACCESS-TIMESTAMP", ts_ms.parse()?);

    let (ws, _) = connect_async(req).await?;
    let (mut writer, mut reader) = ws.split();

    // ticker → (event_id, local book)
    let mut by_ticker: HashMap<String, (String, LocalBook)> = HashMap::new();
    for m in watchlist {
        by_ticker.insert(m.ticker.clone(), (m.event_id.clone(), LocalBook::default()));
    }

    let tickers: Vec<&str> = watchlist.iter().map(|m| m.ticker.as_str()).collect();

    // Subscribe to orderbook_delta (Kalshi sends initial snapshot then deltas)
    let sub_book = json!({
        "id": 1, "cmd": "subscribe",
        "params": { "channels": ["orderbook_delta"], "market_tickers": tickers }
    });
    writer.send(Message::Text(sub_book.to_string())).await?;

    // Subscribe to ticker_v2 for fills
    let sub_ticker = json!({
        "id": 2, "cmd": "subscribe",
        "params": { "channels": ["ticker_v2"], "market_tickers": tickers }
    });
    writer.send(Message::Text(sub_ticker.to_string())).await?;

    let mut refresh = tokio::time::interval(Duration::from_secs(WATCHLIST_REFRESH_SECS));
    refresh.tick().await;

    loop {
        tokio::select! {
            _ = refresh.tick() => {
                tracing::info!("🔄 Kalshi WS watchlist refresh");
                let _ = writer.send(Message::Close(None)).await;
                return Ok(());
            }
            msg = reader.next() => {
                let Some(msg) = msg else { return Ok(()); };
                let msg = msg?;
                match msg {
                    Message::Text(txt)   => handle_text(pool, &txt, &mut by_ticker).await,
                    Message::Binary(b)   => {
                        if let Ok(s) = std::str::from_utf8(&b) {
                            handle_text(pool, s, &mut by_ticker).await;
                        }
                    }
                    Message::Ping(p)     => { let _ = writer.send(Message::Pong(p)).await; }
                    Message::Close(_)    => return Ok(()),
                    _ => {}
                }
            }
        }
    }
}

async fn handle_text(
    pool: &PgPool,
    txt: &str,
    by_ticker: &mut HashMap<String, (String, LocalBook)>,
) {
    let v: Value = match serde_json::from_str(txt) { Ok(v) => v, Err(_) => return };

    let Some(msg_type) = v.get("type").and_then(|t| t.as_str()) else { return; };
    let Some(payload)  = v.get("msg") else { return; };

    match msg_type {
        "orderbook_snapshot" => {
            let Ok(snap) = serde_json::from_value::<OrderbookSnapshotMsg>(payload.clone()) else { return; };
            let Some((event_id, book)) = by_ticker.get_mut(&snap.market_ticker) else { return; };
            book.yes_bids.clear();
            book.yes_asks.clear();
            // Kalshi 'yes' levels = bids on YES; 'no' levels = bids on NO which are asks on YES at (100 - price)
            for [price, size] in &snap.yes { book.yes_bids.insert(*price, *size as f64); }
            for [price, size] in &snap.no  { book.yes_asks.insert(100 - *price, *size as f64); }

            let event_id = event_id.clone();
            let ticker   = snap.market_ticker.clone();
            let snap_book = book.clone();
            if let Err(e) = persist_book(pool, &event_id, &ticker, &snap_book).await {
                tracing::warn!("kalshi persist_book {}: {}", event_id, e);
            }
        }
        "orderbook_delta" => {
            let Ok(d) = serde_json::from_value::<OrderbookDeltaMsg>(payload.clone()) else { return; };
            let Some((event_id, book)) = by_ticker.get_mut(&d.market_ticker) else { return; };

            let (target, key_price) = if d.side.eq_ignore_ascii_case("yes") {
                (&mut book.yes_bids, d.price)
            } else {
                (&mut book.yes_asks, 100 - d.price)
            };

            let new_size = target.get(&key_price).copied().unwrap_or(0.0) + d.delta as f64;
            if new_size <= 0.0 { target.remove(&key_price); } else { target.insert(key_price, new_size); }

            let event_id = event_id.clone();
            let ticker   = d.market_ticker.clone();
            let snap_book = book.clone();
            if let Err(e) = persist_book(pool, &event_id, &ticker, &snap_book).await {
                tracing::warn!("kalshi persist_book(delta) {}: {}", event_id, e);
            }
        }
        "ticker_v2" => {
            let Ok(t) = serde_json::from_value::<TickerV2Msg>(payload.clone()) else { return; };
            let Some((event_id, _)) = by_ticker.get(&t.market_ticker) else { return; };
            let event_id = event_id.clone();

            let price_cents = t.price.or(t.yes_bid).or(t.yes_ask);
            let Some(pc) = price_cents else { return; };
            let price = pc as f64 / 100.0;
            let size  = t.volume_delta.unwrap_or(0).max(0) as f64;
            if size <= 0.0 { return; }
            let side = t.last_side.map(|s| s.to_lowercase());

            if let Err(e) = persist_trade(pool, &event_id, price, size, side).await {
                tracing::warn!("kalshi persist_trade {}: {}", event_id, e);
            }
        }
        _ => {}
    }
}

async fn persist_book(pool: &PgPool, event_id: &str, ticker: &str, book: &LocalBook) -> anyhow::Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM public.orderbook_snapshots WHERE event_id = $1 AND platform = 'Kalshi'")
        .bind(event_id).execute(&mut *tx).await?;

    let mut bids: Vec<(f64, f64)> = book.yes_bids.iter()
        .map(|(p, s)| (*p as f64 / 100.0, *s)).collect();
    bids.sort_by(|a, b| b.0.total_cmp(&a.0));
    bids.truncate(20);

    let mut asks: Vec<(f64, f64)> = book.yes_asks.iter()
        .map(|(p, s)| (*p as f64 / 100.0, *s)).collect();
    asks.sort_by(|a, b| a.0.total_cmp(&b.0));
    asks.truncate(20);

    let now: DateTime<Utc> = Utc::now();
    for (level, (price, size)) in bids.iter().enumerate() {
        sqlx::query(
            "INSERT INTO public.orderbook_snapshots
                (event_id, platform, token_id, side, price, size, level, captured_at)
             VALUES ($1, 'Kalshi', $2, 'bid', $3, $4, $5, $6)"
        )
        .bind(event_id).bind(ticker).bind(price).bind(size).bind(level as i32).bind(now)
        .execute(&mut *tx).await?;
    }
    for (level, (price, size)) in asks.iter().enumerate() {
        sqlx::query(
            "INSERT INTO public.orderbook_snapshots
                (event_id, platform, token_id, side, price, size, level, captured_at)
             VALUES ($1, 'Kalshi', $2, 'ask', $3, $4, $5, $6)"
        )
        .bind(event_id).bind(ticker).bind(price).bind(size).bind(level as i32).bind(now)
        .execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}

async fn persist_trade(pool: &PgPool, event_id: &str, price: f64, size: f64, side: Option<String>) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO public.raw_trades
            (id, event_id, platform, price, size, side, trade_timestamp, ingested_at)
         VALUES ($1, $2, 'Kalshi', $3, $4, $5, NOW(), NOW())
         ON CONFLICT (event_id, platform, trade_timestamp, price, size) DO NOTHING"
    )
    .bind(Uuid::new_v4()).bind(event_id).bind(price).bind(size).bind(side.as_deref())
    .execute(pool).await?;
    Ok(())
}
