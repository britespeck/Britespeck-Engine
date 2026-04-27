//! Polymarket CLOB WebSocket client.
//! Streams depth (book) + price changes + last-trade fills for top-200 markets.

use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::PgPool;
use std::collections::HashMap;
use std::time::Duration;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use uuid::Uuid;

const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const WATCHLIST_SIZE: i64 = 200;
const WATCHLIST_REFRESH_SECS: u64 = 300;
const RECONNECT_BACKOFF_SECS: u64 = 5;

#[derive(Debug, Deserialize)]
struct WsLevel { price: String, size: String }

#[derive(Debug, Deserialize)]
#[serde(tag = "event_type")]
enum WsMessage {
    #[serde(rename = "book")]
    Book { asset_id: String, bids: Vec<WsLevel>, asks: Vec<WsLevel> },
    #[serde(rename = "price_change")]
    PriceChange { asset_id: String, changes: Vec<PriceChange> },
    #[serde(rename = "last_trade_price")]
    LastTradePrice { asset_id: String, price: String, size: String, side: Option<String> },
}

#[derive(Debug, Deserialize)]
struct PriceChange { price: String, side: String, size: String }

#[derive(Debug, Clone)]
struct WatchedMarket { event_id: String, token_id: String }

#[derive(Debug, Default, Clone)]
struct LocalBook {
    bids: HashMap<String, f64>,
    asks: HashMap<String, f64>,
}

pub async fn run_polymarket_clob_loop(pool: PgPool) {
    tracing::info!("📡 Starting Polymarket CLOB WS loop");
    loop {
        let watchlist = match load_watchlist(&pool).await {
            Ok(w) if !w.is_empty() => w,
            Ok(_) => {
                tracing::warn!("Polymarket CLOB watchlist empty — sleeping 30s");
                tokio::time::sleep(Duration::from_secs(30)).await;
                continue;
            }
            Err(e) => {
                tracing::error!("Polymarket CLOB watchlist load failed: {}", e);
                tokio::time::sleep(Duration::from_secs(30)).await;
                continue;
            }
        };

        tracing::info!("📡 Polymarket CLOB connecting with {} markets", watchlist.len());

        if let Err(e) = run_session(&pool, &watchlist).await {
            tracing::warn!("Polymarket CLOB session ended: {} — reconnecting in {}s", e, RECONNECT_BACKOFF_SECS);
        }
        tokio::time::sleep(Duration::from_secs(RECONNECT_BACKOFF_SECS)).await;
    }
}

async fn load_watchlist(pool: &PgPool) -> anyhow::Result<Vec<WatchedMarket>> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT external_id, clob_token_yes
         FROM public.prediction_events
         WHERE platform = 'Polymarket'
           AND clob_token_yes IS NOT NULL
           AND COALESCE(status, 'open') NOT IN ('closed','resolved','expired')
         ORDER BY volume_24h DESC NULLS LAST
         LIMIT $1"
    )
    .bind(WATCHLIST_SIZE)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter()
        .map(|(event_id, token_id)| WatchedMarket { event_id, token_id })
        .collect())
}

async fn run_session(pool: &PgPool, watchlist: &[WatchedMarket]) -> anyhow::Result<()> {
    let (ws, _) = connect_async(WS_URL).await?;
    let (mut writer, mut reader) = ws.split();

    let mut by_token: HashMap<String, (String, LocalBook)> = HashMap::new();
    for m in watchlist {
        by_token.insert(m.token_id.clone(), (m.event_id.clone(), LocalBook::default()));
    }

    let asset_ids: Vec<&str> = watchlist.iter().map(|m| m.token_id.as_str()).collect();
    let sub = json!({ "type": "MARKET", "assets_ids": asset_ids });
    writer.send(Message::Text(sub.to_string())).await?;

    let mut refresh = tokio::time::interval(Duration::from_secs(WATCHLIST_REFRESH_SECS));
    refresh.tick().await;

    loop {
        tokio::select! {
            _ = refresh.tick() => {
                tracing::info!("🔄 Polymarket CLOB watchlist refresh");
                let _ = writer.send(Message::Close(None)).await;
                return Ok(());
            }
            msg = reader.next() => {
                let Some(msg) = msg else { return Ok(()); };
                let msg = msg?;
                match msg {
                    Message::Text(txt)   => handle_text(pool, &txt, &mut by_token).await,
                    Message::Binary(b)   => {
                        if let Ok(s) = std::str::from_utf8(&b) {
                            handle_text(pool, s, &mut by_token).await;
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
    by_token: &mut HashMap<String, (String, LocalBook)>,
) {
    let parsed: Value = match serde_json::from_str(txt) { Ok(v) => v, Err(_) => return };
    let messages: Vec<Value> = match parsed { Value::Array(arr) => arr, v => vec![v] };

    for raw in messages {
        let Ok(msg) = serde_json::from_value::<WsMessage>(raw) else { continue; };
        match msg {
            WsMessage::Book { asset_id, bids, asks } => {
                let Some((event_id, book)) = by_token.get_mut(&asset_id) else { continue; };
                book.bids.clear();
                book.asks.clear();
                for l in &bids { if let Ok(s) = l.size.parse::<f64>() { book.bids.insert(l.price.clone(), s); } }
                for l in &asks { if let Ok(s) = l.size.parse::<f64>() { book.asks.insert(l.price.clone(), s); } }
                let event_id = event_id.clone();
                let snap = book.clone();
                if let Err(e) = persist_book(pool, &event_id, &asset_id, &snap).await {
                    tracing::warn!("poly persist_book {}: {}", event_id, e);
                }
            }
            WsMessage::PriceChange { asset_id, changes } => {
                let Some((event_id, book)) = by_token.get_mut(&asset_id) else { continue; };
                for c in &changes {
                    let Ok(size) = c.size.parse::<f64>() else { continue; };
                    let target = if c.side.eq_ignore_ascii_case("BUY") { &mut book.bids } else { &mut book.asks };
                    if size <= 0.0 { target.remove(&c.price); } else { target.insert(c.price.clone(), size); }
                }
                let event_id = event_id.clone();
                let snap = book.clone();
                if let Err(e) = persist_book(pool, &event_id, &asset_id, &snap).await {
                    tracing::warn!("poly persist_book(delta) {}: {}", event_id, e);
                }
            }
            WsMessage::LastTradePrice { asset_id, price, size, side } => {
                let Some((event_id, _)) = by_token.get(&asset_id) else { continue; };
                let event_id = event_id.clone();
                let (Ok(p), Ok(s)) = (price.parse::<f64>(), size.parse::<f64>()) else { continue; };
                if let Err(e) = persist_trade(pool, &event_id, p, s, side.map(|x| x.to_lowercase())).await {
                    tracing::warn!("poly persist_trade {}: {}", event_id, e);
                }
            }
        }
    }
}

async fn persist_book(pool: &PgPool, event_id: &str, token_id: &str, book: &LocalBook) -> anyhow::Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM public.orderbook_snapshots WHERE event_id = $1 AND platform = 'Polymarket'")
        .bind(event_id).execute(&mut *tx).await?;

    let mut bids: Vec<(f64, f64)> = book.bids.iter()
        .filter_map(|(p, s)| p.parse::<f64>().ok().map(|p| (p, *s))).collect();
    bids.sort_by(|a, b| b.0.total_cmp(&a.0));
    bids.truncate(20);

    let mut asks: Vec<(f64, f64)> = book.asks.iter()
        .filter_map(|(p, s)| p.parse::<f64>().ok().map(|p| (p, *s))).collect();
    asks.sort_by(|a, b| a.0.total_cmp(&b.0));
    asks.truncate(20);

    let now: DateTime<Utc> = Utc::now();
    for (level, (price, size)) in bids.iter().enumerate() {
        sqlx::query(
            "INSERT INTO public.orderbook_snapshots
                (event_id, platform, token_id, side, price, size, level, captured_at)
             VALUES ($1, 'Polymarket', $2, 'bid', $3, $4, $5, $6)"
        )
        .bind(event_id).bind(token_id).bind(price).bind(size).bind(level as i32).bind(now)
        .execute(&mut *tx).await?;
    }
    for (level, (price, size)) in asks.iter().enumerate() {
        sqlx::query(
            "INSERT INTO public.orderbook_snapshots
                (event_id, platform, token_id, side, price, size, level, captured_at)
             VALUES ($1, 'Polymarket', $2, 'ask', $3, $4, $5, $6)"
        )
        .bind(event_id).bind(token_id).bind(price).bind(size).bind(level as i32).bind(now)
        .execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}

async fn persist_trade(pool: &PgPool, event_id: &str, price: f64, size: f64, side: Option<String>) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO public.raw_trades
            (id, event_id, platform, price, size, side, trade_timestamp, ingested_at)
         VALUES ($1, $2, 'Polymarket', $3, $4, $5, NOW(), NOW())
         ON CONFLICT (event_id, platform, trade_timestamp, price, size) DO NOTHING"
    )
    .bind(Uuid::new_v4()).bind(event_id).bind(price).bind(size).bind(side.as_deref())
    .execute(pool).await?;
    Ok(())
}
