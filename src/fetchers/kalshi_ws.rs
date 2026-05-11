//! Kalshi WebSocket real-time price feed.
//!
//! Built using insights from pbeets/kalshi-trade-rs:
//! - Subscribe to ALL markets with empty ticker list
//! - price_dollars is a string "0.6500" (March 2026 migration)
//! - yes_price_dollars for trades
//! - yes_dollars_fp for orderbook levels
//! - Auto-reconnect loop in main.rs

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use sqlx::PgPool;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

use crate::live_prices::publish_price_update;

/// Strip -Y or -N suffix from market ticker to get event ticker
/// KXNBAGAME-26MAY10NYKPHI-Y → KXNBAGAME-26MAY10NYKPHI
fn market_to_event_ticker(ticker: &str) -> &str {
    if ticker.ends_with("-Y") || ticker.ends_with("-N") {
        &ticker[..ticker.len() - 2]
    } else {
        ticker
    }
}

/// Parse price from Kalshi's March 2026 fixed-point format
/// New format: "0.6500" (string) — NO division needed
/// Old format: 65 (integer cents) — divide by 100
fn parse_kalshi_price(value: &Value) -> f64 {
    if let Some(s) = value.as_str() {
        s.parse::<f64>().unwrap_or(0.0)
    } else if let Some(n) = value.as_f64() {
        if n > 1.0 { n / 100.0 } else { n }
    } else {
        0.0
    }
}

pub async fn run_kalshi_ws_loop(pool: PgPool, _tickers: Vec<String>) -> Result<()> {
    let api_key = std::env::var("KALSHI_API_TOKEN").unwrap_or_default();
    let ws_url = "wss://api.elections.kalshi.com/trade-api/ws/v2";

    info!("🔌 Kalshi WS connecting to {}", ws_url);

    let (mut ws, _) = connect_async(ws_url).await?;
    info!("✅ Kalshi WS connected");

    // Authenticate
    let auth_msg = json!({
        "id": 1,
        "cmd": "auth",
        "params": {
            "token": api_key
        }
    });
    ws.send(Message::Text(auth_msg.to_string())).await?;

    // Subscribe to ALL markets globally using empty array
    // Key insight from pbeets/kalshi-trade-rs stream_firehose.rs:
    // passing empty array subscribes to ALL markets
    let sub_ticker = json!({
        "id": 2,
        "cmd": "subscribe",
        "params": {
            "channels": ["ticker"],
            "market_tickers": []
        }
    });
    ws.send(Message::Text(sub_ticker.to_string())).await?;

    let sub_trade = json!({
        "id": 3,
        "cmd": "subscribe",
        "params": {
            "channels": ["trade"],
            "market_tickers": []
        }
    });
    ws.send(Message::Text(sub_trade.to_string())).await?;

    let sub_orderbook = json!({
        "id": 4,
        "cmd": "subscribe",
        "params": {
            "channels": ["orderbook_delta"],
            "market_tickers": []
        }
    });
    ws.send(Message::Text(sub_orderbook.to_string())).await?;

    info!("📡 Kalshi WS subscribed: ticker + trade + orderbook_delta (global)");

    while let Some(msg) = ws.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if let Ok(v) = serde_json::from_str::<Value>(&text) {
                    if let Err(e) = handle_msg(&pool, &v).await {
                        warn!("kalshi msg handle err: {}", e);
                    }
                }
            }
            Ok(Message::Ping(data)) => {
                ws.send(Message::Pong(data)).await.ok();
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
            // price_dollars = "0.6500" — March 2026 format, already 0-1 range
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

            let event_ticker = market_to_event_ticker(ticker);
            let event_id = format!("kalshi:{}", event_ticker);

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
            // yes_price_dollars = "0.6500" — March 2026 format
            let price = msg.get("yes_price_dollars")
                .or_else(|| msg.get("price_dollars"))
                .or_else(|| msg.get("yes_price"))
                .map(parse_kalshi_price)
                .unwrap_or(0.0);

            let ticker = msg.get("market_ticker")
                .and_then(|x| x.as_str())
                .unwrap_or("");

            if ticker.is_empty() || price < 0.01 || price > 0.99 { return Ok(()); }

            let event_ticker = market_to_event_ticker(ticker);
            let event_id = format!("kalshi:{}", event_ticker);

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

            // Save to trades_tape
            let taker_side = msg.get("taker_side")
                .and_then(|x| x.as_str())
                .unwrap_or("yes");
            let size = msg.get("count")
                .and_then(|x| x.as_f64())
                .unwrap_or(0.0);

            sqlx::query(
                "INSERT INTO trades_tape
                 (event_id, platform, market_id, side, taker_side, price, size, trade_timestamp)
                 VALUES ($1, 'Kalshi', $2, $3, $4, $5, $6, NOW())
                 ON CONFLICT DO NOTHING"
            )
            .bind(&event_id)
            .bind(ticker)
            .bind(taker_side)
            .bind(taker_side)
            .bind(price)
            .bind(size)
            .execute(pool)
            .await
            .ok();
        }

        "orderbook_delta" | "orderbook_snapshot" => {
            let ticker = msg.get("market_ticker")
                .and_then(|x| x.as_str())
                .unwrap_or("");

            if ticker.is_empty() { return Ok(()); }

            let event_ticker = market_to_event_ticker(ticker);
            let event_id = format!("kalshi:{}", event_ticker);

            // yes_dollars_fp = [["0.6500", "100.00"], ...] — March 2026 format
            let yes_levels = msg.get("yes_dollars_fp")
                .or_else(|| msg.get("yes"))
                .and_then(|x| x.as_array());

            if let Some(levels) = yes_levels {
                if let Some(best) = levels.first() {
                    let price = best.get(0)
                        .map(parse_kalshi_price)
                        .unwrap_or(0.0);

                    if price > 0.01 && price < 0.99 {
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