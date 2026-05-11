//! Kalshi WebSocket real-time price feed.
//!
//! Built from pbeets/kalshi-trade-rs examples:
//! - stream_firehose.rs  → global subscription with &[]
//! - stream_ticker.rs    → price parsing pattern
//! - stream_reconnect.rs → ConnectStrategy::Retry
//! - orderbook_aggregator.rs → OrderbookDelta channel

use anyhow::Result;
use sqlx::PgPool;
use tracing::{info, warn};

use crate::live_prices::publish_price_update;

pub async fn run_kalshi_ws_loop(pool: PgPool, _tickers: Vec<String>) -> Result<()> {
    use kalshi_trade_rs::{
        auth::KalshiConfig,
        ws::{Channel, ConnectStrategy, KalshiStreamClient, StreamMessage},
    };

    let config = KalshiConfig::from_env()?;

    // Connect with ConnectStrategy::Retry — auto-reconnects and replays subscriptions
    // Learned from stream_reconnect.rs
    let client = KalshiStreamClient::connect_with_strategy(
        &config,
        ConnectStrategy::Retry,
    ).await?;
    
    let mut handle = client.handle();
    info!("✅ Kalshi WS connected");

    // KEY INSIGHT from stream_firehose.rs:
    // Pass &[] (empty slice) to subscribe to ALL markets globally
    // No need to specify individual tickers
    handle.subscribe(Channel::Ticker, &[]).await?;
    handle.subscribe(Channel::Trade, &[]).await?;
    handle.subscribe(Channel::OrderbookDelta, &[]).await?;
    handle.subscribe(Channel::MarketLifecycle, &[]).await?;
    
    info!("📡 Kalshi WS subscribed: Ticker + Trade + OrderbookDelta + MarketLifecycle (global)");

    loop {
        match handle.update_receiver.recv().await {
            Ok(update) => match &update.msg {
                
                // Price update from ticker channel
                // price_dollars is already in dollar format "0.6500" (March 2026 migration)
                // NO division by 100 — learned from stream_firehose.rs
                StreamMessage::Ticker(ticker) => {
                    let price: f64 = ticker.price_dollars.parse().unwrap_or(0.0);
                    let event_id = format!("kalshi:{}", ticker.market_ticker);

                    if price > 0.01 && price < 0.99 {
                        let r = sqlx::query(
                            "UPDATE public.prediction_events
                             SET odds = $1, updated_at = NOW()
                             WHERE external_id = $2
                               AND status = 'active'"
                        )
                        .bind(price)
                        .bind(&event_id)
                        .execute(&pool)
                        .await;

                        if let Ok(res) = r {
                            if res.rows_affected() > 0 {
                                publish_price_update(&event_id, &event_id, "Kalshi", price);
                            }
                        }
                    }
                }

                // Trade fill — also updates price
                // trade.yes_price_dollars = "0.6500"
                StreamMessage::Trade(trade) => {
                    let price: f64 = trade.yes_price_dollars.parse().unwrap_or(0.0);
                    let event_id = format!("kalshi:{}", trade.market_ticker);

                    if price > 0.01 && price < 0.99 {
                        sqlx::query(
                            "UPDATE public.prediction_events
                             SET odds = $1, updated_at = NOW()
                             WHERE external_id = $2
                               AND status = 'active'"
                        )
                        .bind(price)
                        .bind(&event_id)
                        .execute(&pool)
                        .await
                        .ok();

                        publish_price_update(&event_id, &event_id, "Kalshi", price);

                        // Save to trades_tape
                        sqlx::query(
                            "INSERT INTO trades_tape
                             (event_id, platform, market_id, side, taker_side, price, size, trade_timestamp)
                             VALUES ($1, 'Kalshi', $2, $3, $4, $5, $6, NOW())
                             ON CONFLICT DO NOTHING"
                        )
                        .bind(&event_id)
                        .bind(&trade.market_ticker)
                        .bind(trade.taker_side.as_deref().unwrap_or("yes"))
                        .bind(trade.taker_side.as_deref().unwrap_or("yes"))
                        .bind(price)
                        .bind(trade.count_fp.parse::<f64>().unwrap_or(0.0))
                        .execute(&pool)
                        .await
                        .ok();
                    }
                }

                // Orderbook delta — best bid/ask update
                // summary.best_bid and best_ask are in cents (i32)
                StreamMessage::OrderbookDelta(ob) => {
                    let event_id = format!("kalshi:{}", ob.market_ticker);

                    // Use midpoint if available, otherwise best bid
                    let price = if let Some(mid) = ob.summary.midpoint {
                        mid / 100.0
                    } else if let Some(bid) = ob.summary.best_bid {
                        bid as f64 / 100.0
                    } else {
                        continue
                    };

                    if price > 0.01 && price < 0.99 {
                        sqlx::query(
                            "UPDATE public.prediction_events
                             SET odds = $1, updated_at = NOW()
                             WHERE external_id = $2
                               AND status = 'active'"
                        )
                        .bind(price)
                        .bind(&event_id)
                        .execute(&pool)
                        .await
                        .ok();

                        // Save orderbook snapshot
                        save_orderbook(&pool, &event_id, &ob.market_ticker, ob).await;
                    }
                }

                // Market lifecycle — detect when contracts resolve
                StreamMessage::MarketLifecycle(lc) => {
                    let event_id = format!("kalshi:{}", lc.market_ticker);
                    
                    // If market closed/determined, update status
                    if let Some(result) = &lc.result {
                        let status = if result.as_str() == "" { "active" } else { "determined" };
                        sqlx::query(
                            "UPDATE public.prediction_events
                             SET status = $1, updated_at = NOW()
                             WHERE external_id = $2"
                        )
                        .bind(status)
                        .bind(&event_id)
                        .execute(&pool)
                        .await
                        .ok();
                        
                        info!("🏁 Kalshi market resolved: {} → {}", lc.market_ticker, status);
                    }
                }

                StreamMessage::Closed { reason } => {
                    warn!("Kalshi WS closed: {}", reason);
                    break;
                }

                StreamMessage::ConnectionLost { reason, .. } => {
                    warn!("Kalshi WS lost: {} — ConnectStrategy::Retry will reconnect", reason);
                    break;
                }

                _ => {}
            },

            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                warn!("Kalshi WS dropped {} messages (slow consumer)", n);
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                warn!("Kalshi WS channel closed");
                break;
            }
        }
    }

    Ok(())
}

async fn save_orderbook(
    pool: &PgPool,
    event_id: &str,
    market_ticker: &str,
    ob: &kalshi_trade_rs::ws::messages::OrderbookDeltaUpdate,
) {
    let now = chrono::Utc::now();
    let mut tx = match pool.begin().await {
        Ok(t) => t,
        Err(_) => return,
    };

    sqlx::query("DELETE FROM orderbook_snapshots WHERE event_id = $1 AND platform = 'Kalshi'")
        .bind(event_id)
        .execute(&mut *tx)
        .await
        .ok();

    for (i, (price_cents, qty)) in ob.yes_levels.iter().enumerate().take(10) {
        let price_f = *price_cents as f64 / 100.0;
        sqlx::query(
            "INSERT INTO orderbook_snapshots
             (event_id, platform, token_id, side, price, size, level, captured_at)
             VALUES ($1,'Kalshi',$2,'bid',$3,$4,$5,$6)"
        )
        .bind(event_id).bind(market_ticker)
        .bind(price_f).bind(*qty as f64).bind(i as i32).bind(now)
        .execute(&mut *tx).await.ok();
    }

    tx.commit().await.ok();
}