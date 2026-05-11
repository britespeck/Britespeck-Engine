//! Kalshi WebSocket client using kalshi-trade-rs crate.
//! Handles the March 2026 fixed-point price migration automatically.
//! Subscribes to Ticker + Orderbook channels for real-time prices.

use anyhow::Result;
use kalshi_trade_rs::{KalshiConfig, KalshiStreamClient, Channel, StreamMessage};
use sqlx::PgPool;
use tracing::{info, warn};

use crate::live_prices::publish_price_update;

/// Run the Kalshi WebSocket loop using the kalshi-trade-rs crate.
/// Caller is responsible for restart-on-error.
pub async fn run_kalshi_ws_loop(pool: PgPool, tickers: Vec<String>) -> Result<()> {
    if tickers.is_empty() {
        warn!("kalshi_ws: empty ticker list");
        return Ok(());
    }

    let config = KalshiConfig::from_env()?;
    let client = KalshiStreamClient::connect(&config).await?;
    let mut handle = client.handle();

    info!("✅ Kalshi WS connected ({} tickers)", tickers.len());

    // Subscribe to ticker channel for live prices
    // and orderbook channel for best bid/ask
    let ticker_refs: Vec<&str> = tickers.iter().map(|s| s.as_str()).collect();

    handle.subscribe(Channel::Ticker, &ticker_refs).await?;
    handle.subscribe(Channel::Orderbook, &ticker_refs).await?;
    handle.subscribe(Channel::Trade, &ticker_refs).await?;

    loop {
        match handle.update_receiver.recv().await {
            Ok(update) => {
                match &update.msg {
                    StreamMessage::Ticker(t) => {
                        // price_dollars is already in 0.0-1.0 format
                        let price: f64 = t.price_dollars.parse().unwrap_or(0.0);
                        let event_id = format!("kalshi:{}", strip_market_suffix(&t.market_ticker));

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
                        }
                    }

                    StreamMessage::Orderbook(ob) => {
                        // Get best yes bid from orderbook
                        let event_id = format!("kalshi:{}", strip_market_suffix(&ob.market_ticker));

                        let best_bid = ob.yes_bids
                            .first()
                            .and_then(|b| b.price_dollars.parse::<f64>().ok());

                        if let Some(price) = best_bid {
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

                                // Save orderbook snapshot
                                save_orderbook_snapshot(&pool, &event_id, &ob.market_ticker, ob).await;
                            }
                        }
                    }

                    StreamMessage::Trade(t) => {
                        // Save to trades_tape
                        let price: f64 = t.price_dollars.parse().unwrap_or(0.0);
                        let event_id = format!("kalshi:{}", strip_market_suffix(&t.market_ticker));

                        if price > 0.0 {
                            // Update odds from trade price
                            let yes_price = if t.taker_side.as_deref() == Some("yes") {
                                price
                            } else {
                                1.0 - price
                            };

                            if yes_price > 0.01 && yes_price < 0.99 {
                                sqlx::query(
                                    "UPDATE public.prediction_events
                                     SET odds = $1, updated_at = NOW()
                                     WHERE external_id = $2
                                       AND status = 'active'"
                                )
                                .bind(yes_price)
                                .bind(&event_id)
                                .execute(&pool)
                                .await
                                .ok();

                                publish_price_update(&event_id, &event_id, "Kalshi", yes_price);
                            }

                            // Insert into trades_tape
                            sqlx::query(
                                "INSERT INTO trades_tape
                                 (event_id, platform, market_id, side, taker_side, price, size, trade_timestamp)
                                 VALUES ($1, 'Kalshi', $2, $3, $4, $5, $6, NOW())
                                 ON CONFLICT DO NOTHING"
                            )
                            .bind(&event_id)
                            .bind(&t.market_ticker)
                            .bind(t.taker_side.as_deref().unwrap_or("yes"))
                            .bind(t.taker_side.as_deref().unwrap_or("yes"))
                            .bind(price)
                            .bind(t.count as f64)
                            .execute(&pool)
                            .await
                            .ok();
                        }
                    }

                    _ => {} // Ignore other message types
                }
            }
            Err(e) => {
                warn!("Kalshi WS recv error: {e}");
                break;
            }
        }
    }

    Ok(())
}

/// Strip -Y or -N suffix from market ticker to get event ticker
/// KXNBAGAME-26MAY10NYKPHI-Y → KXNBAGAME-26MAY10NYKPHI
fn strip_market_suffix(ticker: &str) -> &str {
    if ticker.ends_with("-Y") || ticker.ends_with("-N") {
        &ticker[..ticker.len() - 2]
    } else {
        ticker
    }
}

async fn save_orderbook_snapshot(
    pool: &PgPool,
    event_id: &str,
    market_ticker: &str,
    ob: &kalshi_trade_rs::ws::messages::OrderbookMessage,
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

    for (i, bid) in ob.yes_bids.iter().take(10).enumerate() {
        let price: f64 = bid.price_dollars.parse().unwrap_or(0.0);
        let size: f64 = bid.quantity_dollars.parse().unwrap_or(0.0);
        sqlx::query(
            "INSERT INTO orderbook_snapshots
             (event_id, platform, token_id, side, price, size, level, captured_at)
             VALUES ($1,'Kalshi',$2,'bid',$3,$4,$5,$6)"
        )
        .bind(event_id).bind(market_ticker)
        .bind(price).bind(size).bind(i as i32).bind(now)
        .execute(&mut *tx).await.ok();
    }

    tx.commit().await.ok();
}