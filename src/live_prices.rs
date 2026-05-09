//! Live Prices SSE endpoint — streams real-time price updates to the browser.
//!
//! Uses Server-Sent Events (SSE) to push price changes instantly when
//! Kalshi WS or Polymarket CLOB receives an update.
//!
//! Exposes:
//!   GET /live_prices           — SSE stream of all price updates
//!   GET /live_prices/:event_id — SSE stream for one contract
//!
//! Wire into main.rs:
//!   mod live_prices;
//!   .merge(live_prices::routes())
//!   // Pass the broadcaster to kalshi_ws and polymarket_clob

use axum::{
    extract::{Path, State},
    response::{sse::{Event, KeepAlive, Sse}, IntoResponse},
    routing::get,
    Router,
};
use futures_util::stream::Stream;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::{
    collections::HashMap,
    convert::Infallible,
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::sync::broadcast;
use tokio_stream::{wrappers::BroadcastStream, StreamExt};

// ── Price Update Event ─────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceUpdate {
    pub event_id: String,
    pub external_id: String,
    pub platform: String,
    pub odds: f64,
    pub odds_cents: i64,
    pub updated_at: String,
}

// ── Global Broadcaster ─────────────────────────────────────────────
// All WS handlers send to this channel.
// All SSE clients receive from it.

#[derive(Clone)]
pub struct PriceBroadcaster {
    sender: broadcast::Sender<PriceUpdate>,
}

impl PriceBroadcaster {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(1000);
        Self { sender }
    }

    pub fn send(&self, update: PriceUpdate) {
        // Ignore send errors — no subscribers connected is fine
        let _ = self.sender.send(update);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<PriceUpdate> {
        self.sender.subscribe()
    }
}

// Global singleton broadcaster
pub fn get_broadcaster() -> &'static PriceBroadcaster {
    use std::sync::OnceLock;
    static BROADCASTER: OnceLock<PriceBroadcaster> = OnceLock::new();
    BROADCASTER.get_or_init(PriceBroadcaster::new)
}

// ── Routes ─────────────────────────────────────────────────────────

pub fn routes() -> Router<PgPool> {
    Router::new()
        .route("/live_prices", get(sse_all_prices_handler))
        .route("/live_prices/:event_id", get(sse_single_price_handler))
}

// ── GET /live_prices ───────────────────────────────────────────────
// Streams ALL price updates to connected browser clients

async fn sse_all_prices_handler(
    State(pool): State<PgPool>,
) -> impl IntoResponse {
    let receiver = get_broadcaster().subscribe();
    let stream = BroadcastStream::new(receiver)
        .filter_map(|result| {
            result.ok().map(|update| {
                let json = serde_json::to_string(&update).unwrap_or_default();
                Ok::<Event, Infallible>(
                    Event::default()
                        .event("price_update")
                        .data(json)
                )
            })
        });

    Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("ping")
        )
}

// ── GET /live_prices/:event_id ─────────────────────────────────────
// Streams price updates for ONE specific contract

async fn sse_single_price_handler(
    State(pool): State<PgPool>,
    Path(event_id): Path<String>,
) -> impl IntoResponse {
    let receiver = get_broadcaster().subscribe();
    let filter_id = event_id.clone();

    let stream = BroadcastStream::new(receiver)
        .filter_map(move |result| {
            result.ok().and_then(|update| {
                // Only pass through updates for this specific event
                if update.event_id == filter_id || update.external_id == filter_id {
                    let json = serde_json::to_string(&update).unwrap_or_default();
                    Some(Ok::<Event, Infallible>(
                        Event::default()
                            .event("price_update")
                            .data(json)
                    ))
                } else {
                    None
                }
            })
        });

    Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("ping")
        )
}

// ── Price Publisher ────────────────────────────────────────────────
// Called from kalshi_ws.rs and polymarket_clob.rs after every price update

pub fn publish_price_update(
    event_id: &str,
    external_id: &str,
    platform: &str,
    odds: f64,
) {
    let update = PriceUpdate {
        event_id: event_id.to_string(),
        external_id: external_id.to_string(),
        platform: platform.to_string(),
        odds,
        odds_cents: (odds * 100.0).round() as i64,
        updated_at: chrono::Utc::now().to_rfc3339(),
    };
    get_broadcaster().send(update);
}