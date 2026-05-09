//! Live Prices SSE endpoint — streams real-time price updates to the browser.
//! Uses Server-Sent Events (SSE) to push price changes instantly.
//!
//! Exposes:
//!   GET /live_prices           — SSE stream of all price updates
//!   GET /live_prices/:event_id — SSE stream for one contract

use axum::{
    extract::{Path, State},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse,
    },
    routing::get,
    Router,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::{convert::Infallible, time::Duration};
use tokio::sync::broadcast;
use futures_util::stream;
use futures_util::StreamExt;

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
        let _ = self.sender.send(update);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<PriceUpdate> {
        self.sender.subscribe()
    }
}

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

async fn sse_all_prices_handler(
    State(_pool): State<PgPool>,
) -> impl IntoResponse {
    let mut receiver = get_broadcaster().subscribe();

    let stream = stream::unfold(receiver, |mut rx| async move {
        loop {
            match rx.recv().await {
                Ok(update) => {
                    let json = serde_json::to_string(&update).unwrap_or_default();
                    let event = Ok::<Event, Infallible>(
                        Event::default().event("price_update").data(json)
                    );
                    return Some((event, rx));
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    });

    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("ping"),
    )
}

// ── GET /live_prices/:event_id ─────────────────────────────────────

async fn sse_single_price_handler(
    State(_pool): State<PgPool>,
    Path(event_id): Path<String>,
) -> impl IntoResponse {
    let receiver = get_broadcaster().subscribe();
    let filter_id = event_id.clone();

    let stream = stream::unfold((receiver, filter_id), |(mut rx, fid)| async move {
        loop {
            match rx.recv().await {
                Ok(update) => {
                    if update.event_id == fid || update.external_id == fid {
                        let json = serde_json::to_string(&update).unwrap_or_default();
                        let event = Ok::<Event, Infallible>(
                            Event::default().event("price_update").data(json)
                        );
                        return Some((event, (rx, fid)));
                    }
                    continue;
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    });

    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("ping"),
    )
}

// ── Price Publisher ────────────────────────────────────────────────

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