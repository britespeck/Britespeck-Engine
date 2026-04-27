//! GET /orderbook/:event_id — returns the latest depth snapshot.

use axum::{extract::{Path, State}, routing::get, Json, Router};
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::PgPool;

#[derive(Serialize)]
pub struct OrderbookLevel {
    pub price: f64,
    pub size:  f64,
    pub level: i32,
}

#[derive(Serialize)]
pub struct OrderbookSnapshot {
    pub event_id:    String,
    pub platform:    String,
    pub token_id:    Option<String>,
    pub captured_at: DateTime<Utc>,
    pub bids:        Vec<OrderbookLevel>,
    pub asks:        Vec<OrderbookLevel>,
    pub spread:      Option<f64>,
    pub mid_price:   Option<f64>,
}

#[derive(Serialize)]
pub struct OrderbookResponse {
    pub snapshot: Option<OrderbookSnapshot>,
}

pub fn routes() -> Router<PgPool> {
    Router::new().route("/orderbook/:event_id", get(get_orderbook))
}

async fn get_orderbook(
    State(pool): State<PgPool>,
    Path(event_id): Path<String>,
) -> Json<OrderbookResponse> {
    let rows: Vec<(String, Option<String>, String, f64, f64, i32, DateTime<Utc>)> =
        sqlx::query_as(
            "SELECT platform, token_id, side, price, size, level, captured_at
             FROM public.orderbook_snapshots
             WHERE event_id = $1
             ORDER BY captured_at DESC, side, level ASC
             LIMIT 200"
        )
        .bind(&event_id)
        .fetch_all(&pool)
        .await
        .unwrap_or_default();

    if rows.is_empty() {
        return Json(OrderbookResponse { snapshot: None });
    }

    let latest_ts = rows[0].6;
    let platform  = rows[0].0.clone();
    let token_id  = rows[0].1.clone();

    let mut bids = Vec::new();
    let mut asks = Vec::new();
    for (_plat, _tok, side, price, size, level, ts) in rows {
        if ts != latest_ts { continue; }
        let lvl = OrderbookLevel { price, size, level };
        if side == "bid" { bids.push(lvl); } else { asks.push(lvl); }
    }

    let best_bid = bids.iter().map(|l| l.price).fold(f64::NEG_INFINITY, f64::max);
    let best_ask = asks.iter().map(|l| l.price).fold(f64::INFINITY,     f64::min);
    let (spread, mid) = if best_bid.is_finite() && best_ask.is_finite() {
        (Some(best_ask - best_bid), Some((best_bid + best_ask) / 2.0))
    } else {
        (None, None)
    };

    Json(OrderbookResponse {
        snapshot: Some(OrderbookSnapshot {
            event_id, platform, token_id,
            captured_at: latest_ts,
            bids, asks, spread, mid_price: mid,
        }),
    })
}
