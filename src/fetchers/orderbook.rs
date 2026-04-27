use axum::{
    extract::{Path, Query, State},
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

#[derive(Serialize)]
pub struct OrderbookLevel {
    pub price: f64,
    pub size: f64,
    pub level: i32,
}

#[derive(Serialize)]
pub struct OrderbookSnapshot {
    pub event_id: String,
    pub platform: String,
    pub token_id: Option<String>,
    pub captured_at: String,
    pub bids: Vec<OrderbookLevel>,
    pub asks: Vec<OrderbookLevel>,
    pub spread: Option<f64>,
    pub mid_price: Option<f64>,
}

#[derive(Serialize)]
pub struct OrderbookResponse {
    pub snapshot: Option<OrderbookSnapshot>,
}

pub async fn get_orderbook(
    State(pool): State<PgPool>,
    Path(event_id): Path<String>,
) -> Json<OrderbookResponse> {
    // Latest captured_at for this event
    let latest: Option<(chrono::DateTime<chrono::Utc>, String, Option<String>)> = sqlx::query_as(
        "SELECT captured_at, platform, token_id
         FROM orderbook_snapshots
         WHERE event_id = $1
         ORDER BY captured_at DESC
         LIMIT 1"
    )
    .bind(&event_id)
    .fetch_optional(&pool)
    .await
    .ok()
    .flatten();

    let Some((captured_at, platform, token_id)) = latest else {
        return Json(OrderbookResponse { snapshot: None });
    };

    // All rows from that snapshot moment
    let rows: Vec<(String, f64, f64, i32)> = sqlx::query_as(
        "SELECT side, price, size, level
         FROM orderbook_snapshots
         WHERE event_id = $1 AND captured_at = $2
         ORDER BY level ASC"
    )
    .bind(&event_id)
    .bind(captured_at)
    .fetch_all(&pool)
    .await
    .unwrap_or_default();

    let mut bids: Vec<OrderbookLevel> = rows.iter().filter(|r| r.0 == "bid")
        .map(|r| OrderbookLevel { price: r.1, size: r.2, level: r.3 }).collect();
    let mut asks: Vec<OrderbookLevel> = rows.iter().filter(|r| r.0 == "ask")
        .map(|r| OrderbookLevel { price: r.1, size: r.2, level: r.3 }).collect();

    bids.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap_or(std::cmp::Ordering::Equal));
    asks.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal));

    let best_bid = bids.first().map(|b| b.price);
    let best_ask = asks.first().map(|a| a.price);
    let spread = match (best_bid, best_ask) { (Some(b), Some(a)) => Some(a - b), _ => None };
    let mid_price = match (best_bid, best_ask) { (Some(b), Some(a)) => Some((a + b) / 2.0), _ => None };

    Json(OrderbookResponse {
        snapshot: Some(OrderbookSnapshot {
            event_id, platform, token_id,
            captured_at: captured_at.to_rfc3339(),
            bids: bids.into_iter().take(10).collect(),
            asks: asks.into_iter().take(10).collect(),
            spread, mid_price,
        }),
    })
}

#[derive(Deserialize)]
pub struct TradesQuery { pub limit: Option<i64> }

#[derive(Serialize)]
pub struct TradeRow {
    pub id: String,
    pub event_id: String,
    pub platform: String,
    pub price: f64,
    pub size: f64,
    pub trade_timestamp: String,
    pub side: Option<String>,
}

#[derive(Serialize)]
pub struct TradesResponse { pub trades: Vec<TradeRow> }

pub async fn get_trades(
    State(pool): State<PgPool>,
    Path(event_id): Path<String>,
    Query(q): Query<TradesQuery>,
) -> Json<TradesResponse> {
    let limit = q.limit.unwrap_or(50).min(200);

    let rows: Vec<(i64, String, String, f64, f64, chrono::DateTime<chrono::Utc>, Option<String>)> =
        sqlx::query_as(
            "SELECT id, event_id, platform, price, size, trade_timestamp, side
             FROM trades_tape
             WHERE event_id = $1
             ORDER BY trade_timestamp DESC
             LIMIT $2"
        )
        .bind(&event_id)
        .bind(limit)
        .fetch_all(&pool)
        .await
        .unwrap_or_default();

    let trades = rows.into_iter().map(|r| TradeRow {
        id: r.0.to_string(),
        event_id: r.1,
        platform: r.2,
        price: r.3,
        size: r.4,
        trade_timestamp: r.5.to_rfc3339(),
        side: r.6,
    }).collect();

    Json(TradesResponse { trades })
}

/// Routes exposed by this module — merged into the main router via
/// `.merge(orderbook::routes())` in `main.rs`.
pub fn routes() -> Router<PgPool> {
    Router::new()
        .route("/orderbook/:event_id", get(get_orderbook))
        .route("/trades/:event_id", get(get_trades))
}
