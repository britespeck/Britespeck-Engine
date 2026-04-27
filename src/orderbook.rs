use axum::{
    extract::{Path, Query, State},
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::Arc;

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
}

#[derive(Serialize)]
pub struct OrderbookResponse {
    pub snapshots: Vec<OrderbookSnapshot>,
}

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
pub struct TradesResponse {
    pub trades: Vec<TradeRow>,
}

#[derive(Deserialize)]
pub struct OrderbookQuery {
    pub platform: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Deserialize)]
pub struct TradesQuery {
    pub platform: Option<String>,
    pub limit: Option<i64>,
}

pub async fn get_orderbook(
    State(pool): State<Arc<PgPool>>,
    Path(event_id): Path<String>,
    Query(params): Query<OrderbookQuery>,
) -> Json<OrderbookResponse> {
    let limit = params.limit.unwrap_or(25).clamp(1, 100);

    let rows = sqlx::query_as::<
        _,
        (
            String,
            String,
            Option<String>,
            chrono::DateTime<chrono::Utc>,
            serde_json::Value,
            serde_json::Value,
        ),
    >(
        r#"
        SELECT
            event_id,
            platform,
            token_id,
            captured_at,
            bids,
            asks
        FROM orderbook_snapshots
        WHERE event_id = $1
          AND ($2::text IS NULL OR platform = $2)
        ORDER BY captured_at DESC
        LIMIT $3
        "#,
    )
    .bind(&event_id)
    .bind(params.platform.as_deref())
    .bind(limit)
    .fetch_all(pool.as_ref())
    .await
    .unwrap_or_default();

    let snapshots = rows
        .into_iter()
        .map(|r| OrderbookSnapshot {
            event_id: r.0,
            platform: r.1,
            token_id: r.2,
            captured_at: r.3.to_rfc3339(),
            bids: parse_levels(r.4),
            asks: parse_levels(r.5),
        })
        .collect();

    Json(OrderbookResponse { snapshots })
}

pub async fn get_trades(
    State(pool): State<Arc<PgPool>>,
    Path(event_id): Path<String>,
    Query(params): Query<TradesQuery>,
) -> Json<TradesResponse> {
    let limit = params.limit.unwrap_or(100).clamp(1, 500);

    let rows = sqlx::query_as::<
        _,
        (
            uuid::Uuid,
            String,
            String,
            f64,
            f64,
            chrono::DateTime<chrono::Utc>,
            Option<String>,
        ),
    >(
        r#"
        SELECT
            id,
            event_id,
            platform,
            price,
            size,
            trade_timestamp,
            side
        FROM raw_trades
        WHERE event_id = $1
          AND ($2::text IS NULL OR platform = $2)
        ORDER BY trade_timestamp DESC
        LIMIT $3
        "#,
    )
    .bind(&event_id)
    .bind(params.platform.as_deref())
    .bind(limit)
    .fetch_all(pool.as_ref())
    .await
    .unwrap_or_default();

    let trades = rows
        .into_iter()
        .map(|r| TradeRow {
            id: r.0.to_string(),
            event_id: r.1,
            platform: r.2,
            price: r.3,
            size: r.4,
            trade_timestamp: r.5.to_rfc3339(),
            side: r.6,
        })
        .collect();

    Json(TradesResponse { trades })
}

fn parse_levels(value: serde_json::Value) -> Vec<OrderbookLevel> {
    serde_json::from_value::<Vec<OrderbookLevelRaw>>(value)
        .unwrap_or_default()
        .into_iter()
        .enumerate()
        .map(|(idx, level)| OrderbookLevel {
            price: level.price,
            size: level.size,
            level: level.level.unwrap_or((idx + 1) as i32),
        })
        .collect()
}

#[derive(Deserialize)]
struct OrderbookLevelRaw {
    price: f64,
    size: f64,
    level: Option<i32>,
}

/// Routes exposed by this module — merged into the main router via
/// `.merge(orderbook::routes())` in `main.rs`.
pub fn routes() -> Router<Arc<PgPool>> {
    Router::new()
        .route("/orderbook/:event_id", get(get_orderbook))
        .route("/trades/:event_id", get(get_trades))
}
