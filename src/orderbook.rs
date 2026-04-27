use axum::{Router, routing::get, extract::{Path, Query, State}, Json};
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
pub struct TradesQuery {
    pub limit: Option<i64>,
}

pub async fn get_orderbook(
    State(pool): State<PgPool>,
    Path(event_id): Path<String>,
) -> Json<Option<OrderbookSnapshot>> {
    let row = sqlx::query_as::<_, (String, String, Option<String>, chrono::DateTime<chrono::Utc>, serde_json::Value, serde_json::Value)>(
        r#"
        SELECT event_id, platform, token_id, captured_at, bids, asks
        FROM orderbook_snapshots
        WHERE event_id = $1
        ORDER BY captured_at DESC
        LIMIT 1
        "#,
    )
    .bind(&event_id)
    .fetch_optional(&pool)
    .await
    .ok()
    .flatten();

    let snapshot = row.map(|(event_id, platform, token_id, captured_at, bids, asks)| {
        let parse_levels = |v: serde_json::Value| -> Vec<OrderbookLevel> {
            v.as_array()
                .map(|arr| {
                    arr.iter()
                        .enumerate()
                        .filter_map(|(i, lvl)| {
                            let price = lvl.get("price")?.as_f64()?;
                            let size = lvl.get("size")?.as_f64()?;
                            Some(OrderbookLevel { price, size, level: i as i32 })
                        })
                        .collect()
                })
                .unwrap_or_default()
        };
        OrderbookSnapshot {
            event_id,
            platform,
            token_id,
            captured_at: captured_at.to_rfc3339(),
            bids: parse_levels(bids),
            asks: parse_levels(asks),
        }
    });

    Json(snapshot)
}

pub async fn get_trades(
    State(pool): State<PgPool>,
    Path(event_id): Path<String>,
    Query(q): Query<TradesQuery>,
) -> Json<TradesResponse> {
    let limit = q.limit.unwrap_or(50).clamp(1, 500);
    let rows = sqlx::query_as::<_, (uuid::Uuid, String, String, f64, f64, chrono::DateTime<chrono::Utc>, Option<String>)>(
        r#"
        SELECT id, event_id, platform, price, size, trade_timestamp, side
        FROM raw_trades
        WHERE event_id = $1
        ORDER BY trade_timestamp DESC
        LIMIT $2
        "#,
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
