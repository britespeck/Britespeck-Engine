use axum::{
    extract::{Path, State},
    routing::get,
    Json, Router,
};
use serde::Serialize;
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
pub struct OrderbookResponse {
    pub snapshot: Option<OrderbookSnapshot>,
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

/// Resolve a UUID or external_id into all possible keys used across
/// orderbook_snapshots and trades tables.
///
/// Polymarket CLOB writes event_id = external_id (e.g. "polymarket:304265")
/// and token_id = clob_token_yes (the long number).
/// Kalshi WS writes event_id = "kalshi:{ticker}".
/// Frontend always passes the UUID from the URL.
///
/// So we look up the UUID, grab both external_id and clob_token_yes,
/// and return all three as candidate keys.
async fn resolve_event_keys(pool: &PgPool, param: &str) -> Vec<String> {
    let mut keys = vec![param.to_string()];

    if uuid::Uuid::parse_str(param).is_ok() {
        // UUID path — look up external_id and clob_token_yes
        if let Ok(Some(row)) = sqlx::query_as::<_, (Option<String>, Option<String>)>(
            "SELECT external_id, clob_token_yes
             FROM public.prediction_events
             WHERE id = $1::uuid
             LIMIT 1",
        )
        .bind(param)
        .fetch_optional(pool)
        .await
        {
            // Add external_id (e.g. "polymarket:304265" or "kalshi:KXTICKER")
            if let Some(ext) = row.0 {
                if !ext.is_empty() && !keys.contains(&ext) {
                    keys.push(ext);
                }
            }
            // Add clob_token_yes (the long Polymarket token number)
            if let Some(token) = row.1 {
                if !token.is_empty() && !keys.contains(&token) {
                    keys.push(token);
                }
            }
        }
    } else {
        // external_id path — also resolve UUID for any legacy rows
        if let Ok(Some(row)) = sqlx::query_as::<_, (uuid::Uuid, Option<String>)>(
            "SELECT id, clob_token_yes
             FROM public.prediction_events
             WHERE external_id = $1
             LIMIT 1",
        )
        .bind(param)
        .fetch_optional(pool)
        .await
        {
            let uid = row.0.to_string();
            if !keys.contains(&uid) {
                keys.push(uid);
            }
            if let Some(token) = row.1 {
                if !token.is_empty() && !keys.contains(&token) {
                    keys.push(token);
                }
            }
        }
    }

    keys
}

async fn get_orderbook(
    State(pool): State<PgPool>,
    Path(event_id): Path<String>,
) -> Json<OrderbookResponse> {
    let keys = resolve_event_keys(&pool, &event_id).await;

    // Latest snapshot row across any matching key
    let head = sqlx::query_as::<_, (String, String, Option<String>, chrono::DateTime<chrono::Utc>)>(
        "SELECT event_id, platform, token_id, captured_at
           FROM public.orderbook_snapshots
          WHERE event_id = ANY($1)
             OR token_id = ANY($1)
          ORDER BY captured_at DESC
          LIMIT 1",
    )
    .bind(&keys)
    .fetch_optional(&pool)
    .await
    .ok()
    .flatten();

    let Some((ev_id, platform, token_id, captured_at)) = head else {
        return Json(OrderbookResponse { snapshot: None });
    };

    // Pull all levels from that exact snapshot timestamp
    let rows = sqlx::query_as::<_, (String, f64, f64, i32)>(
        "SELECT side, price, size, level
           FROM public.orderbook_snapshots
          WHERE event_id = $1 AND captured_at = $2
          ORDER BY level ASC",
    )
    .bind(&ev_id)
    .bind(captured_at)
    .fetch_all(&pool)
    .await
    .unwrap_or_default();

    let mut bids = Vec::new();
    let mut asks = Vec::new();
    for (side, price, size, level) in rows {
        let lvl = OrderbookLevel { price, size, level };
        match side.to_lowercase().as_str() {
            "bid" | "buy" | "yes" => bids.push(lvl),
            "ask" | "sell" | "no" => asks.push(lvl),
            _ => {}
        }
    }

    Json(OrderbookResponse {
        snapshot: Some(OrderbookSnapshot {
            event_id: ev_id,
            platform,
            token_id,
            captured_at: captured_at.to_rfc3339(),
            bids,
            asks,
        }),
    })
}

async fn get_trades(
    State(pool): State<PgPool>,
    Path(event_id): Path<String>,
) -> Json<TradesResponse> {
    let keys = resolve_event_keys(&pool, &event_id).await;

    // UNION raw_trades (Polymarket REST poll) + trades_tape (Kalshi WS)
    let rows = sqlx::query_as::<_, (
        String,
        String,
        String,
        f64,
        f64,
        chrono::DateTime<chrono::Utc>,
        Option<String>,
    )>(
        "SELECT id::text, event_id, platform, price, size, trade_timestamp, side
           FROM public.raw_trades
          WHERE event_id = ANY($1)
         UNION ALL
         SELECT id::text, event_id, platform, price, size, trade_timestamp, side
           FROM public.trades_tape
          WHERE event_id = ANY($1)
            AND trade_timestamp IS NOT NULL
          ORDER BY 6 DESC
          LIMIT 100",
    )
    .bind(&keys)
    .fetch_all(&pool)
    .await
    .unwrap_or_default();

    let trades = rows
        .into_iter()
        .map(|r| TradeRow {
            id: r.0,
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

pub fn routes() -> Router<PgPool> {
    Router::new()
        .route("/orderbook/:event_id", get(get_orderbook))
        .route("/trades/:event_id", get(get_trades))
}