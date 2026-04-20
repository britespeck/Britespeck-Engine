//! /market_history endpoint + snapshot writer.
//! Reads & writes the `public.market_history` table on RDS.

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Json,
    routing::get,
    Router,
};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

// ── Types ──────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct HistoryQuery {
    /// Either `event_id` (UUID) or `title` must be provided.
    pub event_id: Option<String>,
    pub title: Option<String>,
    /// `1h` | `1d` | `1w` | `1m` | `all` (default `1d`)
    pub timeframe: Option<String>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct HistoryPoint {
    pub price: f64,
    pub platform: String,
    pub recorded_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
struct LiveRow {
    odds: f64,
    platform: String,
}

#[derive(Serialize)]
pub struct MarketHistoryResponse {
    pub live_price: Option<f64>,
    pub live_no_price: Option<f64>,
    pub live_platform: String,
    pub history: Vec<HistoryPoint>,
    pub timeframe: String,
    pub fetched_at: DateTime<Utc>,
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

// ── Routes ─────────────────────────────────────────────────────────

pub fn routes() -> Router<PgPool> {
    Router::new().route("/market_history", get(get_market_history))
}

// ── Handler ────────────────────────────────────────────────────────

async fn get_market_history(
    State(pool): State<PgPool>,
    Query(params): Query<HistoryQuery>,
) -> Result<Json<MarketHistoryResponse>, (StatusCode, Json<ErrorResponse>)> {
    if params.event_id.is_none() && params.title.is_none() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "event_id or title query param required".into(),
            }),
        ));
    }

    let timeframe = params.timeframe.unwrap_or_else(|| "1d".to_string());
    let since = match timeframe.as_str() {
        "1h" => Utc::now() - Duration::hours(1),
        "1d" => Utc::now() - Duration::days(1),
        "1w" => Utc::now() - Duration::weeks(1),
        "1m" => Utc::now() - Duration::days(30),
        "all" => DateTime::<Utc>::from_timestamp(0, 0).unwrap_or_else(Utc::now),
        _ => Utc::now() - Duration::days(1),
    };

    // ── Live price (from prediction_events) ────────────────────────
    let live: Option<LiveRow> = if let Some(ref eid) = params.event_id {
        match uuid::Uuid::parse_str(eid) {
            Ok(uid) => sqlx::query_as::<_, LiveRow>(
                "SELECT odds, platform FROM public.prediction_events WHERE id = $1 LIMIT 1",
            )
            .bind(uid)
            .fetch_optional(&pool)
            .await
            .unwrap_or(None),
            Err(_) => None,
        }
    } else if let Some(ref t) = params.title {
        sqlx::query_as::<_, LiveRow>(
            "SELECT odds, platform FROM public.prediction_events
             WHERE title = $1 ORDER BY volume_24h DESC NULLS LAST LIMIT 1",
        )
        .bind(t)
        .fetch_optional(&pool)
        .await
        .unwrap_or(None)
    } else {
        None
    };

    let (live_price, live_no_price, live_platform) = match live {
        Some(r) => {
            let p = r.odds.clamp(0.01, 0.99);
            (Some(p), Some(1.0 - p), r.platform)
        }
        None => (None, None, "aggregated".to_string()),
    };

    // ── History rows ───────────────────────────────────────────────
    let history: Vec<HistoryPoint> = if let Some(ref eid) = params.event_id {
        match uuid::Uuid::parse_str(eid) {
            Ok(uid) => sqlx::query_as::<_, HistoryPoint>(
                "SELECT price, platform, recorded_at FROM public.market_history
                 WHERE event_id = $1 AND recorded_at >= $2
                 ORDER BY recorded_at ASC LIMIT 5000",
            )
            .bind(uid)
            .bind(since)
            .fetch_all(&pool)
            .await
            .unwrap_or_default(),
            Err(_) => vec![],
        }
    } else if let Some(ref t) = params.title {
        sqlx::query_as::<_, HistoryPoint>(
            "SELECT price, platform, recorded_at FROM public.market_history
             WHERE event_title = $1 AND recorded_at >= $2
             ORDER BY recorded_at ASC LIMIT 5000",
        )
        .bind(t)
        .bind(since)
        .fetch_all(&pool)
        .await
        .unwrap_or_default()
    } else {
        vec![]
    };

    Ok(Json(MarketHistoryResponse {
        live_price,
        live_no_price,
        live_platform,
        history,
        timeframe,
        fetched_at: Utc::now(),
    }))
}

// ── Snapshot writer (called from the sync loop) ────────────────────

/// Writes a snapshot row for every event currently in the sync batch.
/// Called from main.rs after each successful upsert.
pub async fn write_snapshots(
    pool: &PgPool,
    ids: &[uuid::Uuid],
    titles: &[String],
    platforms: &[String],
    odds: &[f64],
    volumes: &[f64],
) -> Result<u64, sqlx::Error> {
    if ids.is_empty() {
        return Ok(0);
    }
    let now = vec![Utc::now(); ids.len()];

    let res = sqlx::query(
        r#"
        INSERT INTO public.market_history
            (event_id, event_title, platform, price, volume_24h, recorded_at)
        SELECT * FROM UNNEST(
            $1::uuid[], $2::text[], $3::text[], $4::float8[], $5::float8[], $6::timestamptz[]
        )
        "#,
    )
    .bind(ids)
    .bind(titles)
    .bind(platforms)
    .bind(odds)
    .bind(volumes)
    .bind(&now)
    .execute(pool)
    .await?;

    Ok(res.rows_affected())
}
