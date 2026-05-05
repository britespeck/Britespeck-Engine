//! /market_history endpoint + snapshot writer + Polymarket prices-history backfill.
//! Reads & writes the `public.market_history` table on RDS.
 
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Json,
    routing::get,
    Router,
};
use chrono::{DateTime, Duration, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::time;
 
// ── Types ──────────────────────────────────────────────────────────
 
#[derive(Debug, Deserialize)]
pub struct HistoryQuery {
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
 
// ── Polymarket prices-history response ─────────────────────────────
 
#[derive(Debug, Deserialize)]
struct PolyPricePoint {
    t: i64,   // unix timestamp
    p: f64,   // price
}
 
#[derive(Debug, Deserialize)]
struct PolyPricesHistory {
    history: Vec<PolyPricePoint>,
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
        "1h"  => Utc::now() - Duration::hours(1),
        "1d"  => Utc::now() - Duration::days(1),
        "1w"  => Utc::now() - Duration::weeks(1),
        "1m"  => Utc::now() - Duration::days(30),
        "all" => DateTime::<Utc>::from_timestamp(0, 0).unwrap_or_else(Utc::now),
        _     => Utc::now() - Duration::days(1),
    };
 
    // ── Live price ─────────────────────────────────────────────────
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
 
// ── Snapshot writer ────────────────────────────────────────────────
 
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
 
// ── Polymarket prices-history backfill ─────────────────────────────
//
// Calls the confirmed public endpoint:
// GET https://clob.polymarket.com/prices-history?market={token}&startTs={unix}&endTs={unix}&fidelity=60
//
// fidelity=60  = one point per hour
// fidelity=1   = one point per minute (use for recent data)
 
async fn fetch_poly_price_history(
    client: &Client,
    token_id: &str,
    days_back: i64,
    fidelity: u32,
) -> anyhow::Result<Vec<PolyPricePoint>> {
    let end_ts   = Utc::now().timestamp();
    let start_ts = (Utc::now() - Duration::days(days_back)).timestamp();
 
    let url = format!(
        "https://clob.polymarket.com/prices-history?market={}&startTs={}&endTs={}&fidelity={}",
        token_id, start_ts, end_ts, fidelity
    );
 
    let resp = client
        .get(&url)
        .header("User-Agent", "Britespeck/1.0")
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;
 
    if !resp.status().is_success() {
        anyhow::bail!("prices-history {} returned {}", token_id, resp.status());
    }
 
    let data: PolyPricesHistory = resp.json().await?;
    Ok(data.history)
}
 
/// Persist Polymarket price history points into market_history table.
async fn persist_poly_history(
    pool: &PgPool,
    event_id: uuid::Uuid,
    title: &str,
    points: &[PolyPricePoint],
) -> anyhow::Result<usize> {
    if points.is_empty() {
        return Ok(0);
    }
 
    let mut inserted = 0usize;
 
    // Batch in chunks of 500
    for chunk in points.chunks(500) {
        let mut ids:          Vec<uuid::Uuid>   = Vec::with_capacity(chunk.len());
        let mut titles:       Vec<String>        = Vec::with_capacity(chunk.len());
        let mut platforms:    Vec<String>        = Vec::with_capacity(chunk.len());
        let mut prices:       Vec<f64>           = Vec::with_capacity(chunk.len());
        let mut volumes:      Vec<f64>           = Vec::with_capacity(chunk.len());
        let mut recorded_ats: Vec<DateTime<Utc>> = Vec::with_capacity(chunk.len());
 
        for point in chunk {
            let ts = DateTime::<Utc>::from_timestamp(point.t, 0)
                .unwrap_or_else(Utc::now);
            ids.push(event_id);
            titles.push(title.to_string());
            platforms.push("Polymarket".to_string());
            prices.push(point.p);
            volumes.push(0.0); // volume not available from prices-history
            recorded_ats.push(ts);
        }
 
        let res = sqlx::query(
            r#"
            INSERT INTO public.market_history
                (event_id, event_title, platform, price, volume_24h, recorded_at)
            SELECT * FROM UNNEST(
                $1::uuid[], $2::text[], $3::text[], $4::float8[], $5::float8[], $6::timestamptz[]
            )
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(&ids)
        .bind(&titles)
        .bind(&platforms)
        .bind(&prices)
        .bind(&volumes)
        .bind(&recorded_ats)
        .execute(pool)
        .await?;
 
        inserted += res.rows_affected() as usize;
    }
 
    Ok(inserted)
}
 
// ── Background backfill loop ───────────────────────────────────────
//
// Runs every 30 minutes. For each active Polymarket contract:
//   1. Check if we have history older than 1 hour
//   2. If not (new contract or first run) — fetch 7 days of hourly data
//   3. If yes — fetch last 2 hours of minute-level data to keep fresh
//
// Wire into main.rs:
//   tokio::spawn(market_history::run_poly_history_loop(api_pool.clone()));
 
pub async fn run_poly_history_loop(pool: PgPool) {
    tracing::info!("📈 Starting Polymarket prices-history backfill loop (30min interval)");
 
    let client = Client::builder()
        .user_agent("Britespeck/1.0")
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .expect("reqwest client");
 
    let mut interval = tokio::time::interval(time::Duration::from_secs(1800)); // 30 minutes
 
    loop {
        interval.tick().await;
 
        // Get top 200 active Polymarket contracts with clob_token_yes
        let markets: Vec<(uuid::Uuid, String, String)> = match sqlx::query_as(
            "SELECT pe.id, pe.title, pe.clob_token_yes
             FROM public.prediction_events pe
             WHERE pe.platform = 'Polymarket'
               AND pe.status = 'active'
               AND pe.clob_token_yes IS NOT NULL
               AND (pe.end_date IS NULL OR pe.end_date > NOW())
               AND pe.volume_24h > 1000
             ORDER BY pe.volume_24h DESC NULLS LAST
             LIMIT 200",
        )
        .fetch_all(&pool)
        .await
        {
            Ok(rows) => rows,
            Err(e) => {
                tracing::error!("Poly history loop: failed to load markets: {}", e);
                continue;
            }
        };
 
        if markets.is_empty() {
            continue;
        }
 
        tracing::info!("📈 Backfilling prices-history for {} Polymarket markets", markets.len());
 
        let mut total_inserted = 0usize;
        let mut errors = 0usize;
 
        for (event_id, title, token_id) in &markets {
            // Check how old our most recent history point is
            let latest: Option<(DateTime<Utc>,)> = sqlx::query_as(
                "SELECT MAX(recorded_at) FROM public.market_history
                 WHERE event_id = $1 AND platform = 'Polymarket'",
            )
            .bind(event_id)
            .fetch_optional(&pool)
            .await
            .ok()
            .flatten();
 
            let latest_ts = latest.and_then(|(ts,)| Some(ts));
            let age_hours = latest_ts
                .map(|ts| (Utc::now() - ts).num_hours())
                .unwrap_or(9999);
 
            // Decide fetch strategy
            let (days_back, fidelity) = if age_hours > 24 {
                // No recent data — fetch 7 days of hourly candles
                (7, 60)
            } else if age_hours > 1 {
                // Has data but stale — fetch last 2 hours at minute level
                (1, 1)
            } else {
                // Fresh — skip
                continue;
            };
 
            match fetch_poly_price_history(&client, token_id, days_back, fidelity).await {
                Ok(points) if !points.is_empty() => {
                    match persist_poly_history(&pool, *event_id, title, &points).await {
                        Ok(n) => total_inserted += n,
                        Err(e) => {
                            errors += 1;
                            if errors <= 3 {
                                tracing::warn!("Poly history persist failed for {}: {}", event_id, e);
                            }
                        }
                    }
                }
                Ok(_) => {} // empty history
                Err(e) => {
                    errors += 1;
                    if errors <= 3 {
                        tracing::warn!("Poly history fetch failed for {}: {}", token_id, e);
                    }
                }
            }
 
            // Small delay between requests to be a good API citizen
            tokio::time::sleep(time::Duration::from_millis(200)).await;
        }
 
        if total_inserted > 0 {
            tracing::info!(
                "✅ Polymarket history: inserted {} new price points ({} errors)",
                total_inserted, errors
            );
        }
    }
}
 
/// Prune old snapshots — keep DB clean
pub async fn prune_old_snapshots(pool: &PgPool) -> Result<u64, sqlx::Error> {
    let cutoff = Utc::now() - Duration::days(90);
    let res = sqlx::query(
        "DELETE FROM public.market_history WHERE recorded_at < $1"
    )
    .bind(cutoff)
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}