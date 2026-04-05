mod models;
mod fetcher;
mod strategy;
mod user_bots;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::time::Duration;
use crate::fetcher::MarketFetcher;
use std::env;
use std::str::FromStr;
use dotenv::dotenv;
use reqwest::header::{HeaderMap, HeaderValue};
use axum::{routing::{get, post, patch}, extract::{State, Query, Path}, Json, Router};
use axum::response::IntoResponse;
use http::StatusCode;
use tower_http::cors::CorsLayer;
use serde::{Serialize, Deserialize};
use user_bots::{get_user_bot, create_user_bot, update_user_bot};

#[derive(Debug, Deserialize)]
struct PredictionQuery {
    platform: Option<String>,
    category: Option<String>,
    title_like: Option<String>,
    odds_gt: Option<f64>,
    odds_lt: Option<f64>,
    updated_at_gte: Option<String>,
    order_by: Option<String>,
    order_asc: Option<bool>,
    limit: Option<i64>,
    is_live: Option<bool>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
struct PredictionRow {
    id: String,
    title: String,
    platform: String,
    odds: f64,
    category: Option<String>,
    external_id: String,
    updated_at: Option<String>,
    volume_24h: Option<f64>,
    icon_url: Option<String>,
    outcomes: Option<serde_json::Value>,
    slug: Option<String>,
    market_url: Option<String>,
    status: Option<String>,
    end_date: Option<String>,
    rsi_signal: Option<f64>,
    sentiment_score: Option<f64>,
    is_live: Option<bool>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
struct IndexHistoryEntry {
    value: f64,
    market_count: i32,
    timestamp: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PatchIconBody {
    icon_url: String,
}

async fn get_predictions(
    State(pool): State<sqlx::PgPool>,
    Query(params): Query<PredictionQuery>,
) -> Json<Vec<PredictionRow>> {
    let mut query = String::from(
        "SELECT id::text, title, platform, odds, category, external_id, \
         updated_at::text, volume_24h, icon_url, outcomes, \
         NULL::text as slug, market_url, status, end_date::text, \
         rsi_signal, sentiment_score, is_live \
         FROM public.prediction_events WHERE 1=1"
    );
    let mut bind_idx = 0u32;
    let mut binds_str: Vec<String> = Vec::new();
    let mut binds_f64: Vec<(u32, f64)> = Vec::new();
    let mut binds_bool: Vec<(u32, bool)> = Vec::new();

    if let Some(ref platform) = params.platform {
        bind_idx += 1;
        query.push_str(&format!(" AND platform = ${}", bind_idx));
        binds_str.push(platform.clone());
    }

    if let Some(ref category) = params.category {
        let cats: Vec<&str> = category.split(',').collect();
        if cats.len() == 1 {
            bind_idx += 1;
            query.push_str(&format!(" AND category = ${}", bind_idx));
            binds_str.push(cats[0].to_string());
        } else {
            let placeholders: Vec<String> = cats.iter().map(|c| {
                bind_idx += 1;
                binds_str.push(c.to_string());
                format!("${}", bind_idx)
            }).collect();
            query.push_str(&format!(" AND category IN ({})", placeholders.join(",")));
        }
    }

    if let Some(ref title_like) = params.title_like {
        bind_idx += 1;
        query.push_str(&format!(" AND title ILIKE ${}", bind_idx));
        binds_str.push(format!("%{}%", title_like));
    }

    if let Some(odds_gt) = params.odds_gt {
        bind_idx += 1;
        query.push_str(&format!(" AND odds > ${}", bind_idx));
        binds_f64.push((bind_idx, odds_gt));
    }

    if let Some(odds_lt) = params.odds_lt {
        bind_idx += 1;
        query.push_str(&format!(" AND odds < ${}", bind_idx));
        binds_f64.push((bind_idx, odds_lt));
    }

    if let Some(ref updated_at_gte) = params.updated_at_gte {
        bind_idx += 1;
        query.push_str(&format!(" AND updated_at >= ${}::timestamptz", bind_idx));
        binds_str.push(updated_at_gte.clone());
    }

    if let Some(is_live) = params.is_live {
        bind_idx += 1;
        query.push_str(&format!(" AND is_live = ${}", bind_idx));
        binds_bool.push((bind_idx, is_live));
    }

    let order_col = match params.order_by.as_deref() {
        Some("volume_24h") => "volume_24h",
        Some("odds") => "odds",
        Some("updated_at") => "updated_at",
        Some("title") => "title",
        _ => "volume_24h",
    };
    let order_dir = if params.order_asc.unwrap_or(false) { "ASC" } else { "DESC" };
    query.push_str(&format!(" ORDER BY {} {} NULLS LAST", order_col, order_dir));

    let limit = params.limit.unwrap_or(100).min(5000);
    query.push_str(&format!(" LIMIT {}", limit));

    // Build the query using sqlx's raw query with dynamic binds.
    // Since sqlx doesn't support truly dynamic bind types easily,
    // we'll use query_as with raw SQL and bind all as strings,
    // casting in SQL where needed.
    // 
    // Simpler approach: rebuild with all-string binds and SQL casts
    let mut rebuilt = String::from(
        "SELECT id::text, title, platform, odds, category, external_id, \
         updated_at::text, volume_24h, icon_url, outcomes, \
         NULL::text as slug, market_url, status, end_date::text, \
         rsi_signal, sentiment_score, is_live \
         FROM public.prediction_events WHERE 1=1"
    );
    let mut all_binds: Vec<String> = Vec::new();
    let mut pidx = 0u32;

    if let Some(ref platform) = params.platform {
        pidx += 1;
        rebuilt.push_str(&format!(" AND platform = ${}", pidx));
        all_binds.push(platform.clone());
    }
    if let Some(ref category) = params.category {
        let cats: Vec<&str> = category.split(',').collect();
        if cats.len() == 1 {
            pidx += 1;
            rebuilt.push_str(&format!(" AND category = ${}", pidx));
            all_binds.push(cats[0].to_string());
        } else {
            let placeholders: Vec<String> = cats.iter().map(|c| {
                pidx += 1;
                all_binds.push(c.to_string());
                format!("${}", pidx)
            }).collect();
            rebuilt.push_str(&format!(" AND category IN ({})", placeholders.join(",")));
        }
    }
    if let Some(ref title_like) = params.title_like {
        pidx += 1;
        rebuilt.push_str(&format!(" AND title ILIKE ${}", pidx));
        all_binds.push(format!("%{}%", title_like));
    }
    if let Some(odds_gt) = params.odds_gt {
        pidx += 1;
        rebuilt.push_str(&format!(" AND odds > ${}::float8", pidx));
        all_binds.push(odds_gt.to_string());
    }
    if let Some(odds_lt) = params.odds_lt {
        pidx += 1;
        rebuilt.push_str(&format!(" AND odds < ${}::float8", pidx));
        all_binds.push(odds_lt.to_string());
    }
    if let Some(ref updated_at_gte) = params.updated_at_gte {
        pidx += 1;
        rebuilt.push_str(&format!(" AND updated_at >= ${}::timestamptz", pidx));
        all_binds.push(updated_at_gte.clone());
    }
    if let Some(is_live) = params.is_live {
        pidx += 1;
        rebuilt.push_str(&format!(" AND is_live = ${}::bool", pidx));
        all_binds.push(is_live.to_string());
    }

    rebuilt.push_str(&format!(" ORDER BY {} {} NULLS LAST", order_col, order_dir));
    rebuilt.push_str(&format!(" LIMIT {}", limit));

    let mut q = sqlx::query_as::<_, PredictionRow>(&rebuilt);
    for b in &all_binds {
        q = q.bind(b);
    }

    let rows = q.fetch_all(&pool).await.unwrap_or_else(|e| {
        println!("❌ GET /prediction_events query failed: {}", e);
        vec![]
    });

    Json(rows)
}

async fn get_backtest(
    State(pool): State<sqlx::PgPool>,
    Query(params): Query<strategy::BacktestParams>,
) -> Json<strategy::BacktestResult> {
    let res = strategy::run_backtest(&pool, &params).await.unwrap_or_else(|e| {
        println!("❌ Backtest error: {}", e);
        strategy::BacktestResult {
            total_trades: 0,
            estimated_profit: 0.0,
            win_rate: 0.0,
        }
    });
    Json(res)
}

async fn get_index_history(State(pool): State<sqlx::PgPool>) -> Json<Vec<IndexHistoryEntry>> {
    let rows = sqlx::query_as::<_, IndexHistoryEntry>(
        "SELECT value, market_count, timestamp FROM public.index_history ORDER BY timestamp DESC LIMIT 100"
    )
    .fetch_all(&pool)
    .await
    .unwrap_or_else(|e| {
        println!("❌ GET /index_history query failed: {}", e);
        vec![]
    });
    Json(rows)
}

async fn patch_event_icon(
    State(pool): State<sqlx::PgPool>,
    Path(id): Path<String>,
    Json(body): Json<PatchIconBody>,
) -> impl IntoResponse {
    let result = sqlx::query(
        "UPDATE prediction_events SET icon_url = $1, updated_at = NOW() WHERE id = $2::uuid"
    )
    .bind(&body.icon_url)
    .bind(&id)
    .execute(&pool)
    .await;

    match result {
        Ok(r) => {
            if r.rows_affected() == 0 {
                (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "Event not found"}))).into_response()
            } else {
                println!("✅ PATCH icon for {} → {}", id, body.icon_url);
                (StatusCode::OK, Json(serde_json::json!({"success": true}))).into_response()
            }
        }
        Err(e) => {
            eprintln!("❌ PATCH icon error: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response()
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = dotenv();

    let database_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL environment variable must be set");

    let connect_options = PgConnectOptions::from_str(&database_url)?
        .statement_cache_capacity(0);

    let api_pool = PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(Duration::from_secs(5))
        .connect_with(connect_options.clone())
        .await?;

    let sync_pool = PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(Duration::from_secs(15))
        .connect_with(connect_options)
        .await?;

    println!("✅ Connected to database (dual pool: 10 API + 10 sync)");

    let app = Router::new()
        .route("/prediction_events", get(get_predictions))
        .route("/prediction_events/:id/icon", patch(patch_event_icon))
        .route("/index_history", get(get_index_history))
        .route("/backtest", get(get_backtest))
        .route("/user_bots", get(get_user_bot).post(create_user_bot))
        .route("/user_bots/:id", patch(update_user_bot))
        .layer(CorsLayer::permissive())
        .with_state(api_pool);

    tokio::spawn(async move {
        let fetcher = MarketFetcher::new();
        let mut kalshi_headers = HeaderMap::new();
        if let Ok(token) = env::var("KALSHI_API_TOKEN") {
            kalshi_headers.insert("Authorization", HeaderValue::from_str(&format!("Bearer {}", token)).unwrap());
        }
        kalshi_headers.insert("Accept", HeaderValue::from_static("application/json"));
        kalshi_headers.insert("User-Agent", HeaderValue::from_static("Mozilla/5.0"));
        let kalshi_client = reqwest::Client::builder().default_headers(kalshi_headers).build().unwrap();

        let mut poly_headers = HeaderMap::new();
        poly_headers.insert("Accept", HeaderValue::from_static("application/json"));
        poly_headers.insert("User-Agent", HeaderValue::from_static("Mozilla/5.0"));
        let poly_client = reqwest::Client::builder().default_headers(poly_headers).build().unwrap();

        println!("🚀 Britespeck sync engine started");

        loop {
            println!("\n🔄 Starting sync cycle...");
            let events = fetcher.fetch_all(&kalshi_client, &poly_client).await;

            let mut success_count = 0u64;
            let mut fail_count = 0u64;

            for event in &events {
                let outcomes_json = serde_json::to_value(&event.outcomes).unwrap_or(serde_json::Value::Null);

                match sqlx::query(
                    r#"
                    INSERT INTO public.prediction_events 
                    (id, title, platform, odds, category, status, icon_url, external_id, volume_24h, updated_at, outcomes, market_url, end_date, is_live)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                    ON CONFLICT (external_id) DO UPDATE SET
                        odds = EXCLUDED.odds,
                        volume_24h = EXCLUDED.volume_24h,
                        updated_at = EXCLUDED.updated_at,
                        outcomes = EXCLUDED.outcomes,
                        icon_url = COALESCE(EXCLUDED.icon_url, public.prediction_events.icon_url),
                        status = EXCLUDED.status,
                        market_url = COALESCE(EXCLUDED.market_url, public.prediction_events.market_url),
                        category = COALESCE(EXCLUDED.category, public.prediction_events.category),
                        is_live = EXCLUDED.is_live
                    "#
                )
                .bind(event.id)
                .bind(&event.title)
                .bind(&event.platform)
                .bind(event.odds)
                .bind(&event.category)
                .bind(&event.status)
                .bind(&event.icon_url)
                .bind(&event.external_id)
                .bind(event.volume_24h)
                .bind(event.updated_at)
                .bind(outcomes_json)
                .bind(&event.market_url)
                .bind(event.end_date)
                .bind(event.is_live)
                .execute(&sync_pool)
                .await
                {
                    Ok(_) => success_count += 1,
                    Err(e) => {
                        if fail_count < 3 {
                            println!("❌ Upsert failed [{}]: {}", event.external_id, e);
                        }
                        fail_count += 1;
                    }
                }
            }

            let live_count = events.iter().filter(|e| e.is_live).count();
            println!("💾 Persisted {}/{} events ({} failed, {} live)", success_count, events.len(), fail_count, live_count);

            // --- RUN OMG STRATEGY BRAIN ---
            if let Err(e) = strategy::run_omg_strategy(&sync_pool).await {
                println!("⚠️ OMG Strategy Warning: {}", e);
            }

            println!("💤 Sleeping 30s...");
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
    });

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    println!("📡 Britespeck API listening on port 8080");
    axum::serve(listener, app).await?;

    Ok(())
}