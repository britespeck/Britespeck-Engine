mod models;
mod fetcher;
mod strategy;
mod user_bots;
mod trades;
mod alpha;
mod ev_engine;
mod endpoints;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::time::Duration;
use crate::fetcher::MarketFetcher;
use std::env;
use std::str::FromStr;
use dotenv::dotenv;
use reqwest::header::{HeaderMap, HeaderValue};
use axum::{routing::{get, patch}, extract::{State, Query, Path}, Json, Router};
use tower_http::cors::CorsLayer;
use serde::{Serialize, Deserialize};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use user_bots::{get_user_bot, create_user_bot, update_user_bot};

#[derive(Serialize, sqlx::FromRow)]
struct PredictionEvent {
    id: uuid::Uuid,
    title: String,
    platform: String,
    odds: f64,
    category: Option<String>,
    status: String,
    icon_url: Option<String>,
    external_id: String,
    volume_24h: Option<f64>,
    updated_at: Option<chrono::DateTime<chrono::Utc>>,
    outcomes: Option<serde_json::Value>,
    market_url: Option<String>,
    end_date: Option<chrono::DateTime<chrono::Utc>>,
    rsi_signal: Option<f64>,
    sentiment_score: Option<f64>,
}

#[derive(Serialize, sqlx::FromRow)]
struct IndexHistoryEntry {
    value: f64,
    market_count: i32,
    timestamp: chrono::DateTime<chrono::Utc>,
}

#[derive(Deserialize)]
struct BacktestParams {
    rsi: f64,
    days: i32,
}

#[derive(Deserialize)]
struct PatchIconBody {
    icon_url: String,
}

// FIX 1:  LIMIT 50000 for sub-second website loading
async fn get_predictions(State(pool): State<sqlx::PgPool>) -> Json<Vec<PredictionEvent>> {
    let rows = sqlx::query_as::<_, PredictionEvent>(
        "SELECT id, title, platform, odds, category, status, icon_url, external_id, volume_24h, updated_at, outcomes, market_url, end_date, rsi_signal, sentiment_score FROM public.prediction_events ORDER BY volume_24h DESC NULLS LAST LIMIT 50000"
    )
    .fetch_all(&pool)
    .await
    .unwrap_or_else(|e| {
        println!("❌ GET /prediction_events query failed: {}", e);
        vec![]
    });
    println!("📤 GET /prediction_events returning {} rows", rows.len());
    Json(rows)
}

async fn get_backtest(
    State(pool): State<sqlx::PgPool>,
    Query(params): Query<BacktestParams>
) -> Json<strategy::BacktestResult> {
    let res = strategy::run_backtest(&pool, params.rsi, params.days).await.unwrap_or_else(|e| {
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

    tracing_subscriber::fmt::init();

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
        .merge(endpoints::alpha_routes())
        .layer(CorsLayer::permissive())
        .with_state(api_pool.clone());

    let trade_pool = api_pool.clone();
    let trade_client = reqwest::Client::new();
    tokio::spawn(trades::run_trade_ingestion_loop(trade_pool, trade_client));
    tokio::spawn(alpha::run_alpha_detection_loop(api_pool.clone()));

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
                    (id, title, platform, odds, category, status, icon_url, external_id, volume_24h, updated_at, outcomes, market_url, end_date)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                    ON CONFLICT (external_id) DO UPDATE SET
                        odds = EXCLUDED.odds,
                        volume_24h = EXCLUDED.volume_24h,
                        updated_at = EXCLUDED.updated_at,
                        outcomes = EXCLUDED.outcomes,
                        icon_url = COALESCE(EXCLUDED.icon_url, public.prediction_events.icon_url),
                        status = EXCLUDED.status,
                        market_url = COALESCE(EXCLUDED.market_url, public.prediction_events.market_url),
                        category = COALESCE(EXCLUDED.category, public.prediction_events.category)
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

            println!("💾 Persisted {}/{} events ({} failed)", success_count, events.len(), fail_count);

            if let Err(e) = strategy::run_omg_strategy(&sync_pool).await {
                println!("⚠️ OMG Strategy Warning: {}", e);
            }

            // Sync Loop stays at 30s but processes fewer rows via get_predictions above
            println!("💤 Sleeping 30s...");
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
    });

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    println!("📡 Britespeck API listening on port 8080");
    axum::serve(listener, app).await?;

    Ok(())
}