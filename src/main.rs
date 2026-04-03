mod models;
mod fetcher;
mod strategy;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::time::Duration;
use crate::fetcher::MarketFetcher;
use std::env;
use std::str::FromStr;
use dotenv::dotenv;
use reqwest::header::{HeaderMap, HeaderValue};
use axum::{routing::get, extract::{State, Query}, Json, Router};
use tower_http::cors::CorsLayer;
use serde::{Serialize, Deserialize};

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

// 1. API Handlers - Limit set back to 50,000
async fn get_predictions(State(pool): State<sqlx::PgPool>) -> Json<Vec<PredictionEvent>> {
    let rows = sqlx::query_as::<_, PredictionEvent>(
        "SELECT id, title, platform, odds, category, status, icon_url, external_id, volume_24h, updated_at, outcomes, market_url, end_date, rsi_signal, sentiment_score FROM public.prediction_events ORDER BY updated_at DESC LIMIT 50000"
    )
    .fetch_all(&pool)
    .await
    .unwrap_or_default();
    Json(rows)
}

async fn get_backtest(
    State(pool): State<sqlx::PgPool>,
    Query(params): Query<BacktestParams>
) -> Json<strategy::BacktestResult> {
    let res = strategy::run_backtest(&pool, params.rsi, params.days).await.unwrap_or_else(|_| strategy::BacktestResult {
        total_trades: 0,
        estimated_profit: 0.0,
        win_rate: 0.0,
    });
    Json(res)
}

async fn get_index_history(State(pool): State<sqlx::PgPool>) -> Json<Vec<IndexHistoryEntry>> {
    let rows = sqlx::query_as::<_, IndexHistoryEntry>(
        "SELECT value, market_count, timestamp FROM public.index_history ORDER BY timestamp DESC LIMIT 100"
    )
    .fetch_all(&pool)
    .await
    .unwrap_or_default();
    Json(rows)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = dotenv();

    let database_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL environment variable must be set");

    let connect_options = PgConnectOptions::from_str(&database_url)?
        .statement_cache_capacity(0);

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(connect_options)
        .await?;

    println!("✅ Connected to database");

    // --- API SERVER SETUP ---
    let api_pool = pool.clone();
    let app = Router::new()
        .route("/prediction_events", get(get_predictions))
        .route("/index_history", get(get_index_history))
        .route("/backtest", get(get_backtest))
        .layer(CorsLayer::permissive())
        .with_state(api_pool);

    // --- SYNC ENGINE ---
    let sync_pool = pool.clone();
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
            
            // --- SAVE TO DATABASE (UPSERT) ---
            for event in &events {
                // FIXED: Convert the outcomes vector to a JSON value so SQLx can save it to JSONB
                let outcomes_json = serde_json::to_value(&event.outcomes).unwrap_or(serde_json::Value::Null);

                let _ = sqlx::query(
                    r#"
                    INSERT INTO public.prediction_events 
                    (id, title, platform, odds, category, status, icon_url, external_id, volume_24h, updated_at, outcomes, market_url, end_date)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                    ON CONFLICT (id) DO UPDATE SET
                        odds = EXCLUDED.odds,
                        volume_24h = EXCLUDED.volume_24h,
                        updated_at = EXCLUDED.updated_at,
                        outcomes = EXCLUDED.outcomes
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
                .bind(outcomes_json) // Use the converted JSON value here
                .bind(&event.market_url)
                .bind(event.end_date)
                .execute(&sync_pool)
                .await;
            }
            println!("💾 Persisted {} events to RDS for Lovable", events.len());

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