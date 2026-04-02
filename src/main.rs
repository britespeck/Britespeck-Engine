mod models;
mod fetcher;
mod strategy; // <--- 1. REGISTER THE NEW MODULE

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::time::Duration;
use crate::fetcher::MarketFetcher;
use std::env;
use std::str::FromStr;
use dotenv::dotenv;
use reqwest::header::{HeaderMap, HeaderValue};
use axum::{routing::get, extract::{State, Query}, Json, Router}; // Added Query for Backtesting
use tower_http::cors::CorsLayer;
use serde::{Serialize, Deserialize};

// 1. Data Structures for Lovable
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
    // --- 2. ADDED OMG COLUMNS ---
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

// 2. API Handlers
async fn get_predictions(State(pool): State<sqlx::PgPool>) -> Json<Vec<PredictionEvent>> {
    let rows = sqlx::query_as::<_, PredictionEvent>(
        "SELECT id, title, platform, odds, category, status, icon_url, external_id, volume_24h, updated_at, outcomes, market_url, end_date, rsi_signal, sentiment_score FROM prediction_events ORDER BY updated_at DESC LIMIT 50000"
    )
    .fetch_all(&pool)
    .await
    .unwrap_or_default();
    Json(rows)
}

// --- 3. ADDED BACKTEST HANDLER ---
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
        "SELECT value, market_count, timestamp FROM index_history ORDER BY timestamp DESC LIMIT 100"
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
        .route("/backtest", get(get_backtest)) // <--- 4. ADDED BACKTEST ROUTE
        .layer(CorsLayer::permissive())
        .with_state(api_pool);

    // --- SYNC ENGINE (Worker in the background) ---
    let sync_pool = pool.clone();
    tokio::spawn(async move {
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

        let fetcher = MarketFetcher::new();
        println!("🚀 Britespeck sync engine started in background");

        loop {
            println!("\n🔄 Starting sync cycle...");
            let _events = fetcher.fetch_all(&kalshi_client, &poly_client).await;

            // --- 5. RUN THE OMG STRATEGY BRAIN ---
            if let Err(e) = strategy::run_omg_strategy(&sync_pool).await {
                println!("⚠️ OMG Strategy Warning: {}", e);
            }

            println!("💤 Sleeping 30s...");
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
    });

    // Run the API server in the foreground
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    println!("📡 Britespeck API listening on port 8080");
    axum::serve(listener, app).await?;

    Ok(())
}