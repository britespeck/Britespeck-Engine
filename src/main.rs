mod models;
mod fetcher;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::time::Duration;
use crate::fetcher::MarketFetcher;
use std::env;
use std::str::FromStr;
use dotenv::dotenv;
use reqwest::header::{HeaderMap, HeaderValue};
// New imports for the API
use axum::{routing::get, extract::State, Json, Router};
use tower_http::cors::CorsLayer;
use serde::Serialize;

// 1. Define the Data Structure for Lovable
#[derive(Serialize, sqlx::FromRow)]
struct PredictionEvent {
    id: uuid::Uuid,
    title: String,
    platform: String,
    odds: f64,
    category: Option<String>,
    status: String,
}

// 2. The "Waiter" function (API Handler)
async fn get_predictions(State(pool): State(sqlx::PgPool)) -> Json<Vec<PredictionEvent>> {
    let rows = sqlx::query_as!(PredictionEvent, 
        "SELECT id, title, platform, odds, category, status FROM prediction_events ORDER BY updated_at DESC LIMIT 100")
        .fetch_all(&pool)
        .await
        .unwrap_or_default();
    Json(rows)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = dotenv();

    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://localhost/britespeck".to_string());

    let connect_options = PgConnectOptions::from_str(&database_url)?
        .statement_cache_capacity(0);

    let pool = PgPoolOptions::new()
        .max_connections(5) // Increased slightly to handle both API and Sync
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(connect_options)
        .await?;

    println!("✅ Connected to database");

    // --- NEW: API SERVER SETUP (For Lovable) ---
    let api_pool = pool.clone();
    let app = Router::new()
        .route("/prediction_events", get(get_predictions))
        .layer(CorsLayer::permissive()) // Required for Lovable browser access
        .with_state(api_pool);

    // --- SYNC ENGINE (Moved to background task) ---
    let sync_pool = pool.clone();
    tokio::spawn(async move {
        // Move your client setups inside here to keep the main thread clean
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
            let events = fetcher.fetch_all(&kalshi_client, &poly_client).await;
            
            if !events.is_empty() {
                // ... Your existing chunking and SQL logic stays exactly here ...
                // Note: Use 'sync_pool' here instead of 'pool'
                for chunk in events.chunks(100) {
                    // [Your existing INSERT/UPSERT loop code]
                }
            }
            
            // [Your existing BPS-100 Calculation code]

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