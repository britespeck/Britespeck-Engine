mod models;
mod fetcher;

use sqlx::postgres::{PgPoolOptions, Postgres}; // Explicitly import Postgres for query_as
use std::time::Duration;
use crate::fetcher::MarketFetcher;
use std::env;
use dotenv::dotenv;
use serde_json;
use reqwest::header::{HeaderMap, HeaderValue}; // Added for stealth headers

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = dotenv();

    // 1. Get DB URL and handle potential empty strings
    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://localhost/placeholder".to_string());

    println!("🚀 Connecting to Supabase...");
    
    // 2. Build the pool
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&database_url)
        .await?;

    // --- STEALTH CLIENT SETUP ---
    let mut headers = HeaderMap::new();
    headers.insert("Accept", HeaderValue::from_static("application/json, text/plain, */*"));
    headers.insert("Accept-Language", HeaderValue::from_static("en-US,en;q=0.9"));
    headers.insert("Origin", HeaderValue::from_static("https://polymarket.com"));
    headers.insert("Referer", HeaderValue::from_static("https://polymarket.com"));

    let client = reqwest::Client::builder()
        .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
        .default_headers(headers)
        .timeout(Duration::from_secs(20))
        .build()?;
    // ----------------------------

    let fetcher = MarketFetcher::new();
    println!("📈 BPS-100 & Multi-Tab Engine Live!");

    loop {
        println!("Checking markets...");
        let events = fetcher.get_unified_events(&client).await;
        
        if events.is_empty() {
            println!("⚠️ 0 events found. Check API paths.");
        } else {
            println!("💎 Syncing {} active events...", events.len());

            for event in &events {
                let outcomes_json = serde_json::to_value(&event.outcomes).unwrap_or_default();

                let res = sqlx::query(
                    "INSERT INTO prediction_events (
                        id, title, platform, odds, category, external_id, 
                        volume_24h, icon_url, updated_at, status, end_date, outcomes
                    )
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                     ON CONFLICT (external_id) DO UPDATE
                     SET odds = EXCLUDED.odds,
                         category = EXCLUDED.category,
                         volume_24h = EXCLUDED.volume_24h,
                         icon_url = EXCLUDED.icon_url,
                         updated_at = EXCLUDED.updated_at,
                         status = EXCLUDED.status,
                         end_date = EXCLUDED.end_date,
                         outcomes = EXCLUDED.outcomes"
                )
                .bind(event.id)
                .bind(&event.title)
                .bind(&event.platform)
                .bind(event.odds)
                .bind(&event.category)
                .bind(&event.external_id)
                .bind(event.volume_24h)
                .bind(&event.icon_url)
                .bind(event.updated_at)
                .bind(&event.status)
                .bind(event.end_date)
                .bind(outcomes_json)
                .execute(&pool)
                .await;

                if let Err(e) = res {
                    println!("❌ SQL Error on event {}: {}", event.external_id, e);
                }
            }

            let top_100 = sqlx::query_as::<Postgres, (f64,)> (
                "SELECT odds FROM prediction_events WHERE status = 'active' ORDER BY volume_24h DESC LIMIT 100"
            ).fetch_all(&pool).await;

            match top_100 {
                Ok(rows) if !rows.is_empty() => {
                    let sum: f64 = rows.iter().map(|row| row.0).sum();
                    let index_value = sum / rows.len() as f64;

                    let _ = sqlx::query("INSERT INTO index_history (value, market_count) VALUES ($1, $2)")
                        .bind(index_value)
                        .bind(rows.len() as i32)
                        .execute(&pool)
                        .await;

                    println!("📊 BPS-100 INDEX UPDATED: {:.4}", index_value);
                },
                Err(e) => println!("❌ BPS-100 Query Error: {}", e),
                _ => {}
            }
        }
        
        println!("💤 Sleeping 30s...");
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}
Use code with caution.

Ready to push this alongside the corrected fetcher.rs paths and see those "0 events" turn into real data?




Ask anything


