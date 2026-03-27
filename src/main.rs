mod models;
mod fetcher;

use sqlx::postgres::{PgPoolOptions, Postgres, PgConnectOptions}; // Added PgConnectOptions
use std::time::Duration;
use crate::fetcher::MarketFetcher;
use std::env;
use std::str::FromStr; // Added for parsing the URL
use dotenv::dotenv;
use serde_json;
use reqwest::header::{HeaderMap, HeaderValue};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = dotenv();

    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://localhost/placeholder".to_string());

    println!("🚀 Connecting to Supabase (Forced Simple Protocol)...");

    // FIXED: Using PgConnectOptions to force statement_cache_capacity to 0
    // This is the only way to stop the "prepared statement already exists" error on port 6543
    let options = PgConnectOptions::from_str(&database_url)?
        .statement_cache_capacity(0); 

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(options) // Use connect_with instead of connect
        .await?;

    // --- STEALTH CLIENT SETUP ---
    let mut headers = HeaderMap::new();
    headers.insert("Accept", HeaderValue::from_static("application/json, text/plain, */*"));
    headers.insert("Accept-Language", HeaderValue::from_static("en-US,en;q=0.9"));
    headers.insert("Origin", HeaderValue::from_static("https://polymarket.com"));
    headers.insert("Referer", HeaderValue::from_static("https://polymarket.com"));

    let mut client_builder = reqwest::Client::builder()
        .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
        .default_headers(headers)
        .cookie_store(true)
        .timeout(Duration::from_secs(25));

    if let Ok(proxy_url) = env::var("PROXY_URL") {
        println!("🌐 Using Proxy: {}", proxy_url);
        let proxy = reqwest::Proxy::all(proxy_url)?;
        client_builder = client_builder.proxy(proxy);
    }

    let client = client_builder.build()?;
    let fetcher = MarketFetcher::new();
    println!("📈 BPS-100 & Multi-Tab Engine Live!");

    loop {
        println!("Checking markets...");
        let events = fetcher.fetch_all(&client).await;
        
        if events.is_empty() {
            println!("⚠️ 0 events found. Check API paths or Proxy status.");
        } else {
            let total_events = events.len();
            println!("💎 Syncing {} active events in bulk chunks...", total_events);

            for chunk in events.chunks(400) {
                let mut query_builder: sqlx::QueryBuilder<Postgres> = sqlx::QueryBuilder::new(
                    "INSERT INTO prediction_events (
                        id, title, platform, odds, category, external_id,
                        volume_24h, icon_url, updated_at, status, end_date, outcomes
                    ) "
                );

                query_builder.push_values(chunk, |mut b, event| {
                    let outcomes_json = serde_json::to_value(&event.outcomes).unwrap_or_default();
                    b.push_bind(event.id)
                     .push_bind(&event.title)
                     .push_bind(&event.platform)
                     .push_bind(event.odds)
                     .push_bind(&event.category)
                     .push_bind(&event.external_id)
                     .push_bind(event.volume_24h)
                     .push_bind(&event.icon_url)
                     .push_bind(event.updated_at)
                     .push_bind(&event.status)
                     .push_bind(event.end_date)
                     .push_bind(outcomes_json);
                });

                query_builder.push(
                    " ON CONFLICT (external_id) DO UPDATE
                     SET odds = EXCLUDED.odds,
                         category = EXCLUDED.category,
                         volume_24h = EXCLUDED.volume_24h,
                         icon_url = EXCLUDED.icon_url,
                         updated_at = EXCLUDED.updated_at,
                         status = EXCLUDED.status,
                         end_date = EXCLUDED.end_date,
                         outcomes = EXCLUDED.outcomes"
                );

                // build().persistent(false) is still used as a secondary safety
                let res = query_builder.build().persistent(false).execute(&pool).await;

                if let Err(e) = res {
                    println!("❌ Bulk SQL Error on chunk: {}", e);
                }
            }

            // --- BPS-100 CALCULATION ---
            let top_100 = sqlx::query_as::<Postgres, (f64,)>(
                "SELECT odds FROM prediction_events WHERE status = 'active' ORDER BY volume_24h DESC LIMIT 100"
            )
            .persistent(false)
            .fetch_all(&pool).await;

            match top_100 {
                Ok(rows) if !rows.is_empty() => {
                    let sum: f64 = rows.iter().map(|row| row.0).sum();
                    let index_value = sum / rows.len() as f64;

                    let _ = sqlx::query("INSERT INTO index_history (value, market_count) VALUES ($1, $2)")
                        .persistent(false)
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