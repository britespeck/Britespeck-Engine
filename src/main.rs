mod models;
mod fetcher;

use sqlx::postgres::PgPoolOptions;
use std::time::Duration;
use crate::fetcher::MarketFetcher;
use std::env;
use dotenv::dotenv;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set in .env");

    println!("🚀 Connecting to Supabase...");
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await?;

    let client = reqwest::Client::builder()
        .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
        .timeout(Duration::from_secs(15))
        .build()?;

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
                let _ = sqlx::query(
                    "INSERT INTO prediction_events (id, title, platform, odds, category, external_id, volume_24h, icon_url, updated_at, status, end_date)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                     ON CONFLICT (external_id) DO UPDATE
                     SET odds = EXCLUDED.odds,
                         category = EXCLUDED.category,
                         volume_24h = EXCLUDED.volume_24h,
                         icon_url = EXCLUDED.icon_url,
                         updated_at = EXCLUDED.updated_at,
                         status = EXCLUDED.status,
                         end_date = EXCLUDED.end_date"
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
                .execute(&pool)
                .await;
            }

            // BPS-100: only compute from active contracts
            let top_100: Vec<(f64,)> = sqlx::query_as(
                "SELECT odds FROM prediction_events WHERE status = 'active' ORDER BY volume_24h DESC LIMIT 100"
            ).fetch_all(&pool).await.unwrap_or_default();

            if !top_100.is_empty() {
                let sum: f64 = top_100.iter().map(|row| row.0).sum();
                let index_value = sum / top_100.len() as f64;

                let _ = sqlx::query("INSERT INTO index_history (value, market_count) VALUES ($1, $2)")
                    .bind(index_value)
                    .bind(top_100.len() as i32)
                    .execute(&pool)
                    .await;

                println!("📊 BPS-100 INDEX UPDATED: {:.4}", index_value);
            }
        }
        
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}