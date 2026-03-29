mod models;
mod fetcher;

use sqlx::postgres::PgConnectOptions;
use std::time::Duration;
use crate::fetcher::MarketFetcher;
use std::env;
use std::str::FromStr;
use dotenv::dotenv;
use serde_json;
use reqwest::header::{HeaderMap, HeaderValue};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = dotenv();

    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://localhost/britespeck".to_string());

    let connect_options = PgConnectOptions::from_str(&database_url)?
        .statement_cache_capacity(0);

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(3)
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(connect_options)
        .await?;

    println!("✅ Connected to database");

    let mut kalshi_headers = HeaderMap::new();
    if let Ok(token) = env::var("KALSHI_API_TOKEN") {
        kalshi_headers.insert(
            "Authorization",
            HeaderValue::from_str(&format!("Bearer {}", token))
                .unwrap_or_else(|_| HeaderValue::from_static("")),
        );
    }
    kalshi_headers.insert("Accept", HeaderValue::from_static("application/json"));
    kalshi_headers.insert(
        "User-Agent",
        HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"),
    );

    let kalshi_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .default_headers(kalshi_headers)
        .build()?;

    let mut poly_headers = HeaderMap::new();
    poly_headers.insert("Accept", HeaderValue::from_static("application/json"));
    poly_headers.insert(
        "User-Agent",
        HeaderValue::from_static("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"),
    );
    poly_headers.insert("Origin", HeaderValue::from_static("https://polymarket.com"));
    poly_headers.insert("Referer", HeaderValue::from_static("https://polymarket.com/"));

    let poly_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .default_headers(poly_headers)
        .build()?;

    let fetcher = MarketFetcher::new();

    println!("🚀 Britespeck sync engine started");

    loop {
        println!("\n🔄 Starting sync cycle...");

        let events = fetcher.fetch_all(&kalshi_client, &poly_client).await;
        println!("📦 Fetched {} total events", events.len());

        if events.is_empty() {
            println!("⚠️ No events fetched, skipping upsert");
        } else {
            let mut success_count: u64 = 0;
            let mut error_count: usize = 0;

            for chunk in events.chunks(100) {
                let mut values_parts: Vec<String> = Vec::new();

                for e in chunk {
                    let id = e.id.to_string();
                    let title = e.title.replace('\'', "''");
                    let platform = e.platform.replace('\'', "''");
                    let odds = e.odds;
                    let category = format!("'{}'", e.category.replace('\'', "''"));
                    let external_id = e.external_id.replace('\'', "''");
                    let volume_24h = e.volume_24h;
                    let icon_url = match &e.icon_url {
                        Some(u) => format!("'{}'", u.replace('\'', "''")),
                        None => "NULL".to_string(),
                    };
                    let status = format!("'{}'", e.status.replace('\'', "''"));
                    let end_date = match &e.end_date {
                        Some(d) => format!("'{}'", d.to_rfc3339()),
                        None => "NULL".to_string(),
                    };
                    let market_url = match &e.market_url {
                        Some(u) => format!("'{}'", u.replace('\'', "''")),
                        None => "NULL".to_string(),
                    };

                    let outcomes_json = {
                        let json_str = serde_json::to_string(&e.outcomes)
                            .unwrap_or_else(|_| "[]".to_string())
                            .replace('\'', "''");
                        format!("'{}'::jsonb", json_str)
                    };

                    values_parts.push(format!(
                        "('{}', '{}', '{}', {}, {}, '{}', {}, {}, {}, {}, {}, {})",
                        id, title, platform, odds, category, external_id,
                        volume_24h, icon_url, status, end_date,
                        market_url, outcomes_json
                    ));
                }

                let raw_sql = format!(
                    "INSERT INTO prediction_events \
                     (id, title, platform, odds, category, external_id, \
                      volume_24h, icon_url, status, end_date, \
                      market_url, outcomes) \
                     VALUES {} \
                     ON CONFLICT (external_id) DO UPDATE SET \
                       title = EXCLUDED.title, \
                       platform = EXCLUDED.platform, \
                       odds = EXCLUDED.odds, \
                       category = EXCLUDED.category, \
                       volume_24h = EXCLUDED.volume_24h, \
                       icon_url = EXCLUDED.icon_url, \
                       status = EXCLUDED.status, \
                       end_date = EXCLUDED.end_date, \
                       market_url = EXCLUDED.market_url, \
                       outcomes = EXCLUDED.outcomes, \
                       updated_at = now()",
                    values_parts.join(", ")
                );

                match sqlx::query(&raw_sql)
                    .persistent(false)
                    .execute(&pool)
                    .await
                {
                    Ok(result) => {
                        success_count += result.rows_affected();
                    }
                    Err(e) => {
                        error_count += chunk.len();
                        eprintln!("❌ Chunk insert error: {}", e);
                        eprintln!("   SQL preview: {}...", &raw_sql[..raw_sql.len().min(500)]);
                    }
                }
            }

            println!("✅ Upserted {} rows, {} errors", success_count, error_count);
        }

        // ── BPS-100 Index Calculation ──
        println!("📊 Calculating BPS-100 index...");
        let bps_query = "SELECT odds, platform FROM prediction_events \
                         WHERE status = 'active' \
                         ORDER BY volume_24h DESC NULLS LAST \
                         LIMIT 100";

        match sqlx::query_as::<_, (f64, String)>(bps_query)
            .persistent(false)
            .fetch_all(&pool)
            .await
        {
            Ok(rows) if !rows.is_empty() => {
                let total = rows.len() as f64;
                let sum: f64 = rows.iter().map(|(odds, _)| odds).sum();
                let index_value = sum / total;

                let poly_count = rows.iter()
                    .filter(|(_, p)| p.to_lowercase() == "polymarket")
                    .count() as i32;
                let kalshi_count = rows.iter()
                    .filter(|(_, p)| p.to_lowercase() == "kalshi")
                    .count() as i32;
                let total_contracts = rows.len() as i32;

                let bps_sql = format!(
                    "INSERT INTO index_history (value, market_count, timestamp) \
                     VALUES ({}, {}, now())",
                    index_value, total_contracts
                );

                match sqlx::query(&bps_sql)
                    .persistent(false)
                    .execute(&pool)
                    .await
                {
                    Ok(_) => println!("📊 BPS-100 INDEX: {:.4} ({} contracts, {}K/{}P)",
                        index_value, total_contracts, kalshi_count, poly_count),
                    Err(e) => eprintln!("❌ BPS-100 insert error: {}", e),
                }
            }
            Ok(_) => println!("⚠️ No active contracts for BPS-100"),
            Err(e) => eprintln!("❌ BPS-100 query error: {}", e),
        }

        println!("💤 Sleeping 30s...");
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}