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

    // ── Pool with ZERO statement cache to prevent sqlx_s_ errors ──
    let connect_options = PgConnectOptions::from_str(&database_url)?
        .statement_cache_capacity(0);

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(3)
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(connect_options)
        .await?;

    println!("✅ Connected to database");

    // ── Shared HTTP client ──
    let mut headers = HeaderMap::new();
    if let Ok(kalshi_token) = env::var("KALSHI_API_TOKEN") {
        headers.insert(
            "Authorization",
            HeaderValue::from_str(&format!("Bearer {}", kalshi_token))
                .unwrap_or_else(|_| HeaderValue::from_static("")),
        );
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .default_headers(headers)
        .build()?;

    let fetcher = MarketFetcher::new();

    println!("🚀 Britespeck sync engine started");

    loop {
        println!("\n🔄 Starting sync cycle...");

        match fetcher.fetch_all(&client).await {
            Ok(events) => {
                println!("📦 Fetched {} total events", events.len());

                if events.is_empty() {
                    println!("⚠️ No events fetched, skipping upsert");
                } else {
                    let mut success_count = 0;
                    let mut error_count = 0;

                    // ── Upsert in chunks of 100 using RAW SQL ──
                    for chunk in events.chunks(100) {
                        let mut values_parts: Vec<String> = Vec::new();

                        for e in chunk {
                            let id = e.id.to_string();
                            let title = e.title.replace('\'', "''");
                            let platform = e.platform.replace('\'', "''");
                            let odds = e.odds;
                            let category = match &e.category {
                                Some(c) => format!("'{}'", c.replace('\'', "''")),
                                None => "NULL".to_string(),
                            };
                            let external_id = e.external_id.replace('\'', "''");
                            let volume_24h = e.volume_24h.unwrap_or(0.0);
                            let icon_url = match &e.icon_url {
                                Some(u) => format!("'{}'", u.replace('\'', "''")),
                                None => "NULL".to_string(),
                            };
                            let image_url = match &e.image_url {
                                Some(u) => format!("'{}'", u.replace('\'', "''")),
                                None => "NULL".to_string(),
                            };
                            let status = match &e.status {
                                Some(s) => format!("'{}'", s.replace('\'', "''")),
                                None => "'active'".to_string(),
                            };
                            let end_date = match &e.end_date {
                                Some(d) => format!("'{}'", d.to_rfc3339()),
                                None => "NULL".to_string(),
                            };
                            let market_url = match &e.market_url {
                                Some(u) => format!("'{}'", u.replace('\'', "''")),
                                None => "NULL".to_string(),
                            };
                            let slug = match &e.slug {
                                Some(s) => format!("'{}'", s.replace('\'', "''")),
                                None => "NULL".to_string(),
                            };

                            // Serialize outcomes to JSONB
                            let outcomes_json = match &e.outcomes {
                                Some(outcomes) => {
                                    let json_str = serde_json::to_string(outcomes)
                                        .unwrap_or_else(|_| "[]".to_string())
                                        .replace('\'', "''");
                                    format!("'{}'::jsonb", json_str)
                                }
                                None => "NULL".to_string(),
                            };

                            values_parts.push(format!(
                                "('{}', '{}', '{}', {}, {}, '{}', {}, {}, {}, {}, {}, {}, {}, {})",
                                id, title, platform, odds, category, external_id,
                                volume_24h, icon_url, image_url, status, end_date,
                                market_url, slug, outcomes_json
                            ));
                        }

                        let raw_sql = format!(
                            "INSERT INTO prediction_events \
                             (id, title, platform, odds, category, external_id, \
                              volume_24h, icon_url, image_url, status, end_date, \
                              market_url, slug, outcomes) \
                             VALUES {} \
                             ON CONFLICT (external_id) DO UPDATE SET \
                               title = EXCLUDED.title, \
                               platform = EXCLUDED.platform, \
                               odds = EXCLUDED.odds, \
                               category = EXCLUDED.category, \
                               volume_24h = EXCLUDED.volume_24h, \
                               icon_url = EXCLUDED.icon_url, \
                               image_url = EXCLUDED.image_url, \
                               status = EXCLUDED.status, \
                               end_date = EXCLUDED.end_date, \
                               market_url = EXCLUDED.market_url, \
                               slug = EXCLUDED.slug, \
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
                                // Print first 500 chars of SQL for debugging
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
                            Ok(_) => println!("📊 BPS-100 INDEX UPDATED: {:.4} ({} contracts, {}K/{}P)",
                                index_value, total_contracts, kalshi_count, poly_count),
                            Err(e) => eprintln!("❌ BPS-100 insert error: {}", e),
                        }
                    }
                    Ok(_) => println!("⚠️ No active contracts for BPS-100"),
                    Err(e) => eprintln!("❌ BPS-100 query error: {}", e),
                }
            }
            Err(e) => {
                eprintln!("❌ Fetch error: {}", e);
            }
        }

        println!("💤 Sleeping 30s...");
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}
