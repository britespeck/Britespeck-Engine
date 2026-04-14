mod models;
mod fetcher;

use sqlx::postgres::{PgPoolOptions, Postgres, PgConnectOptions};
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
        .unwrap_or_else(|_| "postgres://localhost/placeholder".to_string());

    println!("🚀 Connecting to Supabase (Forced Simple Protocol)...");

    let options = PgConnectOptions::from_str(&database_url)?
        .statement_cache_capacity(0);

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(options)
        .await?;

    // --- KALSHI CLIENT (clean headers) ---
    let mut kalshi_builder = reqwest::Client::builder()
        .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
        .timeout(Duration::from_secs(25));

    if let Ok(proxy_url) = env::var("PROXY_URL") {
        println!("🌐 Using Proxy for Kalshi: {}", proxy_url);
        let proxy = reqwest::Proxy::all(&proxy_url)?;
        kalshi_builder = kalshi_builder.proxy(proxy);
    }

    let kalshi_client = kalshi_builder.build()?;

    // --- POLYMARKET CLIENT (stealth headers) ---
    let mut poly_headers = HeaderMap::new();
    poly_headers.insert("Accept", HeaderValue::from_static("application/json, text/plain, */*"));
    poly_headers.insert("Accept-Language", HeaderValue::from_static("en-US,en;q=0.9"));
    poly_headers.insert("Origin", HeaderValue::from_static("https://polymarket.com"));
    poly_headers.insert("Referer", HeaderValue::from_static("https://polymarket.com"));

    let mut poly_builder = reqwest::Client::builder()
        .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
        .default_headers(poly_headers)
        .cookie_store(true)
        .timeout(Duration::from_secs(25));

    if let Ok(proxy_url) = env::var("PROXY_URL") {
        let proxy = reqwest::Proxy::all(proxy_url)?;
        poly_builder = poly_builder.proxy(proxy);
    }

    let poly_client = poly_builder.build()?;

    let fetcher = MarketFetcher::new();
    println!("📈 BPS-100 & Multi-Tab Engine Live!");

    loop {
        println!("Checking markets...");
        let events = fetcher.fetch_all(&kalshi_client, &poly_client).await;

        if events.is_empty() {
            println!("⚠️ 0 events found. Check API paths or Proxy status.");
        } else {
            let total_events = events.len();
            println!("💎 Syncing {} active events in chunks of 100...", total_events);

            // ═══════════════════════════════════════════════════════════════
            //  FIX #1: RAW SQL UPSERT — eliminates "sqlx_s_X already exists"
            //  No QueryBuilder, no prepared statements, no cache collisions.
            // ═══════════════════════════════════════════════════════════════
            for (chunk_idx, chunk) in events.chunks(100).enumerate() {
                let mut values_parts: Vec<String> = Vec::with_capacity(chunk.len());

                for event in chunk {
                    let esc = |s: &str| s.replace('\'', "''");

                    let title = esc(&event.title);
                    let platform = esc(&event.platform);
                    let category = esc(&event.category);
                    let external_id = esc(&event.external_id);
                    let status = esc(&event.status);

                    let icon_url = match &event.icon_url {
                        Some(u) => format!("'{}'", esc(u)),
                        None => "NULL".to_string(),
                    };
                    let market_url = match &event.market_url {
                        Some(u) => format!("'{}'", esc(u)),
                        None => "NULL".to_string(),
                    };
                    let end_date = match &event.end_date {
                        Some(d) => format!("'{}'", d.format("%Y-%m-%dT%H:%M:%S%.fZ")),
                        None => "NULL".to_string(),
                    };
                    let outcomes_json = serde_json::to_string(&event.outcomes)
                        .unwrap_or_else(|_| "[]".to_string());
                    let outcomes_sql = format!("'{}'::jsonb", esc(&outcomes_json));

                    let updated_at = event.updated_at.format("%Y-%m-%dT%H:%M:%S%.fZ");

                    values_parts.push(format!(
                        "('{}', '{}', '{}', {}, '{}', '{}', {}, {}, '{}', '{}', {}, {}, {})",
                        event.id,
                        title,
                        platform,
                        event.odds,
                        category,
                        external_id,
                        event.volume_24h,
                        icon_url,
                        updated_at,
                        status,
                        end_date,
                        outcomes_sql,
                        market_url,
                    ));
                }

                let sql = format!(
                    "INSERT INTO prediction_events \
                     (id, title, platform, odds, category, external_id, \
                      volume_24h, icon_url, updated_at, status, end_date, outcomes, market_url) \
                     VALUES {} \
                     ON CONFLICT (external_id) DO UPDATE SET \
                       odds = EXCLUDED.odds, \
                       category = EXCLUDED.category, \
                       volume_24h = EXCLUDED.volume_24h, \
                       icon_url = EXCLUDED.icon_url, \
                       updated_at = EXCLUDED.updated_at, \
                       status = EXCLUDED.status, \
                       end_date = EXCLUDED.end_date, \
                       outcomes = EXCLUDED.outcomes, \
                       market_url = EXCLUDED.market_url",
                    values_parts.join(", ")
                );

                match sqlx::query(&sql)
                    .persistent(false)
                    .execute(&pool)
                    .await
                {
                    Ok(r) => {
                        if chunk_idx % 10 == 0 {
                            println!("  ✅ Chunk {} done ({} rows)", chunk_idx + 1, r.rows_affected());
                        }
                    }
                    Err(e) => println!("  ❌ Chunk {} error: {}", chunk_idx + 1, e),
                }
            }

            println!("✅ Sync complete — {} events written", total_events);

            // ═══════════════════════════════════════════════════════
            //  FIX #2: BPS-100 — raw SQL for index_history too
            // ═══════════════════════════════════════════════════════
            match sqlx::query_as::<Postgres, (f64,)>(
                "SELECT odds FROM prediction_events WHERE status = 'active' ORDER BY volume_24h DESC LIMIT 100"
            )
            .persistent(false)
            .fetch_all(&pool).await
            {
                Ok(rows) if !rows.is_empty() => {
                    let sum: f64 = rows.iter().map(|row| row.0).sum();
                    let index_value = sum / rows.len() as f64;
                    let market_count = rows.len() as i32;

                    // Use raw SQL here too — no prepared statement
                    let bps_sql = format!(
                        "INSERT INTO index_history (value, market_count) VALUES ({}, {})",
                        index_value, market_count
                    );
                    match sqlx::query(&bps_sql)
                        .persistent(false)
                        .execute(&pool)
                        .await
                    {
                        Ok(_) => println!("📊 BPS-100 INDEX UPDATED: {:.4} (top {} markets)", index_value, market_count),
                        Err(e) => println!("❌ BPS-100 Insert Error: {}", e),
                    }
                },
                Err(e) => println!("❌ BPS-100 Query Error: {}", e),
                _ => println!("⚠️ No active markets for BPS-100"),
            }
        }

        println!("💤 Sleeping 30s...");
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}
