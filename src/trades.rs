//! Raw trade ingestion from Polymarket CLOB and Kalshi APIs.
//!
//! Polls both platforms every 30-60 seconds for active markets,
//! storing individual trades (not bucketed candles) so that hidden
//! wicks, volume spikes, and liquidity gaps are preserved.

use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::time::Duration;
use tokio::time;
use uuid::Uuid;

// ── Types ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct RawTrade {
    pub id: Uuid,
    pub event_id: String,
    pub platform: String,
    pub price: f64,
    pub size: f64,
    pub side: Option<String>,
    pub trade_timestamp: DateTime<Utc>,
    pub ingested_at: DateTime<Utc>,
}

/// Polymarket CLOB trade from their API
#[derive(Debug, Deserialize)]
struct PolyTrade {
    #[serde(rename = "asset_id")]
    asset_id: String,
    price: String,
    size: String,
    side: Option<String>,
    #[serde(rename = "match_time")]
    match_time: Option<String>,
    // Polymarket also returns "id", "market", etc.
}

/// Kalshi trade from their API
#[derive(Debug, Deserialize)]
struct KalshiTrade {
    ticker: String,
    #[serde(rename = "yes_price")]
    yes_price: f64,
    #[serde(rename = "no_price")]
    no_price: f64,
    count: Option<f64>,
    #[serde(rename = "created_time")]
    created_time: Option<String>,
    taker_side: Option<String>,
}

#[derive(Debug, Deserialize)]
struct KalshiTradesResponse {
    trades: Vec<KalshiTrade>,
    cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PolyTradesResponse {
    #[serde(default)]
    data: Vec<PolyTrade>,
    next_cursor: Option<String>,
}

// ── Constants ──────────────────────────────────────────────────────

const POLYMARKET_CLOB_URL: &str = "https://clob.polymarket.com";
const KALSHI_API_URL: &str = "https://api.elections.kalshi.com/trade-api/v2";
const INGEST_INTERVAL_SECS: u64 = 45; // 45-second polling cycle
const MAX_TRADES_PER_FETCH: usize = 500;

// ── Polymarket CLOB Ingestion ──────────────────────────────────────

/// Fetch raw trades for a specific Polymarket token/asset.
/// Uses the CLOB API which returns individual fills, not bucketed data.
pub async fn fetch_polymarket_trades(
    client: &Client,
    token_id: &str,
    since: Option<DateTime<Utc>>,
) -> anyhow::Result<Vec<RawTrade>> {
    let mut trades = Vec::new();
    let mut cursor: Option<String> = None;

    loop {
        let mut url = format!(
            "{}/trades?asset_id={}&limit={}",
            POLYMARKET_CLOB_URL, token_id, MAX_TRADES_PER_FETCH
        );
        if let Some(ref c) = cursor {
            url.push_str(&format!("&next_cursor={}", c));
        }

        let resp = client
            .get(&url)
            .timeout(Duration::from_secs(10))
            .send()
            .await?;

        if !resp.status().is_success() {
            tracing::warn!("Polymarket CLOB returned {}", resp.status());
            break;
        }

        let body: PolyTradesResponse = resp.json().await?;

        for t in &body.data {
            let price: f64 = t.price.parse().unwrap_or(0.0);
            let size: f64 = t.size.parse().unwrap_or(0.0);
            let ts = t
                .match_time
                .as_deref()
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(Utc::now);

            // Skip trades older than our watermark
            if let Some(ref since_ts) = since {
                if ts <= *since_ts {
                    continue;
                }
            }

            trades.push(RawTrade {
                id: Uuid::new_v4(),
                event_id: t.asset_id.clone(),
                platform: "polymarket".to_string(),
                price,
                size,
                side: t.side.clone(),
                trade_timestamp: ts,
                ingested_at: Utc::now(),
            });
        }

        // Pagination
        cursor = body.next_cursor;
        if cursor.is_none() || body.data.len() < MAX_TRADES_PER_FETCH {
            break;
        }
    }

    Ok(trades)
}

// ── Kalshi Trade Ingestion ─────────────────────────────────────────

/// Fetch raw trades for a specific Kalshi ticker.
pub async fn fetch_kalshi_trades(
    client: &Client,
    ticker: &str,
    since: Option<DateTime<Utc>>,
) -> anyhow::Result<Vec<RawTrade>> {
    let mut trades = Vec::new();
    let mut cursor: Option<String> = None;

    loop {
        let mut url = format!(
            "{}/markets/{}/trades?limit={}",
            KALSHI_API_URL, ticker, MAX_TRADES_PER_FETCH
        );
        if let Some(ref c) = cursor {
            url.push_str(&format!("&cursor={}", c));
        }
        if let Some(ref since_ts) = since {
            url.push_str(&format!("&min_ts={}", since_ts.timestamp()));
        }

        let resp = client
            .get(&url)
            .timeout(Duration::from_secs(10))
            .send()
            .await?;

        if !resp.status().is_success() {
            tracing::warn!("Kalshi trades API returned {}", resp.status());
            break;
        }

        let body: KalshiTradesResponse = resp.json().await?;

        for t in &body.trades {
            let ts = t
                .created_time
                .as_deref()
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(Utc::now);

            let price = t.yes_price / 100.0; // Kalshi prices are in cents
            let size = t.count.unwrap_or(1.0);

            trades.push(RawTrade {
                id: Uuid::new_v4(),
                event_id: t.ticker.clone(),
                platform: "kalshi".to_string(),
                price,
                size,
                side: t.taker_side.clone(),
                trade_timestamp: ts,
                ingested_at: Utc::now(),
            });
        }

        cursor = body.cursor;
        if cursor.is_none() || body.trades.len() < MAX_TRADES_PER_FETCH {
            break;
        }
    }

    Ok(trades)
}

// ── Database Persistence ───────────────────────────────────────────

/// Bulk insert raw trades into RDS.
/// Uses ON CONFLICT to skip duplicates.
pub async fn persist_trades(pool: &PgPool, trades: &[RawTrade]) -> anyhow::Result<usize> {
    if trades.is_empty() {
        return Ok(0);
    }

    let mut inserted = 0usize;

    // Batch insert in chunks of 100 for efficiency
    for chunk in trades.chunks(100) {
        let mut query = String::from(
            "INSERT INTO raw_trades (id, event_id, platform, price, size, side, trade_timestamp, ingested_at) VALUES "
        );
        let mut params: Vec<String> = Vec::new();

        for (i, _t) in chunk.iter().enumerate() {
            let offset = i * 8;
            params.push(format!(
                "(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})",
                offset + 1, offset + 2, offset + 3, offset + 4,
                offset + 5, offset + 6, offset + 7, offset + 8
            ));
        }

        query.push_str(&params.join(", "));
        query.push_str(" ON CONFLICT DO NOTHING");

        let mut q = sqlx::query(&query);
        for t in chunk {
            q = q
                .bind(t.id)
                .bind(&t.event_id)
                .bind(&t.platform)
                .bind(t.price)
                .bind(t.size)
                .bind(&t.side)
                .bind(t.trade_timestamp)
                .bind(t.ingested_at);
        }

        let result = q.execute(pool).await?;
        inserted += result.rows_affected() as usize;
    }

    Ok(inserted)
}

/// Get the latest trade timestamp for a given event to use as watermark.
pub async fn get_latest_trade_ts(
    pool: &PgPool,
    event_id: &str,
    platform: &str,
) -> anyhow::Result<Option<DateTime<Utc>>> {
    let row: Option<(DateTime<Utc>,)> = sqlx::query_as(
        "SELECT trade_timestamp FROM raw_trades 
         WHERE event_id = $1 AND platform = $2 
         ORDER BY trade_timestamp DESC LIMIT 1"
    )
    .bind(event_id)
    .bind(platform)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| r.0))
}

/// Fetch stored trades for an event within a time range.
pub async fn get_trades(
    pool: &PgPool,
    event_id: &str,
    since: Option<DateTime<Utc>>,
    limit: i64,
) -> anyhow::Result<Vec<RawTrade>> {
    let since_ts = since.unwrap_or(DateTime::UNIX_EPOCH);

    let trades = sqlx::query_as::<_, RawTrade>(
        "SELECT id, event_id, platform, price, size, side, trade_timestamp, ingested_at
         FROM raw_trades
         WHERE event_id = $1 AND trade_timestamp >= $2
         ORDER BY trade_timestamp ASC
         LIMIT $3"
    )
    .bind(event_id)
    .bind(since_ts)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(trades)
}

// ── Background Ingestion Loop ──────────────────────────────────────

/// Represents a market to track for raw trade ingestion.
#[derive(Debug, Clone)]
pub struct TrackedMarket {
    pub event_id: String,       // Our internal event ID
    pub platform: String,       // "polymarket" or "kalshi"
    pub platform_token: String, // Platform-specific token/ticker ID
}

/// Get list of active markets to track from the prediction_events table.
pub async fn get_active_markets(pool: &PgPool) -> anyhow::Result<Vec<TrackedMarket>> {
    // Pull markets that are live and have recent activity
    let rows: Vec<(Uuid, String, String)> = sqlx::query_as(
        "SELECT id, platform, external_id FROM prediction_events 
         WHERE is_live = true 
         AND updated_at > NOW() - INTERVAL '24 hours'
         ORDER BY volume_24h DESC NULLS LAST
         LIMIT 200"
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(id, platform, ext_id)| TrackedMarket {
            event_id: id.to_string(),
            platform,
            platform_token: ext_id,
        })
        .collect())
}

/// Main background loop: polls active markets for raw trades.
/// Call this from your main.rs as a spawned task.
pub async fn run_trade_ingestion_loop(pool: PgPool, client: Client) {
    tracing::info!("🔄 Starting raw trade ingestion loop ({}s interval)", INGEST_INTERVAL_SECS);

    let mut interval = time::interval(Duration::from_secs(INGEST_INTERVAL_SECS));

    loop {
        interval.tick().await;

        let markets = match get_active_markets(&pool).await {
            Ok(m) => m,
            Err(e) => {
                tracing::error!("Failed to fetch active markets: {}", e);
                continue;
            }
        };

        tracing::info!("📊 Ingesting trades for {} active markets", markets.len());

        let mut total_ingested = 0usize;

        for market in &markets {
            let watermark = get_latest_trade_ts(&pool, &market.event_id, &market.platform)
                .await
                .ok()
                .flatten();

            let trades = match market.platform.to_lowercase().as_str() {
                "polymarket" => {
                    fetch_polymarket_trades(&client, &market.platform_token, watermark).await
                }
                "kalshi" => {
                    fetch_kalshi_trades(&client, &market.platform_token, watermark).await
                }
                _ => continue,
            };

            match trades {
                Ok(trades) if !trades.is_empty() => {
                    match persist_trades(&pool, &trades).await {
                        Ok(n) => total_ingested += n,
                        Err(e) => tracing::error!(
                            "Failed to persist trades for {}: {}",
                            market.event_id, e
                        ),
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "Trade fetch failed for {} ({}): {}",
                        market.event_id, market.platform, e
                    );
                }
                _ => {} // No new trades
            }

            // Small delay between markets to avoid rate limits
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        if total_ingested > 0 {
            tracing::info!("✅ Ingested {} new trades this cycle", total_ingested);
        }
    }
}
