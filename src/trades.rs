//! Raw trade ingestion from Polymarket CLOB and Kalshi APIs.

use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::time::Duration;
use uuid::Uuid;

// ── Types ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct RawTrade {
    pub id: Uuid,
    pub event_id: String,
    pub platform: String,
    pub price: f64,
    pub size: f64,
    pub side: Option<String>, // "buy" | "sell" | None when platform omits
    pub trade_timestamp: DateTime<Utc>,
    pub ingested_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct ActiveMarket {
    event_id: String,
    platform: String,
    external_id: String,
}

// ── Polymarket CLOB ────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct PolymarketTrade {
    #[serde(default)]
    price: Option<String>,
    #[serde(default)]
    size: Option<String>,
    #[serde(default)]
    side: Option<String>,
    #[serde(default, alias = "match_time", alias = "timestamp")]
    timestamp: Option<String>,
}

async fn fetch_polymarket_trades(
    client: &Client,
    market_id: &str,
) -> anyhow::Result<Vec<(f64, f64, Option<String>, DateTime<Utc>)>> {
    let url = format!(
        "https://clob.polymarket.com/trades?market={}&limit=50",
        market_id
    );

    let resp = client
        .get(&url)
        .timeout(Duration::from_secs(8))
        .send()
        .await?;

    if !resp.status().is_success() {
        anyhow::bail!("Polymarket {} returned {}", market_id, resp.status());
    }

    let trades: Vec<PolymarketTrade> = resp.json().await.unwrap_or_default();

    let parsed = trades
        .into_iter()
        .filter_map(|t| {
            let price = t.price?.parse::<f64>().ok()?;
            let size = t.size?.parse::<f64>().ok()?;
            let ts = t
                .timestamp
                .and_then(|s| {
                    s.parse::<i64>()
                        .ok()
                        .and_then(|n| DateTime::<Utc>::from_timestamp(n, 0))
                        .or_else(|| DateTime::parse_from_rfc3339(&s).ok().map(|d| d.with_timezone(&Utc)))
                })
                .unwrap_or_else(Utc::now);
            Some((price, size, t.side.map(|s| s.to_lowercase()), ts))
        })
        .collect();

    Ok(parsed)
}

// ── Kalshi ─────────────────────────────────────────────────────────

#[derive(Debug, Default, Deserialize)]
struct KalshiTradesResponse {
    #[serde(default)]
    trades: Vec<KalshiTrade>,
}

#[derive(Debug, Deserialize)]
struct KalshiTrade {
    #[serde(default)]
    yes_price: Option<i64>, // cents (0-100)
    #[serde(default)]
    count: Option<i64>,
    #[serde(default)]
    taker_side: Option<String>,
    #[serde(default)]
    created_time: Option<String>,
}

async fn fetch_kalshi_trades(
    client: &Client,
    ticker: &str,
) -> anyhow::Result<Vec<(f64, f64, Option<String>, DateTime<Utc>)>> {
    let url = format!(
        "https://api.elections.kalshi.com/trade-api/v2/markets/trades?ticker={}&limit=50",
        ticker
    );

    let resp = client
        .get(&url)
        .timeout(Duration::from_secs(8))
        .send()
        .await?;

    if !resp.status().is_success() {
        anyhow::bail!("Kalshi {} returned {}", ticker, resp.status());
    }

    let body: KalshiTradesResponse = resp.json().await.unwrap_or_default();

    let parsed = body
        .trades
        .into_iter()
        .filter_map(|t| {
            let price = (t.yes_price? as f64) / 100.0;
            let size = t.count? as f64;
            let ts = t
                .created_time
                .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                .map(|d| d.with_timezone(&Utc))
                .unwrap_or_else(Utc::now);
            Some((price, size, t.taker_side.map(|s| s.to_lowercase()), ts))
        })
        .collect();

    Ok(parsed)
}

// ── Persistence ────────────────────────────────────────────────────

async fn persist_trades(
    pool: &PgPool,
    market: &ActiveMarket,
    trades: &[(f64, f64, Option<String>, DateTime<Utc>)],
) -> anyhow::Result<usize> {
    if trades.is_empty() {
        return Ok(0);
    }

    let mut inserted = 0usize;

    for (price, size, side, ts) in trades {
        let result = sqlx::query(
            "INSERT INTO raw_trades
                (id, event_id, platform, price, size, side, trade_timestamp, ingested_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             ON CONFLICT (event_id, platform, trade_timestamp, price, size) DO NOTHING"
        )
        .bind(Uuid::new_v4())
        .bind(&market.event_id)
        .bind(&market.platform)
        .bind(price)
        .bind(size)
        .bind(side.as_deref())
        .bind(ts)
        .bind(Utc::now())
        .execute(pool)
        .await?;

        inserted += result.rows_affected() as usize;
    }

    Ok(inserted)
}

// ── Public query API (used by endpoints.rs) ────────────────────────

/// Query persisted trades for an event, optionally filtered by `since` timestamp.
pub async fn get_trades(
    pool: &PgPool,
    event_id: &str,
    since: Option<DateTime<Utc>>,
    limit: i64,
) -> anyhow::Result<Vec<RawTrade>> {
    let rows = if let Some(ts) = since {
        sqlx::query_as::<_, RawTrade>(
            "SELECT id, event_id, platform, price, size, side, trade_timestamp, ingested_at
             FROM public.raw_trades
             WHERE event_id = $1 AND trade_timestamp >= $2
             ORDER BY trade_timestamp DESC
             LIMIT $3"
        )
        .bind(event_id)
        .bind(ts)
        .bind(limit)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query_as::<_, RawTrade>(
            "SELECT id, event_id, platform, price, size, side, trade_timestamp, ingested_at
             FROM public.raw_trades
             WHERE event_id = $1
             ORDER BY trade_timestamp DESC
             LIMIT $2"
        )
        .bind(event_id)
        .bind(limit)
        .fetch_all(pool)
        .await?
    };

    Ok(rows)
}

// ── Active markets selector ────────────────────────────────────────

async fn get_active_markets(pool: &PgPool) -> anyhow::Result<Vec<ActiveMarket>> {
    // Only pull markets refreshed in the last 30 min — avoids fetching
    // trades for stale or closed contracts.
    let rows: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT external_id, platform, external_id
         FROM public.prediction_events
         WHERE updated_at > NOW() - INTERVAL '30 minutes'
           AND COALESCE(status, 'open') NOT IN ('closed', 'resolved', 'expired')
           AND external_id IS NOT NULL
         ORDER BY volume_24h DESC NULLS LAST
         LIMIT 50"
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(event_id, platform, external_id)| ActiveMarket {
            event_id,
            platform,
            external_id,
        })
        .collect())
}

// ── Main loop ──────────────────────────────────────────────────────

pub async fn run_trade_ingestion_loop(pool: PgPool) {
    tracing::info!("📥 Starting trade ingestion loop");

    let client = Client::builder()
        .user_agent("Britespeck-Engine/1.0")
        .build()
        .expect("reqwest client");

    let mut interval = tokio::time::interval(Duration::from_secs(60));

    loop {
        interval.tick().await;

        let markets = match get_active_markets(&pool).await {
            Ok(m) => m,
            Err(e) => {
                tracing::error!("Failed to load active markets: {}", e);
                continue;
            }
        };

        if markets.is_empty() {
            tracing::debug!("Trade loop: no active markets, skipping cycle");
            continue;
        }

        let mut total_ingested = 0usize;

        for market in &markets {
            let fetch_result = match market.platform.as_str() {
                "polymarket" => fetch_polymarket_trades(&client, &market.external_id).await,
                "kalshi" => fetch_kalshi_trades(&client, &market.external_id).await,
                other => {
                    tracing::debug!("Skipping unsupported platform: {}", other);
                    continue;
                }
            };

            match fetch_result {
                Ok(trades) if !trades.is_empty() => match persist_trades(&pool, market, &trades).await {
                    Ok(n) => total_ingested += n,
                    Err(e) => tracing::error!("Persist failed for {}: {}", market.event_id, e),
                },
                Ok(_) => {} // no new trades — silent
                Err(e) => tracing::warn!(
                    "Fetch failed for {} ({}): {}",
                    market.event_id, market.platform, e
                ),
            }

            tokio::time::sleep(Duration::from_millis(150)).await;
        }

        if total_ingested > 0 {
            tracing::info!("✅ Ingested {} new trades this cycle", total_ingested);
        }
    }
}
