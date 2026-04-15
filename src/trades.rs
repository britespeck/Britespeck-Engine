//! Raw trade ingestion from Polymarket CLOB and Kalshi APIs.
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::time::Duration;
use tokio::time;
use uuid::Uuid;

// Auth imports
use hmac::{Hmac, Mac};
use sha2::Sha256;
use base64::{Engine as _, engine::general_purpose};
use std::env;

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

#[derive(Debug, Deserialize)]
struct PolyTrade {
    #[serde(rename = "asset_id")]
    asset_id: String,
    price: String,
    size: String,
    side: Option<String>,
    #[serde(rename = "match_time")]
    match_time: Option<String>,
}

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

pub struct TrackedMarket {
    pub event_id: String,
    pub platform: String,
    pub platform_token: String,
}

// ── Constants ──────────────────────────────────────────────────────

const POLYMARKET_CLOB_URL: &str = "https://polymarket.com";
const KALSHI_API_URL: &str = "https://kalshi.com";
const INGEST_INTERVAL_SECS: u64 = 45; 
const MAX_TRADES_PER_FETCH: usize = 500;

// ── Polymarket CLOB Ingestion ──────────────────────────────────────

pub async fn fetch_polymarket_trades(
    client: &Client,
    token_id: &str,
    since: Option<DateTime<Utc>>,
) -> anyhow::Result<Vec<RawTrade>> {
    // SAFETY: Skip if we accidentally passed a UUID instead of an Asset ID
    if token_id.contains('-') {
        return Ok(Vec::new());
    }

    let mut trades = Vec::new();
    let mut cursor: Option<String> = None;

    // Pulls from ECS Environment automatically
    let api_key = env::var("POLYMARKET_API_KEY").unwrap_or_default();
    let secret_str = env::var("POLYMARKET_SECRET").unwrap_or_default();
    let passphrase = env::var("POLYMARKET_PASSPHRASE").unwrap_or_default();

    loop {
        let timestamp = (Utc::now().timestamp() - 3).to_string();
        let method = "GET";
        
        let mut request_path = format!("/trades?asset_id={}&limit={}", token_id, MAX_TRADES_PER_FETCH);
        if let Some(ref c) = cursor {
            request_path.push_str(&format!("&next_cursor={}", c));
        }

        // Generate POLY-SIGNATURE
        let message = format!("{}{}{}", timestamp, method, request_path);
        let mut mac = Hmac::<Sha256>::new_from_slice(secret_str.as_bytes())
            .map_err(|e| anyhow::anyhow!("HMAC error: {}", e))?;
        mac.update(message.as_bytes());
        let signature = general_purpose::STANDARD.encode(mac.finalize().into_bytes());

        let url = format!("{}{}", POLYMARKET_CLOB_URL, request_path);

        let resp = client
            .get(&url)
            .header("POLY-API-KEY", &api_key)
            .header("POLY-PASSPHRASE", &passphrase)
            .header("POLY-TIMESTAMP", &timestamp)
            .header("POLY-SIGNATURE", &signature)
            .timeout(Duration::from_secs(10))
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let body_err = resp.text().await.unwrap_or_default();
            tracing::error!("Polymarket API Error ({}) for {}: {}", status, token_id, body_err);
            break;
        }

        // Capture body as text first to debug decoding errors
        let body_text = resp.text().await?;
        let body: PolyTradesResponse = match serde_json::from_str(&body_text) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!("JSON Decode Failure for {}: {}. Body: {}", token_id, e, body_text);
                break;
            }
        };

        for t in &body.data {
            let price: f64 = t.price.parse().unwrap_or(0.0);
            let size: f64 = t.size.parse().unwrap_or(0.0);
            let ts = t
                .match_time
                .as_deref()
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(Utc::now);

            if let Some(ref since_ts) = since {
                if ts <= *since_ts { continue; }
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

        cursor = body.next_cursor;
        if cursor.is_none() || body.data.len() < MAX_TRADES_PER_FETCH {
            break;
        }
    }

    Ok(trades)
}

// ── Kalshi Trade Ingestion ─────────────────────────────────────────

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

            let price = t.yes_price / 100.0; 
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

// ── Database Operations ────────────────────────────────────────────

pub async fn persist_trades(pool: &PgPool, trades: &[RawTrade]) -> anyhow::Result<usize> {
    if trades.is_empty() { return Ok(0); }
    let mut inserted = 0usize;

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

pub async fn get_trades(
    pool: &PgPool,
    event_id: &str,
    since: Option<DateTime<Utc>>,
    limit: i64,
) -> anyhow::Result<Vec<RawTrade>> {
    let trades = if let Some(since_ts) = since {
        sqlx::query_as::<_, RawTrade>(
            "SELECT id, event_id, platform, price, size, side, trade_timestamp, ingested_at FROM raw_trades 
             WHERE event_id = $1 AND trade_timestamp > $2 
             ORDER BY trade_timestamp DESC LIMIT $3"
        )
        .bind(event_id)
        .bind(since_ts)
        .bind(limit)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query_as::<_, RawTrade>(
            "SELECT id, event_id, platform, price, size, side, trade_timestamp, ingested_at FROM raw_trades 
             WHERE event_id = $1 
             ORDER BY trade_timestamp DESC LIMIT $2"
        )
        .bind(event_id)
        .bind(limit)
        .fetch_all(pool)
        .await?
    };

    Ok(trades)
}

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

pub async fn get_active_markets(pool: &PgPool) -> anyhow::Result<Vec<TrackedMarket>> {
    let rows: Vec<(Uuid, String, String)> = sqlx::query_as(
        "SELECT id, platform, external_id FROM prediction_events 
         WHERE status = 'active' OR status = 'open'
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

// ── Loops ──────────────────────────────────────────────────────────

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

        let mut total_ingested = 0usize;

        for market in &markets {
            let watermark = get_latest_trade_ts(&pool, &market.event_id, &market.platform)
                .await
                .ok()
                .flatten();

            let trades = match market.platform.to_lowercase().as_str() {
                "polymarket" => fetch_polymarket_trades(&client, &market.platform_token, watermark).await,
                "kalshi" => fetch_kalshi_trades(&client, &market.platform_token, watermark).await,
                _ => continue,
            };

            match trades {
                Ok(t) if !t.is_empty() => {
                    match persist_trades(&pool, &t).await {
                        Ok(n) => total_ingested += n,
                        Err(e) => tracing::error!("Persist failed for {}: {}", market.event_id, e),
                    }
                }
                Err(e) => tracing::warn!("Fetch failed for {}: {}", market.event_id, e),
                _ => {}
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        if total_ingested > 0 {
            tracing::info!("✅ Ingested {} new trades this cycle", total_ingested);
        }
    }
}