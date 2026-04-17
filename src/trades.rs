//! Raw trade ingestion from Polymarket CLOB and Kalshi APIs.

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
struct PolyTradesResponse {
    #[serde(default)]
    data: Vec<PolyTrade>,
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct KalshiTrade {
    ticker: String,
    #[serde(rename = "yes_price")]
    yes_price: f64,
    #[serde(rename = "no_price", default)]
    #[allow(dead_code)]
    no_price: f64,
    count: Option<f64>,
    #[serde(rename = "created_time")]
    created_time: Option<String>,
    taker_side: Option<String>,
}

#[derive(Debug, Deserialize)]
struct KalshiTradesResponse {
    #[serde(default)]
    trades: Vec<KalshiTrade>,
    cursor: Option<String>,
}

pub struct TrackedMarket {
    pub event_id: String,        // canonical "<platform>:<external_id>"
    pub platform: String,        // "polymarket" | "kalshi"
    pub platform_token: String,  // Polymarket: CLOB token (0x…). Kalshi: market ticker.
}

// ── Constants ──────────────────────────────────────────────────────

// ✅ Real API hosts (not the consumer frontends)
const POLYMARKET_CLOB_URL: &str = "https://clob.polymarket.com";
const KALSHI_API_URL: &str = "https://api.elections.kalshi.com/trade-api/v2";

const INGEST_INTERVAL_SECS: u64 = 45;
const MAX_TRADES_PER_FETCH: usize = 500;

// ── Polymarket CLOB Ingestion ──────────────────────────────────────
//
// Public endpoint — no HMAC required.
// GET https://clob.polymarket.com/trades?market=<CLOB_TOKEN>&limit=500
//
// `token_id` MUST be a CLOB token id (0x-prefixed hex). Anything else
// (e.g. Gamma event UUID) is skipped — populate `clob_token_yes` in
// the fetcher so this receives a real token.

pub async fn fetch_polymarket_trades(
    client: &Client,
    token_id: &str,
    since: Option<DateTime<Utc>>,
) -> anyhow::Result<Vec<RawTrade>> {
    if token_id.is_empty() || !token_id.starts_with("0x") {
        return Ok(Vec::new());
    }

    let mut trades = Vec::new();
    let mut cursor: Option<String> = None;

    loop {
        let mut url = format!(
            "{}/trades?market={}&limit={}",
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

        let status = resp.status();
        let content_type = resp
            .headers()
            .get("content-type")
            .and_then(|h| h.to_str().ok())
            .unwrap_or_default()
            .to_string();

        if !status.is_success() || !content_type.contains("application/json") {
            tracing::warn!(
                "Polymarket trades non-OK ({} / {}) for token {}",
                status, content_type, token_id
            );
            break;
        }

        let body_text = resp.text().await?;
        let body: PolyTradesResponse = match serde_json::from_str(&body_text) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!("Polymarket trades JSON parse error: {}", e);
                break;
            }
        };

        let page_len = body.data.len();

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
                if ts <= *since_ts {
                    continue;
                }
            }

            // 🔑 Canonical event_id matches prediction_events.external_id
            // (Polymarket fetcher prefixes with "polymarket:")
            trades.push(RawTrade {
                id: Uuid::new_v4(),
                event_id: format!("polymarket:{}", t.asset_id),
                platform: "polymarket".to_string(),
                price,
                size,
                side: t.side.clone(),
                trade_timestamp: ts,
                ingested_at: Utc::now(),
            });
        }

        cursor = body.next_cursor;
        if cursor.is_none() || page_len < MAX_TRADES_PER_FETCH {
            break;
        }
    }

    Ok(trades)
}

// ── Kalshi Trade Ingestion ─────────────────────────────────────────
//
// GET https://api.elections.kalshi.com/trade-api/v2/markets/trades
//     ?ticker=<MARKET_TICKER>&limit=500&min_ts=<UNIX>

pub async fn fetch_kalshi_trades(
    client: &Client,
    ticker: &str,
    since: Option<DateTime<Utc>>,
) -> anyhow::Result<Vec<RawTrade>> {
    if ticker.is_empty() {
        return Ok(Vec::new());
    }

    let mut trades = Vec::new();
    let mut cursor: Option<String> = None;

    loop {
        let mut url = format!(
            "{}/markets/trades?ticker={}&limit={}",
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
            tracing::warn!("Kalshi trades API returned {} for {}", resp.status(), ticker);
            break;
        }

        let body: KalshiTradesResponse = match resp.json().await {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!("Kalshi trades JSON parse error: {}", e);
                break;
            }
        };

        let page_len = body.trades.len();

        for t in &body.trades {
            let ts = t
                .created_time
                .as_deref()
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(Utc::now);

            // Kalshi yes_price is in cents (0–100) → dollars
            let price = if t.yes_price > 1.0 {
                t.yes_price / 100.0
            } else {
                t.yes_price
            };
            let size = t.count.unwrap_or(1.0);

            // 🔑 Canonical event_id matches prediction_events.external_id
            trades.push(RawTrade {
                id: Uuid::new_v4(),
                event_id: format!("kalshi:{}", t.ticker),
                platform: "kalshi".to_string(),
                price,
                size,
                side: t.taker_side.clone(),
                trade_timestamp: ts,
                ingested_at: Utc::now(),
            });
        }

        cursor = body.cursor;
        if cursor.is_none() || page_len < MAX_TRADES_PER_FETCH {
            break;
        }
    }

    Ok(trades)
}

// ── Database Operations ────────────────────────────────────────────

pub async fn persist_trades(pool: &PgPool, trades: &[RawTrade]) -> anyhow::Result<usize> {
    if trades.is_empty() {
        return Ok(0);
    }
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
            "SELECT id, event_id, platform, price, size, side, trade_timestamp, ingested_at \
             FROM raw_trades \
             WHERE event_id = $1 AND trade_timestamp > $2 \
             ORDER BY trade_timestamp DESC LIMIT $3"
        )
        .bind(event_id)
        .bind(since_ts)
        .bind(limit)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query_as::<_, RawTrade>(
            "SELECT id, event_id, platform, price, size, side, trade_timestamp, ingested_at \
             FROM raw_trades \
             WHERE event_id = $1 \
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
        "SELECT trade_timestamp FROM raw_trades \
         WHERE event_id = $1 AND platform = $2 \
         ORDER BY trade_timestamp DESC LIMIT 1"
    )
    .bind(event_id)
    .bind(platform)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| r.0))
}

// ── Active markets selector ────────────────────────────────────────
//
// IMPORTANT:
// - For Kalshi, `platform_token` is the market ticker (stripped of the
//   "kalshi:" prefix from `external_id`).
// - For Polymarket, `platform_token` MUST be the CLOB token id
//   (0x-prefixed hex). Add a `clob_token_yes TEXT` column to
//   `prediction_events` and populate it in fetcher.rs from
//   `market.clobTokenIds[0]`. This selector reads it via COALESCE so
//   markets without a CLOB token are simply skipped at fetch time.

pub async fn get_active_markets(pool: &PgPool) -> anyhow::Result<Vec<TrackedMarket>> {
    let rows: Vec<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT external_id, platform, clob_token_yes \
         FROM prediction_events \
         WHERE (status ILIKE 'active' OR status ILIKE 'open') \
           AND (platform ILIKE 'polymarket' OR platform ILIKE 'kalshi') \
         ORDER BY volume_24h DESC NULLS LAST \
         LIMIT 500"
    )
    .fetch_all(pool)
    .await?;

    let mut markets = Vec::with_capacity(rows.len());
    for (external_id, platform, clob_token) in rows {
        let plat_lower = platform.to_lowercase();

        // Strip the "<platform>:" prefix to recover the raw upstream id
        let raw_id = external_id
            .strip_prefix(&format!("{}:", plat_lower))
            .unwrap_or(&external_id)
            .to_string();

        let platform_token = match plat_lower.as_str() {
            "polymarket" => match clob_token {
                Some(tok) if tok.starts_with("0x") => tok,
                _ => continue, // no CLOB token → can't query /trades
            },
            "kalshi" => raw_id,
            _ => continue,
        };

        markets.push(TrackedMarket {
            event_id: external_id, // canonical "<platform>:<id>"
            platform: plat_lower,
            platform_token,
        });
    }

    Ok(markets)
}

// ── Loops ──────────────────────────────────────────────────────────

pub async fn run_trade_ingestion_loop(pool: PgPool, client: Client) {
    tracing::info!(
        "🔄 Starting raw trade ingestion loop ({}s interval)",
        INGEST_INTERVAL_SECS
    );
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

            let trades = match market.platform.as_str() {
                "polymarket" => {
                    fetch_polymarket_trades(&client, &market.platform_token, watermark).await
                }
                "kalshi" => {
                    fetch_kalshi_trades(&client, &market.platform_token, watermark).await
                }
                _ => continue,
            };

            match trades {
                Ok(t) if !t.is_empty() => match persist_trades(&pool, &t).await {
                    Ok(n) => total_ingested += n,
                    Err(e) => {
                        tracing::error!("Persist failed for {}: {}", market.event_id, e)
                    }
                },
                Err(e) => tracing::warn!(
                    "Fetch failed for {} ({}): {}",
                    market.event_id, market.platform, e
                ),
                _ => {}
            }

            tokio::time::sleep(Duration::from_millis(150)).await;
        }

        if total_ingested > 0 {
            tracing::info!("✅ Ingested {} new trades this cycle", total_ingested);
        }
    }
}