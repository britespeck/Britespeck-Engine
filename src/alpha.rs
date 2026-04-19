//! Alpha signal detection: hidden wicks, volume spikes, and liquidity gaps.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

// ── Types ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct AlphaSignal {
    pub id: Uuid,
    pub event_id: String,
    pub signal_type: String, // "wick", "volume_spike", "liquidity_gap"
    pub magnitude: f64,
    pub metadata: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WickMetadata {
    pub wick_high: f64,
    pub wick_low: f64,
    pub wick_range: f64,
    pub bucketed_high: f64,
    pub bucketed_low: f64,
    pub hidden_cents: f64,
    pub direction: String,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub trade_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeSpikeMetadata {
    pub window_volume: f64,
    pub avg_volume: f64,
    pub spike_ratio: f64,
    pub dominant_side: Option<String>,
    pub buy_volume: f64,
    pub sell_volume: f64,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiquidityGapMetadata {
    pub gap_price_from: f64,
    pub gap_price_to: f64,
    pub gap_size_cents: f64,
    pub time_between_trades_secs: f64,
    pub occurred_at: DateTime<Utc>,
}

// ── Constants ──────────────────────────────────────────────────────

const WICK_THRESHOLD: f64 = 0.05;        // 5 cents
const VOLUME_SPIKE_RATIO: f64 = 2.0;
const LIQUIDITY_GAP_THRESHOLD: f64 = 0.03; // 3 cents
const VOLUME_WINDOW_MINS: i64 = 15;
const VOLUME_BASELINE_MINS: i64 = 60;
const BUCKET_WINDOW_MINS: i64 = 5;

// ── Wick Detection ─────────────────────────────────────────────────

pub async fn detect_wicks(
    pool: &PgPool,
    event_id: &str,
    lookback_hours: i64,
) -> anyhow::Result<Vec<AlphaSignal>> {
    let since = Utc::now() - Duration::hours(lookback_hours);
    let mut signals = Vec::new();

    let trades: Vec<(f64, f64, DateTime<Utc>)> = sqlx::query_as(
        "SELECT price, size, trade_timestamp FROM raw_trades
         WHERE event_id = $1 AND trade_timestamp >= $2
         ORDER BY trade_timestamp ASC"
    )
    .bind(event_id)
    .bind(since)
    .fetch_all(pool)
    .await?;

    if trades.len() < 10 {
        return Ok(signals);
    }

    let bucket_duration = Duration::minutes(BUCKET_WINDOW_MINS);
    let mut bucket_start = trades[0].2;
    let mut bucket_trades: Vec<&(f64, f64, DateTime<Utc>)> = Vec::new();

    for trade in &trades {
        if trade.2 >= bucket_start + bucket_duration {
            if let Some(signal) = analyze_bucket_for_wicks(event_id, &bucket_trades) {
                signals.push(signal);
            }
            bucket_start = trade.2;
            bucket_trades.clear();
        }
        bucket_trades.push(trade);
    }

    if let Some(signal) = analyze_bucket_for_wicks(event_id, &bucket_trades) {
        signals.push(signal);
    }

    Ok(signals)
}

fn analyze_bucket_for_wicks(
    event_id: &str,
    trades: &[&(f64, f64, DateTime<Utc>)],
) -> Option<AlphaSignal> {
    if trades.len() < 3 {
        return None;
    }

    let raw_high = trades.iter().map(|t| t.0).fold(f64::MIN, f64::max);
    let raw_low = trades.iter().map(|t| t.0).fold(f64::MAX, f64::min);
    let raw_range = raw_high - raw_low;

    let open = trades.first().unwrap().0;
    let close = trades.last().unwrap().0;
    let bucketed_high = open.max(close);
    let bucketed_low = open.min(close);

    let hidden_up = raw_high - bucketed_high;
    let hidden_down = bucketed_low - raw_low;
    let max_hidden = hidden_up.max(hidden_down);

    if max_hidden >= WICK_THRESHOLD {
        let direction = if hidden_up > hidden_down { "up_wick" } else { "down_wick" };

        let meta = WickMetadata {
            wick_high: raw_high,
            wick_low: raw_low,
            wick_range: raw_range,
            bucketed_high,
            bucketed_low,
            hidden_cents: max_hidden,
            direction: direction.to_string(),
            window_start: trades.first().unwrap().2,
            window_end: trades.last().unwrap().2,
            trade_count: trades.len(),
        };

        return Some(AlphaSignal {
            id: Uuid::new_v4(),
            event_id: event_id.to_string(),
            signal_type: "wick".to_string(),
            magnitude: max_hidden,
            metadata: serde_json::to_value(meta).unwrap_or_default(),
            created_at: Utc::now(),
        });
    }

    None
}

// ── Volume Spike Detection ─────────────────────────────────────────

pub async fn detect_volume_spikes(
    pool: &PgPool,
    event_id: &str,
    lookback_hours: i64,
) -> anyhow::Result<Vec<AlphaSignal>> {
    let since = Utc::now() - Duration::hours(lookback_hours);
    let mut signals = Vec::new();

    // FIXED: was `(EXTRACT ... % $3) * INTERVAL '1 minute'` which is invalid PG.
    // Now uses make_interval(mins => ...) — type-safe, no string concat.
    let windows: Vec<(DateTime<Utc>, f64, f64, f64)> = sqlx::query_as(
        "WITH bucketed AS (
            SELECT
                date_trunc('minute', trade_timestamp)
                    - make_interval(mins => (EXTRACT(MINUTE FROM trade_timestamp)::int % $3))
                    AS window_start,
                size,
                COALESCE(side, '') AS side
            FROM raw_trades
            WHERE event_id = $1 AND trade_timestamp >= $2
        )
        SELECT
            window_start,
            SUM(size)::float8                                         AS total_volume,
            SUM(CASE WHEN side = 'buy'  THEN size ELSE 0 END)::float8 AS buy_vol,
            SUM(CASE WHEN side = 'sell' THEN size ELSE 0 END)::float8 AS sell_vol
        FROM bucketed
        GROUP BY window_start
        ORDER BY window_start ASC"
    )
    .bind(event_id)
    .bind(since)
    .bind(VOLUME_WINDOW_MINS as i32)
    .fetch_all(pool)
    .await?;

    if windows.len() < 4 {
        return Ok(signals);
    }

    let baseline_windows = (VOLUME_BASELINE_MINS / VOLUME_WINDOW_MINS) as usize;

    for i in baseline_windows..windows.len() {
        let avg_volume: f64 = windows[i.saturating_sub(baseline_windows)..i]
            .iter()
            .map(|w| w.1)
            .sum::<f64>()
            / baseline_windows as f64;

        if avg_volume < 1.0 {
            continue;
        }

        let current_volume = windows[i].1;
        let spike_ratio = current_volume / avg_volume;

        if spike_ratio >= VOLUME_SPIKE_RATIO {
            let buy_vol = windows[i].2;
            let sell_vol = windows[i].3;
            let dominant = if buy_vol > sell_vol * 1.5 {
                Some("buy".to_string())
            } else if sell_vol > buy_vol * 1.5 {
                Some("sell".to_string())
            } else {
                None
            };

            let meta = VolumeSpikeMetadata {
                window_volume: current_volume,
                avg_volume,
                spike_ratio,
                dominant_side: dominant,
                buy_volume: buy_vol,
                sell_volume: sell_vol,
                window_start: windows[i].0,
                window_end: windows[i].0 + Duration::minutes(VOLUME_WINDOW_MINS),
            };

            signals.push(AlphaSignal {
                id: Uuid::new_v4(),
                event_id: event_id.to_string(),
                signal_type: "volume_spike".to_string(),
                magnitude: spike_ratio,
                metadata: serde_json::to_value(meta).unwrap_or_default(),
                created_at: Utc::now(),
            });
        }
    }

    Ok(signals)
}

// ── Liquidity Gap Detection ────────────────────────────────────────

pub async fn detect_liquidity_gaps(
    pool: &PgPool,
    event_id: &str,
    lookback_hours: i64,
) -> anyhow::Result<Vec<AlphaSignal>> {
    let since = Utc::now() - Duration::hours(lookback_hours);
    let mut signals = Vec::new();

    let trades: Vec<(f64, DateTime<Utc>)> = sqlx::query_as(
        "SELECT price, trade_timestamp FROM raw_trades
         WHERE event_id = $1 AND trade_timestamp >= $2
         ORDER BY trade_timestamp ASC"
    )
    .bind(event_id)
    .bind(since)
    .fetch_all(pool)
    .await?;

    for window in trades.windows(2) {
        let (prev_price, prev_ts) = window[0];
        let (curr_price, curr_ts) = window[1];
        let gap = (curr_price - prev_price).abs();

        if gap >= LIQUIDITY_GAP_THRESHOLD {
            let time_gap = (curr_ts - prev_ts).num_seconds() as f64;

            let meta = LiquidityGapMetadata {
                gap_price_from: prev_price,
                gap_price_to: curr_price,
                gap_size_cents: gap,
                time_between_trades_secs: time_gap,
                occurred_at: curr_ts,
            };

            signals.push(AlphaSignal {
                id: Uuid::new_v4(),
                event_id: event_id.to_string(),
                signal_type: "liquidity_gap".to_string(),
                magnitude: gap,
                metadata: serde_json::to_value(meta).unwrap_or_default(),
                created_at: Utc::now(),
            });
        }
    }

    Ok(signals)
}

// ── Database Persistence ───────────────────────────────────────────

pub async fn persist_signals(pool: &PgPool, signals: &[AlphaSignal]) -> anyhow::Result<usize> {
    if signals.is_empty() {
        return Ok(0);
    }

    let mut inserted = 0usize;

    for signal in signals {
        let result = sqlx::query(
            "INSERT INTO alpha_signals (id, event_id, signal_type, magnitude, metadata, created_at)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT DO NOTHING"
        )
        .bind(signal.id)
        .bind(&signal.event_id)
        .bind(&signal.signal_type)
        .bind(signal.magnitude)
        .bind(&signal.metadata)
        .bind(signal.created_at)
        .execute(pool)
        .await?;

        inserted += result.rows_affected() as usize;
    }

    Ok(inserted)
}

pub async fn get_signals(
    pool: &PgPool,
    event_id: &str,
    signal_type: Option<&str>,
    limit: i64,
) -> anyhow::Result<Vec<AlphaSignal>> {
    let limit = limit.clamp(1, 500);

    let signals = if let Some(st) = signal_type {
        sqlx::query_as::<_, AlphaSignal>(
            "SELECT id, event_id, signal_type, magnitude, metadata, created_at
             FROM alpha_signals
             WHERE event_id = $1 AND signal_type = $2
             ORDER BY created_at DESC
             LIMIT $3"
        )
        .bind(event_id)
        .bind(st)
        .bind(limit)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query_as::<_, AlphaSignal>(
            "SELECT id, event_id, signal_type, magnitude, metadata, created_at
             FROM alpha_signals
             WHERE event_id = $1
             ORDER BY created_at DESC
             LIMIT $2"
        )
        .bind(event_id)
        .bind(limit)
        .fetch_all(pool)
        .await?
    };

    Ok(signals)
}

// ── Background Analysis Loop ───────────────────────────────────────

pub async fn run_alpha_detection_loop(pool: PgPool) {
    tracing::info!("🔍 Starting alpha signal detection loop");

    let mut interval = tokio::time::interval(std::time::Duration::from_secs(90));

    loop {
        interval.tick().await;

        let active_events: Vec<(String,)> = match sqlx::query_as(
            "SELECT DISTINCT event_id FROM raw_trades
             WHERE ingested_at > NOW() - INTERVAL '10 minutes'"
        )
        .fetch_all(&pool)
        .await
        {
            Ok(e) => e,
            Err(e) => {
                tracing::error!("Failed to get active events for alpha: {}", e);
                continue;
            }
        };

        if active_events.is_empty() {
            tracing::debug!("Alpha loop: no recently-active events, skipping cycle");
            continue;
        }

        let mut total_signals = 0usize;

        for (event_id,) in &active_events {
            let mut all_signals = Vec::new();

            if let Ok(wicks) = detect_wicks(&pool, event_id, 4).await {
                all_signals.extend(wicks);
            }
            if let Ok(spikes) = detect_volume_spikes(&pool, event_id, 4).await {
                all_signals.extend(spikes);
            }
            if let Ok(gaps) = detect_liquidity_gaps(&pool, event_id, 4).await {
                all_signals.extend(gaps);
            }

            if !all_signals.is_empty() {
                match persist_signals(&pool, &all_signals).await {
                    Ok(n) => total_signals += n,
                    Err(e) => tracing::error!(
                        "Failed to persist alpha signals for {}: {}",
                        event_id, e
                    ),
                }
            }
        }

        if total_signals > 0 {
            tracing::info!("🎯 Generated {} alpha signals this cycle", total_signals);
        }
    }
}
