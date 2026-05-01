//! OMG Indicator Engine — computes all 12 signals per market.
//!
//! Exposes:
//!   GET /indicators/:event_id   — latest computed indicators
//!   POST /indicators/:event_id/compute — force recompute now
//!
//! Background loop runs every 60s, computes for all active markets,
//! writes results to public.market_indicators.
//!
//! Wire into main.rs:
//!   mod indicators;
//!   // in router: .merge(indicators::routes())
//!   // in spawns: tokio::spawn(indicators::run_indicator_loop(api_pool.clone()));

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::time;
use uuid::Uuid;

// ── Response types ─────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow, Clone)]
pub struct MarketIndicators {
    pub id: Uuid,
    pub event_id: Uuid,
    pub computed_at: DateTime<Utc>,

    // Price signals
    pub vwap: Option<f64>,
    pub price_momentum_1h: Option<f64>,
    pub price_momentum_6h: Option<f64>,
    pub price_momentum_24h: Option<f64>,

    // Order flow
    pub book_imbalance: Option<f64>,
    pub spread_pct: Option<f64>,
    pub volume_spike: Option<f64>,

    // Risk
    pub ev_yes: Option<f64>,
    pub ev_no: Option<f64>,
    pub kelly_fraction: Option<f64>,
    pub cross_platform_delta: Option<f64>,
    pub resolution_risk: Option<f64>,

    // Sentiment
    pub open_interest_delta: Option<f64>,
    pub news_sentiment: Option<f64>,

    // Composite
    pub omg_score: Option<f64>,
    pub omg_signal: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct IndicatorsResponse {
    pub event_id: String,
    pub indicators: Option<MarketIndicators>,
    pub computed_at: DateTime<Utc>,
    pub data_age_secs: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

// ── Routes ─────────────────────────────────────────────────────────

pub fn routes() -> Router<PgPool> {
    Router::new()
        .route("/indicators/:event_id", get(get_indicators_handler))
        .route("/indicators/:event_id/compute", post(force_compute_handler))
}

// ── GET /indicators/:event_id ──────────────────────────────────────

async fn get_indicators_handler(
    State(pool): State<PgPool>,
    Path(event_id): Path<String>,
) -> Result<Json<IndicatorsResponse>, (StatusCode, Json<ErrorResponse>)> {
    let uid = parse_or_lookup_uuid(&pool, &event_id).await.ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Event not found: {}", event_id),
            }),
        )
    })?;

    let row = sqlx::query_as::<_, MarketIndicators>(
        "SELECT id, event_id, computed_at,
                vwap, price_momentum_1h, price_momentum_6h, price_momentum_24h,
                book_imbalance, spread_pct, volume_spike,
                ev_yes, ev_no, kelly_fraction, cross_platform_delta, resolution_risk,
                open_interest_delta, news_sentiment,
                omg_score, omg_signal
         FROM public.market_indicators
         WHERE event_id = $1
         ORDER BY computed_at DESC
         LIMIT 1",
    )
    .bind(uid)
    .fetch_optional(&pool)
    .await
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("DB error: {}", e),
            }),
        )
    })?;

    let data_age_secs = row.as_ref().map(|r| {
        (Utc::now() - r.computed_at).num_seconds()
    });

    Ok(Json(IndicatorsResponse {
        event_id: uid.to_string(),
        indicators: row,
        computed_at: Utc::now(),
        data_age_secs,
    }))
}

// ── POST /indicators/:event_id/compute ────────────────────────────

async fn force_compute_handler(
    State(pool): State<PgPool>,
    Path(event_id): Path<String>,
) -> Result<Json<IndicatorsResponse>, (StatusCode, Json<ErrorResponse>)> {
    let uid = parse_or_lookup_uuid(&pool, &event_id).await.ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Event not found: {}", event_id),
            }),
        )
    })?;

    match compute_and_persist(&pool, uid).await {
        Ok(ind) => Ok(Json(IndicatorsResponse {
            event_id: uid.to_string(),
            indicators: Some(ind),
            computed_at: Utc::now(),
            data_age_secs: Some(0),
        })),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("Compute failed: {}", e),
            }),
        )),
    }
}

// ── Core computation ───────────────────────────────────────────────

pub async fn compute_and_persist(
    pool: &PgPool,
    event_id: Uuid,
) -> anyhow::Result<MarketIndicators> {

    // Run all computations concurrently
    let (
        vwap_res,
        momentum_res,
        book_res,
        ev_res,
        cross_res,
        resolution_res,
        oi_res,
        sentiment_res,
        volume_spike_res,
    ) = tokio::join!(
        compute_vwap(pool, event_id),
        compute_price_momentum(pool, event_id),
        compute_book_signals(pool, event_id),
        compute_ev_kelly(pool, event_id),
        compute_cross_platform_delta(pool, event_id),
        compute_resolution_risk(pool, event_id),
        compute_open_interest_delta(pool, event_id),
        fetch_news_sentiment(pool, event_id),
        compute_volume_spike(pool, event_id),
    );

    let vwap = vwap_res.ok().flatten();
    let (mom_1h, mom_6h, mom_24h) = momentum_res.unwrap_or((None, None, None));
    let (book_imbalance, spread_pct) = book_res.unwrap_or((None, None));
    let (ev_yes, ev_no, kelly) = ev_res.unwrap_or((None, None, None));
    let cross_platform_delta = cross_res.ok().flatten();
    let resolution_risk = resolution_res.ok().flatten();
    let open_interest_delta = oi_res.ok().flatten();
    let news_sentiment = sentiment_res.ok().flatten();
    let volume_spike = volume_spike_res.ok().flatten();

    // ── OMG Composite Score ────────────────────────────────────
    // Weighted 0-100 score. Weights sum to 1.0.
    // Higher weight = more influence on the composite.
    let omg_score = compute_omg_composite(
        ev_yes,
        kelly,
        book_imbalance,
        mom_24h,
        volume_spike,
        cross_platform_delta,
        news_sentiment,
        resolution_risk,
        spread_pct,
    );

    let omg_signal = omg_score.map(|s| {
        match s as i32 {
            75..=100 => "strong_buy",
            60..=74  => "buy",
            40..=59  => "hold",
            25..=39  => "sell",
            _        => "strong_sell",
        }
        .to_string()
    });

    // ── Persist to market_indicators ───────────────────────────
    let id = Uuid::new_v4();
    let now = Utc::now();

    // Upsert: if we computed within the last minute, update instead of insert
    sqlx::query(
        "INSERT INTO public.market_indicators (
            id, event_id, computed_at,
            vwap, price_momentum_1h, price_momentum_6h, price_momentum_24h,
            book_imbalance, spread_pct, volume_spike,
            ev_yes, ev_no, kelly_fraction, cross_platform_delta, resolution_risk,
            open_interest_delta, news_sentiment,
            omg_score, omg_signal
         )
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
         ON CONFLICT (event_id, date_trunc('minute', computed_at))
         DO UPDATE SET
            vwap = EXCLUDED.vwap,
            price_momentum_1h  = EXCLUDED.price_momentum_1h,
            price_momentum_6h  = EXCLUDED.price_momentum_6h,
            price_momentum_24h = EXCLUDED.price_momentum_24h,
            book_imbalance     = EXCLUDED.book_imbalance,
            spread_pct         = EXCLUDED.spread_pct,
            volume_spike       = EXCLUDED.volume_spike,
            ev_yes             = EXCLUDED.ev_yes,
            ev_no              = EXCLUDED.ev_no,
            kelly_fraction     = EXCLUDED.kelly_fraction,
            cross_platform_delta = EXCLUDED.cross_platform_delta,
            resolution_risk    = EXCLUDED.resolution_risk,
            open_interest_delta = EXCLUDED.open_interest_delta,
            news_sentiment     = EXCLUDED.news_sentiment,
            omg_score          = EXCLUDED.omg_score,
            omg_signal         = EXCLUDED.omg_signal,
            computed_at        = EXCLUDED.computed_at"
    )
    .bind(id)
    .bind(event_id)
    .bind(now)
    .bind(vwap)
    .bind(mom_1h)
    .bind(mom_6h)
    .bind(mom_24h)
    .bind(book_imbalance)
    .bind(spread_pct)
    .bind(volume_spike)
    .bind(ev_yes)
    .bind(ev_no)
    .bind(kelly)
    .bind(cross_platform_delta)
    .bind(resolution_risk)
    .bind(open_interest_delta)
    .bind(news_sentiment)
    .bind(omg_score)
    .bind(omg_signal.clone())
    .execute(pool)
    .await?;

    Ok(MarketIndicators {
        id,
        event_id,
        computed_at: now,
        vwap,
        price_momentum_1h: mom_1h,
        price_momentum_6h: mom_6h,
        price_momentum_24h: mom_24h,
        book_imbalance,
        spread_pct,
        volume_spike,
        ev_yes,
        ev_no,
        kelly_fraction: kelly,
        cross_platform_delta,
        resolution_risk,
        open_interest_delta,
        news_sentiment,
        omg_score,
        omg_signal,
    })
}

// ── Indicator 1: VWAP ──────────────────────────────────────────────
// Volume-weighted average price from raw_trades last 24h

async fn compute_vwap(pool: &PgPool, event_id: Uuid) -> anyhow::Result<Option<f64>> {
    let since = Utc::now() - Duration::hours(24);

    let row: Option<(Option<f64>,)> = sqlx::query_as(
        "SELECT
            SUM(price * size) / NULLIF(SUM(size), 0) AS vwap
         FROM public.raw_trades
         WHERE event_id = $1::text
            OR event_id = $2::text
         AND trade_timestamp >= $3",
    )
    .bind(event_id.to_string())
    .bind(format!("polymarket:{}", event_id))
    .bind(since)
    .fetch_optional(pool)
    .await?;

    Ok(row.and_then(|r| r.0))
}

// ── Indicator 2: Price Momentum ────────────────────────────────────
// Rate of change over 1h, 6h, 24h windows from market_history

async fn compute_price_momentum(
    pool: &PgPool,
    event_id: Uuid,
) -> anyhow::Result<(Option<f64>, Option<f64>, Option<f64>)> {
    let now = Utc::now();

    // Get current price and prices at 1h, 6h, 24h ago
    let row: Option<(Option<f64>, Option<f64>, Option<f64>, Option<f64>)> =
        sqlx::query_as(
            "SELECT
                -- Current (most recent)
                (SELECT price FROM public.market_history
                 WHERE event_id = $1 ORDER BY recorded_at DESC LIMIT 1),
                -- 1h ago
                (SELECT price FROM public.market_history
                 WHERE event_id = $1
                   AND recorded_at <= $2 - INTERVAL '1 hour'
                 ORDER BY recorded_at DESC LIMIT 1),
                -- 6h ago
                (SELECT price FROM public.market_history
                 WHERE event_id = $1
                   AND recorded_at <= $2 - INTERVAL '6 hours'
                 ORDER BY recorded_at DESC LIMIT 1),
                -- 24h ago
                (SELECT price FROM public.market_history
                 WHERE event_id = $1
                   AND recorded_at <= $2 - INTERVAL '24 hours'
                 ORDER BY recorded_at DESC LIMIT 1)",
        )
        .bind(event_id)
        .bind(now)
        .fetch_optional(pool)
        .await?;

    let Some((current, p1h, p6h, p24h)) = row else {
        return Ok((None, None, None));
    };

    let momentum = |past: Option<f64>| -> Option<f64> {
        match (current, past) {
            (Some(c), Some(p)) if p > 0.0 => Some((c - p) / p * 100.0),
            _ => None,
        }
    };

    Ok((momentum(p1h), momentum(p6h), momentum(p24h)))
}

// ── Indicators 4 & 5: Book Imbalance + Spread ─────────────────────
// From latest orderbook_snapshots snapshot

async fn compute_book_signals(
    pool: &PgPool,
    event_id: Uuid,
) -> anyhow::Result<(Option<f64>, Option<f64>)> {
    // Get top 5 bid and ask levels from latest snapshot
    let rows: Vec<(String, f64, f64)> = sqlx::query_as(
        "SELECT side, price, size
         FROM public.orderbook_snapshots
         WHERE event_id = $1
            OR event_id = $2
         ORDER BY captured_at DESC, level ASC
         LIMIT 20",
    )
    .bind(event_id.to_string())
    .bind(format!("polymarket:{}", event_id))
    .fetch_all(pool)
    .await?;

    if rows.is_empty() {
        return Ok((None, None));
    }

    let bids: Vec<(f64, f64)> = rows
        .iter()
        .filter(|(s, _, _)| matches!(s.as_str(), "bid" | "buy" | "yes"))
        .map(|(_, p, sz)| (*p, *sz))
        .collect();

    let asks: Vec<(f64, f64)> = rows
        .iter()
        .filter(|(s, _, _)| matches!(s.as_str(), "ask" | "sell" | "no"))
        .map(|(_, p, sz)| (*p, *sz))
        .collect();

    // Book imbalance: (bid_vol - ask_vol) / (bid_vol + ask_vol)
    // Range: -1.0 (all asks) to +1.0 (all bids)
    let bid_vol: f64 = bids.iter().map(|(_, sz)| sz).sum();
    let ask_vol: f64 = asks.iter().map(|(_, sz)| sz).sum();
    let total = bid_vol + ask_vol;

    let book_imbalance = if total > 0.0 {
        Some((bid_vol - ask_vol) / total)
    } else {
        None
    };

    // Spread: best ask - best bid, as % of midpoint
    let best_bid = bids.iter().map(|(p, _)| *p).fold(f64::MIN, f64::max);
    let best_ask = asks.iter().map(|(p, _)| *p).fold(f64::MAX, f64::min);

    let spread_pct = if best_bid > 0.0 && best_ask < f64::MAX && best_ask > best_bid {
        let mid = (best_bid + best_ask) / 2.0;
        Some((best_ask - best_bid) / mid * 100.0)
    } else {
        None
    };

    Ok((book_imbalance, spread_pct))
}

// ── Indicator 7: EV + Kelly ────────────────────────────────────────
// Uses current market price and treats 50% as AI baseline if no signal

async fn compute_ev_kelly(
    pool: &PgPool,
    event_id: Uuid,
) -> anyhow::Result<(Option<f64>, Option<f64>, Option<f64>)> {
    // Get current price from prediction_events
    let row: Option<(f64, Option<f64>)> = sqlx::query_as(
        "SELECT odds, sentiment_score
         FROM public.prediction_events
         WHERE id = $1
         LIMIT 1",
    )
    .bind(event_id)
    .fetch_optional(pool)
    .await?;

    let Some((market_price, _sentiment)) = row else {
        return Ok((None, None, None));
    };

    let market_price = market_price.clamp(0.001, 0.999);

    // Check if there's an existing trade signal with an AI probability
    let ai_prob_row: Option<(f64,)> = sqlx::query_as(
        "SELECT ai_probability
         FROM public.trade_signals
         WHERE event_id = $1::text
         ORDER BY created_at DESC
         LIMIT 1",
    )
    .bind(event_id.to_string())
    .fetch_optional(pool)
    .await?;

    // Use most recent AI probability, or fall back to 50% (neutral)
    let ai_prob = ai_prob_row
        .map(|(p,)| p)
        .unwrap_or(0.5)
        .clamp(0.001, 0.999);

    // EV calculation (same as ev_engine.rs)
    let ev_yes = (ai_prob * (1.0 - market_price)) - ((1.0 - ai_prob) * market_price);
    let ev_no = ((1.0 - ai_prob) * market_price) - (ai_prob * (1.0 - market_price));

    // Kelly fraction for the better side
    let b_yes = (1.0 - market_price) / market_price;
    let kelly_yes = ((ai_prob * b_yes) - (1.0 - ai_prob)) / b_yes;
    let b_no = market_price / (1.0 - market_price);
    let kelly_no = (((1.0 - ai_prob) * b_no) - ai_prob) / b_no;
    let kelly = kelly_yes.max(kelly_no).clamp(0.0, 0.25) / 4.0; // quarter kelly

    Ok((Some(ev_yes), Some(ev_no), Some(kelly)))
}

// ── Indicator 8: Cross-Platform Delta ─────────────────────────────
// Price difference between Kalshi and Polymarket for same event title

async fn compute_cross_platform_delta(
    pool: &PgPool,
    event_id: Uuid,
) -> anyhow::Result<Option<f64>> {
    // Get this event's title and platform
    let row: Option<(String, String, f64)> = sqlx::query_as(
        "SELECT title, platform, odds
         FROM public.prediction_events
         WHERE id = $1
         LIMIT 1",
    )
    .bind(event_id)
    .fetch_optional(pool)
    .await?;

    let Some((title, platform, this_price)) = row else {
        return Ok(None);
    };

    // Find the same event on the other platform by title similarity
    let other_platform = if platform.to_lowercase() == "kalshi" {
        "Polymarket"
    } else {
        "Kalshi"
    };

    // Try pg_trgm similarity first, fall back to LIKE if extension not enabled
    let other: Option<(f64,)> = match sqlx::query_as(
        "SELECT odds
         FROM public.prediction_events
         WHERE platform = $1
           AND LOWER(title) % LOWER($2)
         ORDER BY similarity(LOWER(title), LOWER($2)) DESC,
                  volume_24h DESC NULLS LAST
         LIMIT 1",
    )
    .bind(other_platform)
    .bind(&title)
    .fetch_optional(pool)
    .await
    {
        Ok(result) => result,
        Err(_) => {
            // pg_trgm not available — fall back to keyword LIKE match
            let keywords = title
                .split_whitespace()
                .take(4)
                .collect::<Vec<_>>()
                .join("%");
            sqlx::query_as(
                "SELECT odds
                 FROM public.prediction_events
                 WHERE platform = $1
                   AND LOWER(title) LIKE $2
                 ORDER BY volume_24h DESC NULLS LAST
                 LIMIT 1",
            )
            .bind(other_platform)
            .bind(format!("%{}%", keywords))
            .fetch_optional(pool)
            .await
            .unwrap_or(None)
        }
    };

    Ok(other.map(|(other_price,)| {
        if platform.to_lowercase() == "kalshi" {
            this_price - other_price  // positive = kalshi higher
        } else {
            other_price - this_price  // positive = kalshi higher
        }
    }))
}

// ── Indicator 9: Resolution Risk Score ────────────────────────────
// 0.0 = low risk, 1.0 = high risk
// Factors: days to resolution, category risk, price extremity

async fn compute_resolution_risk(
    pool: &PgPool,
    event_id: Uuid,
) -> anyhow::Result<Option<f64>> {
    let row: Option<(f64, Option<String>, Option<DateTime<Utc>>)> = sqlx::query_as(
        "SELECT odds, category, end_date
         FROM public.prediction_events
         WHERE id = $1
         LIMIT 1",
    )
    .bind(event_id)
    .fetch_optional(pool)
    .await?;

    let Some((price, category, end_date)) = row else {
        return Ok(None);
    };

    let mut risk: f64 = 0.0;

    // Factor 1: Days to resolution (closer = higher risk of surprise)
    if let Some(end) = end_date {
        let days_left = (end - Utc::now()).num_days();
        let time_risk = match days_left {
            d if d <= 1  => 0.40,  // resolves tomorrow — very risky
            d if d <= 7  => 0.30,  // this week
            d if d <= 30 => 0.20,  // this month
            d if d <= 90 => 0.10,  // this quarter
            _            => 0.05,  // far future
        };
        risk += time_risk;
    } else {
        risk += 0.15; // unknown end date = moderate risk
    }

    // Factor 2: Category risk profile
    let cat_lower = category.as_deref().unwrap_or("").to_lowercase();
    let category_risk = if cat_lower.contains("crypto") || cat_lower.contains("bitcoin") {
        0.35 // crypto = high volatility
    } else if cat_lower.contains("election") || cat_lower.contains("politic") {
        0.30 // elections = high uncertainty
    } else if cat_lower.contains("sports") || cat_lower.contains("nba") || cat_lower.contains("nfl") {
        0.20 // sports = moderate
    } else if cat_lower.contains("econ") || cat_lower.contains("fed") || cat_lower.contains("rate") {
        0.25 // economic = moderate-high
    } else {
        0.15 // general
    };
    risk += category_risk;

    // Factor 3: Price extremity (very cheap or very expensive = riskier)
    // Markets at 5¢ or 95¢ have high resolution risk (binary cliff)
    let price_risk = if price < 0.05 || price > 0.95 {
        0.25
    } else if price < 0.10 || price > 0.90 {
        0.15
    } else if price < 0.20 || price > 0.80 {
        0.08
    } else {
        0.02
    };
    risk += price_risk;

    Ok(Some(risk.min(1.0)))
}

// ── Indicator 11: Open Interest Delta ─────────────────────────────
// % change in volume_24h vs 24h ago (proxy for OI since we don't
// have direct OI tracking yet)

async fn compute_open_interest_delta(
    pool: &PgPool,
    event_id: Uuid,
) -> anyhow::Result<Option<f64>> {
    // Use volume_24h from prediction_events as direct source
    let vol_row: Option<(Option<f64>,)> = sqlx::query_as(
        "SELECT volume_24h FROM public.prediction_events WHERE id = $1",
    )
    .bind(event_id)
    .fetch_optional(pool)
    .await?;

    // Get yesterday's volume from market_history snapshots
    // volume_24h is written into market_history as volume_24h column
    let hist_row: Option<(Option<f64>,)> = sqlx::query_as(
        "SELECT volume_24h
         FROM public.market_history
         WHERE event_id = $1
           AND recorded_at <= NOW() - INTERVAL '24 hours'
         ORDER BY recorded_at DESC
         LIMIT 1",
    )
    .bind(event_id)
    .fetch_optional(pool)
    .await?;

    let current_vol = vol_row.and_then(|(v,)| v);
    let past_vol = hist_row.and_then(|(v,)| v);

    match (current_vol, past_vol) {
        (Some(curr), Some(past)) if past > 0.0 => {
            Ok(Some((curr - past) / past * 100.0))
        }
        _ => Ok(None),
    }
}

// ── Volume Spike (from alpha_signals) ─────────────────────────────

async fn compute_volume_spike(
    pool: &PgPool,
    event_id: Uuid,
) -> anyhow::Result<Option<f64>> {
    let row: Option<(f64,)> = sqlx::query_as(
        "SELECT magnitude
         FROM public.alpha_signals
         WHERE event_id = $1::text
           AND signal_type = 'volume_spike'
           AND created_at >= NOW() - INTERVAL '2 hours'
         ORDER BY created_at DESC, magnitude DESC
         LIMIT 1",
    )
    .bind(event_id.to_string())
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|(m,)| m))
}

// ── News Sentiment ─────────────────────────────────────────────────

async fn fetch_news_sentiment(
    pool: &PgPool,
    event_id: Uuid,
) -> anyhow::Result<Option<f64>> {
    let row: Option<(Option<f64>,)> = sqlx::query_as(
        "SELECT sentiment_score
         FROM public.prediction_events
         WHERE id = $1",
    )
    .bind(event_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.and_then(|(s,)| s))
}

// ── OMG Composite Score ────────────────────────────────────────────
// Weighted combination of all signals → 0 to 100
// 50 = neutral, >65 = bullish, <35 = bearish

fn compute_omg_composite(
    ev_yes: Option<f64>,
    kelly: Option<f64>,
    book_imbalance: Option<f64>,
    price_momentum_24h: Option<f64>,
    volume_spike: Option<f64>,
    cross_platform_delta: Option<f64>,
    news_sentiment: Option<f64>,
    resolution_risk: Option<f64>,
    spread_pct: Option<f64>,
) -> Option<f64> {
    // Weights — must sum to 1.0
    // EV and Kelly are the most reliable signals for prediction markets
    const W_EV:        f64 = 0.25;
    const W_KELLY:     f64 = 0.15;
    const W_BOOK:      f64 = 0.15;
    const W_MOMENTUM:  f64 = 0.12;
    const W_VOLUME:    f64 = 0.10;
    const W_CROSS:     f64 = 0.08;
    const W_SENTIMENT: f64 = 0.08;
    const W_RISK:      f64 = 0.05; // inverted (lower risk = higher score)
    const W_SPREAD:    f64 = 0.02; // inverted (tighter spread = higher score)

    let mut score = 0.0f64;
    let mut total_weight = 0.0f64;

    // EV: normalize from [-1, 1] → [0, 100]
    if let Some(ev) = ev_yes {
        let normalized = ((ev + 1.0) / 2.0 * 100.0).clamp(0.0, 100.0);
        score += normalized * W_EV;
        total_weight += W_EV;
    }

    // Kelly: normalize from [0, 0.25] → [0, 100]
    if let Some(k) = kelly {
        let normalized = (k / 0.25 * 100.0).clamp(0.0, 100.0);
        // Kelly > 0 means positive edge — shift to 50+ range
        let shifted = 50.0 + normalized / 2.0;
        score += shifted * W_KELLY;
        total_weight += W_KELLY;
    }

    // Book imbalance: from [-1, 1] → [0, 100]
    if let Some(bi) = book_imbalance {
        let normalized = ((bi + 1.0) / 2.0 * 100.0).clamp(0.0, 100.0);
        score += normalized * W_BOOK;
        total_weight += W_BOOK;
    }

    // Price momentum: from [-50%, +50%] → [0, 100]
    if let Some(mom) = price_momentum_24h {
        let normalized = ((mom + 50.0) / 100.0 * 100.0).clamp(0.0, 100.0);
        score += normalized * W_MOMENTUM;
        total_weight += W_MOMENTUM;
    }

    // Volume spike: >2x = bullish signal, normalize [0, 5x] → [50, 100]
    if let Some(vs) = volume_spike {
        let normalized = if vs >= 1.0 {
            (50.0 + (vs - 1.0) / 4.0 * 50.0).min(100.0)
        } else {
            50.0
        };
        score += normalized * W_VOLUME;
        total_weight += W_VOLUME;
    }

    // Cross-platform delta: small delta = fair price, large = arb opportunity
    // |delta| > 5 cents is significant
    if let Some(cpd) = cross_platform_delta {
        // Positive delta (kalshi higher) = slight sell signal for kalshi
        // We normalize to 50 = parity, above/below = directional signal
        let normalized = (50.0 - cpd * 100.0).clamp(0.0, 100.0);
        score += normalized * W_CROSS;
        total_weight += W_CROSS;
    }

    // News sentiment: already [-1, 1] → [0, 100]
    if let Some(ns) = news_sentiment {
        let normalized = ((ns + 1.0) / 2.0 * 100.0).clamp(0.0, 100.0);
        score += normalized * W_SENTIMENT;
        total_weight += W_SENTIMENT;
    }

    // Resolution risk: INVERTED — lower risk = higher score
    if let Some(rr) = resolution_risk {
        let normalized = ((1.0 - rr) * 100.0).clamp(0.0, 100.0);
        score += normalized * W_RISK;
        total_weight += W_RISK;
    }

    // Spread: INVERTED — tighter spread = higher score (more liquid)
    if let Some(sp) = spread_pct {
        let normalized = ((1.0 - (sp / 20.0).min(1.0)) * 100.0).clamp(0.0, 100.0);
        score += normalized * W_SPREAD;
        total_weight += W_SPREAD;
    }

    if total_weight < 0.10 {
        // Not enough signals to compute a meaningful composite
        return None;
    }

    // Normalize by actual weight used (handles missing signals gracefully)
    Some((score / total_weight).clamp(0.0, 100.0))
}

// ── UUID resolver ──────────────────────────────────────────────────

async fn parse_or_lookup_uuid(pool: &PgPool, param: &str) -> Option<Uuid> {
    if let Ok(uid) = Uuid::parse_str(param) {
        return Some(uid);
    }
    // Try external_id lookup
    sqlx::query_as::<_, (Uuid,)>(
        "SELECT id FROM public.prediction_events WHERE external_id = $1 LIMIT 1",
    )
    .bind(param)
    .fetch_optional(pool)
    .await
    .ok()
    .flatten()
    .map(|(id,)| id)
}

// ── Background loop ────────────────────────────────────────────────

pub async fn run_indicator_loop(pool: PgPool) {
    tracing::info!("📊 Starting OMG indicator computation loop (60s interval)");

    let mut interval = tokio::time::interval(time::Duration::from_secs(60));

    loop {
        interval.tick().await;

        // Get active markets with recent price activity
        let active: Vec<(Uuid,)> = match sqlx::query_as(
            "SELECT id
             FROM public.prediction_events
             WHERE updated_at > NOW() - INTERVAL '2 hours'
               AND COALESCE(status, 'open') NOT IN ('closed', 'resolved', 'expired')
               AND volume_24h > 0
             ORDER BY volume_24h DESC NULLS LAST
             LIMIT 500",
        )
        .fetch_all(&pool)
        .await
        {
            Ok(rows) => rows,
            Err(e) => {
                tracing::error!("Indicator loop: failed to load markets: {}", e);
                continue;
            }
        };

        if active.is_empty() {
            tracing::debug!("Indicator loop: no active markets");
            continue;
        }

        tracing::info!("📊 Computing indicators for {} markets", active.len());

        let mut computed = 0usize;
        let mut errors = 0usize;

        for (event_id,) in &active {
            match compute_and_persist(&pool, *event_id).await {
                Ok(_) => computed += 1,
                Err(e) => {
                    errors += 1;
                    if errors <= 5 {
                        tracing::warn!("Indicator compute failed for {}: {}", event_id, e);
                    }
                }
            }
            // Small delay to avoid DB saturation
            tokio::time::sleep(time::Duration::from_millis(50)).await;
        }

        tracing::info!(
            "✅ Indicators computed: {} success, {} errors",
            computed, errors
        );
    }
}