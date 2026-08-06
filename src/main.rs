mod models;
mod fetcher;
mod strategy;
mod user_bots;
mod trades;
mod alpha;
mod ev_engine;
mod endpoints;
mod market_history;
mod fetchers;
mod orderbook;
mod indicators;
mod live_stats;
mod strategy_signals;
mod contract_parser;
mod player_stats;
mod live_prices;
mod arb_detector;
mod lead_lag;
mod lead_lag_detector;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::time::Duration;
use crate::fetcher::MarketFetcher;
use std::env;
use std::str::FromStr;
use dotenv::dotenv;
use reqwest::header::{HeaderMap, HeaderValue};
use axum::{routing::{get, patch}, extract::{State, Query, Path}, Json, Router};
use tower_http::cors::CorsLayer;
use tower_http::compression::CompressionLayer;
use serde::{Serialize, Deserialize};
use axum::http::StatusCode;
use axum::response::IntoResponse;

#[derive(Serialize, sqlx::FromRow)]
struct PredictionEvent {
    id: uuid::Uuid,
    title: String,
    platform: String,
    odds: f64,
    category: Option<String>,
    status: String,
    icon_url: Option<String>,
    external_id: String,
    volume_24h: Option<f64>,
    updated_at: Option<chrono::DateTime<chrono::Utc>>,
    outcomes: Option<serde_json::Value>,
    market_url: Option<String>,
    end_date: Option<chrono::DateTime<chrono::Utc>>,
    rsi_signal: Option<f64>,
    sentiment_score: Option<f64>,
    clob_token_yes: Option<String>,
}

#[derive(Serialize, sqlx::FromRow)]
struct IndexHistoryEntry {
    value: f64,
    market_count: i32,
    timestamp: chrono::DateTime<chrono::Utc>,
}

#[derive(Deserialize)]
struct BacktestParams {
    rsi: f64,
    days: i32,
}

#[derive(Deserialize)]
struct PatchIconBody {
    icon_url: String,
}

#[derive(serde::Deserialize, Default)]
struct PredictionEventsQuery {
    live: Option<bool>,
}

async fn get_predictions(
    State(pool): State<sqlx::PgPool>,
    Query(params): Query<PredictionEventsQuery>,
) -> Json<Vec<PredictionEvent>> {
    let live_only = params.live.unwrap_or(false);
    let rows = sqlx::query_as::<_, PredictionEvent>(
        "SELECT id, title, platform, odds, category, status, icon_url, external_id,
                volume_24h, updated_at, outcomes, market_url, end_date,
                rsi_signal, sentiment_score, clob_token_yes
         FROM public.prediction_events
         WHERE status IN ('active', 'determined')
           AND (end_date IS NULL OR end_date > NOW() - INTERVAL '24 hours')
           AND odds > 0.02
           AND odds < 0.98
           AND (
             $1::boolean = false
             OR (
               category = 'Sports'
               AND updated_at > NOW() - INTERVAL '5 minutes'
             )
           )
         ORDER BY
           CASE WHEN status = 'active' THEN 0 ELSE 1 END,
           CASE WHEN updated_at > NOW() - INTERVAL '5 minutes' THEN 0 ELSE 1 END,
           volume_24h DESC NULLS LAST
         LIMIT 20000"
    )
    .bind(live_only)
    .fetch_all(&pool)
    .await
    .unwrap_or_else(|e| {
        println!("❌ GET /prediction_events query failed: {}", e);
        vec![]
    });
    println!("📤 GET /prediction_events returning {} rows", rows.len());
    Json(rows)
}

async fn get_backtest(
    State(pool): State<sqlx::PgPool>,
    Query(params): Query<BacktestParams>,
) -> Json<strategy::BacktestResult> {
    let res = strategy::run_backtest(&pool, params.rsi, params.days).await.unwrap_or_else(|e| {
        println!("❌ Backtest error: {}", e);
        strategy::BacktestResult {
            total_trades: 0,
            estimated_profit: 0.0,
            win_rate: 0.0,
        }
    });
    Json(res)
}

async fn get_index_history(State(pool): State<sqlx::PgPool>) -> Json<Vec<IndexHistoryEntry>> {
    let rows = sqlx::query_as::<_, IndexHistoryEntry>(
        "SELECT value, market_count, timestamp FROM public.index_history ORDER BY timestamp DESC LIMIT 200"
    )
    .fetch_all(&pool)
    .await
    .unwrap_or_else(|e| {
        println!("❌ GET /index_history query failed: {}", e);
        vec![]
    });
    Json(rows)
}

async fn patch_event_icon(
    State(pool): State<sqlx::PgPool>,
    Path(id): Path<String>,
    Json(body): Json<PatchIconBody>,
) -> impl IntoResponse {
    let result = sqlx::query(
        "UPDATE prediction_events SET icon_url = $1, updated_at = NOW() WHERE id = $2::uuid"
    )
    .bind(&body.icon_url)
    .bind(&id)
    .execute(&pool)
    .await;

    match result {
        Ok(r) => {
            if r.rows_affected() == 0 {
                (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "Event not found"}))).into_response()
            } else {
                println!("✅ PATCH icon for {} → {}", id, body.icon_url);
                (StatusCode::OK, Json(serde_json::json!({"success": true}))).into_response()
            }
        }
        Err(e) => {
            eprintln!("❌ PATCH icon error: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response()
        }
    }
}

async fn get_arb_signals_handler(
    axum::extract::State(pool): axum::extract::State<sqlx::PgPool>,
) -> impl axum::response::IntoResponse {
    match arb_detector::get_active_arb_signals(&pool).await {
        Ok(signals) => axum::Json(serde_json::json!({ "signals": signals, "count": signals.len() })).into_response(),
        Err(e) => {
            tracing::error!("arb_signals error: {}", e);
            (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "error").into_response()
        }
    }
}

// ── Lead/Lag API Handlers ──────────────────────────────────────────────────

async fn get_lead_lag_signals_handler(
    axum::extract::State(pool): axum::extract::State<sqlx::PgPool>,
) -> impl axum::response::IntoResponse {
    let rows = sqlx::query(
        "SELECT id, leader_ticker, leader_title, leader_pct_move, leader_move_bucket,
                lagger_ticker, lagger_title, lagger_price_w0, lagger_implied_wstar,
                deviation_delta, z_score, signal_strength, relationship_type,
                detected_at, entry_triggered, pnl_dollars
         FROM lead_lag_signals
         ORDER BY detected_at DESC
         LIMIT 100"
    )
    .fetch_all(&pool)
    .await
    .unwrap_or_default();

    use sqlx::Row;
    let signals: Vec<serde_json::Value> = rows.iter().map(|r| serde_json::json!({
        "id": r.try_get::<uuid::Uuid, _>("id").map(|u| u.to_string()).unwrap_or_default(),
        "leader_ticker": r.try_get::<String, _>("leader_ticker").unwrap_or_default(),
        "leader_title": r.try_get::<Option<String>, _>("leader_title").unwrap_or(None),
        "leader_pct_move": r.try_get::<f64, _>("leader_pct_move").unwrap_or(0.0),
        "leader_move_bucket": r.try_get::<Option<String>, _>("leader_move_bucket").unwrap_or(None),
        "lagger_ticker": r.try_get::<String, _>("lagger_ticker").unwrap_or_default(),
        "lagger_title": r.try_get::<Option<String>, _>("lagger_title").unwrap_or(None),
        "lagger_price_w0": r.try_get::<f64, _>("lagger_price_w0").unwrap_or(0.0),
        "lagger_implied_wstar": r.try_get::<f64, _>("lagger_implied_wstar").unwrap_or(0.0),
        "deviation_delta": r.try_get::<f64, _>("deviation_delta").unwrap_or(0.0),
        "z_score": r.try_get::<f64, _>("z_score").unwrap_or(0.0),
        "signal_strength": r.try_get::<String, _>("signal_strength").unwrap_or_default(),
        "relationship_type": r.try_get::<String, _>("relationship_type").unwrap_or_default(),
        "detected_at": r.try_get::<chrono::DateTime<chrono::Utc>, _>("detected_at")
            .map(|t| t.to_rfc3339()).unwrap_or_default(),
        "entry_triggered": r.try_get::<bool, _>("entry_triggered").unwrap_or(false),
        "pnl_dollars": r.try_get::<Option<f64>, _>("pnl_dollars").unwrap_or(None),
    })).collect();

    axum::Json(serde_json::json!({ "signals": signals, "count": signals.len() })).into_response()
}

async fn get_lead_lag_throughput_handler(
    axum::extract::State(pool): axum::extract::State<sqlx::PgPool>,
) -> impl axum::response::IntoResponse {
    let rows = sqlx::query(
        "SELECT scan_at, scan_duration_ms, contracts_scanned, pairs_detected,
                signals_fired, empty_scans_pct, avg_depth
         FROM lead_lag_throughput
         ORDER BY scan_at DESC LIMIT 20"
    )
    .fetch_all(&pool)
    .await
    .unwrap_or_default();

    use sqlx::Row;
    let data: Vec<serde_json::Value> = rows.iter().map(|r| serde_json::json!({
        "scan_at": r.try_get::<chrono::DateTime<chrono::Utc>, _>("scan_at")
            .map(|t| t.to_rfc3339()).unwrap_or_default(),
        "scan_duration_ms": r.try_get::<i64, _>("scan_duration_ms").unwrap_or(0),
        "contracts_scanned": r.try_get::<i32, _>("contracts_scanned").unwrap_or(0),
        "pairs_detected": r.try_get::<i32, _>("pairs_detected").unwrap_or(0),
        "signals_fired": r.try_get::<i32, _>("signals_fired").unwrap_or(0),
        "avg_depth": r.try_get::<f64, _>("avg_depth").unwrap_or(0.0),
    })).collect();

    axum::Json(serde_json::json!({ "throughput": data })).into_response()
}

async fn get_lead_lag_pairs_handler(
    axum::extract::State(pool): axum::extract::State<sqlx::PgPool>,
) -> impl axum::response::IntoResponse {
    let rows = sqlx::query(
        "SELECT COALESCE(leader_series, leader_ticker) AS leader,
                COALESCE(lagger_series, lagger_ticker) AS lagger,
                relationship_type, elasticity::float8, source
         FROM lead_lag_pairs WHERE is_active = TRUE ORDER BY elasticity DESC"
    )
    .fetch_all(&pool)
    .await
    .unwrap_or_default();

    use sqlx::Row;
    let pairs: Vec<serde_json::Value> = rows.iter().map(|r| serde_json::json!({
        "leader": r.try_get::<String, _>("leader").unwrap_or_default(),
        "lagger": r.try_get::<String, _>("lagger").unwrap_or_default(),
        "relationship_type": r.try_get::<String, _>("relationship_type").unwrap_or_default(),
        "elasticity": r.try_get::<f64, _>("elasticity").unwrap_or(0.0),
        "source": r.try_get::<String, _>("source").unwrap_or_default(),
    })).collect();

    axum::Json(serde_json::json!({ "pairs": pairs })).into_response()
}

// ── Lead/Lag Background Loop ───────────────────────────────────────────────

async fn run_lead_lag_loop(pool: sqlx::PgPool) {
    use crate::lead_lag::{upsert_signal, leader_move_bucket, LeadLagSignal, LeadLagDetector};
    use crate::lead_lag_detector::{ContractMeta, RelationshipDetector};
    use chrono::Utc;

    const CADENCE_SECS: u64 = 30;

    tracing::info!("🔗 Lead/lag detection loop started ({}s cadence)", CADENCE_SECS);

    let mut ticker = tokio::time::interval(Duration::from_secs(CADENCE_SECS));

    loop {
        ticker.tick().await;
        let scan_start = Utc::now();

        // Read from prediction_events — already synced by main loop, no extra API calls
        let rows = sqlx::query(
            "SELECT external_id, title, odds, category, end_date, volume_24h
             FROM public.prediction_events
             WHERE platform = 'Kalshi'
               AND status = 'active'
               AND odds > 0.02 AND odds < 0.98
               AND end_date > NOW()
               AND end_date < NOW() + INTERVAL '48 hours'
               AND updated_at > NOW() - INTERVAL '10 minutes'
             ORDER BY volume_24h DESC NULLS LAST
             LIMIT 500"
        )
        .fetch_all(&pool)
        .await
        .unwrap_or_default();

        use sqlx::Row;
        let contracts: Vec<ContractMeta> = rows.iter().filter_map(|r| {
            let external_id: String = r.try_get("external_id").ok()?;
            let ticker_str = external_id.replace("kalshi:", "");
            let odds: f64 = r.try_get("odds").unwrap_or(0.0);
            if odds <= 0.01 { return None; }
            let close_time = r.try_get::<Option<chrono::DateTime<Utc>>, _>("end_date")
                .unwrap_or(None);

            Some(ContractMeta::from_api(
                &ticker_str,
                &ticker_str,
                r.try_get("title").ok(),
                None,
                r.try_get("category").ok(),
                close_time,
                odds,
                r.try_get("volume_24h").unwrap_or(0.0),
                2000.0, // depth placeholder
                0.04,   // spread placeholder
            ))
        }).collect();

        if contracts.is_empty() {
            tracing::debug!("Lead/lag: no live contracts from DB");
            continue;
        }

        let (pairs, stats) = RelationshipDetector::detect_all(&contracts);
        let scan_duration_ms = (Utc::now() - scan_start).num_milliseconds();

        let mut signals_fired = 0i32;
        let mut blocked_depth = 0i32;
        let mut blocked_z = 0i32;

        for pair in &pairs {
            // Get recent price history for σ
            let recent: Vec<f64> = sqlx::query_scalar(
                "SELECT odds FROM public.market_history
                 WHERE external_id = $1
                 ORDER BY recorded_at DESC LIMIT 20"
            )
            .bind(format!("kalshi:{}", pair.lagger.ticker))
            .fetch_all(&pool)
            .await
            .unwrap_or_default();

            // Previous leader price for before/after comparison
            let leader_price_before: f64 = sqlx::query_scalar(
                "SELECT odds FROM public.market_history
                 WHERE external_id = $1
                 ORDER BY recorded_at DESC LIMIT 1 OFFSET 1"
            )
            .bind(format!("kalshi:{}", pair.leader.ticker))
            .fetch_optional(&pool)
            .await
            .unwrap_or(None)
            .unwrap_or(pair.leader.yes_price * 0.92);

            let lagger_bid = pair.lagger.yes_price - pair.lagger.spread / 2.0;
            let lagger_ask = pair.lagger.yes_price + pair.lagger.spread / 2.0;
            let secs_to_close = pair.lagger.close_time
                .map(|ct| (ct - Utc::now()).num_seconds())
                .unwrap_or(3600);

            match LeadLagDetector::detect(
                &pair.leader.ticker,
                pair.leader.title.as_deref().unwrap_or(&pair.leader.ticker),
                leader_price_before,
                pair.leader.yes_price,
                &pair.lagger.ticker,
                pair.lagger.title.as_deref().unwrap_or(&pair.lagger.ticker),
                pair.lagger.yes_price,
                &recent,
                pair.lagger.depth_dollars,
                lagger_bid,
                lagger_ask,
                secs_to_close,
                &pair.relationship_type,
            ) {
                Some(ll_pair) => {
                    signals_fired += 1;
                    let _bucket = leader_move_bucket(ll_pair.leader_pct_move);
                    let signal = LeadLagSignal {
                        id: uuid::Uuid::new_v4(),
                        pair: ll_pair,
                        entry_triggered: false,
                        entry_blocked_reason: None,
                        entry_price: None,
                        entry_at: None,
                        position_size_dollars: None,
                        exit_price: None,
                        exit_at: None,
                        exit_reason: None,
                        convergence_achieved_pct: None,
                        pnl_dollars: None,
                        pnl_vs_model_optimal: None,
                        entry_latency_ms: None,
                        exit_latency_vs_optimal_ms: None,
                    };
                    if let Err(e) = upsert_signal(&pool, &signal).await {
                        tracing::warn!("lead_lag upsert: {}", e);
                    }
                }
                None => {
                    if pair.lagger.depth_dollars < 1500.0 { blocked_depth += 1; }
                    else { blocked_z += 1; }
                }
            }
        }

        // Log throughput
        let _ = sqlx::query(
            "INSERT INTO lead_lag_throughput (
                scan_at, scan_duration_ms, scan_cadence_seconds,
                contracts_scanned, pairs_evaluated, pairs_detected, signals_fired,
                blocked_by_depth, blocked_by_z,
                signals_by_relationship, signals_by_sport,
                avg_depth
             ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)"
        )
        .bind(stats.scan_at)
        .bind(scan_duration_ms)
        .bind(CADENCE_SECS as i32)
        .bind(contracts.len() as i32)
        .bind(stats.pairs_evaluated as i32)
        .bind(stats.pairs_detected as i32)
        .bind(signals_fired)
        .bind(blocked_depth)
        .bind(blocked_z)
        .bind(serde_json::to_value(&stats.signals_by_relationship).unwrap_or_default())
        .bind(serde_json::to_value(&stats.signals_by_sport).unwrap_or_default())
        .bind(stats.avg_depth)
        .execute(&pool)
        .await;

        tracing::info!(
            "🔗 Lead/lag: {} contracts → {} pairs → {} signals | {}ms",
            contracts.len(), pairs.len(), signals_fired, scan_duration_ms
        );
    }
}

// ── Main ───────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = dotenv();
    tracing_subscriber::fmt::init();

    let database_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL environment variable must be set");

    let connect_options = PgConnectOptions::from_str(&database_url)?
        .statement_cache_capacity(0);

    let api_pool = PgPoolOptions::new()
        .max_connections(25)
        .acquire_timeout(Duration::from_secs(5))
        .connect_with(connect_options.clone())
        .await?;

    let sync_pool = PgPoolOptions::new()
        .max_connections(15)
        .acquire_timeout(Duration::from_secs(15))
        .connect_with(connect_options)
        .await?;

    println!("✅ Connected to database (dual pool: 25 API + 15 sync)");

    let app = Router::new()
        .route("/prediction_events", get(get_predictions))
        .route("/arb_signals", get(get_arb_signals_handler))
        .route("/prediction_events/:id/icon", patch(patch_event_icon))
        .route("/index_history", get(get_index_history))
        .route("/backtest", get(get_backtest))
        .route("/lead_lag/signals",    get(get_lead_lag_signals_handler))
        .route("/lead_lag/throughput", get(get_lead_lag_throughput_handler))
        .route("/lead_lag/pairs",      get(get_lead_lag_pairs_handler))
        .merge(endpoints::alpha_routes())
        .merge(market_history::routes())
        .merge(indicators::routes())
        .merge(live_stats::routes())
        .merge(strategy_signals::routes())
        .merge(player_stats::routes())
        .merge(live_prices::routes())
        .nest("/book", orderbook::routes())
        .layer(CompressionLayer::new())
        .layer(CorsLayer::permissive())
        .with_state(api_pool.clone());

    // ── Background workers ─────────────────────────────────────────────────
    let trade_pool = api_pool.clone();
    tokio::spawn(trades::run_trade_ingestion_loop(trade_pool, reqwest::Client::new()));
    tokio::spawn(alpha::run_alpha_detection_loop(api_pool.clone()));
    tokio::spawn(indicators::run_indicator_loop(api_pool.clone()));
    tokio::spawn(market_history::run_poly_history_loop(api_pool.clone()));
    tokio::spawn(live_stats::run_live_stats_loop(api_pool.clone()));
    tokio::spawn(strategy_signals::run_strategy_signal_loop(api_pool.clone()));
    tokio::spawn(fetchers::polymarket_clob::run_polymarket_clob_loop(api_pool.clone()));

    // ── Lead/Lag detection loop ────────────────────────────────────────────
    tokio::spawn(run_lead_lag_loop(api_pool.clone()));

    // ── Fast Kalshi Price Refresh ──────────────────────────────────────────
    {
        let fast_pool = api_pool.clone();
        let fast_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .user_agent("Britespeck/1.0")
            .build()
            .unwrap();

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(15)).await;

                let top_markets: Vec<(uuid::Uuid, String)> = sqlx::query_as(
                    "SELECT id, external_id FROM public.prediction_events
                     WHERE platform = 'Kalshi' AND status = 'active'
                     AND (end_date IS NULL OR end_date > NOW())
                     ORDER BY volume_24h DESC NULLS LAST LIMIT 200"
                )
                .fetch_all(&fast_pool)
                .await
                .unwrap_or_default();

                let mut updated = 0u32;

                for (id, external_id) in &top_markets {
                    let ticker_str = external_id.replace("kalshi:", "");
                    let url = format!(
                        "https://api.elections.kalshi.com/trade-api/v2/markets/{}",
                        ticker_str
                    );

                    if let Ok(resp) = fast_client.get(&url).send().await {
                        if let Ok(data) = resp.json::<serde_json::Value>().await {
                            let yes_bid = data.get("market")
                                .and_then(|m| m.get("yes_bid_dollars").or_else(|| m.get("yes_bid")))
                                .and_then(|v| {
                                    if let Some(s) = v.as_str() {
                                        s.parse::<f64>().ok()
                                    } else {
                                        v.as_f64().map(|p| if p > 1.0 { p / 100.0 } else { p })
                                    }
                                });

                            if let Some(price) = yes_bid {
                                if price > 0.01 && price < 0.99 {
                                    sqlx::query(
                                        "UPDATE public.prediction_events
                                         SET odds = $1, updated_at = NOW()
                                         WHERE id = $2"
                                    )
                                    .bind(price)
                                    .bind(id)
                                    .execute(&fast_pool)
                                    .await
                                    .ok();

                                    crate::live_prices::publish_price_update(
                                        external_id, external_id, "Kalshi", price
                                    );
                                    updated += 1;
                                }
                            }
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                tracing::info!("⚡ Fast Kalshi refresh: {} markets updated", updated);
            }
        });
    }

    // ── Kalshi WebSocket ───────────────────────────────────────────────────
    let kalshi_pool = api_pool.clone();
    tokio::spawn(async move {
        loop {
            let kalshi_tickers: Vec<String> = sqlx::query_scalar(
                "SELECT REPLACE(external_id, 'kalshi:', '') FROM (
                   SELECT DISTINCT ON (external_id) external_id, volume_24h
                   FROM prediction_events
                   WHERE platform = 'Kalshi'
                     AND external_id IS NOT NULL
                     AND status = 'active'
                   ORDER BY external_id, volume_24h DESC NULLS LAST
                 ) t
                 ORDER BY volume_24h DESC NULLS LAST LIMIT 200"
            )
            .fetch_all(&kalshi_pool)
            .await
            .unwrap_or_else(|e| {
                eprintln!("⚠️  kalshi_ws ticker fetch failed: {e}");
                vec![]
            });

            if kalshi_tickers.is_empty() {
                eprintln!("⚠️  kalshi_ws: no tickers yet, retrying in 30s");
                tokio::time::sleep(Duration::from_secs(30)).await;
                continue;
            }

            println!("🟢 kalshi_ws: connecting with global subscription");
            if let Err(e) = fetchers::kalshi_ws::run_kalshi_ws_loop(
                kalshi_pool.clone(),
                vec![],
            ).await {
                eprintln!("❌ kalshi_ws ended: {e} — reconnecting in 5s");
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });

    // ── Main REST sync loop ────────────────────────────────────────────────
    tokio::spawn(async move {
        let fetcher = MarketFetcher::new();
        let mut kalshi_headers = HeaderMap::new();
        if let Ok(token) = env::var("KALSHI_API_TOKEN") {
            kalshi_headers.insert(
                "Authorization",
                HeaderValue::from_str(&format!("Bearer {}", token)).unwrap(),
            );
        }
        kalshi_headers.insert("Accept", HeaderValue::from_static("application/json"));
        kalshi_headers.insert("User-Agent", HeaderValue::from_static("Mozilla/5.0"));
        let kalshi_client = reqwest::Client::builder()
            .default_headers(kalshi_headers)
            .build()
            .unwrap();

        let mut poly_headers = HeaderMap::new();
        poly_headers.insert("Accept", HeaderValue::from_static("application/json"));
        poly_headers.insert("User-Agent", HeaderValue::from_static("Mozilla/5.0"));
        let poly_client = reqwest::Client::builder()
            .default_headers(poly_headers)
            .build()
            .unwrap();

        println!("🚀 Britespeck sync engine started");

        loop {
            println!("\n🔄 Starting sync cycle...");
            let events = fetcher.fetch_all(&kalshi_client, &poly_client).await;

            if !events.is_empty() {
                let mut ids = Vec::new();
                let mut titles = Vec::new();
                let mut platforms = Vec::new();
                let mut odds = Vec::new();
                let mut categories = Vec::new();
                let mut statuses = Vec::new();
                let mut icons = Vec::new();
                let mut externals = Vec::new();
                let mut volumes: Vec<f64> = Vec::new();
                let mut outcomes = Vec::new();
                let mut urls = Vec::new();
                let mut ends = Vec::new();
                let mut clob_tokens: Vec<Option<String>> = Vec::new();

                for e in &events {
                    ids.push(e.id);
                    titles.push(e.title.clone());
                    platforms.push(e.platform.clone());
                    odds.push(e.odds);
                    categories.push(e.category.clone());
                    statuses.push(e.status.clone());
                    icons.push(e.icon_url.clone());
                    externals.push(e.external_id.clone());
                    volumes.push(e.volume_24h);
                    outcomes.push(
                        serde_json::to_value(&e.outcomes)
                            .unwrap_or(serde_json::Value::Null),
                    );
                    urls.push(e.market_url.clone());
                    ends.push(e.end_date);
                    clob_tokens.push(e.clob_token_yes.clone());
                }

                let result = sqlx::query(
                    r#"
                    INSERT INTO public.prediction_events
                    (id, title, platform, odds, category, status, icon_url, external_id,
                     volume_24h, updated_at, outcomes, market_url, end_date, clob_token_yes)
                    SELECT * FROM UNNEST(
                        $1::uuid[], $2::text[], $3::text[], $4::float8[], $5::text[],
                        $6::text[], $7::text[], $8::text[], $9::float8[], $10::timestamptz[],
                        $11::jsonb[], $12::text[], $13::timestamptz[], $14::text[]
                    )
                    ON CONFLICT (external_id) DO UPDATE SET
                        odds = EXCLUDED.odds,
                        volume_24h = EXCLUDED.volume_24h,
                        updated_at = EXCLUDED.updated_at,
                        outcomes = EXCLUDED.outcomes,
                        icon_url = COALESCE(EXCLUDED.icon_url, public.prediction_events.icon_url),
                        market_url = COALESCE(EXCLUDED.market_url, public.prediction_events.market_url),
                        category = COALESCE(EXCLUDED.category, public.prediction_events.category),
                        clob_token_yes = COALESCE(EXCLUDED.clob_token_yes, public.prediction_events.clob_token_yes),
                        status = CASE
                            WHEN public.prediction_events.status = 'closed' THEN 'closed'
                            ELSE EXCLUDED.status
                        END
                    "#,
                )
                .bind(&ids)
                .bind(&titles)
                .bind(&platforms)
                .bind(&odds)
                .bind(&categories)
                .bind(&statuses)
                .bind(&icons)
                .bind(&externals)
                .bind(&volumes)
                .bind(&vec![chrono::Utc::now(); events.len()])
                .bind(&outcomes)
                .bind(&urls)
                .bind(&ends)
                .bind(&clob_tokens)
                .execute(&sync_pool)
                .await;

                match result {
                    Ok(res) => println!("💾 Batch persisted {} events", res.rows_affected()),
                    Err(e) => eprintln!("❌ Batch upsert failed: {}", e),
                }

                sqlx::query("UPDATE public.prediction_events SET status = 'closed' WHERE status = 'active' AND end_date < NOW()")
                    .execute(&sync_pool).await.ok();
                sqlx::query("UPDATE public.prediction_events SET status = 'closed' WHERE status = 'active' AND (odds > 0.99 OR odds < 0.01)")
                    .execute(&sync_pool).await.ok();
                sqlx::query("UPDATE public.prediction_events SET status = 'closed' WHERE status = 'active' AND volume_24h < 1 AND end_date IS NOT NULL AND end_date < NOW() + INTERVAL '1 hour'")
                    .execute(&sync_pool).await.ok();
                sqlx::query("UPDATE public.prediction_events SET status = 'closed' WHERE status = 'active' AND (odds >= 0.97 OR odds <= 0.03) AND updated_at < NOW() - INTERVAL '1 hour'")
                    .execute(&sync_pool).await.ok();

                sqlx::query("DELETE FROM public.alpha_signals WHERE created_at < NOW() - INTERVAL '6 hours'")
                    .execute(&sync_pool).await.ok();
                sqlx::query("DELETE FROM public.market_indicators WHERE computed_at < NOW() - INTERVAL '24 hours'")
                    .execute(&sync_pool).await.ok();
                sqlx::query("DELETE FROM public.orderbook_snapshots WHERE captured_at < NOW() - INTERVAL '2 hours'")
                    .execute(&sync_pool).await.ok();
                sqlx::query("DELETE FROM public.market_history WHERE recorded_at < NOW() - INTERVAL '7 days'")
                    .execute(&sync_pool).await.ok();
                sqlx::query("DELETE FROM public.raw_trades WHERE ingested_at < NOW() - INTERVAL '30 days'")
                    .execute(&sync_pool).await.ok();

                match market_history::write_snapshots(
                    &sync_pool,
                    &ids,
                    &titles,
                    &platforms,
                    &odds,
                    volumes.as_slice(),
                )
                .await
                {
                    Ok(n) => println!("📈 Wrote {} market_history snapshots", n),
                    Err(e) => eprintln!("⚠️ Snapshot write failed: {}", e),
                }
            }

            if let Err(e) = strategy::run_omg_strategy(&sync_pool).await {
                println!("⚠️ OMG Strategy Warning: {}", e);
            }

            println!("💤 Sleeping 15s...");
            tokio::time::sleep(Duration::from_secs(15)).await;
        }
    });

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    println!("📡 Britespeck API listening on port 8080");
    axum::serve(listener, app).await?;

    Ok(())
}
// force cache bust Fri May 15 16:14:03 EDT 2026