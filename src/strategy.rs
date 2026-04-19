use serde::{Deserialize, Serialize};
use sqlx::{Error, PgPool, Row};

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct BacktestResult {
    pub total_trades: i64,
    pub estimated_profit: f64,
    pub win_rate: f64,
}

#[derive(Debug, sqlx::FromRow)]
struct SignalRow {
    title: String,
    #[allow(dead_code)]
    odds: f64,
    rsi_signal: Option<f64>,
}

/// Run the OMG strategy across every active user bot.
/// - If `event_id` is set on the bot, scope to that single market.
/// - Otherwise scan top markets on the bot's chosen platform.
pub async fn run_omg_strategy(pool: &PgPool) -> Result<(), Error> {
    let active_bots = sqlx::query(
        "SELECT user_id, platform, event_id, max_trade_amount, \
                min_rsi_threshold, rsi_sell_threshold \
         FROM public.user_bots \
         WHERE is_active = true"
    )
    .fetch_all(pool)
    .await?;

    for bot in active_bots {
        let user_id: uuid::Uuid = bot.get("user_id");
        let platform: String = bot.get("platform");
        let event_id: Option<String> = bot.try_get("event_id").ok();
        let buy_threshold: f64 = bot.get("min_rsi_threshold");
        let sell_threshold: f64 = bot.try_get("rsi_sell_threshold").unwrap_or(70.0);

        // Scoped query: single event if set, else scan top markets on platform
        let signals_result = if let Some(ref eid) = event_id {
            sqlx::query_as::<_, SignalRow>(
                "SELECT title, odds, rsi_signal \
                 FROM public.prediction_events \
                 WHERE external_id = $1 AND platform = $2 \
                   AND rsi_signal IS NOT NULL \
                   AND (rsi_signal <= $3 OR rsi_signal >= $4) \
                 LIMIT 1"
            )
            .bind(eid)
            .bind(&platform)
            .bind(buy_threshold)
            .bind(sell_threshold)
            .fetch_all(pool)
            .await
        } else {
            sqlx::query_as::<_, SignalRow>(
                "SELECT title, odds, rsi_signal \
                 FROM public.prediction_events \
                 WHERE platform = $1 \
                   AND rsi_signal IS NOT NULL \
                   AND (rsi_signal <= $2 OR rsi_signal >= $3) \
                 ORDER BY volume_24h DESC NULLS LAST \
                 LIMIT 5"
            )
            .bind(&platform)
            .bind(buy_threshold)
            .bind(sell_threshold)
            .fetch_all(pool)
            .await
        };

        match signals_result {
            Ok(signals) => {
                for signal in signals {
                    let action = match signal.rsi_signal {
                        Some(rsi) if rsi <= buy_threshold => "BUY",
                        Some(rsi) if rsi >= sell_threshold => "SELL",
                        _ => "HOLD",
                    };
                    tracing::info!(
                        "🚀 [OMG-BOT user={}] {} {} | RSI: {:?}",
                        user_id, action, signal.title, signal.rsi_signal
                    );
                }
            }
            Err(e) => {
                tracing::warn!("OMG strategy query failed for user {}: {}", user_id, e);
            }
        }
    }
    Ok(())
}

/// Simple aggregate backtest across all events that triggered a buy signal in
/// the lookback window. Used by the standalone /backtest_simple endpoint
/// (the per-event RSI backtest lives in endpoints.rs::backtest_handler).
pub async fn run_backtest(
    pool: &PgPool,
    rsi_threshold: f64,
    days: i32,
) -> Result<BacktestResult, Error> {
    let row = sqlx::query_as::<_, BacktestResult>(
        r#"
        SELECT
            COUNT(*)::bigint AS total_trades,
            COALESCE(SUM(CASE WHEN odds > 0.5 THEN (1.0 - odds) ELSE -odds END), 0.0)::float8 AS estimated_profit,
            COALESCE((COUNT(CASE WHEN odds > 0.5 THEN 1 END)::float8
                      / NULLIF(COUNT(*), 0)::float8) * 100.0, 0.0) AS win_rate
        FROM public.prediction_events
        WHERE rsi_signal IS NOT NULL
          AND rsi_signal < $1
          AND updated_at > now() - make_interval(days => $2)
        "#
    )
    .bind(rsi_threshold)
    .bind(days)
    .fetch_one(pool)
    .await?;

    Ok(row)
}
