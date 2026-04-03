use sqlx::{PgPool, Error, Row};
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct BacktestResult {
    pub total_trades: i64,
    pub estimated_profit: f64,
    pub win_rate: f64,
}

#[derive(Debug, sqlx::FromRow)]
struct SignalRow {
    title: String,
    odds: f64,
    rsi_signal: Option<f64>,
}

pub async fn run_omg_strategy(pool: &PgPool) -> Result<(), Error> {
    // 1. Get active bots from public schema
    let active_bots = sqlx::query("SELECT user_id, platform, max_trade_amount, min_rsi_threshold FROM public.user_bots WHERE is_active = true")
        .fetch_all(pool)
        .await?;

    for bot in active_bots {
        let user_threshold: f64 = bot.get("min_rsi_threshold");
        let platform: String = bot.get("platform");

        let signals = sqlx::query_as::<_, SignalRow>(
            "SELECT title, odds, rsi_signal FROM public.prediction_events WHERE rsi_signal < $1 AND platform = $2 AND status = 'active' LIMIT 3"
        )
        .bind(user_threshold)
        .bind(platform)
        .fetch_all(pool)
        .await?;

        for signal in signals {
            println!("🚀 [OMG-BOT] Triggered: {} | RSI: {:?}", signal.title, signal.rsi_signal);
        }
    }
    Ok(())
}

pub async fn run_backtest(pool: &PgPool, rsi_threshold: f64, days: i32) -> Result<BacktestResult, Error> {
    let row = sqlx::query_as::<_, BacktestResult>(
        r#"
        SELECT 
            COUNT(*) as total_trades,
            COALESCE(SUM(CASE WHEN odds > 0.5 THEN (1.0 - odds) ELSE -odds END), 0.0) as estimated_profit,
            COALESCE((COUNT(CASE WHEN odds > 0.5 THEN 1 END)::float / NULLIF(COUNT(*), 0)::float) * 100.0, 0.0) as win_rate
        FROM public.prediction_events 
        WHERE rsi_signal < $1 
        AND updated_at > now() - ($2 || ' days')::interval
        "#
    )
    .bind(rsi_threshold)
    .bind(days)
    .fetch_one(pool)
    .await?;

    Ok(row)
}