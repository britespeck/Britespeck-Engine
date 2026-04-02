use sqlx::PgPool;
use serde::{Serialize, Deserialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct BacktestResult {
    pub total_trades: i64,
    pub estimated_profit: f64,
    pub win_rate: f64,
}

// 1. LIVE TRADING ENGINE
pub async fn run_omg_strategy(pool: &PgPool) -> Result<(), sqlx::Error> {
    let active_bots = sqlx::query!(
        "SELECT user_id, platform, max_trade_amount, min_rsi_threshold, min_sentiment_score 
         FROM user_bots 
         WHERE is_active = true"
    )
    .fetch_all(pool)
    .await?;

    for bot in active_bots {
        let rsi_limit = bot.min_rsi_threshold.unwrap_or(30.0);
        let sent_limit = bot.min_sentiment_score.unwrap_or(0.5);

        let matches = sqlx::query!(
            "SELECT id, title, odds, rsi_signal, sentiment_score 
             FROM prediction_events 
             WHERE platform = $1 
             AND rsi_signal < $2 
             AND sentiment_score > $3
             AND status = 'active'
             LIMIT 3",
            bot.platform,
            rsi_limit,
            sent_limit
        )
        .fetch_all(pool)
        .await?;

        for m in matches {
            println!(
                "🤖 [OMG-BOT] User {} -> BUY '{}' | RSI: {:.2} | Target: <{}", 
                bot.user_id, m.title, m.rsi_signal.unwrap_or(0.0), rsi_limit
            );
        }
    }
    Ok(())
}

// 2. THE BACKTESTER
pub async fn run_backtest(
    pool: &PgPool, 
    rsi_threshold: f64, 
    days: i32
) -> Result<BacktestResult, sqlx::Error> {
    let result = sqlx::query_as!(
        BacktestResult,
        r#"
        SELECT 
            COUNT(*) as "total_trades!",
            SUM(CASE WHEN odds > 0.5 THEN (1.0 - odds) ELSE -odds END) as "estimated_profit!",
            (COUNT(CASE WHEN odds > 0.5 THEN 1 END)::float / NULLIF(COUNT(*), 0)::float) * 100.0 as "win_rate!"
        FROM prediction_events 
        WHERE rsi_signal < $1 
        AND updated_at > now() - ($2 || ' days')::interval
        "#,
        rsi_threshold,
        days as f64
    )
    .fetch_one(pool)
    .await?;

    Ok(result)
}
