pub async fn run_omg_strategy(pool: &PgPool) -> Result<(), sqlx::Error> {
    // 1. Get ALL active bots and their custom RSI "triggers"
    let active_bots = sqlx::query!(
        "SELECT user_id, platform, max_trade_amount, min_rsi_threshold FROM user_bots WHERE is_active = true"
    )
    .fetch_all(pool)
    .await?;

    for bot in active_bots {
        let user_threshold = bot.min_rsi_threshold.unwrap_or(30.0); // Default to 30 if null

        // 2. FIND GEMS specifically for THIS user's RSI setting
        let signals = sqlx::query!(
            "SELECT title, odds, rsi_signal 
             FROM prediction_events 
             WHERE rsi_signal < $1 
             AND platform = $2
             AND status = 'active'
             LIMIT 5",
            user_threshold, // <--- The OMG now uses the USER'S custom number!
            bot.platform
        )
        .fetch_all(pool)
        .await?;

        for signal in signals {
            println!(
                "🚀 [OMG EXECUTE] User {} triggered at RSI {:.2} (Threshold was {})",
                bot.user_id,
                signal.rsi_signal.unwrap_or(0.0),
                user_threshold
            );
            // execute_trade_on_kalshi_or_poly(&bot, &signal).await;
        }
    }
    Ok(())
}