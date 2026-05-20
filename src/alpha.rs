
pub async fn get_global_signals(
    pool: &PgPool,
    signal_type: Option<&str>,
    limit: i64,
) -> anyhow::Result<Vec<AlphaSignal>> {
    let rows = sqlx::query!(
        "SELECT id, event_id, signal_type, magnitude, metadata, created_at
         FROM public.alpha_signals
         WHERE ($1::text IS NULL OR signal_type = $1)
         ORDER BY created_at DESC LIMIT $2",
        signal_type,
        limit
    )
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(|r| AlphaSignal {
        id: r.id.unwrap_or_default(),
        event_id: r.event_id.unwrap_or_default(),
        signal_type: r.signal_type.unwrap_or_default(),
        magnitude: r.magnitude.unwrap_or(0.0),
        metadata: r.metadata.unwrap_or(serde_json::Value::Null),
        created_at: r.created_at.unwrap_or_else(Utc::now),
    }).collect())
}
