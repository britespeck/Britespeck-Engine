use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Serialize, Deserialize, sqlx::FromRow)]
pub struct UserBot {
    pub id: Uuid,
    pub user_id: Uuid,
    pub event_id: Option<String>,
    pub platform: String,
    pub encrypted_api_key: Option<String>,
    pub encrypted_api_secret: Option<String>,
    pub is_active: bool,
    pub max_trade_amount: f64,
    pub stop_loss_pct: f64,
    pub min_rsi_threshold: f64,
    pub rsi_sell_threshold: f64,
    pub min_sentiment_score: f64,
}

#[derive(Deserialize)]
pub struct CreateUserBot {
    pub user_id: Uuid,
    pub platform: String,
    pub event_id: Option<String>,
    pub encrypted_api_key: Option<String>,
    pub encrypted_api_secret: Option<String>,
    #[serde(alias = "max_position")]
    pub max_trade_amount: Option<f64>,
    pub stop_loss_pct: Option<f64>,
    #[serde(alias = "rsi_buy")]
    pub min_rsi_threshold: Option<f64>,
    #[serde(alias = "rsi_sell")]
    pub rsi_sell_threshold: Option<f64>,
    pub min_sentiment_score: Option<f64>,
    pub is_active: Option<bool>,
}

#[derive(Deserialize)]
pub struct UpdateUserBot {
    pub is_active: Option<bool>,
    pub event_id: Option<String>,
    pub encrypted_api_key: Option<String>,
    pub encrypted_api_secret: Option<String>,
    #[serde(alias = "max_position")]
    pub max_trade_amount: Option<f64>,
    pub stop_loss_pct: Option<f64>,
    #[serde(alias = "rsi_buy")]
    pub min_rsi_threshold: Option<f64>,
    #[serde(alias = "rsi_sell")]
    pub rsi_sell_threshold: Option<f64>,
    pub min_sentiment_score: Option<f64>,
}

#[derive(Deserialize)]
pub struct UserBotQuery {
    pub user_id: Uuid,
}

pub async fn get_user_bot(
    State(pool): State<PgPool>,
    Query(params): Query<UserBotQuery>,
) -> Result<Json<Option<UserBot>>, StatusCode> {
    let bot = sqlx::query_as::<_, UserBot>(
        "SELECT id, user_id, event_id, platform, encrypted_api_key, encrypted_api_secret, \
                is_active, max_trade_amount, stop_loss_pct, min_rsi_threshold, \
                rsi_sell_threshold, min_sentiment_score \
         FROM user_bots \
         WHERE user_id = $1 \
         ORDER BY id DESC \
         LIMIT 1"
    )
    .bind(params.user_id)
    .fetch_optional(&pool)
    .await
    .map_err(|e| {
        tracing::error!("get_user_bot error: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(bot))
}

pub async fn create_user_bot(
    State(pool): State<PgPool>,
    Json(body): Json<CreateUserBot>,
) -> Result<Json<UserBot>, StatusCode> {
    let bot = sqlx::query_as::<_, UserBot>(
        "INSERT INTO user_bots \
            (user_id, platform, event_id, encrypted_api_key, encrypted_api_secret, is_active, \
             max_trade_amount, stop_loss_pct, min_rsi_threshold, rsi_sell_threshold, min_sentiment_score) \
         VALUES ($1, $2, $3, $4, $5, COALESCE($6, false), \
                 COALESCE($7, 100.0), COALESCE($8, 0.10), COALESCE($9, 30.0), COALESCE($10, 70.0), COALESCE($11, 0.6)) \
         RETURNING id, user_id, event_id, platform, encrypted_api_key, encrypted_api_secret, \
                   is_active, max_trade_amount, stop_loss_pct, min_rsi_threshold, \
                   rsi_sell_threshold, min_sentiment_score"
    )
    .bind(body.user_id)
    .bind(&body.platform)
    .bind(&body.event_id)
    .bind(&body.encrypted_api_key)
    .bind(&body.encrypted_api_secret)
    .bind(body.is_active)
    .bind(body.max_trade_amount)
    .bind(body.stop_loss_pct)
    .bind(body.min_rsi_threshold)
    .bind(body.rsi_sell_threshold)
    .bind(body.min_sentiment_score)
    .fetch_one(&pool)
    .await
    .map_err(|e| {
        tracing::error!("create_user_bot error: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(bot))
}

pub async fn update_user_bot(
    State(pool): State<PgPool>,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateUserBot>,
) -> Result<Json<UserBot>, StatusCode> {
    let bot = sqlx::query_as::<_, UserBot>(
        "UPDATE user_bots SET \
            is_active            = COALESCE($2, is_active), \
            event_id             = COALESCE($3, event_id), \
            encrypted_api_key    = COALESCE($4, encrypted_api_key), \
            encrypted_api_secret = COALESCE($5, encrypted_api_secret), \
            max_trade_amount     = COALESCE($6, max_trade_amount), \
            stop_loss_pct        = COALESCE($7, stop_loss_pct), \
            min_rsi_threshold    = COALESCE($8, min_rsi_threshold), \
            rsi_sell_threshold   = COALESCE($9, rsi_sell_threshold), \
            min_sentiment_score  = COALESCE($10, min_sentiment_score) \
         WHERE id = $1 \
         RETURNING id, user_id, event_id, platform, encrypted_api_key, encrypted_api_secret, \
                   is_active, max_trade_amount, stop_loss_pct, min_rsi_threshold, \
                   rsi_sell_threshold, min_sentiment_score"
    )
    .bind(id)
    .bind(body.is_active)
    .bind(&body.event_id)
    .bind(&body.encrypted_api_key)
    .bind(&body.encrypted_api_secret)
    .bind(body.max_trade_amount)
    .bind(body.stop_loss_pct)
    .bind(body.min_rsi_threshold)
    .bind(body.rsi_sell_threshold)
    .bind(body.min_sentiment_score)
    .fetch_one(&pool)
    .await
    .map_err(|e| {
        tracing::error!("update_user_bot error: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(bot))
}
