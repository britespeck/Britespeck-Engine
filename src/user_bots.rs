use axum::{extract::{Path, Query}, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Serialize, Deserialize, sqlx::FromRow)]
pub struct UserBot {
    pub id: Uuid,
    pub user_id: Uuid,
    pub platform: String,
    pub encrypted_api_key: Option<String>,
    pub encrypted_api_secret: Option<String>,
    pub is_active: bool,
    pub max_trade_amount: f64,
    pub stop_loss_pct: f64,
    pub min_rsi_threshold: f64,
    pub min_sentiment_score: f64,
}

#[derive(Deserialize)]
pub struct CreateUserBot {
    pub user_id: Uuid,
    pub platform: String,
    pub encrypted_api_key: Option<String>,
    pub encrypted_api_secret: Option<String>,
    pub max_trade_amount: Option<f64>,
    pub stop_loss_pct: Option<f64>,
    pub min_rsi_threshold: Option<f64>,
    pub min_sentiment_score: Option<f64>,
}

#[derive(Deserialize)]
pub struct UpdateUserBot {
    pub is_active: Option<bool>,
    pub encrypted_api_key: Option<String>,
    pub encrypted_api_secret: Option<String>,
    pub max_trade_amount: Option<f64>,
    pub stop_loss_pct: Option<f64>,
    pub min_rsi_threshold: Option<f64>,
    pub min_sentiment_score: Option<f64>,
}

#[derive(Deserialize)]
pub struct UserBotQuery {
    pub user_id: Uuid,
}

pub async fn get_user_bot(
    Query(params): Query<UserBotQuery>,
    pool: axum::Extension<PgPool>,
) -> Result<Json<Vec<UserBot>>, StatusCode> {
    let bots = sqlx::query_as::<_, UserBot>(
        "SELECT id, user_id, platform, encrypted_api_key, encrypted_api_secret, is_active, max_trade_amount, stop_loss_pct, min_rsi_threshold, min_sentiment_score FROM user_bots WHERE user_id = $1"
    )
    .bind(params.user_id)
    .fetch_all(&*pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(bots))
}

pub async fn create_user_bot(
    pool: axum::Extension<PgPool>,
    Json(body): Json<CreateUserBot>,
) -> Result<Json<UserBot>, StatusCode> {
    let bot = sqlx::query_as::<_, UserBot>(
        "INSERT INTO user_bots (user_id, platform, encrypted_api_key, encrypted_api_secret, max_trade_amount, stop_loss_pct, min_rsi_threshold, min_sentiment_score) VALUES ($1,$2,$3,$4, COALESCE($5,100.0), COALESCE($6,0.10), COALESCE($7,30.0), COALESCE($8,0.6)) RETURNING *"
    )
    .bind(body.user_id).bind(body.platform)
    .bind(body.encrypted_api_key).bind(body.encrypted_api_secret)
    .bind(body.max_trade_amount).bind(body.stop_loss_pct)
    .bind(body.min_rsi_threshold).bind(body.min_sentiment_score)
    .fetch_one(&*pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(bot))
}

pub async fn update_user_bot(
    Path(id): Path<Uuid>,
    pool: axum::Extension<PgPool>,
    Json(body): Json<UpdateUserBot>,
) -> Result<Json<UserBot>, StatusCode> {
    let bot = sqlx::query_as::<_, UserBot>(
        "UPDATE user_bots SET is_active = COALESCE($2, is_active), encrypted_api_key = COALESCE($3, encrypted_api_key), encrypted_api_secret = COALESCE($4, encrypted_api_secret), max_trade_amount = COALESCE($5, max_trade_amount), stop_loss_pct = COALESCE($6, stop_loss_pct), min_rsi_threshold = COALESCE($7, min_rsi_threshold), min_sentiment_score = COALESCE($8, min_sentiment_score) WHERE id = $1 RETURNING *"
    )
    .bind(id).bind(body.is_active)
    .bind(body.encrypted_api_key).bind(body.encrypted_api_secret)
    .bind(body.max_trade_amount).bind(body.stop_loss_pct)
    .bind(body.min_rsi_threshold).bind(body.min_sentiment_score)
    .fetch_one(&*pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(bot))
}