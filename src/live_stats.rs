//! Live Stats Engine — fetches real-time game/match data for prediction contracts.
//!
//! Supported sports:
//!   Soccer/Football — football-data.org (free tier)
//!   NBA             — NBA Stats API (public, no key)
//!   NFL             — ESPN undocumented public API
//!   MLB             — MLB Stats API (public, no key)
//!   Tennis          — api-tennis.com
//!   Cricket         — cricapi.com
//!   NASCAR          — sportsdata.io
//!   Politics        — Kalshi + Polymarket prices (already in DB)
//!
//! Wire into main.rs:
//!   mod live_stats;
//!   .merge(live_stats::routes())
//!   tokio::spawn(live_stats::run_live_stats_loop(api_pool.clone()));

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::get,
    Router,
};
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::PgPool;
use std::time::Duration;
use uuid::Uuid;

// ── Response types ─────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LiveGameEvent {
    pub minute: Option<String>,
    pub event_type: String,  // "goal", "card", "substitution", "timeout", "quarter_end"
    pub team: Option<String>,
    pub player: Option<String>,
    pub detail: Option<String>,
    pub score: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LiveStats {
    pub sport: String,
    pub status: String,           // "live", "finished", "scheduled", "halftime"
    pub home_team: String,
    pub away_team: String,
    pub home_score: Option<i64>,
    pub away_score: Option<i64>,
    pub minute: Option<String>,   // game time
    pub period: Option<String>,   // "1H", "2H", "Q1", "Q2", "HT", "FT" etc
    pub home_possession: Option<f64>,
    pub away_possession: Option<f64>,
    pub home_shots: Option<i64>,
    pub away_shots: Option<i64>,
    pub home_shots_on_target: Option<i64>,
    pub away_shots_on_target: Option<i64>,
    pub events: Vec<LiveGameEvent>,
    pub extra: Option<Value>,     // sport-specific extra data
    pub fetched_at: DateTime<Utc>,
    pub source: String,
}

#[derive(Debug, Serialize)]
pub struct LiveStatsResponse {
    pub event_id: String,
    pub live_stats: Option<LiveStats>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

// ── Routes ─────────────────────────────────────────────────────────

pub fn routes() -> Router<PgPool> {
    Router::new()
        .route("/live_stats/:event_id", get(get_live_stats_handler))
}

// ── GET /live_stats/:event_id ──────────────────────────────────────

async fn get_live_stats_handler(
    State(pool): State<PgPool>,
    Path(event_id): Path<String>,
) -> Result<Json<LiveStatsResponse>, (StatusCode, Json<ErrorResponse>)> {
    let uid = parse_or_lookup_uuid(&pool, &event_id).await.ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Event not found: {}", event_id),
            }),
        )
    })?;

    // Read from DB cache first
    let row: Option<(Option<Value>, Option<DateTime<Utc>>)> = sqlx::query_as(
        "SELECT live_stats, live_stats_updated_at
         FROM public.prediction_events
         WHERE id = $1",
    )
    .bind(uid)
    .fetch_optional(&pool)
    .await
    .map_err(|e| (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse { error: e.to_string() }),
    ))?;

    let (stats_json, updated_at) = row.unwrap_or((None, None));

    let live_stats = stats_json.and_then(|v| {
        serde_json::from_value::<LiveStats>(v).ok()
    });

    Ok(Json(LiveStatsResponse {
        event_id: uid.to_string(),
        live_stats,
        updated_at,
    }))
}

// ── Sport detector ─────────────────────────────────────────────────

fn detect_sport(title: &str, category: &str) -> &'static str {
    let t = title.to_lowercase();
    let c = category.to_lowercase();

    if t.contains("premier league") || t.contains("champions league")
        || t.contains("la liga") || t.contains("bundesliga")
        || t.contains("serie a") || t.contains("ligue 1")
        || t.contains("mls") || t.contains("copa")
        || t.contains("everton") || t.contains("arsenal")
        || t.contains("manchester") || t.contains("chelsea")
        || c.contains("soccer") || c.contains("football match")
    {
        return "soccer";
    }

    if t.contains("nba") || t.contains("lakers") || t.contains("celtics")
        || t.contains("warriors") || t.contains("knicks")
        || t.contains("nuggets") || t.contains("heat")
        || t.contains("toronto") || t.contains("cleveland")
        || t.contains("orlando") || t.contains("detroit")
        || t.contains("boston") || t.contains("miami")
        || t.contains("chicago") || t.contains("milwaukee")
        || t.contains("philadelphia") || t.contains("brooklyn")
        || t.contains("series winner") && c.contains("sports")
        || t.contains("game 7") && c.contains("sports")
        || t.contains("game 6") && c.contains("sports")
        || t.contains("game 5") && c.contains("sports")
        || t.contains("vs.") && c.contains("sports") && t.contains("game")
    {
        return "nba";
    }

    if t.contains("nfl") || t.contains("super bowl")
        || t.contains("chiefs") || t.contains("eagles")
        || t.contains("patriots") || t.contains("cowboys")
        || t.contains("rams") || t.contains("49ers")
    {
        return "nfl";
    }

    if t.contains("mlb") || t.contains("world series")
        || t.contains("yankees") || t.contains("dodgers")
        || t.contains("red sox") || t.contains("cubs")
    {
        return "mlb";
    }

    if t.contains("tennis") || t.contains("wimbledon")
        || t.contains("us open") || t.contains("french open")
        || t.contains("australian open") || t.contains("atp")
        || t.contains("wta") || t.contains("vs") && c.contains("tennis")
    {
        return "tennis";
    }

    if t.contains("cricket") || t.contains("ipl")
        || t.contains("test match") || t.contains("odi")
        || t.contains("t20") || t.contains("mumbai indians")
        || t.contains("lucknow") || t.contains("chennai")
        || t.contains("kolkata") || t.contains("rajasthan royals")
        || t.contains("delhi capitals") || t.contains("punjab kings")
        || t.contains("sunrisers") || t.contains("royal challengers")
        || t.contains("gujarat titans")
    {
        return "cricket";
    }

    if t.contains("nascar") || t.contains("daytona")
        || t.contains("talladega") || t.contains("cup series")
    {
        return "nascar";
    }

    if t.contains("formula 1") || t.contains("formula one")
        || t.contains("f1 ") || t.contains(" f1")
        || t.contains("grand prix") || t.contains("gp ")
        || t.contains("verstappen") || t.contains("hamilton")
        || t.contains("leclerc") || t.contains("norris")
        || t.contains("ferrari") || t.contains("red bull racing")
        || t.contains("mercedes amg") || t.contains("mclaren")
    {
        return "f1";
    }

    if t.contains("nhl") || t.contains("stanley cup")
        || t.contains("maple leafs") || t.contains("bruins")
        || t.contains("lightning") || t.contains("canadiens")
    {
        return "nhl";
    }

    "unknown"
}

// ── Soccer — football-data.org ─────────────────────────────────────

async fn fetch_soccer_stats(
    client: &Client,
    title: &str,
) -> anyhow::Result<Option<LiveStats>> {
    let api_key = std::env::var("FOOTBALL_DATA_API_KEY").unwrap_or_default();
    if api_key.is_empty() {
        return Ok(None);
    }

    // Search for live matches
    let url = "https://api.football-data.org/v4/matches?status=LIVE,IN_PLAY,PAUSED,HALFTIME";

    let resp = client
        .get(url)
        .header("X-Auth-Token", &api_key)
        .timeout(Duration::from_secs(8))
        .send()
        .await?;

    if !resp.status().is_success() {
        return Ok(None);
    }

    let data: Value = resp.json().await?;
    let matches = data.get("matches").and_then(|m| m.as_array()).cloned().unwrap_or_default();

    // Find the match that best matches our contract title
    let title_lower = title.to_lowercase();
    let matched = matches.iter().find(|m| {
        let home = m.get("homeTeam").and_then(|t| t.get("name"))
            .and_then(|n| n.as_str()).unwrap_or("").to_lowercase();
        let away = m.get("awayTeam").and_then(|t| t.get("name"))
            .and_then(|n| n.as_str()).unwrap_or("").to_lowercase();
        title_lower.contains(&home) || title_lower.contains(&away)
            || home.split_whitespace().any(|w| w.len() > 3 && title_lower.contains(w))
            || away.split_whitespace().any(|w| w.len() > 3 && title_lower.contains(w))
    });

    let Some(m) = matched else { return Ok(None); };

    let home = m.get("homeTeam").and_then(|t| t.get("name"))
        .and_then(|n| n.as_str()).unwrap_or("Home").to_string();
    let away = m.get("awayTeam").and_then(|t| t.get("name"))
        .and_then(|n| n.as_str()).unwrap_or("Away").to_string();

    let home_score = m.get("score").and_then(|s| s.get("fullTime"))
        .and_then(|ft| ft.get("home")).and_then(|v| v.as_i64());
    let away_score = m.get("score").and_then(|s| s.get("fullTime"))
        .and_then(|ft| ft.get("away")).and_then(|v| v.as_i64());

    let minute = m.get("minute").and_then(|v| v.as_i64())
        .map(|n| format!("{}'", n));

    let status_raw = m.get("status").and_then(|s| s.as_str()).unwrap_or("");
    let status = match status_raw {
        "IN_PLAY" => "live",
        "PAUSED" | "HALFTIME" => "halftime",
        "FINISHED" => "finished",
        _ => "scheduled",
    }.to_string();

    // Extract goals from goals array
    let events = m.get("goals").and_then(|g| g.as_array())
        .cloned()
        .unwrap_or_default()
        .iter()
        .map(|g| {
            let scorer = g.get("scorer").and_then(|s| s.get("name"))
                .and_then(|n| n.as_str()).map(|s| s.to_string());
            let team = g.get("team").and_then(|t| t.get("name"))
                .and_then(|n| n.as_str()).map(|s| s.to_string());
            let min = g.get("minute").and_then(|v| v.as_i64())
                .map(|n| format!("{}'", n));
            LiveGameEvent {
                minute: min,
                event_type: "goal".to_string(),
                team,
                player: scorer,
                detail: None,
                score: None,
            }
        })
        .collect();

    Ok(Some(LiveStats {
        sport: "soccer".to_string(),
        status,
        home_team: home,
        away_team: away,
        home_score,
        away_score,
        minute,
        period: None,
        home_possession: None,
        away_possession: None,
        home_shots: None,
        away_shots: None,
        home_shots_on_target: None,
        away_shots_on_target: None,
        events,
        extra: None,
        fetched_at: Utc::now(),
        source: "football-data.org".to_string(),
    }))
}

// ── NBA — NBA Stats API (public) ───────────────────────────────────

async fn fetch_nba_stats(
    client: &Client,
    title: &str,
) -> anyhow::Result<Option<LiveStats>> {
    // NBA today's scoreboard — public, no key needed
    let url = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json";

    let resp = client
        .get(url)
        .header("User-Agent", "Mozilla/5.0")
        .header("Referer", "https://www.nba.com/")
        .timeout(Duration::from_secs(8))
        .send()
        .await?;

    if !resp.status().is_success() {
        return Ok(None);
    }

    let data: Value = resp.json().await?;
    let games = data.get("scoreboard").and_then(|s| s.get("games"))
        .and_then(|g| g.as_array()).cloned().unwrap_or_default();

    let title_lower = title.to_lowercase();
    let matched = games.iter().find(|g| {
        let home = g.get("homeTeam").and_then(|t| t.get("teamName"))
            .and_then(|n| n.as_str()).unwrap_or("").to_lowercase();
        let away = g.get("awayTeam").and_then(|t| t.get("teamName"))
            .and_then(|n| n.as_str()).unwrap_or("").to_lowercase();
        let home_city = g.get("homeTeam").and_then(|t| t.get("teamCity"))
            .and_then(|n| n.as_str()).unwrap_or("").to_lowercase();
        let away_city = g.get("awayTeam").and_then(|t| t.get("teamCity"))
            .and_then(|n| n.as_str()).unwrap_or("").to_lowercase();
        // Match on full name, city, or any word > 3 chars
        title_lower.contains(&home) || title_lower.contains(&away)
            || title_lower.contains(&home_city) || title_lower.contains(&away_city)
            || home.split_whitespace().any(|w| w.len() > 3 && title_lower.contains(w))
            || away.split_whitespace().any(|w| w.len() > 3 && title_lower.contains(w))
            || home_city.split_whitespace().any(|w| w.len() > 3 && title_lower.contains(w))
            || away_city.split_whitespace().any(|w| w.len() > 3 && title_lower.contains(w))
    });

    let Some(g) = matched else { return Ok(None); };

    let home_name = format!(
        "{} {}",
        g.get("homeTeam").and_then(|t| t.get("teamCity")).and_then(|n| n.as_str()).unwrap_or(""),
        g.get("homeTeam").and_then(|t| t.get("teamName")).and_then(|n| n.as_str()).unwrap_or("")
    );
    let away_name = format!(
        "{} {}",
        g.get("awayTeam").and_then(|t| t.get("teamCity")).and_then(|n| n.as_str()).unwrap_or(""),
        g.get("awayTeam").and_then(|t| t.get("teamName")).and_then(|n| n.as_str()).unwrap_or("")
    );

    let home_score = g.get("homeTeam").and_then(|t| t.get("score")).and_then(|v| v.as_i64());
    let away_score = g.get("awayTeam").and_then(|t| t.get("score")).and_then(|v| v.as_i64());

    let period = g.get("period").and_then(|v| v.as_i64())
        .map(|p| format!("Q{}", p));
    let clock = g.get("gameClock").and_then(|v| v.as_str()).map(|s| s.to_string());

    let status_raw = g.get("gameStatusText").and_then(|v| v.as_str()).unwrap_or("");
    let status = if status_raw.contains("Halftime") {
        "halftime"
    } else if status_raw == "Final" {
        "finished"
    } else if g.get("gameStatus").and_then(|v| v.as_i64()) == Some(2) {
        "live"
    } else {
        "scheduled"
    }.to_string();

    Ok(Some(LiveStats {
        sport: "nba".to_string(),
        status,
        home_team: home_name.trim().to_string(),
        away_team: away_name.trim().to_string(),
        home_score,
        away_score,
        minute: clock,
        period,
        home_possession: None,
        away_possession: None,
        home_shots: None,
        away_shots: None,
        home_shots_on_target: None,
        away_shots_on_target: None,
        events: vec![],
        extra: Some(json!({
            "arena": g.get("arena").and_then(|a| a.get("arenaName")).and_then(|n| n.as_str()),
            "attendance": g.get("arena").and_then(|a| a.get("arenaCapacity")),
        })),
        fetched_at: Utc::now(),
        source: "nba.com".to_string(),
    }))
}

// ── NHL — NHL Stats API (public) ───────────────────────────────────

async fn fetch_nhl_stats(
    client: &Client,
    title: &str,
) -> anyhow::Result<Option<LiveStats>> {
    let url = "https://api-web.nhle.com/v1/score/now";

    let resp = client
        .get(url)
        .header("User-Agent", "Mozilla/5.0")
        .timeout(Duration::from_secs(8))
        .send()
        .await?;

    if !resp.status().is_success() {
        return Ok(None);
    }

    let data: Value = resp.json().await?;
    let games = data.get("games").and_then(|g| g.as_array())
        .cloned().unwrap_or_default();

    let title_lower = title.to_lowercase();
    let matched = games.iter().find(|g| {
        let home = g.get("homeTeam").and_then(|t| t.get("placeName"))
            .and_then(|n| n.get("default")).and_then(|v| v.as_str())
            .unwrap_or("").to_lowercase();
        let away = g.get("awayTeam").and_then(|t| t.get("placeName"))
            .and_then(|n| n.get("default")).and_then(|v| v.as_str())
            .unwrap_or("").to_lowercase();
        title_lower.contains(&home) || title_lower.contains(&away)
    });

    let Some(g) = matched else { return Ok(None); };

    let home = g.get("homeTeam").and_then(|t| t.get("placeName"))
        .and_then(|n| n.get("default")).and_then(|v| v.as_str())
        .unwrap_or("Home").to_string();
    let away = g.get("awayTeam").and_then(|t| t.get("placeName"))
        .and_then(|n| n.get("default")).and_then(|v| v.as_str())
        .unwrap_or("Away").to_string();

    let home_score = g.get("homeTeam").and_then(|t| t.get("score")).and_then(|v| v.as_i64());
    let away_score = g.get("awayTeam").and_then(|t| t.get("score")).and_then(|v| v.as_i64());

    let period = g.get("period").and_then(|v| v.as_i64())
        .map(|p| format!("P{}", p));
    let clock = g.get("clock").and_then(|c| c.get("timeRemaining"))
        .and_then(|v| v.as_str()).map(|s| s.to_string());

    let game_state = g.get("gameState").and_then(|v| v.as_str()).unwrap_or("");
    let status = match game_state {
        "LIVE" | "CRIT" => "live",
        "FINAL" | "OFF" => "finished",
        "INTERMISSION" => "halftime",
        _ => "scheduled",
    }.to_string();

    Ok(Some(LiveStats {
        sport: "nhl".to_string(),
        status,
        home_team: home,
        away_team: away,
        home_score,
        away_score,
        minute: clock,
        period,
        home_possession: None,
        away_possession: None,
        home_shots: g.get("homeTeam").and_then(|t| t.get("sog")).and_then(|v| v.as_i64()),
        away_shots: g.get("awayTeam").and_then(|t| t.get("sog")).and_then(|v| v.as_i64()),
        home_shots_on_target: None,
        away_shots_on_target: None,
        events: vec![],
        extra: None,
        fetched_at: Utc::now(),
        source: "nhle.com".to_string(),
    }))
}

// ── NFL — ESPN undocumented public API ────────────────────────────

async fn fetch_nfl_stats(
    client: &Client,
    title: &str,
) -> anyhow::Result<Option<LiveStats>> {
    let url = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard";

    let resp = client
        .get(url)
        .header("User-Agent", "Mozilla/5.0")
        .timeout(Duration::from_secs(8))
        .send()
        .await?;

    if !resp.status().is_success() {
        return Ok(None);
    }

    let data: Value = resp.json().await?;
    let events = data.get("events").and_then(|e| e.as_array())
        .cloned().unwrap_or_default();

    let title_lower = title.to_lowercase();
    let matched = events.iter().find(|e| {
        let name = e.get("name").and_then(|n| n.as_str()).unwrap_or("").to_lowercase();
        let short = e.get("shortName").and_then(|n| n.as_str()).unwrap_or("").to_lowercase();
        title_lower.contains(&name) || name.split(" at ").any(|t| title_lower.contains(t))
            || short.split('@').any(|t| title_lower.contains(t.trim()))
    });

    let Some(event) = matched else { return Ok(None); };

    let competition = event.get("competitions").and_then(|c| c.as_array())
        .and_then(|arr| arr.first()).cloned().unwrap_or_default();

    let competitors = competition.get("competitors").and_then(|c| c.as_array())
        .cloned().unwrap_or_default();

    let home = competitors.iter().find(|c| c.get("homeAway").and_then(|v| v.as_str()) == Some("home"));
    let away = competitors.iter().find(|c| c.get("homeAway").and_then(|v| v.as_str()) == Some("away"));

    let home_name = home.and_then(|c| c.get("team")).and_then(|t| t.get("displayName"))
        .and_then(|n| n.as_str()).unwrap_or("Home").to_string();
    let away_name = away.and_then(|c| c.get("team")).and_then(|t| t.get("displayName"))
        .and_then(|n| n.as_str()).unwrap_or("Away").to_string();

    let home_score = home.and_then(|c| c.get("score")).and_then(|v| v.as_str())
        .and_then(|s| s.parse::<i64>().ok());
    let away_score = away.and_then(|c| c.get("score")).and_then(|v| v.as_str())
        .and_then(|s| s.parse::<i64>().ok());

    let status_obj = competition.get("status").cloned().unwrap_or_default();
    let period = status_obj.get("period").and_then(|v| v.as_i64())
        .map(|p| format!("Q{}", p));
    let clock = status_obj.get("displayClock").and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let status_type = status_obj.get("type").and_then(|t| t.get("name"))
        .and_then(|n| n.as_str()).unwrap_or("");
    let status = match status_type {
        "STATUS_IN_PROGRESS" => "live",
        "STATUS_FINAL" => "finished",
        "STATUS_HALFTIME" => "halftime",
        _ => "scheduled",
    }.to_string();

    // Extract scoring plays as events
    let plays: Vec<LiveGameEvent> = competition.get("situation")
        .and_then(|s| s.get("lastPlay"))
        .map(|lp| {
            let desc = lp.get("text").and_then(|v| v.as_str()).unwrap_or("").to_string();
            vec![LiveGameEvent {
                minute: clock.clone(),
                event_type: "play".to_string(),
                team: None,
                player: None,
                detail: Some(desc),
                score: None,
            }]
        })
        .unwrap_or_default();

    Ok(Some(LiveStats {
        sport: "nfl".to_string(),
        status,
        home_team: home_name,
        away_team: away_name,
        home_score,
        away_score,
        minute: clock,
        period,
        home_possession: None,
        away_possession: None,
        home_shots: None,
        away_shots: None,
        home_shots_on_target: None,
        away_shots_on_target: None,
        events: plays,
        extra: None,
        fetched_at: Utc::now(),
        source: "espn.com".to_string(),
    }))
}

// ── MLB — MLB Stats API (public) ───────────────────────────────────

async fn fetch_mlb_stats(
    client: &Client,
    title: &str,
) -> anyhow::Result<Option<LiveStats>> {
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let url = format!(
        "https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={}&hydrate=linescore",
        today
    );

    let resp = client
        .get(&url)
        .header("User-Agent", "Mozilla/5.0")
        .timeout(Duration::from_secs(8))
        .send()
        .await?;

    if !resp.status().is_success() {
        return Ok(None);
    }

    let data: Value = resp.json().await?;
    let games = data.get("dates").and_then(|d| d.as_array())
        .and_then(|arr| arr.first())
        .and_then(|d| d.get("games"))
        .and_then(|g| g.as_array())
        .cloned()
        .unwrap_or_default();

    let title_lower = title.to_lowercase();
    let matched = games.iter().find(|g| {
        let home = g.get("teams").and_then(|t| t.get("home"))
            .and_then(|h| h.get("team")).and_then(|t| t.get("name"))
            .and_then(|n| n.as_str()).unwrap_or("").to_lowercase();
        let away = g.get("teams").and_then(|t| t.get("away"))
            .and_then(|a| a.get("team")).and_then(|t| t.get("name"))
            .and_then(|n| n.as_str()).unwrap_or("").to_lowercase();
        title_lower.contains(&home) || title_lower.contains(&away)
            || home.split_whitespace().any(|w| w.len() > 3 && title_lower.contains(w))
            || away.split_whitespace().any(|w| w.len() > 3 && title_lower.contains(w))
    });

    let Some(g) = matched else { return Ok(None); };

    let home = g.get("teams").and_then(|t| t.get("home"))
        .and_then(|h| h.get("team")).and_then(|t| t.get("name"))
        .and_then(|n| n.as_str()).unwrap_or("Home").to_string();
    let away = g.get("teams").and_then(|t| t.get("away"))
        .and_then(|a| a.get("team")).and_then(|t| t.get("name"))
        .and_then(|n| n.as_str()).unwrap_or("Away").to_string();

    let home_score = g.get("teams").and_then(|t| t.get("home"))
        .and_then(|h| h.get("score")).and_then(|v| v.as_i64());
    let away_score = g.get("teams").and_then(|t| t.get("away"))
        .and_then(|a| a.get("score")).and_then(|v| v.as_i64());

    let inning = g.get("linescore").and_then(|l| l.get("currentInning"))
        .and_then(|v| v.as_i64())
        .map(|i| format!("Inning {}", i));

    let inning_half = g.get("linescore").and_then(|l| l.get("inningHalf"))
        .and_then(|v| v.as_str()).map(|s| s.to_string());

    let status_code = g.get("status").and_then(|s| s.get("abstractGameCode"))
        .and_then(|v| v.as_str()).unwrap_or("");
    let status = match status_code {
        "L" => "live",
        "F" => "finished",
        _ => "scheduled",
    }.to_string();

    Ok(Some(LiveStats {
        sport: "mlb".to_string(),
        status,
        home_team: home,
        away_team: away,
        home_score,
        away_score,
        minute: inning,
        period: inning_half,
        home_possession: None,
        away_possession: None,
        home_shots: None,
        away_shots: None,
        home_shots_on_target: None,
        away_shots_on_target: None,
        events: vec![],
        extra: None,
        fetched_at: Utc::now(),
        source: "mlb.com".to_string(),
    }))
}

// ── Tennis — ESPN public API ───────────────────────────────────────

async fn fetch_tennis_stats(
    client: &Client,
    title: &str,
) -> anyhow::Result<Option<LiveStats>> {
    let url = "https://site.api.espn.com/apis/site/v2/sports/tennis/atp/scoreboard";

    let resp = client
        .get(url)
        .header("User-Agent", "Mozilla/5.0")
        .timeout(Duration::from_secs(8))
        .send()
        .await?;

    if !resp.status().is_success() {
        return Ok(None);
    }

    let data: Value = resp.json().await?;
    let events = data.get("events").and_then(|e| e.as_array())
        .cloned().unwrap_or_default();

    let title_lower = title.to_lowercase();
    let matched = events.iter().find(|e| {
        let name = e.get("name").and_then(|n| n.as_str()).unwrap_or("").to_lowercase();
        name.split(" vs. ").any(|p| title_lower.contains(p.trim()))
    });

    let Some(event) = matched else { return Ok(None); };

    let competition = event.get("competitions").and_then(|c| c.as_array())
        .and_then(|arr| arr.first()).cloned().unwrap_or_default();

    let competitors = competition.get("competitors").and_then(|c| c.as_array())
        .cloned().unwrap_or_default();

    let p1 = competitors.first();
    let p2 = competitors.get(1);

    let p1_name = p1.and_then(|c| c.get("athlete")).and_then(|a| a.get("displayName"))
        .and_then(|n| n.as_str()).unwrap_or("Player 1").to_string();
    let p2_name = p2.and_then(|c| c.get("athlete")).and_then(|a| a.get("displayName"))
        .and_then(|n| n.as_str()).unwrap_or("Player 2").to_string();

    let p1_score = p1.and_then(|c| c.get("score")).and_then(|v| v.as_str())
        .and_then(|s| s.parse::<i64>().ok());
    let p2_score = p2.and_then(|c| c.get("score")).and_then(|v| v.as_str())
        .and_then(|s| s.parse::<i64>().ok());

    let status_type = competition.get("status").and_then(|s| s.get("type"))
        .and_then(|t| t.get("name")).and_then(|n| n.as_str()).unwrap_or("");
    let status = match status_type {
        "STATUS_IN_PROGRESS" => "live",
        "STATUS_FINAL" => "finished",
        _ => "scheduled",
    }.to_string();

    Ok(Some(LiveStats {
        sport: "tennis".to_string(),
        status,
        home_team: p1_name,
        away_team: p2_name,
        home_score: p1_score,
        away_score: p2_score,
        minute: None,
        period: None,
        home_possession: None,
        away_possession: None,
        home_shots: None,
        away_shots: None,
        home_shots_on_target: None,
        away_shots_on_target: None,
        events: vec![],
        extra: None,
        fetched_at: Utc::now(),
        source: "espn.com".to_string(),
    }))
}

// ── Cricket — cricapi.com ──────────────────────────────────────────

async fn fetch_cricket_stats(
    client: &Client,
    title: &str,
) -> anyhow::Result<Option<LiveStats>> {
    let api_key = std::env::var("CRICKET_API_KEY").unwrap_or_default();
    if api_key.is_empty() {
        return Ok(None);
    }

    let url = format!(
        "https://api.cricapi.com/v1/currentMatches?apikey={}&offset=0",
        api_key
    );

    let resp = client
        .get(&url)
        .timeout(Duration::from_secs(8))
        .send()
        .await?;

    if !resp.status().is_success() {
        return Ok(None);
    }

    let data: Value = resp.json().await?;
    let matches = data.get("data").and_then(|d| d.as_array())
        .cloned().unwrap_or_default();

    let title_lower = title.to_lowercase();
    let matched = matches.iter().find(|m| {
        let name = m.get("name").and_then(|n| n.as_str()).unwrap_or("").to_lowercase();
        let teams = m.get("teams").and_then(|t| t.as_array())
            .map(|arr| arr.iter()
                .filter_map(|v| v.as_str())
                .any(|t| title_lower.contains(&t.to_lowercase())))
            .unwrap_or(false);
        name.split(" vs ").any(|t| title_lower.contains(t.trim())) || teams
    });

    let Some(m) = matched else { return Ok(None); };

    let teams = m.get("teams").and_then(|t| t.as_array()).cloned().unwrap_or_default();
    let home = teams.first().and_then(|v| v.as_str()).unwrap_or("Team 1").to_string();
    let away = teams.get(1).and_then(|v| v.as_str()).unwrap_or("Team 2").to_string();

    let status = m.get("status").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
    let game_status = if status.contains("live") || status.contains("progress") {
        "live"
    } else if status.contains("won") || status.contains("drawn") {
        "finished"
    } else {
        "scheduled"
    }.to_string();

    Ok(Some(LiveStats {
        sport: "cricket".to_string(),
        status: game_status,
        home_team: home,
        away_team: away,
        home_score: None,
        away_score: None,
        minute: None,
        period: m.get("matchType").and_then(|v| v.as_str()).map(|s| s.to_string()),
        home_possession: None,
        away_possession: None,
        home_shots: None,
        away_shots: None,
        home_shots_on_target: None,
        away_shots_on_target: None,
        events: vec![],
        extra: Some(json!({
            "score": m.get("score"),
            "status": m.get("status"),
        })),
        fetched_at: Utc::now(),
        source: "cricapi.com".to_string(),
    }))
}

// ── NASCAR — ESPN public API ───────────────────────────────────────

async fn fetch_nascar_stats(
    client: &Client,
    _title: &str,
) -> anyhow::Result<Option<LiveStats>> {
    let url = "https://site.api.espn.com/apis/site/v2/sports/racing/nascar-premier/scoreboard";

    let resp = client
        .get(url)
        .header("User-Agent", "Mozilla/5.0")
        .timeout(Duration::from_secs(8))
        .send()
        .await?;

    if !resp.status().is_success() {
        return Ok(None);
    }

    let data: Value = resp.json().await?;
    let events = data.get("events").and_then(|e| e.as_array())
        .cloned().unwrap_or_default();

    let Some(event) = events.first() else { return Ok(None); };

    let race_name = event.get("name").and_then(|n| n.as_str()).unwrap_or("Race").to_string();
    let status_type = event.get("competitions").and_then(|c| c.as_array())
        .and_then(|arr| arr.first())
        .and_then(|c| c.get("status")).and_then(|s| s.get("type"))
        .and_then(|t| t.get("name")).and_then(|n| n.as_str()).unwrap_or("");

    let status = match status_type {
        "STATUS_IN_PROGRESS" => "live",
        "STATUS_FINAL" => "finished",
        _ => "scheduled",
    }.to_string();

    Ok(Some(LiveStats {
        sport: "nascar".to_string(),
        status,
        home_team: race_name.clone(),
        away_team: "Field".to_string(),
        home_score: None,
        away_score: None,
        minute: None,
        period: None,
        home_possession: None,
        away_possession: None,
        home_shots: None,
        away_shots: None,
        home_shots_on_target: None,
        away_shots_on_target: None,
        events: vec![],
        extra: Some(json!({ "race": race_name })),
        fetched_at: Utc::now(),
        source: "espn.com".to_string(),
    }))
}


// ── Formula 1 — ESPN public API ───────────────────────────────────

async fn fetch_f1_stats(
    client: &Client,
    _title: &str,
) -> anyhow::Result<Option<LiveStats>> {
    let url = "https://site.api.espn.com/apis/site/v2/sports/racing/f1/scoreboard";

    let resp = client
        .get(url)
        .header("User-Agent", "Mozilla/5.0")
        .timeout(Duration::from_secs(8))
        .send()
        .await?;

    if !resp.status().is_success() {
        return Ok(None);
    }

    let data: Value = resp.json().await?;
    let events = data.get("events").and_then(|e| e.as_array())
        .cloned().unwrap_or_default();

    let Some(event) = events.first() else { return Ok(None); };

    let race_name = event.get("name").and_then(|n| n.as_str())
        .unwrap_or("Grand Prix").to_string();

    let competition = event.get("competitions").and_then(|c| c.as_array())
        .and_then(|arr| arr.first()).cloned().unwrap_or_default();

    let status_type = competition.get("status").and_then(|s| s.get("type"))
        .and_then(|t| t.get("name")).and_then(|n| n.as_str()).unwrap_or("");

    let status = match status_type {
        "STATUS_IN_PROGRESS" => "live",
        "STATUS_FINAL"       => "finished",
        _                    => "scheduled",
    }.to_string();

    // Top 3 competitors for the leaderboard
    let competitors = competition.get("competitors").and_then(|c| c.as_array())
        .cloned().unwrap_or_default();

    let leader = competitors.first();
    let p2     = competitors.get(1);
    let p3     = competitors.get(2);

    let leader_name = leader
        .and_then(|c| c.get("athlete")).and_then(|a| a.get("displayName"))
        .and_then(|n| n.as_str()).unwrap_or("Leader").to_string();

    let lap = competition.get("situation").and_then(|s| s.get("lastPlay"))
        .and_then(|lp| lp.get("text")).and_then(|v| v.as_str())
        .map(|s| s.to_string());

    // Build podium events
    let mut events_list = Vec::new();
    for (pos, comp) in [(1, leader), (2, p2), (3, p3)] {
        if let Some(c) = comp {
            let name = c.get("athlete").and_then(|a| a.get("displayName"))
                .and_then(|n| n.as_str()).unwrap_or("").to_string();
            let team = c.get("team").and_then(|t| t.get("displayName"))
                .and_then(|n| n.as_str()).unwrap_or("").to_string();
            if !name.is_empty() {
                events_list.push(LiveGameEvent {
                    minute: None,
                    event_type: "position".to_string(),
                    team: Some(team),
                    player: Some(name),
                    detail: Some(format!("P{}", pos)),
                    score: None,
                });
            }
        }
    }

    Ok(Some(LiveStats {
        sport: "f1".to_string(),
        status,
        home_team: leader_name,
        away_team: "Field".to_string(),
        home_score: None,
        away_score: None,
        minute: lap,
        period: None,
        home_possession: None,
        away_possession: None,
        home_shots: None,
        away_shots: None,
        home_shots_on_target: None,
        away_shots_on_target: None,
        events: events_list,
        extra: Some(json!({
            "race": race_name,
            "total_competitors": competitors.len(),
        })),
        fetched_at: Utc::now(),
        source: "espn.com".to_string(),
    }))
}

// ── Main fetch dispatcher ──────────────────────────────────────────

pub async fn fetch_live_stats(
    client: &Client,
    title: &str,
    category: &str,
) -> Option<LiveStats> {
    let sport = detect_sport(title, category);

    match sport {
        "soccer"  => fetch_soccer_stats(client, title).await.ok().flatten(),
        "nba"     => fetch_nba_stats(client, title).await.ok().flatten(),
        "nhl"     => fetch_nhl_stats(client, title).await.ok().flatten(),
        "nfl"     => fetch_nfl_stats(client, title).await.ok().flatten(),
        "mlb"     => fetch_mlb_stats(client, title).await.ok().flatten(),
        "tennis"  => fetch_tennis_stats(client, title).await.ok().flatten(),
        "cricket" => fetch_cricket_stats(client, title).await.ok().flatten(),
        "nascar"  => fetch_nascar_stats(client, title).await.ok().flatten(),
        "f1"      => fetch_f1_stats(client, title).await.ok().flatten(),
        _         => None,
    }
}

// ── Persist live stats ─────────────────────────────────────────────

async fn persist_live_stats(
    pool: &PgPool,
    event_id: Uuid,
    stats: &LiveStats,
) -> anyhow::Result<()> {
    let stats_json = serde_json::to_value(stats)?;
    sqlx::query(
        "UPDATE public.prediction_events
         SET live_stats = $1, live_stats_updated_at = NOW()
         WHERE id = $2"
    )
    .bind(stats_json)
    .bind(event_id)
    .execute(pool)
    .await?;
    Ok(())
}

// ── Background loop ────────────────────────────────────────────────

pub async fn run_live_stats_loop(pool: PgPool) {
    tracing::info!("🏆 Starting live stats loop (30s interval)");

    let client = Client::builder()
        .user_agent("Britespeck/1.0")
        .timeout(Duration::from_secs(10))
        .build()
        .expect("reqwest client");

    let mut interval = tokio::time::interval(Duration::from_secs(30));

    loop {
        interval.tick().await;

        // Get active sports contracts updated recently
        let markets: Vec<(Uuid, String, Option<String>)> = match sqlx::query_as(
            "SELECT id, title, category
             FROM public.prediction_events
             WHERE status = 'active'
               AND category IN ('Sports', 'Gaming')
               AND volume_24h > 1000
               AND (end_date IS NULL OR end_date > NOW())
             ORDER BY volume_24h DESC NULLS LAST
             LIMIT 100",
        )
        .fetch_all(&pool)
        .await
        {
            Ok(rows) => rows,
            Err(e) => {
                tracing::error!("Live stats loop: failed to load markets: {}", e);
                continue;
            }
        };

        if markets.is_empty() {
            continue;
        }

        let mut updated = 0usize;

        for (event_id, title, category) in &markets {
            let cat = category.as_deref().unwrap_or("");
            if let Some(stats) = fetch_live_stats(&client, title, cat).await {
                if let Ok(()) = persist_live_stats(&pool, *event_id, &stats).await {
                    updated += 1;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        if updated > 0 {
            tracing::info!("🏆 Updated live stats for {} markets", updated);
        }
    }
}

// ── UUID resolver ──────────────────────────────────────────────────

async fn parse_or_lookup_uuid(pool: &PgPool, param: &str) -> Option<Uuid> {
    if let Ok(uid) = Uuid::parse_str(param) {
        return Some(uid);
    }
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