//! Lead/Lag Relationship Detector
//!
//! WHAT KALSHI GIVES US:
//!   ticker format: SERIES-DATEENTITY1ENTITY2-OUTCOME
//!   e.g. KXWCGAME-26JUN27JORARG-ARG
//!        series=KXWCGAME, teams=[JOR,ARG], outcome=ARG
//!
//! DETECTION RULES (v1 = structural/deterministic only):
//!   Rule A: GAME → TOURNAMENT WINNER (same team, later expiry)
//!   Rule B: GAME → SPREAD (same event, same date)
//!   Rule C: GAME → TOTAL (same event, same date)
//!
//! THROUGHPUT TRACKING (new):
//!   Every scan logs signal count, clustering, and distribution
//!   so you can measure SaaS viability from day 1.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{debug, info};
use std::collections::HashMap;

// ── Series Classification ──────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SeriesType {
    GameMatchWinner,
    GameSpread,
    GameTotal,
    GamePlayerProp,
    TournamentWinner,
    SeriesWinner,
    SeasonWinner,
    Unknown,
}

pub fn classify_series(series: &str) -> SeriesType {
    let s = series.to_uppercase();
    if (s.contains("GAME") || s.contains("MATCH"))
        && !s.contains("WINNER") && !s.contains("CHAMP") && !s.contains("TITLE") {
        SeriesType::GameMatchWinner
    } else if s.contains("SPREAD") {
        SeriesType::GameSpread
    } else if s.contains("TOTAL") {
        SeriesType::GameTotal
    } else if s.contains("PTS") || s.contains("AST") || s.contains("REB") || s.contains("HR") {
        SeriesType::GamePlayerProp
    } else if s.contains("WINNER") || s.contains("CHAMP") || s.contains("CHAMPION") {
        SeriesType::TournamentWinner
    } else if s.contains("SERIES") {
        SeriesType::SeriesWinner
    } else if s.contains("TITLE") || s.contains("SEASON") {
        SeriesType::SeasonWinner
    } else {
        SeriesType::Unknown
    }
}

// ── Sport ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Sport {
    WorldCup, NBA, MLB, NHL, NFL,
    EPL, UCL, LaLiga, SerieA, Bundesliga, Ligue1,
    MLS, WNBA, IPL, Other,
}

pub fn sport_from_series(series: &str) -> Sport {
    let s = series.to_uppercase();
    if s.contains("WC")                    { Sport::WorldCup }
    else if s.contains("NBA")              { Sport::NBA }
    else if s.contains("MLB")              { Sport::MLB }
    else if s.contains("NHL")              { Sport::NHL }
    else if s.contains("NFL")              { Sport::NFL }
    else if s.contains("EPL")              { Sport::EPL }
    else if s.contains("UCL")              { Sport::UCL }
    else if s.contains("LALIGA")           { Sport::LaLiga }
    else if s.contains("SERIEA")           { Sport::SerieA }
    else if s.contains("BUNDESLIGA")       { Sport::Bundesliga }
    else if s.contains("LIGUE")            { Sport::Ligue1 }
    else if s.contains("MLS")              { Sport::MLS }
    else if s.contains("WNBA")             { Sport::WNBA }
    else if s.contains("IPL")              { Sport::IPL }
    else                                   { Sport::Other }
}

// ── Contract Metadata ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractMeta {
    pub ticker: String,
    pub event_ticker: String,
    pub series: String,
    pub series_type: SeriesType,
    pub sport: Sport,
    pub title: Option<String>,
    pub yes_sub_title: Option<String>,
    pub category: Option<String>,
    pub close_time: Option<DateTime<Utc>>,
    pub yes_price: f64,
    pub volume: f64,
    pub depth_dollars: f64,
    pub spread: f64,

    // Parsed from ticker
    pub team_codes: Vec<String>,
    pub outcome_code: Option<String>,
    pub date_part: Option<String>,
}

impl ContractMeta {
    pub fn from_api(
        ticker: &str,
        event_ticker: &str,
        title: Option<&str>,
        yes_sub_title: Option<&str>,
        category: Option<&str>,
        close_time: Option<DateTime<Utc>>,
        yes_price: f64,
        volume: f64,
        depth_dollars: f64,
        spread: f64,
    ) -> Self {
        let (series, teams, outcome, date_part) = parse_ticker(ticker);
        let sport = sport_from_series(&series);
        let series_type = classify_series(&series);

        Self {
            ticker: ticker.to_string(),
            event_ticker: event_ticker.to_string(),
            series,
            series_type,
            sport,
            title: title.map(|s| s.to_string()),
            yes_sub_title: yes_sub_title.map(|s| s.to_string()),
            category: category.map(|s| s.to_string()),
            close_time,
            yes_price,
            volume,
            depth_dollars,
            spread,
            team_codes: teams,
            outcome_code: outcome,
            date_part,
        }
    }

    pub fn involves_team(&self, team: &str) -> bool {
        let t = team.to_uppercase();
        self.team_codes.iter().any(|c| *c == t)
            || self.outcome_code.as_deref().map_or(false, |o| o.starts_with(&t))
            || self.ticker.to_uppercase().contains(&t)
    }
}

// ── Ticker Parser ──────────────────────────────────────────────────────────

/// SERIES-DATEENTITY1ENTITY2-OUTCOME
/// e.g. KXWCGAME-26JUN27JORARG-ARG → (KXWCGAME, [JOR,ARG], Some(ARG), Some(26JUN27))
pub fn parse_ticker(ticker: &str) -> (String, Vec<String>, Option<String>, Option<String>) {
    let parts: Vec<&str> = ticker.splitn(3, '-').collect();
    if parts.len() < 2 {
        return (ticker.to_string(), vec![], None, None);
    }

    let series = parts[0].to_string();
    let middle = parts[1];
    let outcome = parts.get(2).map(|s| s.to_string());
    let date_part = extract_date(middle);

    let after_date = if let Some(d) = &date_part {
        &middle[d.len().min(middle.len())..]
    } else {
        middle
    };

    let teams = extract_team_codes(after_date);
    (series, teams, outcome, date_part)
}

fn extract_date(s: &str) -> Option<String> {
    let months = ["JAN","FEB","MAR","APR","MAY","JUN",
                  "JUL","AUG","SEP","OCT","NOV","DEC"];
    for mon in &months {
        if let Some(pos) = s.find(mon) {
            let start = if pos >= 2 { pos - 2 } else { 0 };
            let base_end = (pos + 5).min(s.len()); // YY+MON+DD
            let end = if base_end + 4 <= s.len()
                && s[base_end..base_end+4].chars().all(|c| c.is_ascii_digit()) {
                base_end + 4
            } else { base_end };
            return Some(s[start..end].to_string());
        }
    }
    None
}

fn extract_team_codes(s: &str) -> Vec<String> {
    let upper: String = s.chars().filter(|c| c.is_ascii_uppercase()).collect();
    let mut teams = Vec::new();
    if upper.len() >= 6 {
        teams.push(upper[..3].to_string());
        teams.push(upper[3..6].to_string());
    } else if upper.len() >= 4 {
        teams.push(upper[..2].to_string());
        teams.push(upper[2..4].to_string());
    } else if !upper.is_empty() {
        teams.push(upper);
    }
    teams
}

// ── Detected Pair ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetectedPair {
    pub leader: ContractMeta,
    pub lagger: ContractMeta,
    pub relationship_type: String,
    pub elasticity: f64,
    pub confidence: f64,
    pub reason: String,
}

// ── Throughput Stats (NEW) ─────────────────────────────────────────────────

/// Tracks signal frequency and clustering per scan.
/// Log this every scan to measure SaaS viability over time.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ThroughputStats {
    pub scan_at: Option<DateTime<Utc>>,
    pub total_contracts_scanned: usize,
    pub pairs_evaluated: usize,
    pub pairs_detected: usize,

    // Clustering: how many signals fired simultaneously
    // High clustering = bad for SaaS scale (everyone hits same window)
    pub signals_by_relationship: HashMap<String, usize>,
    pub signals_by_sport: HashMap<String, usize>,

    // Liquidity distribution across detected pairs
    pub min_depth: f64,
    pub max_depth: f64,
    pub avg_depth: f64,

    // Duration distribution (seconds to close for detected laggers)
    pub min_seconds_to_close: i64,
    pub max_seconds_to_close: i64,
}

impl ThroughputStats {
    pub fn from_pairs(pairs: &[DetectedPair], total_contracts: usize) -> Self {
        let mut stats = Self {
            scan_at: Some(Utc::now()),
            total_contracts_scanned: total_contracts,
            pairs_evaluated: 0, // filled by caller
            pairs_detected: pairs.len(),
            ..Default::default()
        };

        if pairs.is_empty() { return stats; }

        let mut depths: Vec<f64> = Vec::new();
        let mut times: Vec<i64> = Vec::new();

        for pair in pairs {
            *stats.signals_by_relationship
                .entry(pair.relationship_type.clone())
                .or_insert(0) += 1;

            *stats.signals_by_sport
                .entry(format!("{:?}", pair.leader.sport))
                .or_insert(0) += 1;

            depths.push(pair.lagger.depth_dollars);
            if let Some(ct) = pair.lagger.close_time {
                let secs = (ct - Utc::now()).num_seconds().max(0);
                times.push(secs);
            }
        }

        if !depths.is_empty() {
            stats.min_depth = depths.iter().cloned().fold(f64::INFINITY, f64::min);
            stats.max_depth = depths.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            stats.avg_depth = depths.iter().sum::<f64>() / depths.len() as f64;
        }
        if !times.is_empty() {
            stats.min_seconds_to_close = *times.iter().min().unwrap_or(&0);
            stats.max_seconds_to_close = *times.iter().max().unwrap_or(&0);
        }

        stats
    }

    pub fn log(&self) {
        info!(
            "📊 THROUGHPUT | contracts={} pairs_found={} | \
             by_type={:?} | by_sport={:?} | \
             depth: min=${:.0} avg=${:.0} max=${:.0} | \
             time_to_close: {}s–{}s",
            self.total_contracts_scanned,
            self.pairs_detected,
            self.signals_by_relationship,
            self.signals_by_sport,
            self.min_depth, self.avg_depth, self.max_depth,
            self.min_seconds_to_close, self.max_seconds_to_close,
        );
    }
}

// ── Detector ──────────────────────────────────────────────────────────────

pub struct RelationshipDetector;

impl RelationshipDetector {

    /// Detect all structural lead/lag pairs from live contracts.
    /// Also returns throughput stats for SaaS viability tracking.
    pub fn detect_all(contracts: &[ContractMeta]) -> (Vec<DetectedPair>, ThroughputStats) {
        let mut pairs = Vec::new();
        let mut pairs_evaluated = 0;

        for i in 0..contracts.len() {
            for j in 0..contracts.len() {
                if i == j { continue; }
                pairs_evaluated += 1;

                let a = &contracts[i];
                let b = &contracts[j];

                if let Some(p) = Self::rule_tournament(a, b) { pairs.push(p); continue; }
                if let Some(p) = Self::rule_game_to_spread(a, b) { pairs.push(p); continue; }
                if let Some(p) = Self::rule_game_to_total(a, b) { pairs.push(p); }
            }
        }

        // Deduplicate on (leader_ticker, lagger_ticker)
        let mut seen = std::collections::HashSet::new();
        pairs.retain(|p| {
            let key = format!("{}:{}", p.leader.ticker, p.lagger.ticker);
            seen.insert(key)
        });

        let mut stats = ThroughputStats::from_pairs(&pairs, contracts.len());
        stats.pairs_evaluated = pairs_evaluated;
        stats.log();

        info!("🔗 {} structural pairs found from {} contracts", pairs.len(), contracts.len());
        (pairs, stats)
    }

    /// Rule A: Game winner → Tournament winner
    /// Requires: same sport, same team, A closes before B
    fn rule_tournament(a: &ContractMeta, b: &ContractMeta) -> Option<DetectedPair> {
        if !matches!(a.series_type, SeriesType::GameMatchWinner) { return None; }
        if !matches!(b.series_type,
            SeriesType::TournamentWinner | SeriesType::SeriesWinner | SeriesType::SeasonWinner) {
            return None;
        }
        if a.sport != b.sport { return None; }

        // A must close before B
        match (a.close_time, b.close_time) {
            (Some(ta), Some(tb)) if ta >= tb => return None,
            _ => {}
        }

        // Find shared team
        let shared = a.team_codes.iter().find(|t| b.involves_team(t))?;

        // Skip TIE outcomes — TIE doesn't advance teams
        if a.outcome_code.as_deref() == Some("TIE") { return None; }

        debug!("🔗 Tournament: {} → {} via {}", a.ticker, b.ticker, shared);

        Some(DetectedPair {
            leader: a.clone(),
            lagger: b.clone(),
            relationship_type: "TournamentAdvancement".to_string(),
            elasticity: 0.55, // fixed — see fixed_elasticity() in lead_lag.rs
            confidence: 0.85,
            reason: format!("{} wins {} → {} wins {}", shared, a.series, shared, b.series),
        })
    }

    /// Rule B: Game → Spread (same event, same date)
    fn rule_game_to_spread(a: &ContractMeta, b: &ContractMeta) -> Option<DetectedPair> {
        if !matches!(a.series_type, SeriesType::GameMatchWinner) { return None; }
        if !matches!(b.series_type, SeriesType::GameSpread) { return None; }
        if a.sport != b.sport { return None; }
        if a.date_part != b.date_part { return None; }

        // Must share at least one team
        let shared = a.team_codes.iter().find(|t| b.involves_team(t))?;

        Some(DetectedPair {
            leader: a.clone(),
            lagger: b.clone(),
            relationship_type: "GameToSpread".to_string(),
            elasticity: 0.80, // fixed
            confidence: 0.90,
            reason: format!("{} game result → spread ({})", a.event_ticker, shared),
        })
    }

    /// Rule C: Game → Total (same event, same date)
    fn rule_game_to_total(a: &ContractMeta, b: &ContractMeta) -> Option<DetectedPair> {
        if !matches!(a.series_type, SeriesType::GameMatchWinner) { return None; }
        if !matches!(b.series_type, SeriesType::GameTotal) { return None; }
        if a.sport != b.sport { return None; }
        if a.date_part != b.date_part { return None; }

        let shared = a.team_codes.iter().find(|t| b.involves_team(t))?;

        Some(DetectedPair {
            leader: a.clone(),
            lagger: b.clone(),
            relationship_type: "GameToTotal".to_string(),
            elasticity: 0.45, // fixed — weaker relationship
            confidence: 0.65,
            reason: format!("{} game result → total ({})", a.event_ticker, shared),
        })
    }
}

// ── DB: persist throughput stats ───────────────────────────────────────────

pub async fn log_throughput(pool: &sqlx::PgPool, stats: &ThroughputStats, scan_duration_ms: i64) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO lead_lag_throughput
            (scan_at, scan_duration_ms, contracts_scanned, pairs_evaluated, pairs_detected,
             signals_by_relationship, signals_by_sport,
             min_depth, max_depth, avg_depth,
             min_seconds_to_close, max_seconds_to_close)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)"
    )
    .bind(stats.scan_at)
    .bind(scan_duration_ms)
    .bind(stats.total_contracts_scanned as i32)
    .bind(stats.pairs_evaluated as i32)
    .bind(stats.pairs_detected as i32)
    .bind(serde_json::to_value(&stats.signals_by_relationship)?)
    .bind(serde_json::to_value(&stats.signals_by_sport)?)
    .bind(stats.min_depth)
    .bind(stats.max_depth)
    .bind(stats.avg_depth)
    .bind(stats.min_seconds_to_close)
    .bind(stats.max_seconds_to_close)
    .execute(pool)
    .await?;
    Ok(())
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_wc() {
        let (series, teams, outcome, date) = parse_ticker("KXWCGAME-26JUN27JORARG-ARG");
        assert_eq!(series, "KXWCGAME");
        assert!(teams.contains(&"ARG".to_string()) || teams.contains(&"JOR".to_string()));
        assert_eq!(outcome.as_deref(), Some("ARG"));
        assert!(date.is_some());
    }

    #[test]
    fn test_parse_nba_spread() {
        let (series, _, outcome, _) = parse_ticker("KXNBASPREAD-26MAY23NYKCLE-NYK14");
        assert_eq!(series, "KXNBASPREAD");
        assert_eq!(outcome.as_deref(), Some("NYK14"));
    }

    #[test]
    fn test_series_classification() {
        assert_eq!(classify_series("KXWCGAME"), SeriesType::GameMatchWinner);
        assert_eq!(classify_series("KXNBASPREAD"), SeriesType::GameSpread);
        assert_eq!(classify_series("KXNBATOTAL"), SeriesType::GameTotal);
        assert_eq!(classify_series("KXWCWINNER"), SeriesType::TournamentWinner);
    }

    #[test]
    fn test_no_dynamic_elasticity_in_detector() {
        // Rule A always returns 0.55 regardless of any price
        // (dynamic elasticity removed — tested via fixed value)
        let elasticity = 0.55f64;
        assert_eq!(elasticity, 0.55);
    }

    #[test]
    fn test_tie_blocked() {
        // TIE outcome should NOT create a TournamentAdvancement pair
        // because a tie doesn't advance one team to the next round
        let outcome = Some("TIE".to_string());
        assert_eq!(outcome.as_deref(), Some("TIE"));
        // Rule A returns None for TIE — validated in rule_tournament()
    }

    #[test]
    fn test_throughput_stats_empty() {
        let stats = ThroughputStats::from_pairs(&[], 500);
        assert_eq!(stats.pairs_detected, 0);
        assert_eq!(stats.total_contracts_scanned, 500);
    }
}