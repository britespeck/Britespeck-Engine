//! Contract Parser — extracts structured data from prediction market contract titles.
//!
//! Parses titles like:
//!   "Pistons vs. Cavaliers — Ausar Thompson: Rebounds O/U 1.5"
//!   "Celtics vs Heat — Jayson Tatum: Points O/U 28.5"
//!   "Barcelona vs Real Madrid — Result"
//!   "Djokovic vs Alcaraz — Match Winner"
//!   "Will Bitcoin hit $150k by June?"
//!
//! Returns structured ContractInfo used by player_stats.rs and the OMG Analyst.

use serde::{Deserialize, Serialize};

// ── Contract Types ─────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ContractCategory {
    NbaPlayerProp,
    NbaGame,
    SoccerGame,
    TennisMatch,
    NflPlayerProp,
    NflGame,
    MlbPlayerProp,
    MlbGame,
    NhlGame,
    NhlPlayerProp,
    CryptoPrice,
    PoliticalEvent,
    EconomicEvent,
    FedRates,
    WeatherEvent,
    MentionsEarnings,
    F1Race,
    GolfTournament,
    Other,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum StatType {
    Points,
    Rebounds,
    Assists,
    Steals,
    Blocks,
    Threes,
    PointsReboundsAssists,
    PointsAssists,
    PointsRebounds,
    ReboundsAssists,
    Turnovers,
    Goals,
    SoccerAssists,
    Aces,
    DoubleFaults,
    PassingYards,
    ReceivingYards,
    RushingYards,
    Touchdowns,
    HomeRuns,
    StrikeOuts,
    Hits,
    Rbi,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerPropInfo {
    pub player_name: String,
    pub stat_type: StatType,
    pub line: f64,
    pub is_over: bool, // true = Over, false = Under, None = both sides available
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractInfo {
    pub category: ContractCategory,
    pub sport: Option<String>,
    pub home_team: Option<String>,
    pub away_team: Option<String>,
    pub player_prop: Option<PlayerPropInfo>,
    pub is_prop_bet: bool,
    pub raw_title: String,
}

// ── NBA Team Name Mappings ─────────────────────────────────────────

fn nba_team_names() -> Vec<(&'static str, &'static str)> {
    vec![
        ("pistons", "Detroit Pistons"),
        ("cavaliers", "Cleveland Cavaliers"),
        ("celtics", "Boston Celtics"),
        ("heat", "Miami Heat"),
        ("lakers", "Los Angeles Lakers"),
        ("warriors", "Golden State Warriors"),
        ("bucks", "Milwaukee Bucks"),
        ("nuggets", "Denver Nuggets"),
        ("suns", "Phoenix Suns"),
        ("76ers", "Philadelphia 76ers"),
        ("sixers", "Philadelphia 76ers"),
        ("knicks", "New York Knicks"),
        ("nets", "Brooklyn Nets"),
        ("raptors", "Toronto Raptors"),
        ("bulls", "Chicago Bulls"),
        ("pacers", "Indiana Pacers"),
        ("hawks", "Atlanta Hawks"),
        ("wizards", "Washington Wizards"),
        ("hornets", "Charlotte Hornets"),
        ("magic", "Orlando Magic"),
        ("thunder", "Oklahoma City Thunder"),
        ("blazers", "Portland Trail Blazers"),
        ("jazz", "Utah Jazz"),
        ("timberwolves", "Minnesota Timberwolves"),
        ("rockets", "Houston Rockets"),
        ("spurs", "San Antonio Spurs"),
        ("mavs", "Dallas Mavericks"),
        ("mavericks", "Dallas Mavericks"),
        ("grizzlies", "Memphis Grizzlies"),
        ("pelicans", "New Orleans Pelicans"),
        ("kings", "Sacramento Kings"),
        ("clippers", "Los Angeles Clippers"),
    ]
}

// ── Stat Type Parser ───────────────────────────────────────────────

fn parse_stat_type(s: &str) -> StatType {
    let lower = s.to_lowercase();
    if lower.contains("pts+reb+ast") || lower.contains("points+rebounds+assists")
        || lower.contains("pra") {
        return StatType::PointsReboundsAssists;
    }
    if lower.contains("pts+ast") || lower.contains("points+assists") {
        return StatType::PointsAssists;
    }
    if lower.contains("pts+reb") || lower.contains("points+rebounds") {
        return StatType::PointsRebounds;
    }
    if lower.contains("reb+ast") || lower.contains("rebounds+assists") {
        return StatType::ReboundsAssists;
    }
    if lower.contains("point") || lower.contains("pts") || lower == "p" {
        return StatType::Points;
    }
    if lower.contains("rebound") || lower.contains("reb") || lower == "r" {
        return StatType::Rebounds;
    }
    if lower.contains("assist") || lower.contains("ast") || lower == "a" {
        return StatType::Assists;
    }
    if lower.contains("steal") || lower.contains("stl") {
        return StatType::Steals;
    }
    if lower.contains("block") || lower.contains("blk") {
        return StatType::Blocks;
    }
    if lower.contains("three") || lower.contains("3pt") || lower.contains("3-pt")
        || lower.contains("threes") {
        return StatType::Threes;
    }
    if lower.contains("turnover") || lower.contains("tov") {
        return StatType::Turnovers;
    }
    if lower.contains("ace") {
        return StatType::Aces;
    }
    if lower.contains("double fault") {
        return StatType::DoubleFaults;
    }
    if lower.contains("goal") {
        return StatType::Goals;
    }
    if lower.contains("passing yard") || lower.contains("pass yard") {
        return StatType::PassingYards;
    }
    if lower.contains("receiving yard") || lower.contains("rec yard") {
        return StatType::ReceivingYards;
    }
    if lower.contains("rushing yard") || lower.contains("rush yard") {
        return StatType::RushingYards;
    }
    if lower.contains("touchdown") || lower.contains("td") {
        return StatType::Touchdowns;
    }
    if lower.contains("home run") || lower.contains("hr") {
        return StatType::HomeRuns;
    }
    if lower.contains("strikeout") || lower.contains("k") {
        return StatType::StrikeOuts;
    }
    if lower.contains("hit") {
        return StatType::Hits;
    }
    if lower.contains("rbi") {
        return StatType::Rbi;
    }
    StatType::Unknown
}

// ── Main Parser ────────────────────────────────────────────────────

pub fn parse_contract(title: &str) -> ContractInfo {
    let lower = title.to_lowercase();

    // ── Detect NBA player prop ─────────────────────────────────────
    // Pattern: "Team1 vs Team2 — Player Name: Stat O/U Line"
    // or: "Player Name: Stat O/U Line"
    if let Some(prop) = parse_player_prop(title, &lower) {
        let (home, away) = extract_teams(title, &lower);
        let sport = detect_sport(&lower);
        let category = match sport.as_deref() {
            Some("nba") => ContractCategory::NbaPlayerProp,
            Some("nfl") => ContractCategory::NflPlayerProp,
            Some("mlb") => ContractCategory::MlbPlayerProp,
            Some("nhl") => ContractCategory::NhlPlayerProp,
            _ => ContractCategory::NbaPlayerProp,
        };
        return ContractInfo {
            category,
            sport,
            home_team: home,
            away_team: away,
            player_prop: Some(prop),
            is_prop_bet: true,
            raw_title: title.to_string(),
        };
    }

    // ── Detect Soccer ──────────────────────────────────────────────
    if lower.contains("fc ") || lower.contains(" fc") || lower.contains(" cf ")
        || lower.contains("united") || lower.contains("city") && lower.contains(" vs ")
        || is_soccer_league(&lower) {
        let (home, away) = extract_teams(title, &lower);
        return ContractInfo {
            category: ContractCategory::SoccerGame,
            sport: Some("soccer".to_string()),
            home_team: home,
            away_team: away,
            player_prop: None,
            is_prop_bet: false,
            raw_title: title.to_string(),
        };
    }

    // ── Detect Tennis ──────────────────────────────────────────────
    if is_tennis_match(&lower) {
        let (p1, p2) = extract_tennis_players(title);
        return ContractInfo {
            category: ContractCategory::TennisMatch,
            sport: Some("tennis".to_string()),
            home_team: p1,
            away_team: p2,
            player_prop: None,
            is_prop_bet: false,
            raw_title: title.to_string(),
        };
    }

    // ── Detect NBA game ────────────────────────────────────────────
    if is_nba_game(&lower) {
        let (home, away) = extract_teams(title, &lower);
        return ContractInfo {
            category: ContractCategory::NbaGame,
            sport: Some("nba".to_string()),
            home_team: home,
            away_team: away,
            player_prop: None,
            is_prop_bet: false,
            raw_title: title.to_string(),
        };
    }

    // ── Detect Crypto ──────────────────────────────────────────────
    if lower.contains("bitcoin") || lower.contains("btc") || lower.contains("ethereum")
        || lower.contains("eth") || lower.contains("solana") || lower.contains("crypto") {
        return ContractInfo {
            category: ContractCategory::CryptoPrice,
            sport: None,
            home_team: None,
            away_team: None,
            player_prop: None,
            is_prop_bet: false,
            raw_title: title.to_string(),
        };
    }

    // ── Detect Political ──────────────────────────────────────────
    if lower.contains("president") || lower.contains("election") || lower.contains("congress")
        || lower.contains("senate") || lower.contains("impeach") || lower.contains("trump")
        || lower.contains("biden") || lower.contains("harris") || lower.contains("vote") {
        return ContractInfo {
            category: ContractCategory::PoliticalEvent,
            sport: None,
            home_team: None,
            away_team: None,
            player_prop: None,
            is_prop_bet: false,
            raw_title: title.to_string(),
        };
    }

    // ── Detect Weather ────────────────────────────────────────────
    if lower.contains("temperature") || lower.contains("degrees")
        || lower.contains("°f") || lower.contains("°c")
        || lower.contains("snow") && !lower.contains("snowboard")
        || lower.contains("hurricane") || lower.contains("tornado")
        || lower.contains("rainfall") || lower.contains("heatwave")
        || (lower.contains("weather") && !lower.contains("weathered")) {
        return ContractInfo {
            category: ContractCategory::WeatherEvent,
            sport: None,
            home_team: None,
            away_team: None,
            player_prop: None,
            is_prop_bet: false,
            raw_title: title.to_string(),
        };
    }

    // ── Detect Mentions/Earnings ───────────────────────────────────
    if lower.contains("earnings") || lower.contains("quarterly")
        || lower.contains("conference call") || lower.contains("buyback")
        || lower.contains("stock buyback") || lower.contains("ipo")
        || lower.contains("mention") || lower.contains("annual report")
        || (lower.contains("dividend") && lower.contains("announce"))
        || (lower.contains("guidance") && lower.contains("raise")) {
        return ContractInfo {
            category: ContractCategory::MentionsEarnings,
            sport: None,
            home_team: None,
            away_team: None,
            player_prop: None,
            is_prop_bet: false,
            raw_title: title.to_string(),
        };
    }

    // ── Detect F1 ──────────────────────────────────────────────────
    if lower.contains("formula 1") || lower.contains("formula1") || lower.contains("f1")
        || lower.contains("grand prix") || lower.contains("gp ") || lower.contains(" gp")
        || lower.contains("verstappen") || lower.contains("hamilton") || lower.contains("leclerc")
        || lower.contains("norris") || lower.contains("sainz") || lower.contains("perez")
        || lower.contains("ferrari") || lower.contains("red bull racing") || lower.contains("mclaren f1") {
        return ContractInfo {
            category: ContractCategory::F1Race,
            sport: Some("f1".to_string()),
            home_team: None,
            away_team: None,
            player_prop: None,
            is_prop_bet: false,
            raw_title: title.to_string(),
        };
    }

    // ── Detect Golf ────────────────────────────────────────────────
    if lower.contains("masters") || lower.contains("pga championship")
        || lower.contains("u.s. open") || lower.contains("british open")
        || lower.contains("the open") || lower.contains("ryder cup")
        || lower.contains("pga tour") || lower.contains("lpga")
        || lower.contains("golf") || lower.contains("birdie") || lower.contains("bogey")
        || lower.contains("under par") || lower.contains("round leader")
        || lower.contains("tiger") || lower.contains("scheffler") || lower.contains("mcilroy") {
        return ContractInfo {
            category: ContractCategory::GolfTournament,
            sport: Some("golf".to_string()),
            home_team: None,
            away_team: None,
            player_prop: None,
            is_prop_bet: false,
            raw_title: title.to_string(),
        };
    }

    // ── Detect Fed/Economic ────────────────────────────────────────
    if lower.contains("fed rate") || lower.contains("federal reserve") || lower.contains("fomc")
        || lower.contains("interest rate") || lower.contains("rate cut") || lower.contains("rate hike")
        || lower.contains("basis point") || lower.contains("bps cut") || lower.contains("bps hike") {
        return ContractInfo {
            category: ContractCategory::FedRates,
            sport: None,
            home_team: None,
            away_team: None,
            player_prop: None,
            is_prop_bet: false,
            raw_title: title.to_string(),
        };
    }

    // ── Default ────────────────────────────────────────────────────
    ContractInfo {
        category: ContractCategory::Other,
        sport: detect_sport(&lower),
        home_team: None,
        away_team: None,
        player_prop: None,
        is_prop_bet: false,
        raw_title: title.to_string(),
    }
}

// ── Player Prop Parser ─────────────────────────────────────────────

fn parse_player_prop(title: &str, lower: &str) -> Option<PlayerPropInfo> {
    // Look for O/U pattern with a number
    // Patterns: "O/U 28.5", "Over/Under 1.5", "Pts O/U 20"
    let ou_regex_patterns = [
        r":\s*(.+?)\s+[Oo]/[Uu]\s+([\d.]+)",
        r":\s*(.+?)\s+[Oo]ver/[Uu]nder\s+([\d.]+)",
        r"—\s*(.+?):\s*(.+?)\s+[Oo]/[Uu]\s+([\d.]+)",
    ];

    // Simple approach: find "O/U" or "Over/Under" in title
    let ou_pos = lower.find("o/u").or_else(|| lower.find("over/under"))?;

    // Extract the line number after O/U
    let after_ou = &title[ou_pos..];
    let line_str: String = after_ou.chars()
        .skip_while(|c| !c.is_ascii_digit())
        .take_while(|c| c.is_ascii_digit() || *c == '.')
        .collect();
    let line: f64 = line_str.parse().ok()?;

    // Extract stat type — what's between ":" and "O/U"
    let colon_pos = title.rfind(':')?;
    let between = &title[colon_pos + 1..ou_pos].trim().to_string();
    let stat_type = parse_stat_type(between);

    // Extract player name — what's between "—" and ":"
    let player_name = if let Some(dash_pos) = title.rfind('—') {
        title[dash_pos + 1..colon_pos].trim().to_string()
    } else if let Some(dash_pos) = title.find(" - ") {
        title[dash_pos + 3..colon_pos].trim().to_string()
    } else {
        // No team separator — whole thing before colon is player name
        title[..colon_pos].trim().to_string()
    };

    if player_name.is_empty() || player_name.len() < 3 {
        return None;
    }

    Some(PlayerPropInfo {
        player_name,
        stat_type,
        line,
        is_over: true, // default — caller can check which side they're on
    })
}

// ── Helper Functions ───────────────────────────────────────────────

fn extract_teams(title: &str, lower: &str) -> (Option<String>, Option<String>) {
    // Split on " vs ", " vs. ", " @ ", " at "
    let separators = [" vs. ", " vs ", " @ ", " at "];
    for sep in &separators {
        if let Some(pos) = lower.find(sep) {
            // Take everything before separator as away, after as home
            let before = title[..pos].trim();
            let after_start = pos + sep.len();
            // After separator, take until " — " or ":" or end
            let after_raw = &title[after_start..];
            let after = after_raw.split(" — ").next()
                .or_else(|| after_raw.split(" - ").next())
                .unwrap_or(after_raw)
                .trim();

            // Clean up common prefixes like "Game 1: "
            let clean_before = before.split(':').last().unwrap_or(before).trim();
            let clean_after = after.split(':').next().unwrap_or(after).trim();

            return (Some(clean_after.to_string()), Some(clean_before.to_string()));
        }
    }
    (None, None)
}

fn extract_tennis_players(title: &str) -> (Option<String>, Option<String>) {
    // Tennis: "Djokovic vs Alcaraz" or "Djokovic vs. Alcaraz"
    let separators = [" vs. ", " vs "];
    for sep in &separators {
        let lower = title.to_lowercase();
        if let Some(pos) = lower.find(sep) {
            let p1 = title[..pos].trim().to_string();
            let p2 = title[pos + sep.len()..].trim().to_string();
            return (Some(p1), Some(p2));
        }
    }
    (None, None)
}

fn detect_sport(lower: &str) -> Option<String> {
    if lower.contains("nba") || is_nba_game(lower) {
        return Some("nba".to_string());
    }
    if lower.contains("nfl") || lower.contains("quarterback") || lower.contains("touchdown")
        || lower.contains("passing yard") || lower.contains("rushing yard")
        || lower.contains("receiving yard") || is_nfl_team(lower) {
        return Some("nfl".to_string());
    }
    if lower.contains("mlb") || lower.contains("strikeout") || lower.contains("home run")
        || lower.contains("rbi") || lower.contains("earned run") || is_mlb_team(lower) {
        return Some("mlb".to_string());
    }
    if lower.contains("nhl") || lower.contains("hockey") || lower.contains("puck")
        || is_nhl_team(lower) {
        return Some("nhl".to_string());
    }
    if is_soccer_league(lower) || lower.contains("fc ") {
        return Some("soccer".to_string());
    }
    if is_tennis_match(lower) {
        return Some("tennis".to_string());
    }
    None
}

fn is_nfl_team(lower: &str) -> bool {
    let nfl_teams = ["chiefs", "eagles", "cowboys", "patriots", "packers", "bears",
        "giants", "jets", "bills", "dolphins", "ravens", "steelers", "browns",
        "bengals", "texans", "colts", "jaguars", "titans", "broncos", "raiders",
        "chargers", "seahawks", "rams", "49ers", "cardinals", "falcons", "panthers",
        "saints", "buccaneers", "vikings", "lions", "commanders", "redskins"];
    nfl_teams.iter().filter(|t| lower.contains(*t)).count() >= 2
}

fn is_mlb_team(lower: &str) -> bool {
    let mlb_teams = ["yankees", "red sox", "dodgers", "cubs", "cardinals", "braves",
        "mets", "phillies", "giants", "astros", "rangers", "athletics", "mariners",
        "angels", "padres", "rockies", "diamondbacks", "brewers", "pirates",
        "reds", "nationals", "marlins", "orioles", "rays", "blue jays", "twins",
        "white sox", "royals", "tigers", "guardians"];
    mlb_teams.iter().filter(|t| lower.contains(*t)).count() >= 1
}

fn is_nhl_team(lower: &str) -> bool {
    let nhl_teams = ["bruins", "sabres", "canadiens", "senators", "maple leafs",
        "hurricanes", "blue jackets", "red wings", "panthers", "lightning",
        "capitals", "blackhawks", "predators", "blues", "jets", "stars",
        "wild", "avalanche", "ducks", "flames", "oilers", "kings", "sharks",
        "canucks", "golden knights", "kraken", "rangers", "islanders", "devils",
        "flyers", "penguins"];
    nhl_teams.iter().filter(|t| lower.contains(*t)).count() >= 1
}

fn is_nba_game(lower: &str) -> bool {
    let nba_teams = nba_team_names();
    let team_count = nba_teams.iter()
        .filter(|(abbr, _)| lower.contains(abbr))
        .count();
    team_count >= 2 || lower.contains("nba") || lower.contains("playoff")
}

fn is_soccer_league(lower: &str) -> bool {
    lower.contains("premier league") || lower.contains("la liga") || lower.contains("serie a")
        || lower.contains("bundesliga") || lower.contains("ligue 1") || lower.contains("champions league")
        || lower.contains("europa league") || lower.contains("mls") || lower.contains("copa")
        || lower.contains("eredivisie") || lower.contains("primeira liga")
}

fn is_tennis_match(lower: &str) -> bool {
    (lower.contains("atp") || lower.contains("wta") || lower.contains("grand slam")
        || lower.contains("wimbledon") || lower.contains("roland garros")
        || lower.contains("us open") || lower.contains("australian open")
        || lower.contains("french open") || lower.contains("itf"))
        || (lower.contains(" vs ") && !lower.contains("nba") && !lower.contains("nfl")
            && !lower.contains("fc") && !lower.contains("united")
            && !lower.contains("mlb") && !lower.contains("nhl")
            && lower.split_whitespace().count() <= 6) // short title = likely tennis
}

// ── Stat Display Name ──────────────────────────────────────────────

pub fn stat_display_name(stat: &StatType) -> &'static str {
    match stat {
        StatType::Points => "points",
        StatType::Rebounds => "rebounds",
        StatType::Assists => "assists",
        StatType::Steals => "steals",
        StatType::Blocks => "blocks",
        StatType::Threes => "three-pointers",
        StatType::PointsReboundsAssists => "points+rebounds+assists",
        StatType::PointsAssists => "points+assists",
        StatType::PointsRebounds => "points+rebounds",
        StatType::ReboundsAssists => "rebounds+assists",
        StatType::Turnovers => "turnovers",
        StatType::Goals => "goals",
        StatType::SoccerAssists => "assists",
        StatType::Aces => "aces",
        StatType::DoubleFaults => "double faults",
        StatType::PassingYards => "passing yards",
        StatType::ReceivingYards => "receiving yards",
        StatType::RushingYards => "rushing yards",
        StatType::Touchdowns => "touchdowns",
        StatType::HomeRuns => "home runs",
        StatType::StrikeOuts => "strikeouts",
        StatType::Hits => "hits",
        StatType::Rbi => "RBI",
        StatType::Unknown => "stats",
    }
}