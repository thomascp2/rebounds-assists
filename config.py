"""
config.py — Central configuration for the PrizePicks NBA prop scoring pipeline.

Fill in your API keys before running. All scoring thresholds and weights
can be tuned here without touching pipeline logic.
"""

# ── API Keys ──────────────────────────────────────────────────────────────────
ODDS_API_KEY = "c02e47a4bcf4c5edb0211c129595b0bb"   # https://the-odds-api.com

# ── NBA Stats API ─────────────────────────────────────────────────────────────
NBA_STATS_BASE = "https://stats.nba.com/stats"
NBA_STATS_HEADERS = {
    "Host": "stats.nba.com",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token": "true",
    "Referer": "https://www.nba.com/",
    "Connection": "keep-alive",
    "Origin": "https://www.nba.com",
}
CURRENT_SEASON = "2025-26"
SEASON_TYPE = "Regular Season"

# ── PrizePicks ────────────────────────────────────────────────────────────────
PP_ENDPOINTS = [
    "https://partner-api.prizepicks.com/projections",
    "https://api.prizepicks.com/projections",
]
PP_PROJECTIONS_URL = PP_ENDPOINTS[0]
PP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json; charset=UTF-8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://app.prizepicks.com/",
    "Origin": "https://app.prizepicks.com",
    "Connection": "keep-alive",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
}
PP_LEAGUE_ID = 7

# Original Rebs+Asts target (kept for backwards compat)
PP_TARGET_STATS = {"Rebs+Asts", "rebounds+assists", "RA"}

# Expanded — all 5 stat categories
PP_TARGET_STATS_EXPANDED = {
    "Rebs+Asts", "rebounds+assists", "RA",
    "Points", "points", "PTS",
    "Rebounds", "rebounds", "REB",
    "Assists", "assists", "AST",
    "3-Pointers Made", "3PM", "3-pointers made", "3 pointers made",
}

# ── The Odds API ──────────────────────────────────────────────────────────────
ODDS_API_BASE = "https://api.the-odds-api.com/v4"
ODDS_SPORT_KEY = "basketball_nba"
ODDS_MARKET_COMBINED  = "player_rebounds_assists"
ODDS_MARKET_REBOUNDS  = "player_rebounds"
ODDS_MARKET_ASSISTS   = "player_assists"
ODDS_MARKET_POINTS    = "player_points"
ODDS_MARKET_3PM       = "player_threes"
ODDS_MARKETS_ALL = ",".join([
    "player_rebounds_assists",
    "player_rebounds",
    "player_assists",
    "player_points",
    "player_threes",
])
ODDS_REGIONS = "us"
ODDS_BOOKMAKERS = ["draftkings", "fanduel", "betmgm", "caesars"]

# Maps Odds API market key → normalized stat category
ODDS_MARKET_TO_STAT = {
    "player_rebounds_assists": "Rebs+Asts",
    "player_rebounds":         "Rebounds",
    "player_assists":          "Assists",
    "player_points":           "Points",
    "player_threes":           "3PM",
}

# ── EV Framework ──────────────────────────────────────────────────────────────
# Conservative per-leg effective multipliers
EV_MULTIPLIER_GOBLIN   = 1.5
EV_MULTIPLIER_STANDARD = 3.0
EV_MULTIPLIER_DEMON    = 5.0

# Break-even hit rates (what P_hit must exceed to be +EV)
BREAKEVEN_GOBLIN   = 0.67
BREAKEVEN_STANDARD = 0.58
BREAKEVEN_DEMON    = 0.50

# Required hit rates to qualify (above break-even for cushion)
HIT_RATE_REQUIRED_GOBLIN   = 0.72
HIT_RATE_REQUIRED_STANDARD = 0.60
HIT_RATE_REQUIRED_DEMON    = 0.50

# ── Stat Variance Tiers ───────────────────────────────────────────────────────
VARIANCE_TIER = {
    "Rebs+Asts": "medium",
    "Rebounds":  "low",
    "Points":    "low_medium",
    "Assists":   "medium",
    "3PM":       "high",
}

# Rolling form window (games) by variance tier
FORM_WINDOW_BY_VARIANCE = {
    "low":        7,
    "low_medium": 10,
    "medium":     10,
    "high":       15,
}

# ── Scoring Thresholds — Shared ───────────────────────────────────────────────
PACE_THRESHOLD       = 100.5
PACE_BOOST_THRESHOLD = 103.0
LINE_GAP_MIN         = 0.5
LINE_GAP_STRONG      = 1.5
FORM_WINDOW          = 10       # legacy default; per-stat uses FORM_WINDOW_BY_VARIANCE
MIN_MINUTES_THRESHOLD = 22.0

# ── Scoring Thresholds — Rebs+Asts / Rebounds ─────────────────────────────────
OPP_REB_RANK_THRESHOLD = 20
OPP_REB_RANK_ELITE     = 25
OPP_FG_PCT_WEAK        = 0.46
OPP_FG_PCT_POOR        = 0.44

# ── Scoring Thresholds — Points ───────────────────────────────────────────────
OPP_DEF_RANK_WEAK = 20     # opp pts-allowed rank 20+ = weak defense
OPP_DEF_RANK_POOR = 25     # rank 25+ = very weak
MIN_USG_PCT       = 0.22   # minimum usage rate to qualify

# ── Scoring Thresholds — 3PM ──────────────────────────────────────────────────
OPP_3PA_RANK_THRESHOLD = 20    # opponent 3PA-allowed rank 20+ qualifies
OPP_3PA_RANK_ELITE     = 25
MIN_3PA_RATE           = 6.0   # player must average 6+ 3PA/game
MIN_3PT_PCT_RECENT     = 0.35  # recent 3PT% floor (not in cold streak)
HOT_STREAK_3PT_PCT     = 0.40  # confirmed hot streak

# ── Scoring Thresholds — Assists ──────────────────────────────────────────────
MIN_AST_PCT          = 0.25    # primary ball handler threshold (AST_PCT)
OPP_AST_RANK_WEAK    = 20      # opponent AST-allowed rank 20+
OPP_PACE_ASSISTS_MIN = 101.0

# ── Scoring Weights — Rebs+Asts (unchanged) ───────────────────────────────────
SCORE_WEIGHTS = {
    "pace_qualifies":       10,
    "pace_elite":           10,
    "opp_reb_weak":         15,
    "opp_reb_very_weak":    10,
    "opp_fg_pct_weak":      8,
    "opp_fg_pct_poor":      7,
    "line_gap_exists":      15,
    "line_gap_strong":      15,
    "form_over_line":       15,
    "form_hit_rate_60":     5,
    "form_hit_rate_70":     5,
    "demon_bonus":          20,
}

# ── Scoring Weights — Points ───────────────────────────────────────────────────
SCORE_WEIGHTS_POINTS = {
    "pace_qualifies":   10,
    "opp_def_weak":     15,
    "opp_def_poor":     10,
    "line_gap_exists":  15,
    "line_gap_strong":  15,
    "form_over_line":   15,
    "form_hit_rate_60": 5,
    "form_hit_rate_70": 5,
    "demon_bonus":      20,
}

# ── Scoring Weights — Rebounds ────────────────────────────────────────────────
SCORE_WEIGHTS_REBOUNDS = {
    "pace_qualifies":    10,
    "opp_reb_weak":      15,
    "opp_reb_very_weak": 10,
    "opp_fg_pct_weak":   8,
    "opp_fg_pct_poor":   7,
    "line_gap_exists":   15,
    "line_gap_strong":   15,
    "form_over_line":    15,
    "form_hit_rate_60":  5,
    "form_hit_rate_70":  5,
    "demon_bonus":       20,
}

# ── Scoring Weights — Assists ─────────────────────────────────────────────────
SCORE_WEIGHTS_ASSISTS = {
    "pace_qualifies":    10,
    "pace_elite":        10,
    "opp_ast_weak":      15,
    "line_gap_exists":   15,
    "line_gap_strong":   15,
    "form_over_line":    15,
    "form_hit_rate_60":  5,
    "form_hit_rate_70":  5,
    "demon_bonus":       20,
}

# ── Scoring Weights — 3PM ─────────────────────────────────────────────────────
SCORE_WEIGHTS_3PM = {
    "pace_qualifies":      10,
    "opp_3pa_rank_weak":   15,
    "opp_3pa_rank_elite":  10,
    "hot_streak":          10,
    "line_gap_exists":     15,
    "line_gap_strong":     15,
    "form_over_line":      15,
    "form_hit_rate_60":    5,
    "form_hit_rate_70":    5,
    "demon_bonus":         20,
}

# ── Trend Detection ───────────────────────────────────────────────────────────
# trend_pct = (L5_avg - L15_avg) / L15_avg
TREND_UP_THRESHOLD   =  0.15   # L5 >= 15% above L15 → confirmed uptrend
TREND_DOWN_THRESHOLD = -0.15   # L5 >= 15% below L15 → confirmed downtrend
TREND_BONUS          =  10     # score bonus for confirmed valid uptrend
TREND_PENALTY        = -15     # score penalty for confirmed downtrend (applies regardless of validity check)

# ── Qualify Thresholds ────────────────────────────────────────────────────────
QUALIFY_SCORE_MIN      = 40
DEMON_QUALIFY_SCORE_MIN = 35

# ── Output ────────────────────────────────────────────────────────────────────
OUTPUT_DIR    = "output"
OUTPUT_CSV    = "nba_picks.csv"
TOP_N_DISPLAY = 20
