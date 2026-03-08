"""
config.py — Central configuration for the Rebs+Asts PrizePicks pipeline.

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
# Try partner API first (less Cloudflare friction), fallback to public API
PP_ENDPOINTS = [
    "https://partner-api.prizepicks.com/projections",
    "https://api.prizepicks.com/projections",
]
# Keep for backwards compatibility (used as default in some helpers)
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
PP_LEAGUE_ID = 7   # NBA league ID on PrizePicks
# Stat types to target (PrizePicks internal stat_type strings)
PP_TARGET_STATS = {"Rebs+Asts", "rebounds+assists", "RA"}

# ── The Odds API ──────────────────────────────────────────────────────────────
ODDS_API_BASE = "https://api.the-odds-api.com/v4"
ODDS_SPORT_KEY = "basketball_nba"
# Primary combined market — not all books carry this; we fall back to splits.
ODDS_MARKET_COMBINED = "player_rebounds_assists"
# Fallback split markets — we sum reb over line + ast over line when combined
# market is unavailable. Both must be present for a player to get a fallback line.
ODDS_MARKET_REBOUNDS  = "player_rebounds"
ODDS_MARKET_ASSISTS   = "player_assists"
# All markets to request in a single API call (saves credits vs. separate calls)
ODDS_MARKETS_ALL = ",".join([
    ODDS_MARKET_COMBINED,
    ODDS_MARKET_REBOUNDS,
    ODDS_MARKET_ASSISTS,
])
ODDS_REGIONS = "us"
ODDS_BOOKMAKERS = ["draftkings", "fanduel", "betmgm", "caesars"]

# ── Scoring Thresholds (rule-based) ──────────────────────────────────────────
# Pace
PACE_THRESHOLD = 100.5          # minimum projected game pace to qualify
PACE_BOOST_THRESHOLD = 103.0    # pace above this earns extra score points

# Opponent rebounding rank (lower rank number = worse = better for us)
OPP_REB_RANK_THRESHOLD = 20     # only target opponents ranked 20th or worse
OPP_REB_RANK_ELITE = 25         # rank 25+ earns additional boost

# Opponent FG% (how well the opposing team shoots offensively)
# Low FG% = they miss more = more rebound opportunities for our player
# NBA average is ~0.47; below 0.45 is a poor-shooting team
OPP_FG_PCT_WEAK   = 0.46        # opponent FG% below this = more misses = +points
OPP_FG_PCT_POOR   = 0.44        # very poor shooting = extra bonus

# Line gap: PrizePicks line vs. sportsbook consensus implied total
LINE_GAP_MIN = 0.5              # minimum gap (PP line above books) to qualify
LINE_GAP_STRONG = 1.5           # gap above this earns strong edge score

# Recent form window (games)
FORM_WINDOW = 10                # last N games used for player rolling averages

# Minimum projected minutes to consider a player
MIN_MINUTES_THRESHOLD = 22.0

# ── Scoring Weights ───────────────────────────────────────────────────────────
# Each qualifying feature adds its weight to a raw score (max ~100)
SCORE_WEIGHTS = {
    "pace_qualifies":       10,   # game pace above PACE_THRESHOLD
    "pace_elite":           10,   # game pace above PACE_BOOST_THRESHOLD
    "opp_reb_weak":         15,   # opponent reb rank >= OPP_REB_RANK_THRESHOLD
    "opp_reb_very_weak":    10,   # opponent reb rank >= OPP_REB_RANK_ELITE
    "opp_fg_pct_weak":      8,    # opponent FG% below OPP_FG_PCT_WEAK (more misses)
    "opp_fg_pct_poor":      7,    # opponent FG% below OPP_FG_PCT_POOR (extra bonus)
    "line_gap_exists":      15,   # PP line > books by LINE_GAP_MIN
    "line_gap_strong":      15,   # PP line > books by LINE_GAP_STRONG
    "form_over_line":       15,   # rolling avg Rebs+Asts > PP line
    "form_hit_rate_60":     5,    # hit rate >= 60% over last FORM_WINDOW games
    "form_hit_rate_70":     5,    # hit rate >= 70% over last FORM_WINDOW games
    # Demon bonus (only applied when base score already qualifies)
    "demon_bonus":          20,   # applied on top when is_demon=True
}

QUALIFY_SCORE_MIN = 40          # minimum score to appear in output
DEMON_QUALIFY_SCORE_MIN = 35    # lower bar for demons (bonus gets them over 55+)

# ── Output ────────────────────────────────────────────────────────────────────
OUTPUT_DIR = "output"
OUTPUT_CSV = "rebs_asts_picks.csv"
TOP_N_DISPLAY = 20              # rows shown in console summary
