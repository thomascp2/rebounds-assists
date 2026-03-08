"""
features/engineer.py — Merges all data sources and engineers scoring features
for all 5 stat categories: Rebs+Asts, Points, Rebounds, Assists, 3PM.
"""

import logging
import unicodedata
from difflib import get_close_matches

import pandas as pd

from config import (
    FORM_WINDOW,
    MIN_MINUTES_THRESHOLD,
    VARIANCE_TIER,
    FORM_WINDOW_BY_VARIANCE,
)

logger = logging.getLogger(__name__)


# ── Name Normalization ────────────────────────────────────────────────────────

_NAME_SUFFIXES = {" jr", " sr", " ii", " iii", " iv", " v"}


def _ascii_fold(name: str) -> str:
    """Normalize Unicode diacritics to plain ASCII (Dončić → doncic)."""
    return (
        unicodedata.normalize("NFKD", name)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
        .strip()
    )


def _strip_suffix(name: str) -> str:
    for suffix in _NAME_SUFFIXES:
        if name.endswith(suffix):
            return name[: -len(suffix)].strip()
    return name


def _build_name_lookup(active_players: pd.DataFrame) -> dict[str, int]:
    lookup = {}
    for _, row in active_players.iterrows():
        raw = str(row.get("DISPLAY_FIRST_LAST", "")).strip()
        pid = int(row["PERSON_ID"])
        if not raw:
            continue
        for variant in {raw.lower(), _ascii_fold(raw)}:
            lookup[variant] = pid
            stripped = _strip_suffix(variant)
            if stripped != variant:
                lookup[stripped] = pid
    return lookup


def _match_player_name(pp_name: str, lookup: dict[str, int]) -> int | None:
    normalized = pp_name.strip().lower()
    folded = _ascii_fold(pp_name)

    if normalized in lookup:
        return lookup[normalized]
    if folded in lookup:
        logger.debug("ASCII-folded match '%s' → '%s'", pp_name, folded)
        return lookup[folded]
    for candidate in {_strip_suffix(normalized), _strip_suffix(folded)}:
        if candidate in lookup:
            logger.debug("Suffix-stripped match '%s' → '%s'", pp_name, candidate)
            return lookup[candidate]
    matches = get_close_matches(folded, lookup.keys(), n=1, cutoff=0.82)
    if matches:
        logger.debug("Fuzzy matched '%s' → '%s'", pp_name, matches[0])
        return lookup[matches[0]]

    logger.warning("Could not match player name: '%s'", pp_name)
    return None


# ── Stat Category Normalization ───────────────────────────────────────────────

_STAT_CATEGORY_MAP = {
    "reboundsassists":    "Rebs+Asts",
    "rebsasts":           "Rebs+Asts",
    "ra":                 "Rebs+Asts",
    "reboundsplusassists":"Rebs+Asts",
    "points":             "Points",
    "pts":                "Points",
    "rebounds":           "Rebounds",
    "reb":                "Rebounds",
    "assists":            "Assists",
    "ast":                "Assists",
    "3pointsmade":        "3PM",
    "3pm":                "3PM",
    "3pointersmade":      "3PM",
    "threepointersmade":  "3PM",
    "threes":             "3PM",
}

# Game-log column for each stat category
_STAT_LOG_COL = {
    "Rebs+Asts": "RA",
    "Points":    "PTS",
    "Rebounds":  "REB",
    "Assists":   "AST",
    "3PM":       "FG3M",
}


def _normalize_stat_category(stat_type: str) -> str:
    key = (
        stat_type.lower()
        .replace(" ", "")
        .replace("+", "")
        .replace("-", "")
    )
    return _STAT_CATEGORY_MAP.get(key, stat_type)


# ── Game Pace ─────────────────────────────────────────────────────────────────

def compute_game_pace(pp_board: pd.DataFrame, team_pace: pd.DataFrame) -> pd.DataFrame:
    pace_map = {}
    if not team_pace.empty and "TEAM_NAME" in team_pace.columns:
        pace_map = dict(zip(team_pace["TEAM_NAME"].str.upper(), team_pace["PACE"]))
    if not team_pace.empty and "TEAM_ABBREVIATION" in team_pace.columns:
        for _, row in team_pace.iterrows():
            pace_map[str(row["TEAM_ABBREVIATION"]).upper()] = row["PACE"]

    def _lookup(team_str):
        return pace_map.get(str(team_str).strip().upper())

    df = pp_board.copy()
    df["player_team_pace"] = df["team"].apply(_lookup)
    df["opp_team_pace"] = df.apply(
        lambda r: _lookup(r["away_team"])
        if r["team"].upper() == r["home_team"].upper()
        else _lookup(r["home_team"]),
        axis=1,
    )
    df["projected_game_pace"] = (
        (df["player_team_pace"].fillna(100) + df["opp_team_pace"].fillna(100)) / 2
    )
    return df


# ── Opponent Helper ───────────────────────────────────────────────────────────

def _build_opp_map(opp_df: pd.DataFrame, val_cols: list[str]) -> dict:
    """Build {TEAM_KEY: (val1, val2, ...)} lookup from a team stats DataFrame."""
    opp_map = {}
    for name_col in ("TEAM_NAME", "TEAM_ABBREVIATION"):
        if name_col not in opp_df.columns:
            continue
        for _, row in opp_df.iterrows():
            key = str(row[name_col]).strip().upper()
            opp_map[key] = tuple(row.get(c) for c in val_cols)
    return opp_map


def _get_opp_team(row) -> str:
    player_team = str(row.get("team", "")).strip().upper()
    home = str(row.get("home_team", "")).strip().upper()
    away = str(row.get("away_team", "")).strip().upper()
    return away if player_team == home else home


def _attach_opp_columns(
    df: pd.DataFrame,
    opp_map: dict,
    val_cols: list[str],
    out_cols: list[str],
) -> pd.DataFrame:
    """Generic helper: attach opponent stats by opposing team lookup."""
    df = df.copy()
    info = df.apply(
        lambda r: opp_map.get(_get_opp_team(r), tuple(None for _ in val_cols)),
        axis=1,
    )
    for i, col in enumerate(out_cols):
        df[col] = info.apply(lambda x: x[i])
    return df


# ── Opponent Rebounding ───────────────────────────────────────────────────────

def attach_opponent_rebounding(df: pd.DataFrame, opponent_stats: pd.DataFrame) -> pd.DataFrame:
    """Attaches opp_reb_allowed and opp_reb_rank to each row."""
    opp_map = _build_opp_map(opponent_stats, ["OPP_REB", "OPP_REB_RANK"])
    df = _attach_opp_columns(df, opp_map, ["OPP_REB", "OPP_REB_RANK"],
                              ["opp_reb_allowed", "opp_reb_rank"])
    df["opponent_team"] = df.apply(_get_opp_team, axis=1)
    return df


# ── Opponent Shooting % ───────────────────────────────────────────────────────

def attach_opponent_shooting(df: pd.DataFrame, team_shooting: pd.DataFrame) -> pd.DataFrame:
    """Attaches opp_fg_pct and opp_fg_pct_rank to each row."""
    shooting_map = {}
    if not team_shooting.empty:
        for name_col in ("TEAM_NAME", "TEAM_ABBREVIATION"):
            if name_col not in team_shooting.columns:
                continue
            for _, row in team_shooting.iterrows():
                key = str(row[name_col]).strip().upper()
                shooting_map[key] = (row.get("FG_PCT"), row.get("FG_PCT_RANK"))

    df = _attach_opp_columns(df, shooting_map, ["FG_PCT", "FG_PCT_RANK"],
                              ["opp_fg_pct", "opp_fg_pct_rank"])
    df["opp_fg_pct"] = pd.to_numeric(df["opp_fg_pct"], errors="coerce")
    return df


# ── Opponent Defense (Points allowed) ────────────────────────────────────────

def attach_opponent_defense(df: pd.DataFrame, opponent_stats: pd.DataFrame) -> pd.DataFrame:
    """Attaches opp_pts_allowed and opp_def_rank to each row (for Points props)."""
    opp_map = _build_opp_map(opponent_stats, ["OPP_PTS", "OPP_PTS_RANK"])
    df = _attach_opp_columns(df, opp_map, ["OPP_PTS", "OPP_PTS_RANK"],
                              ["opp_pts_allowed", "opp_def_rank"])
    df["opp_pts_allowed"] = pd.to_numeric(df["opp_pts_allowed"], errors="coerce")
    return df


# ── Opponent 3PT Defense ──────────────────────────────────────────────────────

def attach_opponent_3pt_defense(df: pd.DataFrame, opponent_stats: pd.DataFrame) -> pd.DataFrame:
    """Attaches opp_3pa_allowed, opp_3pa_rank, opp_3pt_pct_allowed, opp_3pt_pct_rank."""
    opp_map = _build_opp_map(
        opponent_stats,
        ["OPP_FG3A", "OPP_FG3A_RANK", "OPP_FG3_PCT", "OPP_FG3_PCT_RANK"],
    )
    df = _attach_opp_columns(
        df, opp_map,
        ["OPP_FG3A", "OPP_FG3A_RANK", "OPP_FG3_PCT", "OPP_FG3_PCT_RANK"],
        ["opp_3pa_allowed", "opp_3pa_rank", "opp_3pt_pct_allowed", "opp_3pt_pct_rank"],
    )
    for col in ["opp_3pa_allowed", "opp_3pt_pct_allowed"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# ── Opponent Assist Context ───────────────────────────────────────────────────

def attach_opponent_assist_context(df: pd.DataFrame, opponent_stats: pd.DataFrame) -> pd.DataFrame:
    """Attaches opp_ast_allowed and opp_ast_rank to each row (for Assists props)."""
    opp_map = _build_opp_map(opponent_stats, ["OPP_AST", "OPP_AST_RANK"])
    df = _attach_opp_columns(df, opp_map, ["OPP_AST", "OPP_AST_RANK"],
                              ["opp_ast_allowed", "opp_ast_rank"])
    df["opp_ast_allowed"] = pd.to_numeric(df["opp_ast_allowed"], errors="coerce")
    return df


# ── Player Advanced Stats ─────────────────────────────────────────────────────

def attach_player_advanced_stats(
    df: pd.DataFrame,
    player_advanced: pd.DataFrame,
    name_lookup: dict[str, int],
) -> pd.DataFrame:
    """Attaches USG_PCT, REB_PCT, AST_PCT, FG3A_PER_GAME per player."""
    adv_cols = ["usg_pct", "reb_pct", "ast_pct", "fg3a_per_game"]
    df = df.copy()
    for col in adv_cols:
        df[col] = None

    if player_advanced is None or player_advanced.empty:
        return df

    adv_map = {}
    for _, row in player_advanced.iterrows():
        pid = int(row["PLAYER_ID"])
        adv_map[pid] = {
            "usg_pct":       row.get("USG_PCT"),
            "reb_pct":       row.get("REB_PCT"),
            "ast_pct":       row.get("AST_PCT"),
            "fg3a_per_game": row.get("FG3A_PER_GAME"),
        }

    for idx, row in df.iterrows():
        pid = row.get("nba_player_id")
        if pid is None or pd.isna(pid):
            pid = _match_player_name(row["player_name"], name_lookup)
        if pid is None:
            continue
        stats = adv_map.get(int(pid), {})
        for col in adv_cols:
            df.at[idx, col] = stats.get(col)

    for col in adv_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# ── Multi-stat Rolling Form ───────────────────────────────────────────────────

def _window_stats(series: pd.Series, pp_line: float, n: int) -> tuple[float | None, float | None]:
    """Returns (avg, hit_rate) for the last n games in series. None if insufficient data."""
    s = series.iloc[:n].dropna()
    if len(s) < max(3, n // 2):   # need at least half the window filled to be meaningful
        return None, None
    return round(float(s.mean()), 2), round(float((s >= pp_line).mean()), 3)


def _compute_trend(l5_avg, l10_avg, l15_avg) -> tuple[str, float | None]:
    """
    Returns (trend_direction, trend_pct) based on L5 vs L15.

    trend_direction:
        "up"   — L5 avg >= L10 avg >= L15 avg (consistently improving)
        "down" — L5 avg <= L10 avg <= L15 avg (consistently declining)
        "flat" — within ±5% of L15 avg
        "mixed"— no clear monotonic trend

    trend_pct: (L5_avg - L15_avg) / L15_avg  — magnitude of shift.
               Positive = trending up, negative = trending down.
               None if L5 or L15 unavailable.
    """
    if l5_avg is None or l15_avg is None or l15_avg == 0:
        return "unknown", None

    trend_pct = round((l5_avg - l15_avg) / l15_avg, 4)

    if abs(trend_pct) <= 0.05:
        return "flat", trend_pct

    if l10_avg is not None:
        if l5_avg >= l10_avg >= l15_avg:
            return "up", trend_pct
        if l5_avg <= l10_avg <= l15_avg:
            return "down", trend_pct
        return "mixed", trend_pct

    # L10 unavailable — fall back to L5 vs L15 only
    return ("up" if trend_pct > 0 else "down"), trend_pct


def compute_player_form_multi(
    df: pd.DataFrame,
    player_logs: dict[int, pd.DataFrame],
    name_lookup: dict[str, int],
) -> pd.DataFrame:
    """
    Computes rolling form stats for every prop using a variance-aware primary window,
    plus fixed L5 / L10 / L15 windows for trend detection.

    Primary window columns:
        rolling_stat_avg, rolling_stat_std, stat_hit_rate, avg_minutes, games_sampled

    Multi-window columns:
        l5_avg,  l5_hit_rate
        l10_avg, l10_hit_rate
        l15_avg, l15_hit_rate

    Trend columns:
        trend_direction  — "up" | "down" | "flat" | "mixed" | "unknown"
        trend_pct        — (L5_avg - L15_avg) / L15_avg  (signed magnitude)
        trend_is_valid   — True only when L5 avg >= PP line AND L5 hit_rate >= 0.60
                           Guards against trusting a small-sample uptrend that is
                           still below the line.

    Legacy Rebs+Asts columns (backwards compat):
        rolling_ra_avg, rolling_ra_std, hit_rate

    Per-stat display columns:
        rolling_pts_avg, rolling_reb_avg, rolling_ast_avg, rolling_3pm_avg, recent_3pt_pct
    """
    df = df.copy()

    all_new_cols = [
        "rolling_stat_avg", "rolling_stat_std", "stat_hit_rate",
        "avg_minutes", "games_sampled",
        "l5_avg",  "l5_hit_rate",
        "l10_avg", "l10_hit_rate",
        "l15_avg", "l15_hit_rate",
        "trend_direction", "trend_pct", "trend_is_valid",
        "rolling_ra_avg", "rolling_ra_std", "hit_rate",
        "rolling_pts_avg", "rolling_reb_avg", "rolling_ast_avg",
        "rolling_3pm_avg", "recent_3pt_pct",
    ]
    for col in all_new_cols:
        df[col] = None

    for idx, row in df.iterrows():
        pp_name  = row["player_name"]
        pp_line  = row["line"]
        stat_cat = row.get("stat_category", "Rebs+Asts")

        variance    = VARIANCE_TIER.get(stat_cat, "medium")
        form_window = FORM_WINDOW_BY_VARIANCE.get(variance, 10)
        stat_col    = _STAT_LOG_COL.get(stat_cat, "RA")

        pid = row.get("nba_player_id")
        if pid is None or pd.isna(pid):
            pid = _match_player_name(pp_name, name_lookup)
        if pid is None:
            continue

        log = player_logs.get(int(pid), pd.DataFrame())
        if log.empty or stat_col not in log.columns:
            continue

        # All qualifying games (up to 15 — widest window needed)
        log_qual = log[log["MIN"] >= MIN_MINUTES_THRESHOLD].head(15)
        if log_qual.empty:
            continue

        stat_series = log_qual[stat_col].dropna()
        if stat_series.empty:
            continue

        # ── Primary window (variance-aware) ──────────────────────────────────
        primary = stat_series.iloc[:form_window]
        if primary.empty:
            continue

        rolling_avg = round(float(primary.mean()), 2)
        rolling_std = round(float(primary.std()), 2) if len(primary) > 1 else 0.0
        hr          = round(float((primary >= pp_line).mean()), 3)

        df.at[idx, "rolling_stat_avg"] = rolling_avg
        df.at[idx, "rolling_stat_std"] = rolling_std
        df.at[idx, "stat_hit_rate"]    = hr
        df.at[idx, "avg_minutes"]      = round(float(log_qual["MIN"].mean()), 1)
        df.at[idx, "games_sampled"]    = len(primary)

        # ── L5 / L10 / L15 windows ───────────────────────────────────────────
        l5_avg,  l5_hr  = _window_stats(stat_series, pp_line, 5)
        l10_avg, l10_hr = _window_stats(stat_series, pp_line, 10)
        l15_avg, l15_hr = _window_stats(stat_series, pp_line, 15)

        df.at[idx, "l5_avg"]      = l5_avg
        df.at[idx, "l5_hit_rate"] = l5_hr
        df.at[idx, "l10_avg"]     = l10_avg
        df.at[idx, "l10_hit_rate"]= l10_hr
        df.at[idx, "l15_avg"]     = l15_avg
        df.at[idx, "l15_hit_rate"]= l15_hr

        # ── Trend ─────────────────────────────────────────────────────────────
        direction, t_pct = _compute_trend(l5_avg, l10_avg, l15_avg)
        df.at[idx, "trend_direction"] = direction
        df.at[idx, "trend_pct"]       = t_pct

        # Valid trend: L5 avg must clear the PP line AND L5 hit rate >= 60%
        # This prevents trusting a "hot trend" that is still below the line
        trend_valid = (
            l5_avg is not None
            and l5_hr is not None
            and l5_avg >= pp_line
            and l5_hr >= 0.60
        )
        df.at[idx, "trend_is_valid"] = trend_valid

        # ── Legacy RA columns ─────────────────────────────────────────────────
        if stat_cat == "Rebs+Asts":
            df.at[idx, "rolling_ra_avg"] = rolling_avg
            df.at[idx, "rolling_ra_std"] = rolling_std
            df.at[idx, "hit_rate"]        = hr

        # ── Per-stat display columns ──────────────────────────────────────────
        col_map = {
            "Points":   "rolling_pts_avg",
            "Rebounds": "rolling_reb_avg",
            "Assists":  "rolling_ast_avg",
            "3PM":      "rolling_3pm_avg",
        }
        if stat_cat in col_map:
            df.at[idx, col_map[stat_cat]] = rolling_avg

        # ── 3PT% hot streak ───────────────────────────────────────────────────
        if stat_cat == "3PM" and "FG3M" in log_qual.columns and "FG3A" in log_qual.columns:
            total_3pm = log_qual["FG3M"].sum()
            total_3pa = log_qual["FG3A"].sum()
            if total_3pa > 0:
                df.at[idx, "recent_3pt_pct"] = round(float(total_3pm / total_3pa), 3)

    numeric_cols = [
        "rolling_stat_avg", "rolling_stat_std", "stat_hit_rate", "avg_minutes",
        "l5_avg", "l5_hit_rate", "l10_avg", "l10_hit_rate", "l15_avg", "l15_hit_rate",
        "trend_pct",
        "rolling_ra_avg", "rolling_ra_std", "hit_rate",
        "rolling_pts_avg", "rolling_reb_avg", "rolling_ast_avg",
        "rolling_3pm_avg", "recent_3pt_pct",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["games_sampled"] = pd.to_numeric(df["games_sampled"], errors="coerce").astype("Int64")
    return df


# ── Line Gap (multi-stat) ─────────────────────────────────────────────────────

def attach_line_gap_multi(
    df: pd.DataFrame,
    all_consensus_lines: pd.DataFrame,
) -> pd.DataFrame:
    """
    Attaches consensus_line, line_gap, num_books per row.
    Merges on (player_name_norm, stat_category) so each stat gets its own book line.
    """
    if all_consensus_lines.empty:
        df["consensus_line"] = None
        df["line_gap"]       = None
        df["num_books"]      = None
        return df

    consensus = all_consensus_lines.copy()
    consensus["player_name_norm"] = consensus["player_name"].apply(_ascii_fold)

    df = df.copy()
    df["player_name_norm"] = df["player_name"].apply(_ascii_fold)

    merged = df.merge(
        consensus[["player_name_norm", "stat_category", "consensus_line", "num_books"]],
        on=["player_name_norm", "stat_category"],
        how="left",
    )
    merged["line_gap"] = merged["consensus_line"] - merged["line"]
    merged = merged.drop(columns=["player_name_norm"])
    return merged


# Legacy single-stat attach_line_gap (kept for backwards compat)
def attach_line_gap(df: pd.DataFrame, consensus_lines: pd.DataFrame) -> pd.DataFrame:
    if consensus_lines.empty:
        df["consensus_line"] = None
        df["line_gap"]       = None
        df["num_books"]      = None
        return df
    consensus_lines = consensus_lines.copy()
    consensus_lines["player_name_norm"] = consensus_lines["player_name"].apply(_ascii_fold)
    df = df.copy()
    df["player_name_norm"] = df["player_name"].apply(_ascii_fold)
    merged = df.merge(
        consensus_lines[["player_name_norm", "consensus_line", "num_books"]],
        on="player_name_norm", how="left",
    )
    merged["line_gap"] = merged["consensus_line"] - merged["line"]
    return merged.drop(columns=["player_name_norm"])


# ── Master Build ──────────────────────────────────────────────────────────────

def build_feature_dataframe(
    pp_board: pd.DataFrame,
    team_pace: pd.DataFrame,
    opponent_stats: pd.DataFrame,
    team_shooting: pd.DataFrame,
    player_logs: dict[int, pd.DataFrame],
    all_consensus_lines: pd.DataFrame,
    active_players: pd.DataFrame,
    player_advanced_stats: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Orchestrates all feature engineering steps for all stat categories.
    Returns a feature-complete DataFrame ready for the multi-stat scorer.
    """
    if pp_board.empty:
        logger.error("PrizePicks board is empty — nothing to score.")
        return pd.DataFrame()

    logger.info("Building feature DataFrame for %d props...", len(pp_board))

    name_lookup = _build_name_lookup(active_players)

    # 0. Normalize stat_category
    df = pp_board.copy()
    df["stat_category"] = df["stat_type"].apply(_normalize_stat_category)

    # 1. Game pace
    df = compute_game_pace(df, team_pace)
    logger.info("✓ Game pace attached.")

    # 2. Opponent rebounding (used by Rebs+Asts and Rebounds)
    df = attach_opponent_rebounding(df, opponent_stats)
    logger.info("✓ Opponent rebounding attached.")

    # 3. Opponent shooting % (used by Rebs+Asts and Rebounds)
    df = attach_opponent_shooting(df, team_shooting)
    logger.info("✓ Opponent shooting % attached.")

    # 4. Opponent defense quality (used by Points)
    df = attach_opponent_defense(df, opponent_stats)
    logger.info("✓ Opponent defense quality attached.")

    # 5. Opponent 3PT defense (used by 3PM)
    df = attach_opponent_3pt_defense(df, opponent_stats)
    logger.info("✓ Opponent 3PT defense attached.")

    # 6. Opponent assist context (used by Assists)
    df = attach_opponent_assist_context(df, opponent_stats)
    logger.info("✓ Opponent assist context attached.")

    # 7. Player advanced stats (optional — graceful if missing)
    df = attach_player_advanced_stats(df, player_advanced_stats, name_lookup)
    logger.info("✓ Player advanced stats attached.")

    # 8. Sportsbook line gap (multi-stat)
    df = attach_line_gap_multi(df, all_consensus_lines)
    logger.info("✓ Sportsbook line gaps attached.")

    # 9. Rolling form (variance-aware, all stat categories)
    df = compute_player_form_multi(df, player_logs, name_lookup)
    logger.info("✓ Player rolling form attached.")

    return df.reset_index(drop=True)
