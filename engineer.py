"""
features/engineer.py — Merges all data sources and engineers scoring features.

Input DataFrames (from data layer):
  - pp_board:       PrizePicks Rebs+Asts props (from prizepicks.py)
  - team_pace:      Team pace per game (from nba_stats.py)
  - opp_rebounding: Opponent rebounding rank (from nba_stats.py)
  - player_logs:    Dict of {player_id: game_log_df} (from nba_stats.py)
  - consensus_lines: Sportsbook consensus over lines (from odds_api.py)
  - active_players: Player name → ID mapping (from nba_stats.py)

Output:
  - A merged DataFrame ready for the scorer with all features pre-computed.
"""

import logging
from difflib import get_close_matches

import pandas as pd

from config import FORM_WINDOW, MIN_MINUTES_THRESHOLD

logger = logging.getLogger(__name__)


# ── Name Matching ─────────────────────────────────────────────────────────────

def _build_name_lookup(active_players: pd.DataFrame) -> dict[str, int]:
    """
    Builds a dict of {normalized_name: PERSON_ID} for fuzzy matching
    PrizePicks player names to NBA Stats player IDs.
    """
    lookup = {}
    for _, row in active_players.iterrows():
        name = str(row.get("DISPLAY_FIRST_LAST", "")).strip().lower()
        if name:
            lookup[name] = int(row["PERSON_ID"])
    return lookup


def _match_player_name(pp_name: str, lookup: dict[str, int]) -> int | None:
    """
    Attempts an exact then fuzzy match of a PrizePicks player name
    to an NBA Stats PERSON_ID.
    Returns the PERSON_ID or None if not found.
    """
    normalized = pp_name.strip().lower()
    if normalized in lookup:
        return lookup[normalized]

    # Fuzzy fallback with tight cutoff
    matches = get_close_matches(normalized, lookup.keys(), n=1, cutoff=0.85)
    if matches:
        logger.debug("Fuzzy matched '%s' → '%s'", pp_name, matches[0])
        return lookup[matches[0]]

    logger.warning("Could not match player name: '%s'", pp_name)
    return None


# ── Game Pace for Today's Matchups ────────────────────────────────────────────

def compute_game_pace(
    pp_board: pd.DataFrame,
    team_pace: pd.DataFrame,
) -> pd.DataFrame:
    """
    For each prop on the PrizePicks board, looks up both teams' pace and
    computes projected_game_pace = average of the two teams' season pace.

    Adds columns to pp_board:
        player_team_pace, opp_team_pace, projected_game_pace
    """
    pace_map = dict(zip(
        team_pace["TEAM_NAME"].str.upper(),
        team_pace["PACE"],
    ))
    # Also map by abbreviation if available
    if "TEAM_ABBREVIATION" in team_pace.columns:
        for _, row in team_pace.iterrows():
            pace_map[str(row["TEAM_ABBREVIATION"]).upper()] = row["PACE"]

    def _lookup_pace(team_str: str) -> float | None:
        key = str(team_str).strip().upper()
        return pace_map.get(key)

    df = pp_board.copy()
    df["player_team_pace"] = df["team"].apply(_lookup_pace)
    df["opp_team_pace"] = df.apply(
        lambda r: _lookup_pace(r["away_team"])
        if r["team"].upper() == r["home_team"].upper()
        else _lookup_pace(r["home_team"]),
        axis=1,
    )
    df["projected_game_pace"] = (
        (df["player_team_pace"].fillna(100) + df["opp_team_pace"].fillna(100)) / 2
    )
    return df


# ── Opponent Rebounding ───────────────────────────────────────────────────────

def attach_opponent_rebounding(
    df: pd.DataFrame,
    opp_rebounding: pd.DataFrame,
) -> pd.DataFrame:
    """
    Attaches OPP_REB and OPP_REB_RANK to each row based on the opposing team.
    The "opponent" for a given player is the other team in the game_label.
    """
    reb_map_name = dict(zip(
        opp_rebounding["TEAM_NAME"].str.upper(),
        zip(opp_rebounding["OPP_REB"], opp_rebounding["OPP_REB_RANK"]),
    ))
    if "TEAM_ABBREVIATION" in opp_rebounding.columns:
        for _, row in opp_rebounding.iterrows():
            reb_map_name[str(row["TEAM_ABBREVIATION"]).upper()] = (
                row["OPP_REB"], row["OPP_REB_RANK"]
            )

    def _get_opp_team(row) -> str:
        """The team the player is playing AGAINST."""
        player_team = str(row["team"]).strip().upper()
        home = str(row["home_team"]).strip().upper()
        away = str(row["away_team"]).strip().upper()
        if player_team == home:
            return away
        return home

    def _lookup_reb(row):
        opp = _get_opp_team(row)
        return reb_map_name.get(opp, (None, None))

    df = df.copy()
    df["opponent_team"] = df.apply(_get_opp_team, axis=1)
    reb_info = df.apply(_lookup_reb, axis=1)
    df["opp_reb_allowed"] = reb_info.apply(lambda x: x[0])
    df["opp_reb_rank"] = reb_info.apply(lambda x: x[1])
    return df


# ── Rolling Form Stats ────────────────────────────────────────────────────────

def compute_player_form(
    df: pd.DataFrame,
    player_logs: dict[int, pd.DataFrame],
    name_lookup: dict[str, int],
    form_window: int = FORM_WINDOW,
) -> pd.DataFrame:
    """
    For each player, computes rolling stats over the last `form_window` games:
        rolling_ra_avg     — average Rebs+Asts over last N games
        rolling_ra_std     — standard deviation (consistency signal)
        hit_rate           — fraction of games where RA >= PP line
        avg_minutes        — average minutes played
        games_sampled      — how many games were in the window

    Adds these as columns to df.
    """
    df = df.copy()

    form_cols = ["rolling_ra_avg", "rolling_ra_std", "hit_rate", "avg_minutes", "games_sampled"]
    for col in form_cols:
        df[col] = None

    for idx, row in df.iterrows():
        pp_name = row["player_name"]
        pp_line = row["line"]

        # Resolve player ID — from PrizePicks payload or name matching
        pid = row.get("nba_player_id")
        if pid is None or pd.isna(pid):
            pid = _match_player_name(pp_name, name_lookup)

        if pid is None:
            continue

        log = player_logs.get(int(pid), pd.DataFrame())
        if log.empty:
            continue

        # Filter to minimum minutes (exclude DNPs skewing the average)
        log_filtered = log[log["MIN"] >= MIN_MINUTES_THRESHOLD].head(form_window)
        if log_filtered.empty:
            continue

        ra_series = log_filtered["RA"]
        df.at[idx, "rolling_ra_avg"] = round(ra_series.mean(), 2)
        df.at[idx, "rolling_ra_std"] = round(ra_series.std(), 2)
        df.at[idx, "hit_rate"] = round((ra_series >= pp_line).mean(), 3)
        df.at[idx, "avg_minutes"] = round(log_filtered["MIN"].mean(), 1)
        df.at[idx, "games_sampled"] = len(log_filtered)

    for col in ["rolling_ra_avg", "rolling_ra_std", "hit_rate", "avg_minutes"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# ── Line Gap (PrizePicks vs. Books) ───────────────────────────────────────────

def attach_line_gap(df: pd.DataFrame, consensus_lines: pd.DataFrame) -> pd.DataFrame:
    """
    Joins PrizePicks lines to sportsbook consensus lines and computes:
        consensus_line  — median sportsbook over line
        line_gap        — PP line minus consensus_line (positive = PP is higher = easier to beat)
        num_books       — number of books with this prop

    A positive line_gap means PrizePicks has set a LOWER line than the books,
    so the over is relatively easier to hit on PrizePicks.
    Wait — let's be precise: PrizePicks shows the projection you must beat.
    If PP line = 14.5 and books' over line = 16.5, books think 14.5 is easy.
    So line_gap = consensus - PP_line → positive gap = PP is EASIER than books.
    """
    if consensus_lines.empty:
        df["consensus_line"] = None
        df["line_gap"] = None
        df["num_books"] = None
        return df

    # Normalize names for matching
    consensus_lines = consensus_lines.copy()
    consensus_lines["player_name_norm"] = (
        consensus_lines["player_name"].str.strip().str.lower()
    )

    df = df.copy()
    df["player_name_norm"] = df["player_name"].str.strip().str.lower()

    merged = df.merge(
        consensus_lines[["player_name_norm", "consensus_line", "num_books", "books_listed"]],
        on="player_name_norm",
        how="left",
    )

    # line_gap > 0 means books' over line is HIGHER than PP → PP line is easier
    merged["line_gap"] = merged["consensus_line"] - merged["line"]
    merged = merged.drop(columns=["player_name_norm"])
    return merged


# ── Master Merge ──────────────────────────────────────────────────────────────

def build_feature_dataframe(
    pp_board: pd.DataFrame,
    team_pace: pd.DataFrame,
    opp_rebounding: pd.DataFrame,
    player_logs: dict[int, pd.DataFrame],
    consensus_lines: pd.DataFrame,
    active_players: pd.DataFrame,
    form_window: int = FORM_WINDOW,
) -> pd.DataFrame:
    """
    Orchestrates all feature engineering steps and returns a single
    feature-complete DataFrame ready for the scorer.
    """
    if pp_board.empty:
        logger.error("PrizePicks board is empty — nothing to score.")
        return pd.DataFrame()

    logger.info("Building feature DataFrame for %d props...", len(pp_board))

    # Build player name → ID lookup
    name_lookup = _build_name_lookup(active_players)

    # Step 1: Game pace features
    df = compute_game_pace(pp_board, team_pace)
    logger.info("✓ Game pace attached.")

    # Step 2: Opponent rebounding features
    df = attach_opponent_rebounding(df, opp_rebounding)
    logger.info("✓ Opponent rebounding attached.")

    # Step 3: Line gap vs. sportsbooks
    df = attach_line_gap(df, consensus_lines)
    logger.info("✓ Sportsbook line gap attached.")

    # Step 4: Player rolling form
    df = compute_player_form(df, player_logs, name_lookup, form_window)
    logger.info("✓ Player rolling form attached.")

    return df.reset_index(drop=True)
