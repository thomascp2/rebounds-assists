"""
data/nba_stats.py — Pulls from the official NBA Stats API via nba_api.

Fetches:
  - Team pace (LeagueDashTeamStats endpoint)
  - Opponent rebounding rank (LeagueDashTeamStats opponent view)
  - Player game logs for rolling Rebs+Asts averages (PlayerGameLog endpoint)
  - Active player roster (CommonAllPlayers endpoint)
"""

import time
import logging
from typing import Optional

import pandas as pd

from config import CURRENT_SEASON, SEASON_TYPE

logger = logging.getLogger(__name__)

# nba_api imports
from nba_api.stats.endpoints import (
    leaguedashteamstats,
    leaguedashplayerstats,
    playergamelog,
    commonallplayers,
)
from nba_api.stats.static import teams as nba_teams_static


def _team_abbrev_map() -> dict:
    """Returns {TEAM_ID: ABBREVIATION} for all 30 NBA teams."""
    return {t["id"]: t["abbreviation"] for t in nba_teams_static.get_teams()}


# ── Team Pace ─────────────────────────────────────────────────────────────────

def fetch_team_pace() -> pd.DataFrame:
    """
    Returns a DataFrame with columns:
        TEAM_ID, TEAM_NAME, PACE
    sorted descending by PACE.
    """
    logger.info("Fetching team pace from NBA Stats API...")
    time.sleep(0.6)
    endpoint = leaguedashteamstats.LeagueDashTeamStats(
        season=CURRENT_SEASON,
        season_type_all_star=SEASON_TYPE,
        measure_type_detailed_defense="Advanced",
        per_mode_detailed="PerGame",
    )
    df = endpoint.get_data_frames()[0]
    pace_df = df[["TEAM_ID", "TEAM_NAME", "PACE"]].copy()
    pace_df["PACE"] = pd.to_numeric(pace_df["PACE"], errors="coerce")
    abbrev = _team_abbrev_map()
    pace_df["TEAM_ABBREVIATION"] = pace_df["TEAM_ID"].map(abbrev)
    return pace_df.sort_values("PACE", ascending=False).reset_index(drop=True)


# ── Opponent Rebounding ───────────────────────────────────────────────────────

def fetch_opponent_rebounding() -> pd.DataFrame:
    """
    Returns a DataFrame with columns:
        TEAM_ID, TEAM_NAME, OPP_REB, OPP_REB_RANK
    where OPP_REB is opponent rebounds allowed per game and
    OPP_REB_RANK is 1=best defense (fewest allowed), 30=worst.
    """
    logger.info("Fetching opponent rebounding stats...")
    time.sleep(0.6)
    endpoint = leaguedashteamstats.LeagueDashTeamStats(
        season=CURRENT_SEASON,
        season_type_all_star=SEASON_TYPE,
        measure_type_detailed_defense="Opponent",
        per_mode_detailed="PerGame",
    )
    df = endpoint.get_data_frames()[0]

    result = df[["TEAM_ID", "TEAM_NAME", "OPP_REB"]].copy()
    result["OPP_REB"] = pd.to_numeric(result["OPP_REB"], errors="coerce")
    result["OPP_REB_RANK"] = result["OPP_REB"].rank(ascending=True, method="min").astype(int)
    abbrev = _team_abbrev_map()
    result["TEAM_ABBREVIATION"] = result["TEAM_ID"].map(abbrev)
    return result.reset_index(drop=True)


# ── Opponent Shooting % ───────────────────────────────────────────────────────

def fetch_team_shooting() -> pd.DataFrame:
    """
    Returns each team's offensive FG% (field goal percentage) with columns:
        TEAM_ID, TEAM_NAME, FG_PCT, FG3_PCT, FG_PCT_RANK

    Why this matters for Rebs+Asts:
      - Low opponent FG% = they miss more shots = more rebound opportunities
        for our player's team (defensive rebounds).
      - FG_PCT_RANK: 1 = best shooters (fewest misses), 30 = worst (most misses).
        We TARGET opponents ranked 20+ (bad shooters) for extra rebound juice.
    """
    logger.info("Fetching team shooting stats...")
    time.sleep(0.6)
    endpoint = leaguedashteamstats.LeagueDashTeamStats(
        season=CURRENT_SEASON,
        season_type_all_star=SEASON_TYPE,
        measure_type_detailed_defense="Base",
        per_mode_detailed="PerGame",
    )
    df = endpoint.get_data_frames()[0]
    result = df[["TEAM_ID", "TEAM_NAME", "FG_PCT", "FG3_PCT"]].copy()
    for col in ["FG_PCT", "FG3_PCT"]:
        result[col] = pd.to_numeric(result[col], errors="coerce")
    # Rank ascending: rank 1 = best shooters (high FG%), 30 = worst (low FG%)
    result["FG_PCT_RANK"] = result["FG_PCT"].rank(ascending=False, method="min").astype(int)
    abbrev = _team_abbrev_map()
    result["TEAM_ABBREVIATION"] = result["TEAM_ID"].map(abbrev)
    return result.reset_index(drop=True)


# ── Player Game Logs ──────────────────────────────────────────────────────────

def fetch_player_game_log(player_id: int, last_n_games: int = 15) -> pd.DataFrame:
    """
    Returns a DataFrame of the player's recent game logs with columns:
        GAME_DATE, MATCHUP, MIN, REB, AST, RA  (REB+AST combined)
    Limited to last_n_games games.
    """
    logger.debug("Fetching game log for player %d...", player_id)
    time.sleep(0.6)
    endpoint = playergamelog.PlayerGameLog(
        player_id=player_id,
        season=CURRENT_SEASON,
        season_type_all_star=SEASON_TYPE,
    )
    df = endpoint.get_data_frames()[0]
    if df.empty:
        return df

    for col in ["MIN", "PTS", "REB", "AST", "FG3M", "FG3A"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["RA"] = df["REB"] + df["AST"]
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], format="%b %d, %Y", errors="coerce")

    want = ["GAME_DATE", "MATCHUP", "MIN", "PTS", "REB", "AST", "FG3M", "FG3A", "RA"]
    cols = [c for c in want if c in df.columns]
    return df[cols].head(last_n_games).reset_index(drop=True)


def fetch_all_player_logs(player_ids: list[int], last_n_games: int = 15) -> dict[int, pd.DataFrame]:
    """
    Fetches game logs for a list of player IDs.
    Returns a dict: {player_id: DataFrame}
    """
    logs = {}
    for pid in player_ids:
        try:
            logs[pid] = fetch_player_game_log(pid, last_n_games)
        except Exception as exc:
            logger.warning("Could not fetch log for player %d: %s", pid, exc)
            logs[pid] = pd.DataFrame()
    return logs


# ── Player Info Lookup ────────────────────────────────────────────────────────

def fetch_active_players() -> pd.DataFrame:
    """
    Returns a DataFrame of all active NBA players with:
        PERSON_ID, DISPLAY_FIRST_LAST, TEAM_ID, TEAM_ABBREVIATION
    Used to map PrizePicks player names to NBA Stats player IDs.
    """
    logger.info("Fetching active player roster...")
    time.sleep(0.6)
    endpoint = commonallplayers.CommonAllPlayers(
        league_id="00",
        season=CURRENT_SEASON,
        is_only_current_season=1,
    )
    df = endpoint.get_data_frames()[0]
    cols = ["PERSON_ID", "DISPLAY_FIRST_LAST", "TEAM_ID", "TEAM_ABBREVIATION"]
    available = [c for c in cols if c in df.columns]
    return df[available].copy()


# ── Comprehensive Opponent Stats ───────────────────────────────────────────────

def fetch_opponent_stats() -> pd.DataFrame:
    """
    Fetches all per-team opponent stats in a single API call using the
    'Opponent' measure type. Returns columns useful across all 5 stat categories:

        TEAM_ID, TEAM_NAME, TEAM_ABBREVIATION
        OPP_PTS, OPP_PTS_RANK        — points allowed (rank 30 = worst D)
        OPP_REB, OPP_REB_RANK        — rebounds allowed (rank 30 = most given up)
        OPP_AST, OPP_AST_RANK        — assists allowed (rank 30 = most given up)
        OPP_FG3A, OPP_FG3A_RANK     — 3PA allowed (rank 30 = most 3s given up)
        OPP_FG3_PCT, OPP_FG3_PCT_RANK — 3PT% allowed (rank 30 = easiest 3s)
        OPP_OREB, OPP_OREB_RANK     — opponent offensive rebounds (rank 30 = most)
    """
    logger.info("Fetching comprehensive opponent stats (Opponent measure)...")
    time.sleep(0.6)
    endpoint = leaguedashteamstats.LeagueDashTeamStats(
        season=CURRENT_SEASON,
        season_type_all_star=SEASON_TYPE,
        measure_type_detailed_defense="Opponent",
        per_mode_detailed="PerGame",
    )
    df = endpoint.get_data_frames()[0]

    stat_cols = ["OPP_PTS", "OPP_REB", "OPP_AST", "OPP_FG3A", "OPP_FG3_PCT", "OPP_OREB"]
    base_cols = ["TEAM_ID", "TEAM_NAME"] + [c for c in stat_cols if c in df.columns]
    result = df[base_cols].copy()

    for col in stat_cols:
        if col in result.columns:
            result[col] = pd.to_numeric(result[col], errors="coerce")

    # Rank ascending=True → rank 30 = most allowed = worst defense (best for our props)
    rank_pairs = [
        ("OPP_PTS",     "OPP_PTS_RANK"),
        ("OPP_REB",     "OPP_REB_RANK"),
        ("OPP_AST",     "OPP_AST_RANK"),
        ("OPP_FG3A",    "OPP_FG3A_RANK"),
        ("OPP_FG3_PCT", "OPP_FG3_PCT_RANK"),
        ("OPP_OREB",    "OPP_OREB_RANK"),
    ]
    for stat, rank in rank_pairs:
        if stat in result.columns:
            result[rank] = result[stat].rank(ascending=True, method="min").astype(int)

    abbrev = _team_abbrev_map()
    result["TEAM_ABBREVIATION"] = result["TEAM_ID"].map(abbrev)
    return result.reset_index(drop=True)


# ── Player Advanced Stats ─────────────────────────────────────────────────────

def fetch_player_advanced_stats() -> pd.DataFrame:
    """
    Returns per-player advanced + base stats for current season:
        PLAYER_ID, PLAYER_NAME, TEAM_ABBREVIATION,
        USG_PCT, REB_PCT, AST_PCT,
        FG3A_PER_GAME  (3-point attempts per game, from Base measure)

    Used to qualify players for Points (usage), 3PM (volume), Assists (AST_PCT).
    Returns empty DataFrame on failure — all callers must handle gracefully.
    """
    logger.info("Fetching player advanced stats (USG_PCT, REB_PCT, AST_PCT)...")
    try:
        time.sleep(0.6)
        adv_ep = leaguedashplayerstats.LeagueDashPlayerStats(
            season=CURRENT_SEASON,
            season_type_all_star=SEASON_TYPE,
            measure_type_detailed_defense="Advanced",
            per_mode_detailed="PerGame",
        )
        adv_df = adv_ep.get_data_frames()[0]

        time.sleep(0.6)
        base_ep = leaguedashplayerstats.LeagueDashPlayerStats(
            season=CURRENT_SEASON,
            season_type_all_star=SEASON_TYPE,
            measure_type_detailed_defense="Base",
            per_mode_detailed="PerGame",
        )
        base_df = base_ep.get_data_frames()[0]
    except Exception as exc:
        logger.warning("Could not fetch player advanced stats: %s", exc)
        return pd.DataFrame()

    adv_want = ["PLAYER_ID", "PLAYER_NAME", "TEAM_ABBREVIATION", "USG_PCT", "REB_PCT", "AST_PCT"]
    adv_result = adv_df[[c for c in adv_want if c in adv_df.columns]].copy()

    if "PLAYER_ID" in base_df.columns:
        _base_rename = {
            "FG3A": "FG3A_PER_GAME",
            "PTS":  "SEASON_AVG_PTS",
            "REB":  "SEASON_AVG_REB",
            "AST":  "SEASON_AVG_AST",
            "FG3M": "SEASON_AVG_3PM",
        }
        base_want = ["PLAYER_ID"] + [c for c in _base_rename if c in base_df.columns]
        base_result = base_df[base_want].copy().rename(columns=_base_rename)
        for col in base_result.columns:
            if col != "PLAYER_ID":
                base_result[col] = pd.to_numeric(base_result[col], errors="coerce")
        result = adv_result.merge(base_result, on="PLAYER_ID", how="left")
    else:
        result = adv_result

    for col in ["USG_PCT", "REB_PCT", "AST_PCT"]:
        if col in result.columns:
            result[col] = pd.to_numeric(result[col], errors="coerce")

    logger.info("  → Player advanced stats fetched for %d players.", len(result))
    return result.reset_index(drop=True)
