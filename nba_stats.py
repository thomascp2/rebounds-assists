"""
data/nba_stats.py — Pulls from the official NBA Stats API.

Fetches:
  - Team pace (LeagueDashTeamStats endpoint)
  - Opponent rebounding rank (LeagueDashTeamStats opponent view)
  - Player game logs for rolling Rebs+Asts averages (PlayerGameLog endpoint)
  - Today's scheduled matchups (Scoreboard endpoint)
"""

import time
import logging
from datetime import date
from typing import Optional

import requests
import pandas as pd

from config import NBA_STATS_BASE, NBA_STATS_HEADERS, CURRENT_SEASON, SEASON_TYPE

logger = logging.getLogger(__name__)


def _get(endpoint: str, params: dict, retries: int = 3) -> dict:
    """
    Thin wrapper around NBA Stats API GET with retry logic.
    The API is rate-sensitive — we add a short sleep between calls.
    """
    url = f"{NBA_STATS_BASE}/{endpoint}"
    for attempt in range(retries):
        try:
            time.sleep(0.8)   # be polite; NBA Stats blocks aggressive scrapers
            resp = requests.get(url, headers=NBA_STATS_HEADERS, params=params, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            logger.warning("NBA Stats attempt %d/%d failed: %s", attempt + 1, retries, exc)
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)   # exponential back-off


def _result_to_df(data: dict, result_set_index: int = 0) -> pd.DataFrame:
    """Convert NBA Stats API resultSet response to a DataFrame."""
    rs = data["resultSets"][result_set_index]
    return pd.DataFrame(rs["rowSet"], columns=rs["headers"])


# ── Team Pace ─────────────────────────────────────────────────────────────────

def fetch_team_pace() -> pd.DataFrame:
    """
    Returns a DataFrame with columns:
        TEAM_ID, TEAM_NAME, PACE
    sorted descending by PACE.
    """
    logger.info("Fetching team pace from NBA Stats API...")
    data = _get(
        "leaguedashteamstats",
        {
            "Season": CURRENT_SEASON,
            "SeasonType": SEASON_TYPE,
            "MeasureType": "Advanced",
            "PerMode": "PerGame",
            "PaceAdjust": "N",
            "PlusMinus": "N",
            "Rank": "N",
            "LastNGames": 0,
            "Month": 0,
            "OpponentTeamID": 0,
            "PORound": 0,
            "Period": 0,
            "GameScope": "",
            "PlayerExperience": "",
            "PlayerPosition": "",
            "StarterBench": "",
            "DateFrom": "",
            "DateTo": "",
            "GameSegment": "",
            "Location": "",
            "Outcome": "",
            "ShotClockRange": "",
            "Division": "",
            "Conference": "",
            "LeagueID": "00",
        },
    )
    df = _result_to_df(data)
    pace_df = df[["TEAM_ID", "TEAM_NAME", "PACE"]].copy()
    pace_df["PACE"] = pd.to_numeric(pace_df["PACE"], errors="coerce")
    return pace_df.sort_values("PACE", ascending=False).reset_index(drop=True)


# ── Opponent Rebounding ───────────────────────────────────────────────────────

def fetch_opponent_rebounding() -> pd.DataFrame:
    """
    Returns a DataFrame with columns:
        TEAM_ID, TEAM_NAME, OPP_REB, OPP_REB_RANK
    where OPP_REB is opponent rebounds allowed per game and
    OPP_REB_RANK is 1=best defense (fewest allowed), 30=worst.

    We target teams with HIGH OPP_REB (rank 20-30) — they give up more boards.
    """
    logger.info("Fetching opponent rebounding stats...")
    data = _get(
        "leaguedashteamstats",
        {
            "Season": CURRENT_SEASON,
            "SeasonType": SEASON_TYPE,
            "MeasureType": "Base",
            "PerMode": "PerGame",
            "PaceAdjust": "N",
            "PlusMinus": "N",
            "Rank": "Y",
            "LastNGames": 0,
            "Month": 0,
            "OpponentTeamID": 0,
            "PORound": 0,
            "Period": 0,
            "GameScope": "",
            "PlayerExperience": "",
            "PlayerPosition": "",
            "StarterBench": "",
            "DateFrom": "",
            "DateTo": "",
            "GameSegment": "",
            "Location": "",
            "Outcome": "",
            "ShotClockRange": "",
            "Division": "",
            "Conference": "",
            "LeagueID": "00",
        },
    )
    df = _result_to_df(data)

    # OPP_REB = opponent offensive rebounds allowed + defensive rebound contest
    # Best proxy available in base stats: OREB (opponent OFF reb) columns are
    # under the "Opponent" view. We use DREB_RANK as inverse proxy — teams with
    # poor DREB give more offensive boards to opponents.
    # For total opponent rebounds, REB column is all-team rebounds; we want
    # opponent's total. We'll use OPP_REB from the opponent dashboard instead.
    opp_data = _get(
        "leaguedashteamstats",
        {
            "Season": CURRENT_SEASON,
            "SeasonType": SEASON_TYPE,
            "MeasureType": "Opponent",
            "PerMode": "PerGame",
            "PaceAdjust": "N",
            "PlusMinus": "N",
            "Rank": "Y",
            "LastNGames": 0,
            "Month": 0,
            "OpponentTeamID": 0,
            "PORound": 0,
            "Period": 0,
            "GameScope": "",
            "PlayerExperience": "",
            "PlayerPosition": "",
            "StarterBench": "",
            "DateFrom": "",
            "DateTo": "",
            "GameSegment": "",
            "Location": "",
            "Outcome": "",
            "ShotClockRange": "",
            "Division": "",
            "Conference": "",
            "LeagueID": "00",
        },
    )
    opp_df = _result_to_df(opp_data)

    # OPP_REB = total rebounds allowed to opponents per game
    result = opp_df[["TEAM_ID", "TEAM_NAME", "OPP_REB"]].copy()
    result["OPP_REB"] = pd.to_numeric(result["OPP_REB"], errors="coerce")
    # Rank ascending: rank 1 = fewest rebounds allowed (best), 30 = most (worst)
    result["OPP_REB_RANK"] = result["OPP_REB"].rank(ascending=True, method="min").astype(int)
    return result.reset_index(drop=True)


# ── Player Game Logs ──────────────────────────────────────────────────────────

def fetch_player_game_log(player_id: int, last_n_games: int = 15) -> pd.DataFrame:
    """
    Returns a DataFrame of the player's recent game logs with columns:
        GAME_DATE, MATCHUP, MIN, REB, AST, RA  (REB+AST combined)
    Limited to last_n_games games.
    """
    logger.debug("Fetching game log for player %d...", player_id)
    data = _get(
        "playergamelog",
        {
            "PlayerID": player_id,
            "Season": CURRENT_SEASON,
            "SeasonType": SEASON_TYPE,
            "LeagueID": "00",
        },
    )
    df = _result_to_df(data)
    if df.empty:
        return df

    for col in ["MIN", "REB", "AST"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["RA"] = df["REB"] + df["AST"]
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], format="%b %d, %Y", errors="coerce")

    cols = ["GAME_DATE", "MATCHUP", "MIN", "REB", "AST", "RA"]
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


# ── Today's Matchups ──────────────────────────────────────────────────────────

def fetch_todays_matchups() -> pd.DataFrame:
    """
    Returns today's NBA matchups as a DataFrame with columns:
        HOME_TEAM_ID, AWAY_TEAM_ID, HOME_TEAM_ABB, AWAY_TEAM_ABB, GAME_ID
    Uses the scoreboard endpoint.
    """
    today_str = date.today().strftime("%Y-%m-%d")
    logger.info("Fetching today's matchups for %s...", today_str)
    data = _get(
        "scoreboardv2",
        {
            "GameDate": today_str,
            "LeagueID": "00",
            "DayOffset": 0,
        },
    )

    # GameHeader result set contains today's games
    rs = data["resultSets"][0]
    df = pd.DataFrame(rs["rowSet"], columns=rs["headers"])

    if df.empty:
        logger.warning("No games found for %s", today_str)
        return pd.DataFrame()

    matchups = df[["GAME_ID", "HOME_TEAM_ID", "VISITOR_TEAM_ID"]].copy()
    matchups.columns = ["GAME_ID", "HOME_TEAM_ID", "AWAY_TEAM_ID"]
    return matchups


# ── Player Info Lookup ────────────────────────────────────────────────────────

def fetch_active_players() -> pd.DataFrame:
    """
    Returns a DataFrame of all active NBA players with:
        PERSON_ID, DISPLAY_FIRST_LAST, TEAM_ID, TEAM_ABBREVIATION
    Used to map PrizePicks player names to NBA Stats player IDs.
    """
    logger.info("Fetching active player roster...")
    data = _get(
        "commonallplayers",
        {
            "LeagueID": "00",
            "Season": CURRENT_SEASON,
            "IsOnlyCurrentSeason": 1,
        },
    )
    df = _result_to_df(data)
    cols = ["PERSON_ID", "DISPLAY_FIRST_LAST", "TEAM_ID", "TEAM_ABBREVIATION"]
    # Not all columns present in all seasons — be defensive
    available = [c for c in cols if c in df.columns]
    return df[available].copy()
