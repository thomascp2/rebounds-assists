"""
data/prizepicks.py — Scrapes the PrizePicks public projections API.

PrizePicks exposes an unauthenticated JSON endpoint at:
    https://api.prizepicks.com/projections?league_id=7

The response is a JSON:API formatted payload with two top-level keys:
  - data:     list of projection objects (lines, stat types, demon/goblin flags)
  - included: related objects (players, leagues, games)

We parse both to build a flat DataFrame of NBA Rebs+Asts projections.
"""

import logging
import time
from typing import Optional

import requests
import pandas as pd

from config import PP_ENDPOINTS, PP_HEADERS, PP_LEAGUE_ID, PP_TARGET_STATS

logger = logging.getLogger(__name__)


def fetch_raw_projections(league_id: int = PP_LEAGUE_ID) -> dict:
    """
    Tries partner-api.prizepicks.com first, falls back to api.prizepicks.com.
    On 403, moves immediately to the next endpoint (bot protection on one != both).
    Uses a plain requests.Session with app.prizepicks.com headers — same approach
    proven to work in shared/prizepicks_client.py.
    """
    params = {
        "league_id": league_id,
        "per_page": 1000,
        "single_stat": "true",
    }
    session = requests.Session()
    session.headers.update(PP_HEADERS)

    logger.info("Fetching PrizePicks board for league_id=%d...", league_id)
    last_exc = None

    for endpoint in PP_ENDPOINTS:
        # 2s rate-limit between requests, matching shared client behaviour
        time.sleep(2)
        try:
            resp = session.get(endpoint, params=params, timeout=30)
            if resp.status_code == 200:
                content_type = resp.headers.get("Content-Type", "")
                if "application/json" in content_type:
                    logger.info("  PrizePicks OK via %s", endpoint)
                    return resp.json()
                logger.warning("  Non-JSON response from %s, trying next.", endpoint)
            elif resp.status_code == 403:
                logger.warning("  403 from %s (bot protection), trying next.", endpoint)
            else:
                logger.warning("  HTTP %d from %s, trying next.", resp.status_code, endpoint)
        except requests.RequestException as exc:
            logger.warning("  Request error on %s: %s", endpoint, exc)
            last_exc = exc

    if last_exc:
        raise last_exc
    raise requests.HTTPError("All PrizePicks endpoints returned non-200 responses.")


def _parse_included(included: list) -> tuple[dict, dict]:
    """
    Parses the JSON:API `included` array into two lookup dicts:
      - players:  {player_id: {name, team, position, ...}}
      - games:    {game_id: {start_time, away_team, home_team, ...}}
    """
    players = {}
    games = {}

    for obj in included:
        obj_type = obj.get("type")
        obj_id = obj.get("id")
        attrs = obj.get("attributes", {})

        if obj_type == "new_player":
            players[obj_id] = {
                "player_name": attrs.get("display_name") or attrs.get("name", ""),
                "team_abbreviation": attrs.get("team", ""),
                "position": attrs.get("position", ""),
                "nba_player_id": attrs.get("league_player_id"),  # may be None
            }

        elif obj_type == "game":
            # Team abbreviations moved to metadata.game_info.teams in 2026 API
            metadata = attrs.get("metadata", {})
            teams = metadata.get("game_info", {}).get("teams", {})
            away_abb = teams.get("away", {}).get("abbreviation", "")
            home_abb = teams.get("home", {}).get("abbreviation", "")
            game_label = f"{away_abb} @ {home_abb}" if away_abb and home_abb else ""
            games[obj_id] = {
                "game_label": game_label,
                "start_time": attrs.get("start_time", ""),
                "away_team": away_abb,
                "home_team": home_abb,
            }

    return players, games


def _is_target_stat(stat_type: str) -> bool:
    """
    Returns True if the stat_type string matches our Rebs+Asts targets.
    PrizePicks uses various strings for this prop — we cast a wide net.
    """
    normalized = stat_type.lower().replace(" ", "").replace("+", "")
    targets_normalized = {s.lower().replace(" ", "").replace("+", "") for s in PP_TARGET_STATS}
    # Also catch "reboundsassists" and partial matches
    extra = {"reboundsassists", "rebsasts", "ra", "reboundsplusassists"}
    return normalized in targets_normalized | extra


def fetch_rebs_asts_board() -> pd.DataFrame:
    """
    Main entry point. Returns a flat DataFrame of all PrizePicks NBA
    Rebs+Asts projections with columns:

        projection_id   — PrizePicks internal ID
        player_name     — Full display name
        team            — Team abbreviation
        position        — Player position
        nba_player_id   — NBA Stats player ID (if PrizePicks exposes it)
        stat_type       — Raw stat type string from PrizePicks
        line            — The projection line (float)
        is_demon        — True if marked as a Demon projection
        is_goblin       — True if marked as a Goblin projection
        away_team       — Away team abbreviation
        home_team       — Home team abbreviation
        game_label      — e.g. "OKC @ GSW"
        start_time      — Game start time string
    """
    raw = fetch_raw_projections()
    data_list = raw.get("data", [])
    included = raw.get("included", [])

    if not data_list:
        logger.warning("PrizePicks returned empty data list.")
        return pd.DataFrame()

    players_map, games_map = _parse_included(included)

    rows = []
    for proj in data_list:
        if proj.get("type") != "projection":
            continue

        attrs = proj.get("attributes", {})
        rels = proj.get("relationships", {})

        stat_type = attrs.get("stat_type", "")
        if not _is_target_stat(stat_type):
            continue

        # Pull related player and game IDs from relationships
        player_rel = rels.get("new_player", {}).get("data", {})
        game_rel = rels.get("game", {}).get("data", {})
        player_id = player_rel.get("id")
        game_id = game_rel.get("id")

        player_info = players_map.get(player_id, {})
        game_info = games_map.get(game_id, {})

        # Demon/Goblin detection — PrizePicks uses "odds_type" or "projection_type"
        odds_type = attrs.get("odds_type", "").lower()
        projection_type = attrs.get("projection_type", "").lower()
        is_demon = "demon" in odds_type or "demon" in projection_type
        is_goblin = "goblin" in odds_type or "goblin" in projection_type

        line_value = attrs.get("line_score") or attrs.get("projection")
        try:
            line = float(line_value)
        except (TypeError, ValueError):
            logger.debug("Could not parse line for projection %s", proj.get("id"))
            continue

        rows.append(
            {
                "projection_id": proj.get("id"),
                "player_name": player_info.get("player_name", ""),
                "team": player_info.get("team_abbreviation", ""),
                "position": player_info.get("position", ""),
                "nba_player_id": player_info.get("nba_player_id"),
                "stat_type": stat_type,
                "line": line,
                "is_demon": is_demon,
                "is_goblin": is_goblin,
                "away_team": game_info.get("away_team", ""),
                "home_team": game_info.get("home_team", ""),
                "game_label": game_info.get("game_label", ""),
                "start_time": game_info.get("start_time", ""),
            }
        )

    if not rows:
        logger.warning(
            "No Rebs+Asts props found on PrizePicks board. "
            "Stat type strings may have changed — check PP_TARGET_STATS in config.py."
        )
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    n_demon = df["is_demon"].sum()
    n_total = len(df)
    logger.info(
        "PrizePicks board: %d Rebs+Asts props found (%d demons, %d standard).",
        n_total, n_demon, n_total - n_demon,
    )
    return df.reset_index(drop=True)
