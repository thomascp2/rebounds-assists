"""
data/odds_api.py — Fetches sportsbook consensus Rebs+Asts lines via The Odds API.

Strategy (in priority order):
  1. Use the combined `player_rebounds_assists` market if the book carries it.
  2. Fall back to summing `player_rebounds` Over line + `player_assists` Over line
     for the same bookmaker. Both must be present for a given player at a given
     book — we don't mix books across the two splits.
  3. If neither is available for a player, they get no consensus line and the
     line_gap feature will be null (the scorer handles this gracefully).

This means we request all three markets in a single API call per event
(one credit instead of three), then process the response in Python.
"""

import logging

import requests
import pandas as pd

from config import (
    ODDS_API_KEY,
    ODDS_API_BASE,
    ODDS_SPORT_KEY,
    ODDS_MARKETS_ALL,
    ODDS_MARKET_COMBINED,
    ODDS_MARKET_REBOUNDS,
    ODDS_MARKET_ASSISTS,
    ODDS_REGIONS,
    ODDS_BOOKMAKERS,
)

logger = logging.getLogger(__name__)


# ── HTTP helper ───────────────────────────────────────────────────────────────

def _get(path: str, params: dict) -> dict | list:
    """Thin GET wrapper for The Odds API with credit logging."""
    url = f"{ODDS_API_BASE}{path}"
    params = {**params, "apiKey": ODDS_API_KEY}
    resp = requests.get(url, params=params, timeout=15)
    remaining = resp.headers.get("x-requests-remaining", "?")
    used = resp.headers.get("x-requests-used", "?")
    logger.debug("Odds API — credits used: %s  remaining: %s", used, remaining)
    resp.raise_for_status()
    return resp.json()


# ── Event discovery ───────────────────────────────────────────────────────────

def fetch_todays_event_ids() -> list[dict]:
    """
    Returns today's NBA events as a list of dicts:
        [{"id": "...", "home_team": "...", "away_team": "...", "commence_time": "..."}, ...]
    One API credit consumed.
    """
    logger.info("Fetching today's NBA event IDs from Odds API...")
    events = _get(f"/sports/{ODDS_SPORT_KEY}/events", {"regions": ODDS_REGIONS})
    if not isinstance(events, list):
        logger.error("Unexpected Odds API events response: %s", events)
        return []
    logger.info("Found %d NBA events today.", len(events))
    return events


# ── Raw props fetch ───────────────────────────────────────────────────────────

def fetch_player_props_for_event(event_id: str) -> pd.DataFrame:
    """
    Fetches all three markets (combined + reb split + ast split) in one call.
    Returns a flat DataFrame with columns:
        player_name, market, bookmaker, point, price, side
    Returns empty DataFrame if the event has no prop markets.
    One API credit consumed per event.
    """
    try:
        data = _get(
            f"/sports/{ODDS_SPORT_KEY}/events/{event_id}/odds",
            {
                "regions": ODDS_REGIONS,
                "markets": ODDS_MARKETS_ALL,
                "bookmakers": ",".join(ODDS_BOOKMAKERS),
                "oddsFormat": "american",
            },
        )
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else None
        if status == 422:
            logger.debug("No prop markets available for event %s.", event_id)
            return pd.DataFrame()
        raise

    rows = []
    for bookmaker in data.get("bookmakers", []):
        bk_name = bookmaker.get("key", "")
        for market in bookmaker.get("markets", []):
            market_key = market.get("key", "")
            for outcome in market.get("outcomes", []):
                rows.append({
                    "player_name": outcome.get("description", ""),
                    "market":      market_key,
                    "bookmaker":   bk_name,
                    "point":       outcome.get("point"),
                    "price":       outcome.get("price"),
                    "side":        outcome.get("name", "").lower(),
                })

    return pd.DataFrame(rows) if rows else pd.DataFrame()


def fetch_all_player_props(event_ids: list[str]) -> pd.DataFrame:
    """Fetches props for all events and returns a combined DataFrame."""
    frames = []
    for eid in event_ids:
        df = fetch_player_props_for_event(eid)
        if not df.empty:
            df["event_id"] = eid
            frames.append(df)
    if not frames:
        logger.warning("No player prop data returned from Odds API for any event.")
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


# ── Consensus line computation ────────────────────────────────────────────────

def _combined_market_lines(props_df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts lines from the direct `player_rebounds_assists` combined market.
    Returns a DataFrame: player_name, bookmaker, combined_line (Over only).
    """
    mask = (
        (props_df["market"] == ODDS_MARKET_COMBINED) &
        (props_df["side"] == "over")
    )
    sub = props_df[mask].copy()
    sub["point"] = pd.to_numeric(sub["point"], errors="coerce")
    sub = sub.dropna(subset=["point"])
    return sub[["player_name", "bookmaker", "point"]].rename(
        columns={"point": "combined_line"}
    )


def _split_market_lines(props_df: pd.DataFrame) -> pd.DataFrame:
    """
    Constructs synthetic Rebs+Asts lines by summing the Over lines from
    `player_rebounds` and `player_assists` for the same player at the same book.

    Only produces a line when BOTH reb and ast are present at the same bookmaker
    — we never mix books across the two splits.

    Returns a DataFrame: player_name, bookmaker, combined_line.
    """
    overs = props_df[props_df["side"] == "over"].copy()
    overs["point"] = pd.to_numeric(overs["point"], errors="coerce")
    overs = overs.dropna(subset=["point"])

    reb = overs[overs["market"] == ODDS_MARKET_REBOUNDS][
        ["player_name", "bookmaker", "point"]
    ].rename(columns={"point": "reb_line"})

    ast = overs[overs["market"] == ODDS_MARKET_ASSISTS][
        ["player_name", "bookmaker", "point"]
    ].rename(columns={"point": "ast_line"})

    if reb.empty or ast.empty:
        return pd.DataFrame(columns=["player_name", "bookmaker", "combined_line"])

    merged = reb.merge(ast, on=["player_name", "bookmaker"], how="inner")
    merged["combined_line"] = merged["reb_line"] + merged["ast_line"]

    n = len(merged)
    if n:
        logger.debug(
            "Split-market fallback produced %d player×book synthetic RA lines.", n
        )

    return merged[["player_name", "bookmaker", "combined_line"]]


def compute_consensus_lines(props_df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes a consensus Rebs+Asts Over line per player using a two-pass approach:

      Pass 1 — Combined market: use `player_rebounds_assists` Over lines directly.
      Pass 2 — Split fallback: for players with no combined market data, sum the
               `player_rebounds` and `player_assists` Over lines per bookmaker.

    The final consensus_line is the median across all bookmakers that produced
    a valid line for that player (from either source).

    Returns a DataFrame with columns:
        player_name, consensus_line, num_books, books_listed, line_source
    where line_source is "combined", "split", or "mixed" (both sources present).
    """
    if props_df.empty:
        return pd.DataFrame()

    # Pass 1: combined market lines
    combined = _combined_market_lines(props_df)
    combined = combined.copy()
    combined["source"] = "combined"

    # Pass 2: split market fallback
    split = _split_market_lines(props_df)
    split = split.copy()
    split["source"] = "split"

    # Players covered by combined market — use combined exclusively for them
    combined_players = set(combined["player_name"].unique())

    # Only use split lines for players NOT in the combined market
    split_only = split[~split["player_name"].isin(combined_players)].copy()

    all_lines = pd.concat([combined, split_only], ignore_index=True)

    if all_lines.empty:
        logger.warning(
            "No Rebs+Asts lines found in combined OR split markets. "
            "Check that ODDS_BOOKMAKERS carry NBA player props today."
        )
        return pd.DataFrame()

    # Compute per-player consensus
    def _agg(group):
        sources = group["source"].unique().tolist()
        if len(sources) == 1:
            source_label = sources[0]
        else:
            source_label = "mixed"
        return pd.Series({
            "consensus_line": group["combined_line"].median(),
            "num_books":      group["bookmaker"].nunique(),
            "books_listed":   ", ".join(sorted(group["bookmaker"].unique())),
            "line_source":    source_label,
        })

    consensus = (
        all_lines.groupby("player_name")
        .apply(_agg)
        .reset_index()
    )

    n_combined = (consensus["line_source"] == "combined").sum()
    n_split    = (consensus["line_source"] == "split").sum()
    n_mixed    = (consensus["line_source"] == "mixed").sum()

    logger.info(
        "Consensus lines — %d players total  "
        "(combined market: %d | split fallback: %d | mixed: %d)",
        len(consensus), n_combined, n_split, n_mixed,
    )

    return consensus
