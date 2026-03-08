"""
ml/backfill_outcomes.py — Auto-fill hit/miss outcomes for a past picks CSV.

For each prop in the picks CSV, fetches the player's game log from NBA Stats
and determines hit (1) or miss (0) based on whether the stat cleared the PP line.
Writes back to the same CSV with 'hit' and 'actual_stat' columns filled.

Usage:
    python -m ml.backfill_outcomes                       # fills yesterday's picks
    python -m ml.backfill_outcomes --date 2026-03-08
    python -m ml.backfill_outcomes --date 2026-03-08 --dry-run

Notes:
    - Only fills rows where hit is currently null (won't overwrite existing outcomes)
    - Skips players who played < MIN_MINUTES_THRESHOLD (DNP / garbage time)
    - Run the morning after games finish (logs are available ~2-4 hours post-game)
"""

import argparse
import logging
import sys
import time
import unicodedata
from datetime import date, timedelta
from difflib import get_close_matches
from pathlib import Path

import pandas as pd

# Allow running as a module from the project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from data.nba_stats import fetch_active_players, fetch_player_game_log
from config import MIN_MINUTES_THRESHOLD

logger = logging.getLogger(__name__)

_STAT_LOG_COL = {
    "Rebs+Asts": "RA",
    "Points":    "PTS",
    "Rebounds":  "REB",
    "Assists":   "AST",
    "3PM":       "FG3M",
}


def _ascii_fold(name: str) -> str:
    return (
        unicodedata.normalize("NFKD", name)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
        .strip()
    )


def _resolve_player_id(name: str, id_map: dict) -> int | None:
    folded = _ascii_fold(name)
    if folded in id_map:
        return id_map[folded]
    matches = get_close_matches(folded, id_map.keys(), n=1, cutoff=0.82)
    return id_map[matches[0]] if matches else None


def backfill_outcomes(picks_date: date, dry_run: bool = False) -> pd.DataFrame:
    """
    Loads the picks CSV for picks_date, fetches game logs, and fills hit/miss.

    Returns the updated DataFrame (with or without saving, based on dry_run).
    """
    csv_path = Path("output") / f"{picks_date:%Y-%m-%d}_nba_picks.csv"
    if not csv_path.exists():
        logger.error("Picks CSV not found: %s", csv_path)
        sys.exit(1)

    df = pd.read_csv(csv_path)

    if "hit" not in df.columns:
        df["hit"] = None
    if "actual_stat" not in df.columns:
        df["actual_stat"] = None

    pending_mask = df["hit"].isna()
    if not pending_mask.any():
        logger.info("All %d props already have outcomes. Nothing to do.", len(df))
        return df

    logger.info("Backfilling outcomes for %s (%d props pending)...",
                picks_date, pending_mask.sum())

    # Build name → player_id lookup
    active = fetch_active_players()
    id_map = dict(zip(
        active["DISPLAY_FIRST_LAST"].apply(_ascii_fold),
        active["PERSON_ID"].astype(int),
    ))

    log_cache: dict[int, pd.DataFrame] = {}
    target_date_str = picks_date.strftime("%Y-%m-%d")

    hits = misses = skipped = 0

    for idx, row in df[pending_mask].iterrows():
        player_name = str(row["player_name"])
        stat_cat    = str(row.get("stat_category", ""))
        pp_line     = float(row["pp_line"])
        stat_col    = _STAT_LOG_COL.get(stat_cat)

        if stat_col is None:
            logger.warning("Unknown category '%s' for %s — skipping", stat_cat, player_name)
            skipped += 1
            continue

        pid = _resolve_player_id(player_name, id_map)
        if pid is None:
            logger.warning("Could not resolve player ID for '%s' — skipping", player_name)
            skipped += 1
            continue

        if pid not in log_cache:
            try:
                log_cache[pid] = fetch_player_game_log(pid, last_n_games=5)
                time.sleep(0.3)
            except Exception as exc:
                logger.warning("Log fetch failed for %s (%d): %s", player_name, pid, exc)
                log_cache[pid] = pd.DataFrame()

        log = log_cache[pid]
        if log.empty:
            skipped += 1
            continue

        # Find the game matching picks_date
        game_rows = log[log["GAME_DATE"].dt.strftime("%Y-%m-%d") == target_date_str]
        if game_rows.empty:
            logger.debug("%s — no game found on %s", player_name, target_date_str)
            skipped += 1
            continue

        game_row = game_rows.iloc[0]

        # Skip DNP / garbage-time appearances
        mins = float(game_row.get("MIN", 0) or 0)
        if mins < MIN_MINUTES_THRESHOLD:
            logger.debug("%s played only %.0f min — marking as skipped", player_name, mins)
            skipped += 1
            continue

        # Get actual stat value
        if stat_col == "RA":
            reb = float(game_row.get("REB", 0) or 0)
            ast = float(game_row.get("AST", 0) or 0)
            actual = reb + ast
        elif stat_col in game_row.index:
            actual = float(game_row[stat_col] or 0)
        else:
            logger.warning("%s — stat column '%s' missing from log", player_name, stat_col)
            skipped += 1
            continue

        hit = 1 if actual >= pp_line else 0
        df.at[idx, "hit"]         = hit
        df.at[idx, "actual_stat"] = actual

        if hit:
            hits += 1
        else:
            misses += 1

        logger.debug("%s [%s] actual=%.1f vs line=%.1f → %s",
                     player_name, stat_cat, actual, pp_line,
                     "HIT ✅" if hit else "MISS ❌")

    filled = hits + misses
    if filled > 0:
        hit_rate = hits / filled
        logger.info(
            "Filled %d outcomes: %d hits / %d misses (%.1f%% hit rate) | %d skipped",
            filled, hits, misses, 100 * hit_rate, skipped,
        )
    else:
        logger.warning("No outcomes filled. Games may not be final yet or date mismatch.")

    if not dry_run and filled > 0:
        df.to_csv(csv_path, index=False)
        logger.info("Saved → %s", csv_path)
    elif dry_run:
        logger.info("Dry run — changes not written to disk.")

    return df


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Backfill hit/miss outcomes into a dated picks CSV."
    )
    parser.add_argument(
        "--date", type=str, default=None,
        help="Date of the picks CSV (YYYY-MM-DD). Defaults to yesterday.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Compute outcomes but do not write to file.",
    )
    args = parser.parse_args()

    target = (
        date.fromisoformat(args.date)
        if args.date
        else date.today() - timedelta(days=1)
    )

    backfill_outcomes(target, dry_run=args.dry_run)
