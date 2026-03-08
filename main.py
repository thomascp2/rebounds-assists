"""
main.py — Orchestrates the full NBA PrizePicks scoring pipeline.

Covers: Points, Rebounds, Assists, Rebs+Asts, 3PM.
Run daily 30–60 min before first tip-off:
    python main.py
"""

import logging
import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("main")


def run_pipeline():
    import pandas as pd
    import unicodedata
    from difflib import get_close_matches

    from data.prizepicks import fetch_nba_board
    from data.nba_stats import (
        fetch_team_pace,
        fetch_opponent_stats,
        fetch_team_shooting,
        fetch_active_players,
        fetch_all_player_logs,
        fetch_player_advanced_stats,
    )
    from data.odds_api import (
        fetch_todays_event_ids,
        fetch_all_player_props,
        compute_all_consensus_lines,
    )
    from features.engineer import build_feature_dataframe
    from model.scorer import score_all
    from output.report import generate_report
    from config import FORM_WINDOW_BY_VARIANCE, VARIANCE_TIER

    logger.info("=" * 60)
    logger.info("  NBA PRIZEPICKS PIPELINE  —  starting run")
    logger.info("=" * 60)

    # ── Step 1: PrizePicks board ───────────────────────────────────────────────
    logger.info("[1/6] Fetching PrizePicks board (all stat categories)...")
    try:
        pp_board = fetch_nba_board()
    except Exception as exc:
        logger.error("Failed to fetch PrizePicks board: %s", exc)
        sys.exit(1)

    if pp_board.empty:
        logger.warning("PrizePicks board returned no qualifying props. Exiting.")
        sys.exit(0)

    logger.info("  → %d props on board (%d demons, %d goblins)",
                len(pp_board),
                int(pp_board["is_demon"].sum()),
                int(pp_board["is_goblin"].sum()))

    # ── Step 2: NBA Stats ─────────────────────────────────────────────────────
    logger.info("[2/6] Fetching NBA Stats data...")

    try:
        team_pace = fetch_team_pace()
        logger.info("  → Team pace: %d teams.", len(team_pace))
    except Exception as exc:
        logger.error("Failed to fetch team pace: %s", exc)
        team_pace = pd.DataFrame()

    try:
        opponent_stats = fetch_opponent_stats()
        logger.info("  → Opponent stats: %d teams.", len(opponent_stats))
    except Exception as exc:
        logger.error("Failed to fetch opponent stats: %s", exc)
        opponent_stats = pd.DataFrame()

    try:
        team_shooting = fetch_team_shooting()
        logger.info("  → Team shooting: %d teams.", len(team_shooting))
    except Exception as exc:
        logger.error("Failed to fetch team shooting: %s", exc)
        team_shooting = pd.DataFrame()

    try:
        active_players = fetch_active_players()
        logger.info("  → Active players: %d.", len(active_players))
    except Exception as exc:
        logger.error("Failed to fetch active players: %s", exc)
        active_players = pd.DataFrame()

    try:
        player_advanced = fetch_player_advanced_stats()
        logger.info("  → Player advanced stats: %d players.", len(player_advanced))
    except Exception as exc:
        logger.warning("Player advanced stats unavailable (%s). Continuing.", exc)
        player_advanced = pd.DataFrame()

    # Resolve player IDs for game log fetching
    def _ascii_fold(name):
        return (
            unicodedata.normalize("NFKD", name)
            .encode("ascii", "ignore")
            .decode("ascii")
            .lower()
            .strip()
        )

    player_ids = []
    if not active_players.empty and not pp_board.empty:
        id_map = dict(zip(
            active_players["DISPLAY_FIRST_LAST"].apply(_ascii_fold),
            active_players["PERSON_ID"].astype(int),
        ))
        for name in pp_board["player_name"].unique():
            folded = _ascii_fold(name)
            if folded in id_map:
                player_ids.append(id_map[folded])
            else:
                matches = get_close_matches(folded, id_map.keys(), n=1, cutoff=0.82)
                if matches:
                    player_ids.append(id_map[matches[0]])

    # Use the widest form window needed across all stat categories
    max_window = max(FORM_WINDOW_BY_VARIANCE.values()) + 5  # buffer
    try:
        player_logs = fetch_all_player_logs(player_ids, last_n_games=max_window)
        logger.info("  → Game logs: %d players.", len(player_logs))
    except Exception as exc:
        logger.error("Failed to fetch player game logs: %s", exc)
        player_logs = {}

    # ── Step 3: Sportsbook Lines ──────────────────────────────────────────────
    logger.info("[3/6] Fetching sportsbook consensus lines (all markets)...")
    try:
        events    = fetch_todays_event_ids()
        event_ids = [e["id"] for e in events]
        raw_props = fetch_all_player_props(event_ids)
        all_consensus = compute_all_consensus_lines(raw_props)
        logger.info("  → Consensus lines: %d entries.", len(all_consensus))
    except Exception as exc:
        logger.warning("Odds API unavailable (%s). Skipping line gaps.", exc)
        all_consensus = pd.DataFrame()

    # ── Step 4: Feature Engineering ───────────────────────────────────────────
    logger.info("[4/6] Engineering features...")
    try:
        feature_df = build_feature_dataframe(
            pp_board=pp_board,
            team_pace=team_pace,
            opponent_stats=opponent_stats,
            team_shooting=team_shooting,
            player_logs=player_logs,
            all_consensus_lines=all_consensus,
            active_players=active_players,
            player_advanced_stats=player_advanced,
        )
    except Exception as exc:
        logger.error("Feature engineering failed: %s", exc)
        sys.exit(1)

    if feature_df.empty:
        logger.warning("Feature DataFrame is empty. Exiting.")
        sys.exit(0)

    # ── Step 5: Score and Rank ────────────────────────────────────────────────
    logger.info("[5/6] Scoring and ranking props...")
    try:
        scored_df = score_all(feature_df)
    except Exception as exc:
        logger.error("Scoring failed: %s", exc)
        sys.exit(1)

    # ── Step 6: Output ────────────────────────────────────────────────────────
    logger.info("[6/6] Generating report...")
    generate_report(scored_df)

    logger.info("Pipeline complete.")
    return scored_df


if __name__ == "__main__":
    run_pipeline()
