"""
main.py — Orchestrates the full Rebs+Asts PrizePicks scoring pipeline.

Run daily (ideally 30–60 min before first tip-off):
    python main.py

Pipeline steps:
  1. Fetch PrizePicks board (Rebs+Asts props + demon flags)
  2. Fetch NBA Stats (team pace, opponent rebounding, player game logs)
  3. Fetch sportsbook consensus lines (The Odds API)
  4. Engineer features (merge all sources)
  5. Score and rank props (rule-based with demon boost)
  6. Output ranked report + CSV

All configuration lives in config.py — no code changes needed for tuning.
"""

import logging
import sys

# Force UTF-8 output on Windows so Unicode chars in the report print correctly
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

# ── Logging setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("main")


def run_pipeline():
    # ── Imports (deferred so logging is configured first) ─────────────────────
    from data.prizepicks import fetch_rebs_asts_board
    from data.nba_stats import (
        fetch_team_pace,
        fetch_opponent_rebounding,
        fetch_team_shooting,
        fetch_active_players,
        fetch_all_player_logs,
    )
    from data.odds_api import (
        fetch_todays_event_ids,
        fetch_all_player_props,
        compute_consensus_lines,
    )
    from features.engineer import build_feature_dataframe
    from model.scorer import score_all
    from output.report import generate_report
    from config import FORM_WINDOW

    logger.info("=" * 60)
    logger.info("  REBS+ASTS PRIZEPICKS PIPELINE  —  starting run")
    logger.info("=" * 60)

    # ── Step 1: PrizePicks board ───────────────────────────────────────────────
    logger.info("[1/6] Fetching PrizePicks Rebs+Asts board...")
    try:
        pp_board = fetch_rebs_asts_board()
    except Exception as exc:
        logger.error("Failed to fetch PrizePicks board: %s", exc)
        sys.exit(1)

    if pp_board.empty:
        logger.warning("PrizePicks board returned no Rebs+Asts props. Exiting.")
        sys.exit(0)

    logger.info("  → %d props on board (%d demons)",
                len(pp_board), int(pp_board["is_demon"].sum()))

    # ── Step 2: NBA Stats API ─────────────────────────────────────────────────
    logger.info("[2/6] Fetching NBA Stats data...")

    try:
        team_pace = fetch_team_pace()
        logger.info("  → Team pace fetched for %d teams.", len(team_pace))
    except Exception as exc:
        logger.error("Failed to fetch team pace: %s", exc)
        team_pace = __import__("pandas").DataFrame()

    try:
        opp_rebounding = fetch_opponent_rebounding()
        logger.info("  → Opponent rebounding fetched for %d teams.", len(opp_rebounding))
    except Exception as exc:
        logger.error("Failed to fetch opponent rebounding: %s", exc)
        opp_rebounding = __import__("pandas").DataFrame()

    try:
        team_shooting = fetch_team_shooting()
        logger.info("  → Team shooting fetched for %d teams.", len(team_shooting))
    except Exception as exc:
        logger.error("Failed to fetch team shooting: %s", exc)
        team_shooting = __import__("pandas").DataFrame()

    try:
        active_players = fetch_active_players()
        logger.info("  → %d active players in roster.", len(active_players))
    except Exception as exc:
        logger.error("Failed to fetch active players: %s", exc)
        active_players = __import__("pandas").DataFrame()

    # Collect unique player IDs from the board for game log fetching
    player_ids = []
    if not active_players.empty and not pp_board.empty:
        import pandas as pd
        from difflib import get_close_matches

        id_map = dict(zip(
            active_players["DISPLAY_FIRST_LAST"].str.lower(),
            active_players["PERSON_ID"].astype(int),
        ))
        for name in pp_board["player_name"].unique():
            name_lower = name.strip().lower()
            if name_lower in id_map:
                player_ids.append(id_map[name_lower])
            else:
                matches = get_close_matches(name_lower, id_map.keys(), n=1, cutoff=0.85)
                if matches:
                    player_ids.append(id_map[matches[0]])

    try:
        player_logs = fetch_all_player_logs(player_ids, last_n_games=FORM_WINDOW + 5)
        logger.info("  → Game logs fetched for %d players.", len(player_logs))
    except Exception as exc:
        logger.error("Failed to fetch player game logs: %s", exc)
        player_logs = {}

    # ── Step 3: Sportsbook Lines ──────────────────────────────────────────────
    logger.info("[3/6] Fetching sportsbook consensus lines...")

    import pandas as pd

    try:
        events = fetch_todays_event_ids()
        event_ids = [e["id"] for e in events]
        raw_props = fetch_all_player_props(event_ids)
        consensus_lines = compute_consensus_lines(raw_props)
        logger.info("  → Consensus lines for %d players.", len(consensus_lines))
    except Exception as exc:
        logger.warning("Odds API unavailable or key not set (%s). Skipping line gap.", exc)
        consensus_lines = pd.DataFrame()

    # ── Step 4: Feature Engineering ───────────────────────────────────────────
    logger.info("[4/6] Engineering features...")
    try:
        feature_df = build_feature_dataframe(
            pp_board=pp_board,
            team_pace=team_pace,
            opp_rebounding=opp_rebounding,
            team_shooting=team_shooting,
            player_logs=player_logs,
            consensus_lines=consensus_lines,
            active_players=active_players,
            form_window=FORM_WINDOW,
        )
    except Exception as exc:
        logger.error("Feature engineering failed: %s", exc)
        sys.exit(1)

    if feature_df.empty:
        logger.warning("Feature DataFrame is empty after engineering. Exiting.")
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
