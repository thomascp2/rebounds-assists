"""
output/report.py — Formats and exports the scored prop results.

Produces:
  - Console summary table (top N picks, color-coded by score tier)
  - CSV export for further analysis / historical tracking
"""

import logging
import os
from datetime import date

import pandas as pd

from config import OUTPUT_DIR, OUTPUT_CSV, TOP_N_DISPLAY

logger = logging.getLogger(__name__)

# Score tiers for console display
TIER_ELITE = 80
TIER_STRONG = 60
TIER_SOLID = 40


def _tier_label(score: float, is_demon: bool) -> str:
    prefix = "🔴 DEMON " if is_demon else ""
    if score >= TIER_ELITE:
        return f"{prefix}⭐⭐⭐ ELITE"
    elif score >= TIER_STRONG:
        return f"{prefix}⭐⭐ STRONG"
    elif score >= TIER_SOLID:
        return f"{prefix}⭐ SOLID"
    return f"{prefix}— MARGINAL"


def _fmt_optional(val, fmt: str = ".1f", suffix: str = "") -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "N/A"
    try:
        return f"{val:{fmt}}{suffix}"
    except (ValueError, TypeError):
        return str(val)


def print_console_report(scored_df: pd.DataFrame, top_n: int = TOP_N_DISPLAY) -> None:
    """Prints a formatted summary of the top N scored props to stdout."""
    if scored_df.empty:
        print("\n⚠️  No qualifying Rebs+Asts props found today.\n")
        return

    today = date.today().strftime("%A, %B %d %Y")
    total = len(scored_df)
    demon_count = int(scored_df["is_demon"].sum())

    print("\n" + "═" * 72)
    print(f"  🏀  REBS+ASTS PRIZEPICKS PICKS  —  {today}")
    print(f"  {total} qualifying props  |  {demon_count} demon lines")
    print("═" * 72)

    display_df = scored_df.head(top_n).copy()

    for rank, (_, row) in enumerate(display_df.iterrows(), start=1):
        tier = _tier_label(row["final_score"], bool(row["is_demon"]))
        demon_tag = " 🔴 DEMON" if row["is_demon"] else ""
        goblin_tag = " 🟢 GOBLIN" if row["is_goblin"] else ""

        print(f"\n  #{rank:02d}  {row['player_name']} ({row['team']} vs {row['vs']})  "
              f"[{row['position']}]{demon_tag}{goblin_tag}")
        print(f"       {row['game']}  |  {row['start_time']}")
        print(f"       Stat:         {row['stat_type']}")
        print(f"       PP Line:      {row['pp_line']}")
        print(f"       Score:        {row['final_score']:.0f} pts  {tier}")
        print(f"       Edge:         {row['edge_summary']}")

        # Feature detail block
        pace_str = _fmt_optional(row.get("projected_pace"))
        reb_rank = _fmt_optional(row.get("opp_reb_rank"), fmt="d")
        reb_allowed = _fmt_optional(row.get("opp_reb_allowed"))
        fg_pct = _fmt_optional(row.get("opp_fg_pct"), fmt=".1%")
        fg_rank = _fmt_optional(row.get("opp_fg_pct_rank"), fmt="d")
        consensus = _fmt_optional(row.get("consensus_line"))
        gap = _fmt_optional(row.get("line_gap"), fmt="+.1f")
        ra_avg = _fmt_optional(row.get("rolling_ra_avg"))
        ra_std = _fmt_optional(row.get("rolling_ra_std"))
        hit = _fmt_optional(row.get("hit_rate"), fmt=".0%")
        mins = _fmt_optional(row.get("avg_minutes"))
        games = _fmt_optional(row.get("games_sampled"), fmt="d")

        print(f"       ── Features ──────────────────────────────────────────")
        print(f"       Game Pace:    {pace_str} pos/48 min")
        print(f"       Opp Reb Rank: #{reb_rank}  (allowed {reb_allowed} reb/g)")
        print(f"       Opp FG%:      {fg_pct}  (rank #{fg_rank} — lower = more misses)")
        print(f"       Books Line:   {consensus}  (gap vs PP: {gap})")
        print(f"       Last {games}g Avg: {ra_avg} RA  (±{ra_std})  "
              f"Hit Rate: {hit}  Mins: {mins}")
        print(f"       ──────────────────────────────────────────────────────")

    if total > top_n:
        print(f"\n  ... and {total - top_n} more props (see CSV for full list)")
    print("\n" + "═" * 72 + "\n")


def save_csv(scored_df: pd.DataFrame, output_dir: str = OUTPUT_DIR) -> str:
    """
    Saves the full scored DataFrame to a dated CSV file.
    Returns the file path.
    """
    os.makedirs(output_dir, exist_ok=True)
    today_str = date.today().strftime("%Y-%m-%d")
    filename = f"{today_str}_{OUTPUT_CSV}"
    filepath = os.path.join(output_dir, filename)
    scored_df.to_csv(filepath, index=False)
    logger.info("Results saved to %s", filepath)
    return filepath


def generate_report(scored_df: pd.DataFrame) -> str:
    """
    Master output function: prints console report and saves CSV.
    Returns the CSV path.
    """
    print_console_report(scored_df)
    csv_path = save_csv(scored_df)
    print(f"  📁 Full results saved → {csv_path}\n")
    return csv_path
