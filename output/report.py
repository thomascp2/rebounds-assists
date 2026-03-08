"""
output/report.py — Formats and exports scored prop results for all stat categories.

Produces:
  - Console summary with EV metrics, tier labels, and correlation warnings
  - CSV export for historical tracking
"""

import logging
import os
from datetime import date

import pandas as pd

from config import OUTPUT_DIR, OUTPUT_CSV, TOP_N_DISPLAY

logger = logging.getLogger(__name__)

TIER_ELITE  = 80
TIER_STRONG = 60
TIER_SOLID  = 40


def _tier_label(score: float, is_demon: bool, is_goblin: bool) -> str:
    prefix = "🔴 DEMON " if is_demon else ("🟢 GOBLIN " if is_goblin else "")
    if score >= TIER_ELITE:
        return f"{prefix}⭐⭐⭐ ELITE"
    elif score >= TIER_STRONG:
        return f"{prefix}⭐⭐ STRONG"
    elif score >= TIER_SOLID:
        return f"{prefix}⭐ SOLID"
    return f"{prefix}— MARGINAL"


def _fmt(val, fmt=".1f", suffix="", fallback="N/A") -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return fallback
    try:
        return f"{val:{fmt}}{suffix}"
    except (ValueError, TypeError):
        return str(val)


def _ev_label(ev) -> str:
    if ev is None or (isinstance(ev, float) and pd.isna(ev)):
        return "N/A"
    if ev > 0.15:
        return f"strong +EV ({ev:+.2f})"
    if ev > 0:
        return f"+EV ({ev:+.2f})"
    return f"-EV ({ev:+.2f})"


def print_console_report(scored_df: pd.DataFrame, top_n: int = TOP_N_DISPLAY) -> None:
    if scored_df.empty:
        print("\n⚠️  No qualifying props found today.\n")
        return

    today    = date.today().strftime("%A, %B %d %Y")
    total    = len(scored_df)
    n_demon  = int(scored_df["is_demon"].sum())
    n_goblin = int(scored_df["is_goblin"].sum())
    ev_pos   = int((scored_df["ev_estimate"] > 0).sum()) if "ev_estimate" in scored_df.columns else 0
    ev_strong= int((scored_df["ev_estimate"] > 0.15).sum()) if "ev_estimate" in scored_df.columns else 0
    avg_ev   = scored_df["ev_estimate"].mean() if "ev_estimate" in scored_df.columns else None

    print("\n" + "═" * 72)
    print(f"  🏀  NBA PRIZEPICKS PROPS  —  {today}")
    print(f"  {total} qualifying props  |  {n_demon} demon  |  {n_goblin} goblin")
    if "ev_estimate" in scored_df.columns:
        avg_ev_str = f"{avg_ev:+.2f}" if avg_ev is not None and not pd.isna(avg_ev) else "N/A"
        print(f"  📊 EV Summary: {ev_pos} +EV props | {ev_strong} strong +EV | avg ev: {avg_ev_str}")
    print("═" * 72)

    # Category breakdown
    if "stat_category" in scored_df.columns:
        cats = scored_df["stat_category"].value_counts().to_dict()
        cats_str = "  ".join(f"{k}: {v}" for k, v in cats.items())
        print(f"  Categories: {cats_str}")
    print("═" * 72)

    display_df = scored_df.head(top_n).copy()

    for rank, (_, row) in enumerate(display_df.iterrows(), start=1):
        tier     = _tier_label(row["final_score"], bool(row["is_demon"]), bool(row["is_goblin"]))
        cat      = row.get("stat_category", row.get("stat_type", ""))
        var_tier = row.get("variance_tier", "")
        ev_str   = _ev_label(row.get("ev_estimate"))

        print(f"\n  #{rank:02d}  {row['player_name']} ({row['team']} vs {row['vs']})  "
              f"[{row['position']}]  [{cat}]")
        print(f"       {row['game']}  |  {row['start_time']}")
        print(f"       PP Line: {row['pp_line']}  |  Score: {row['final_score']:.0f}  {tier}")
        print(f"       EV:      {ev_str}  |  Tier: {row.get('prop_tier','standard').upper()}"
              f"  |  Variance: {var_tier}")

        req_hr  = row.get("required_hit_rate")
        hr_act  = row.get("stat_hit_rate") or row.get("hit_rate")
        hr_margin = row.get("hit_rate_margin")
        req_str  = _fmt(req_hr, ".0%")
        act_str  = _fmt(hr_act, ".0%")
        mar_str  = _fmt(hr_margin, "+.0%") if hr_margin is not None else "N/A"
        print(f"       Hit Rate: {act_str} actual | {req_str} required | {mar_str} margin")
        print(f"       Edge:     {row['edge_summary']}")

        # Feature block
        pace_s     = _fmt(row.get("projected_pace"))
        cons_s     = _fmt(row.get("consensus_line"))
        gap_s      = _fmt(row.get("line_gap"), fmt="+.1f")
        avg_s      = _fmt(row.get("rolling_stat_avg"))
        std_s      = _fmt(row.get("rolling_stat_std"))
        mins_s     = _fmt(row.get("avg_minutes"))
        games_s    = _fmt(row.get("games_sampled"), fmt="d")
        season_s   = _fmt(row.get("season_avg"))

        print(f"       ── Features ──────────────────────────────────────────")
        print(f"       Game Pace:    {pace_s} pos/48")
        print(f"       Books Line:   {cons_s}  (gap vs PP: {gap_s})")
        print(f"       Last {games_s}g Avg:  {avg_s}  (±{std_s})  Mins: {mins_s}  Season: {season_s}")

        # Trend block — only show when we have multi-window data
        l5_avg  = row.get("l5_avg")
        l10_avg = row.get("l10_avg")
        l15_avg = row.get("l15_avg")
        if any(v is not None and not (isinstance(v, float) and pd.isna(v))
               for v in [l5_avg, l10_avg, l15_avg]):
            l5_s   = _fmt(l5_avg)
            l10_s  = _fmt(l10_avg)
            l15_s  = _fmt(l15_avg)
            l5_hr  = _fmt(row.get("l5_hit_rate"),  ".0%")
            l10_hr = _fmt(row.get("l10_hit_rate"), ".0%")
            l15_hr = _fmt(row.get("l15_hit_rate"), ".0%")
            t_dir  = str(row.get("trend_direction") or "unknown")
            t_pct  = row.get("trend_pct")
            t_pct_s = _fmt(t_pct, "+.0%") if t_pct is not None else "N/A"
            t_icon = {"up": "📈", "down": "📉", "flat": "➡️", "mixed": "〰️"}.get(t_dir, "")
            valid_tag = " ✅" if row.get("trend_is_valid") else ""
            print(f"       Trend:        {t_icon} {t_dir.upper()} {t_pct_s}{valid_tag}")
            print(f"       L5:  avg {l5_s}  HR {l5_hr}  |  "
                  f"L10: avg {l10_s}  HR {l10_hr}  |  "
                  f"L15: avg {l15_s}  HR {l15_hr}")

        # Category-specific feature lines
        if cat == "Rebs+Asts" or cat == "Rebounds":
            reb_rank = _fmt(row.get("opp_reb_rank"), fmt="d")
            reb_all  = _fmt(row.get("opp_reb_allowed"))
            fg_pct   = _fmt(row.get("opp_fg_pct"), fmt=".1%")
            fg_rank  = _fmt(row.get("opp_fg_pct_rank"), fmt="d")
            print(f"       Opp Reb Rank: #{reb_rank}  ({reb_all} reb/g allowed)")
            print(f"       Opp FG%:      {fg_pct}  (rank #{fg_rank})")
        elif cat == "Points":
            def_rank = _fmt(row.get("opp_def_rank"), fmt="d")
            pts_all  = _fmt(row.get("opp_pts_allowed"))
            usg      = _fmt(row.get("usg_pct"), fmt=".1%")
            print(f"       Opp Def Rank: #{def_rank}  ({pts_all} pts/g allowed)")
            print(f"       Usage Rate:   {usg}")
        elif cat == "Assists":
            ast_rank = _fmt(row.get("opp_ast_rank"), fmt="d")
            ast_all  = _fmt(row.get("opp_ast_allowed"))
            ast_pct  = _fmt(row.get("ast_pct"), fmt=".1%")
            print(f"       Opp AST Rank: #{ast_rank}  ({ast_all} ast/g allowed)")
            print(f"       AST% (player):{ast_pct}")
        elif cat == "3PM":
            tpa_rank = _fmt(row.get("opp_3pa_rank"), fmt="d")
            rec_pct  = _fmt(row.get("recent_3pt_pct"), fmt=".1%")
            fg3a     = _fmt(row.get("fg3a_per_game"))
            print(f"       Opp 3PA Rank: #{tpa_rank}  |  Recent 3PT%: {rec_pct}  |  3PA/g: {fg3a}")

        # Correlation warning
        warn = str(row.get("correlation_warning", "")).strip()
        if warn:
            print(f"       {warn}")

        print(f"       ──────────────────────────────────────────────────────")

    if total > top_n:
        print(f"\n  ... and {total - top_n} more props (see CSV for full list)")
    print("\n" + "═" * 72 + "\n")


def save_csv(scored_df: pd.DataFrame, output_dir: str = OUTPUT_DIR) -> str:
    os.makedirs(output_dir, exist_ok=True)
    today_str = date.today().strftime("%Y-%m-%d")
    filename  = f"{today_str}_{OUTPUT_CSV}"
    filepath  = os.path.join(output_dir, filename)
    scored_df.to_csv(filepath, index=False)
    logger.info("Results saved to %s", filepath)
    return filepath


def generate_report(scored_df: pd.DataFrame) -> str:
    print_console_report(scored_df)
    csv_path = save_csv(scored_df)
    print(f"  📁 Full results saved → {csv_path}\n")
    return csv_path
