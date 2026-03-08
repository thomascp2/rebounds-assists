"""
output/lineup_builder.py — Generates optimal Power Play and Flex Play lineups
from the scored prop DataFrame.

Lineup rules:
  - One prop per player per lineup
  - Team concentration cap: max 2 same-team players for size 2–4, max 3 for 5–6
  - No all-goblin lineup (drags down payout)
  - No 2-man lineup with 2 goblins (payout < 2x)
  - No 3+ demons from the same team in the same stat category per lineup

Ranking: Power Play EV = P(all_hit) × payout_multiplier - 1
         Flex EV      = P(all_hit) × flex_multiplier - 1  (full-hit only, conservative)
"""

import logging
import os
from collections import Counter
from datetime import date
from itertools import combinations

import pandas as pd

from config import OUTPUT_DIR

logger = logging.getLogger(__name__)

# ── Payout tables ─────────────────────────────────────────────────────────────
PP_PAYOUTS   = {2: 3.0,  3: 6.0,   4: 10.0,  5: 20.0,  6: 25.0}
FLEX_PAYOUTS = {2: None, 3: 2.5,   4: 5.0,   5: 10.0,  6: 12.5}

# Lineup counts to generate per size
LINEUP_SPECS = {2: 5, 3: 4, 4: 3, 5: 3, 6: 3}

# Maximum props from pool to enumerate over (controls speed vs. coverage)
POOL_TOP_N = 28


# ── Pool builder ─────────────────────────────────────────────────────────────

def build_pool(scored_df: pd.DataFrame) -> pd.DataFrame:
    """
    Builds a clean prop pool from the scored DataFrame:
      - Requires valid stat_hit_rate and ev_estimate
      - Deduplicates to one prop per (player, stat_category) keeping best EV
      - Filters to +EV props only (ev_estimate >= 0)
      - Returns top POOL_TOP_N by ev_estimate
    """
    df = scored_df.copy()
    df = df[df["stat_hit_rate"].notna() & df["ev_estimate"].notna()]
    df = (
        df
        .sort_values(["ev_estimate", "stat_hit_rate"], ascending=[False, False])
        .drop_duplicates(subset=["player_name", "stat_category"])
        .query("ev_estimate >= 0")
        .reset_index(drop=True)
    )
    logger.info("Lineup pool: %d unique +EV props (after dedup)", len(df))
    return df.head(POOL_TOP_N)


# ── Lineup rules ─────────────────────────────────────────────────────────────

def _lineup_valid(legs: list[dict], size: int) -> bool:
    players = [l["player_name"] for l in legs]
    if len(set(players)) != len(players):
        return False

    teams = [l["team"] for l in legs]
    max_same_team = 2 if size <= 4 else 3
    if max(Counter(teams).values(), default=0) > max_same_team:
        return False

    tiers = [l["prop_tier"] for l in legs]
    if all(t == "goblin" for t in tiers):
        return False
    if size == 2 and tiers.count("goblin") >= 2:
        return False

    # No 3+ demons same category same team
    for team in set(teams):
        team_legs = [l for l in legs if l["team"] == team]
        for cat in set(l["stat_category"] for l in team_legs):
            demons = sum(
                1 for l in team_legs
                if l["stat_category"] == cat and l["prop_tier"] == "demon"
            )
            if demons >= 3:
                return False

    return True


# ── EV helpers ────────────────────────────────────────────────────────────────

def _p_all(legs: list[dict]) -> float:
    p = 1.0
    for l in legs:
        p *= l["stat_hit_rate"]
    return round(p, 6)


def _lineup_ev(p: float, size: int, flex: bool = False) -> float | None:
    mult = (FLEX_PAYOUTS if flex else PP_PAYOUTS).get(size)
    if mult is None:
        return None
    return round(p * mult - 1, 4)


# ── Core builder ──────────────────────────────────────────────────────────────

def build_lineups(scored_df: pd.DataFrame) -> dict[int, list[dict]]:
    """
    Generates optimal lineups for each size defined in LINEUP_SPECS.

    Returns a dict: {size: [{"ev_pp", "ev_flex", "p_all", "legs": [...]}, ...]}
    """
    pool = build_pool(scored_df)
    if pool.empty:
        logger.warning("Lineup pool is empty — no +EV props with valid hit rates.")
        return {}

    records = pool.to_dict("records")
    results: dict[int, list[dict]] = {}

    for size, top_n in LINEUP_SPECS.items():
        candidates = []
        for combo in combinations(records, size):
            legs = list(combo)
            if _lineup_valid(legs, size):
                p = _p_all(legs)
                ev_pp   = _lineup_ev(p, size, flex=False)
                ev_flex = _lineup_ev(p, size, flex=True)
                candidates.append({
                    "ev_pp":   ev_pp,
                    "ev_flex": ev_flex,
                    "p_all":   p,
                    "legs":    legs,
                })
        candidates.sort(key=lambda x: -(x["ev_pp"] or -99))
        results[size] = candidates[:top_n]
        logger.info("  Size %d: %d valid combos, kept top %d", size, len(candidates), min(top_n, len(candidates)))

    return results


# ── Console printer ───────────────────────────────────────────────────────────

_TIER_ICON = {"demon": "🔴", "standard": "◻️", "goblin": "🟢"}


def print_lineups(lineups: dict[int, list[dict]]) -> None:
    if not lineups:
        print("\n⚠️  No valid lineups generated.\n")
        return

    print("\n" + "═" * 68)
    print("  🏀  PRIZEPICKS OPTIMAL LINEUPS")
    print("═" * 68)

    for size in sorted(lineups):
        entries = lineups[size]
        if not entries:
            continue
        flex_label = f" | Flex {FLEX_PAYOUTS[size]}x" if FLEX_PAYOUTS.get(size) else ""
        print(f"\n{'─'*68}")
        print(f"  {LINEUP_SPECS[size]}x {size}-MAN  |  Power Play {PP_PAYOUTS[size]:.0f}x{flex_label}")
        print(f"{'─'*68}")

        for i, entry in enumerate(entries, 1):
            ev_pp    = entry["ev_pp"]
            ev_flex  = entry["ev_flex"]
            p        = entry["p_all"]
            legs     = entry["legs"]

            flex_str = f"  |  Flex EV: {ev_flex:+.3f}" if ev_flex is not None else ""
            teams_ct = dict(Counter(l["team"] for l in legs))
            cats_ct  = dict(Counter(l["stat_category"] for l in legs))

            print(f"\n  Lineup #{i}  PP EV: {ev_pp:+.4f}  P(all): {p:.1%}{flex_str}")
            print(f"  Teams: {teams_ct}  |  Cats: {cats_ct}")

            for j, leg in enumerate(legs, 1):
                icon  = _TIER_ICON.get(leg["prop_tier"], "◻️")
                warn  = str(leg.get("correlation_warning") or "").strip()
                wtag  = "  ⚠️" if warn else ""
                print(
                    f"    {j}. {icon} {leg['player_name']:24s} ({leg['team']}) "
                    f"| {leg['stat_category']:12s} o{leg['pp_line']:<6} "
                    f"| EV:{leg['ev_estimate']:+.2f} HR:{leg['stat_hit_rate']:.0%}{wtag}"
                )

    print("\n" + "═" * 68 + "\n")


# ── CSV export ────────────────────────────────────────────────────────────────

def save_lineups_csv(lineups: dict[int, list[dict]], output_dir: str = OUTPUT_DIR) -> str:
    """
    Saves all lineups to a dated CSV with one row per lineup leg.
    Returns the file path.
    """
    os.makedirs(output_dir, exist_ok=True)
    today_str = date.today().strftime("%Y-%m-%d")
    filepath  = os.path.join(output_dir, f"{today_str}_lineups.csv")

    rows = []
    for size, entries in lineups.items():
        for lineup_num, entry in enumerate(entries, 1):
            for leg_num, leg in enumerate(entry["legs"], 1):
                rows.append({
                    "lineup_size":    size,
                    "lineup_num":     lineup_num,
                    "leg_num":        leg_num,
                    "ev_pp":          entry["ev_pp"],
                    "ev_flex":        entry["ev_flex"],
                    "p_all_hit":      entry["p_all"],
                    "player_name":    leg["player_name"],
                    "team":           leg["team"],
                    "stat_category":  leg["stat_category"],
                    "pp_line":        leg["pp_line"],
                    "prop_tier":      leg["prop_tier"],
                    "ev_estimate":    leg["ev_estimate"],
                    "stat_hit_rate":  leg["stat_hit_rate"],
                    "final_score":    leg["final_score"],
                    "edge_summary":   leg.get("edge_summary", ""),
                    "corr_warning":   str(leg.get("correlation_warning") or "").strip(),
                })

    if not rows:
        logger.warning("No lineup rows to save.")
        return ""

    pd.DataFrame(rows).to_csv(filepath, index=False)
    logger.info("Lineups saved → %s", filepath)
    return filepath


# ── Master entry point ────────────────────────────────────────────────────────

def generate_lineups(scored_df: pd.DataFrame) -> str:
    """
    Builds lineups, prints to console, saves CSV.
    Returns the CSV path.
    """
    lineups = build_lineups(scored_df)
    print_lineups(lineups)
    csv_path = save_lineups_csv(lineups)
    if csv_path:
        print(f"  📁 Lineups saved → {csv_path}\n")
    return csv_path
