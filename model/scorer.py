"""
model/scorer.py — Rule-based scoring engine for Rebs+Asts PrizePicks props.

Each qualifying feature awards weighted points toward a raw score.
Demon lines get an additional bonus when the underlying edge is confirmed.
Only props meeting a minimum score threshold appear in the output.

Scoring logic is fully configurable via config.py — no code changes needed
to tune thresholds or weights.
"""

import logging
from dataclasses import dataclass, field

import pandas as pd

from config import (
    SCORE_WEIGHTS,
    QUALIFY_SCORE_MIN,
    DEMON_QUALIFY_SCORE_MIN,
    PACE_THRESHOLD,
    PACE_BOOST_THRESHOLD,
    OPP_REB_RANK_THRESHOLD,
    OPP_REB_RANK_ELITE,
    OPP_FG_PCT_WEAK,
    OPP_FG_PCT_POOR,
    LINE_GAP_MIN,
    LINE_GAP_STRONG,
)

logger = logging.getLogger(__name__)


@dataclass
class ScoredProp:
    """Holds the score breakdown for a single prop."""
    player_name: str
    team: str
    opponent_team: str
    position: str
    game_label: str
    start_time: str
    stat_type: str
    pp_line: float
    is_demon: bool
    is_goblin: bool

    # Feature values
    projected_game_pace: float | None
    opp_reb_rank: int | None
    opp_reb_allowed: float | None
    opp_fg_pct: float | None
    opp_fg_pct_rank: int | None
    consensus_line: float | None
    line_gap: float | None
    num_books: int | None
    rolling_ra_avg: float | None
    rolling_ra_std: float | None
    hit_rate: float | None
    avg_minutes: float | None
    games_sampled: int | None

    # Score components
    score_components: dict = field(default_factory=dict)
    raw_score: float = 0.0
    final_score: float = 0.0  # includes demon bonus

    # Human-readable edge summary
    edge_summary: str = ""


def _safe_float(val) -> float | None:
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _safe_int(val) -> int | None:
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def score_row(row: pd.Series) -> ScoredProp:
    """
    Scores a single row from the feature DataFrame.
    Returns a ScoredProp with the breakdown.
    """
    components = {}
    score = 0.0
    edge_parts = []

    pace = _safe_float(row.get("projected_game_pace"))
    opp_reb_rank = _safe_int(row.get("opp_reb_rank"))
    opp_fg_pct = _safe_float(row.get("opp_fg_pct"))
    line_gap = _safe_float(row.get("line_gap"))
    rolling_avg = _safe_float(row.get("rolling_ra_avg"))
    hit_rate = _safe_float(row.get("hit_rate"))
    pp_line = _safe_float(row.get("line", 0))
    is_demon = bool(row.get("is_demon", False))

    # ── 1. Pace ───────────────────────────────────────────────────────────────
    if pace is not None and pace >= PACE_THRESHOLD:
        w = SCORE_WEIGHTS["pace_qualifies"]
        components["pace_qualifies"] = w
        score += w
        edge_parts.append(f"pace {pace:.1f}")

        if pace >= PACE_BOOST_THRESHOLD:
            w2 = SCORE_WEIGHTS["pace_elite"]
            components["pace_elite"] = w2
            score += w2
            edge_parts.append(f"(elite pace)")

    # ── 2. Opponent Rebounding ────────────────────────────────────────────────
    if opp_reb_rank is not None and opp_reb_rank >= OPP_REB_RANK_THRESHOLD:
        w = SCORE_WEIGHTS["opp_reb_weak"]
        components["opp_reb_weak"] = w
        score += w
        edge_parts.append(f"opp reb rank #{opp_reb_rank}")

        if opp_reb_rank >= OPP_REB_RANK_ELITE:
            w2 = SCORE_WEIGHTS["opp_reb_very_weak"]
            components["opp_reb_very_weak"] = w2
            score += w2
            edge_parts.append(f"(very weak reb D)")

    # ── 3. Opponent Shooting % (miss rate → more boards) ─────────────────────
    if opp_fg_pct is not None and opp_fg_pct < OPP_FG_PCT_WEAK:
        w = SCORE_WEIGHTS["opp_fg_pct_weak"]
        components["opp_fg_pct_weak"] = w
        score += w
        edge_parts.append(f"opp FG% {opp_fg_pct:.1%}")

        if opp_fg_pct < OPP_FG_PCT_POOR:
            w2 = SCORE_WEIGHTS["opp_fg_pct_poor"]
            components["opp_fg_pct_poor"] = w2
            score += w2
            edge_parts.append(f"(very poor shooting)")

    # ── 4. Line Gap (PP vs. Sportsbooks) ─────────────────────────────────────
    if line_gap is not None and line_gap >= LINE_GAP_MIN:
        w = SCORE_WEIGHTS["line_gap_exists"]
        components["line_gap_exists"] = w
        score += w
        edge_parts.append(f"line gap +{line_gap:.1f}")

        if line_gap >= LINE_GAP_STRONG:
            w2 = SCORE_WEIGHTS["line_gap_strong"]
            components["line_gap_strong"] = w2
            score += w2
            edge_parts.append(f"(strong gap)")

    # ── 4. Rolling Form ───────────────────────────────────────────────────────
    if rolling_avg is not None and pp_line is not None and rolling_avg >= pp_line:
        w = SCORE_WEIGHTS["form_over_line"]
        components["form_over_line"] = w
        score += w
        edge_parts.append(f"avg {rolling_avg:.1f} > line {pp_line}")

    if hit_rate is not None:
        if hit_rate >= 0.70:
            w = SCORE_WEIGHTS["form_hit_rate_70"]
            components["form_hit_rate_70"] = w
            score += w
            edge_parts.append(f"hit rate {hit_rate:.0%}")
        elif hit_rate >= 0.60:
            w = SCORE_WEIGHTS["form_hit_rate_60"]
            components["form_hit_rate_60"] = w
            score += w
            edge_parts.append(f"hit rate {hit_rate:.0%}")

    raw_score = score

    # ── 5. Demon Bonus (only when edge is already confirmed) ──────────────────
    final_score = raw_score
    qualify_threshold = DEMON_QUALIFY_SCORE_MIN if is_demon else QUALIFY_SCORE_MIN

    if is_demon and raw_score >= DEMON_QUALIFY_SCORE_MIN:
        demon_w = SCORE_WEIGHTS["demon_bonus"]
        components["demon_bonus"] = demon_w
        final_score += demon_w
        edge_parts.append(f"🔴 DEMON +{demon_w}pts")

    edge_summary = " | ".join(edge_parts) if edge_parts else "no edge detected"

    return ScoredProp(
        player_name=str(row.get("player_name", "")),
        team=str(row.get("team", "")),
        opponent_team=str(row.get("opponent_team", "")),
        position=str(row.get("position", "")),
        game_label=str(row.get("game_label", "")),
        start_time=str(row.get("start_time", "")),
        stat_type=str(row.get("stat_type", "")),
        pp_line=pp_line or 0.0,
        is_demon=is_demon,
        is_goblin=bool(row.get("is_goblin", False)),
        projected_game_pace=pace,
        opp_reb_rank=opp_reb_rank,
        opp_reb_allowed=_safe_float(row.get("opp_reb_allowed")),
        opp_fg_pct=opp_fg_pct,
        opp_fg_pct_rank=_safe_int(row.get("opp_fg_pct_rank")),
        consensus_line=_safe_float(row.get("consensus_line")),
        line_gap=line_gap,
        num_books=_safe_int(row.get("num_books")),
        rolling_ra_avg=rolling_avg,
        rolling_ra_std=_safe_float(row.get("rolling_ra_std")),
        hit_rate=hit_rate,
        avg_minutes=_safe_float(row.get("avg_minutes")),
        games_sampled=_safe_int(row.get("games_sampled")),
        score_components=components,
        raw_score=raw_score,
        final_score=final_score,
        edge_summary=edge_summary,
    )


def score_all(feature_df: pd.DataFrame) -> pd.DataFrame:
    """
    Scores all rows in the feature DataFrame.
    Returns a filtered, sorted output DataFrame with score breakdowns.
    Only rows meeting the minimum score threshold are included.
    """
    if feature_df.empty:
        logger.error("Feature DataFrame is empty — nothing to score.")
        return pd.DataFrame()

    logger.info("Scoring %d props...", len(feature_df))
    scored_props = [score_row(row) for _, row in feature_df.iterrows()]

    # Filter to qualifying scores
    qualified = [
        sp for sp in scored_props
        if sp.final_score >= (DEMON_QUALIFY_SCORE_MIN if sp.is_demon else QUALIFY_SCORE_MIN)
        and sp.final_score > 0
    ]

    logger.info(
        "%d / %d props qualify (score >= threshold).",
        len(qualified), len(scored_props),
    )

    if not qualified:
        logger.warning("No props met the minimum score threshold today.")
        return pd.DataFrame()

    rows = []
    for sp in qualified:
        rows.append(
            {
                "player_name": sp.player_name,
                "team": sp.team,
                "vs": sp.opponent_team,
                "position": sp.position,
                "game": sp.game_label,
                "start_time": sp.start_time,
                "stat_type": sp.stat_type,
                "pp_line": sp.pp_line,
                "is_demon": sp.is_demon,
                "is_goblin": sp.is_goblin,
                "projected_pace": sp.projected_game_pace,
                "opp_reb_rank": sp.opp_reb_rank,
                "opp_reb_allowed": sp.opp_reb_allowed,
                "opp_fg_pct": sp.opp_fg_pct,
                "opp_fg_pct_rank": sp.opp_fg_pct_rank,
                "consensus_line": sp.consensus_line,
                "line_gap": sp.line_gap,
                "num_books": sp.num_books,
                "rolling_ra_avg": sp.rolling_ra_avg,
                "rolling_ra_std": sp.rolling_ra_std,
                "hit_rate": sp.hit_rate,
                "avg_minutes": sp.avg_minutes,
                "games_sampled": sp.games_sampled,
                "raw_score": sp.raw_score,
                "final_score": sp.final_score,
                "score_components": str(sp.score_components),
                "edge_summary": sp.edge_summary,
            }
        )

    result = (
        pd.DataFrame(rows)
        .sort_values("final_score", ascending=False)
        .reset_index(drop=True)
    )

    return result
