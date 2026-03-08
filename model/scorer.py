"""
model/scorer.py — Multi-stat rule-based scoring engine with EV framework.

Covers: Rebs+Asts, Points, Rebounds, Assists, 3PM.
Each category has its own edge conditions, thresholds, and variance tier.
EV is computed per-leg: EV = (hit_rate × multiplier) - (1 - hit_rate).
"""

import logging

import pandas as pd

from config import (
    # Shared
    SCORE_WEIGHTS, QUALIFY_SCORE_MIN, DEMON_QUALIFY_SCORE_MIN,
    PACE_THRESHOLD, PACE_BOOST_THRESHOLD,
    LINE_GAP_MIN, LINE_GAP_STRONG,
    # Rebs+Asts / Rebounds
    OPP_REB_RANK_THRESHOLD, OPP_REB_RANK_ELITE,
    OPP_FG_PCT_WEAK, OPP_FG_PCT_POOR,
    # Points
    OPP_DEF_RANK_WEAK, OPP_DEF_RANK_POOR,
    # 3PM
    OPP_3PA_RANK_THRESHOLD, OPP_3PA_RANK_ELITE,
    MIN_3PA_RATE, MIN_3PT_PCT_RECENT, HOT_STREAK_3PT_PCT,
    # Assists
    OPP_AST_RANK_WEAK, OPP_PACE_ASSISTS_MIN,
    # Weights
    SCORE_WEIGHTS_POINTS, SCORE_WEIGHTS_REBOUNDS,
    SCORE_WEIGHTS_ASSISTS, SCORE_WEIGHTS_3PM,
    # EV
    EV_MULTIPLIER_GOBLIN, EV_MULTIPLIER_STANDARD, EV_MULTIPLIER_DEMON,
    BREAKEVEN_GOBLIN, BREAKEVEN_STANDARD, BREAKEVEN_DEMON,
    HIT_RATE_REQUIRED_GOBLIN, HIT_RATE_REQUIRED_STANDARD, HIT_RATE_REQUIRED_DEMON,
    # Variance
    VARIANCE_TIER,
)

logger = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _sf(val) -> float | None:
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _si(val) -> int | None:
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _prop_tier(is_goblin: bool, is_demon: bool) -> str:
    if is_goblin:
        return "goblin"
    if is_demon:
        return "demon"
    return "standard"


def _required_hit_rate(tier: str) -> float:
    return {
        "goblin":   HIT_RATE_REQUIRED_GOBLIN,
        "standard": HIT_RATE_REQUIRED_STANDARD,
        "demon":    HIT_RATE_REQUIRED_DEMON,
    }.get(tier, HIT_RATE_REQUIRED_STANDARD)


def _compute_ev(hit_rate: float | None, tier: str) -> float | None:
    if hit_rate is None:
        return None
    mult = {
        "goblin":   EV_MULTIPLIER_GOBLIN,
        "standard": EV_MULTIPLIER_STANDARD,
        "demon":    EV_MULTIPLIER_DEMON,
    }.get(tier, EV_MULTIPLIER_STANDARD)
    return round((hit_rate * mult) - (1 - hit_rate), 3)


def _shared_pace(row, components, edge_parts, weights) -> float:
    """Applies pace scoring — shared across all categories."""
    score = 0.0
    pace = _sf(row.get("projected_game_pace"))
    if pace is not None and pace >= PACE_THRESHOLD:
        w = weights.get("pace_qualifies", 0)
        components["pace_qualifies"] = w
        score += w
        edge_parts.append(f"pace {pace:.1f}")
        if pace >= PACE_BOOST_THRESHOLD:
            w2 = weights.get("pace_elite", 0)
            if w2:
                components["pace_elite"] = w2
                score += w2
                edge_parts.append("(elite pace)")
    return score


def _shared_line_gap(row, components, edge_parts, weights) -> float:
    """Applies line gap scoring — shared across all categories."""
    score = 0.0
    line_gap = _sf(row.get("line_gap"))
    if line_gap is not None and line_gap >= LINE_GAP_MIN:
        w = weights.get("line_gap_exists", 0)
        components["line_gap_exists"] = w
        score += w
        edge_parts.append(f"line gap +{line_gap:.1f}")
        if line_gap >= LINE_GAP_STRONG:
            w2 = weights.get("line_gap_strong", 0)
            components["line_gap_strong"] = w2
            score += w2
            edge_parts.append("(strong gap)")
    return score


def _shared_form(row, components, edge_parts, weights, avg_col="rolling_stat_avg",
                 hr_col="stat_hit_rate") -> float:
    """Applies rolling form scoring — shared across all categories."""
    score = 0.0
    rolling_avg = _sf(row.get(avg_col))
    hit_rate = _sf(row.get(hr_col))
    pp_line = _sf(row.get("line", 0))

    if rolling_avg is not None and pp_line is not None and rolling_avg >= pp_line:
        w = weights.get("form_over_line", 0)
        components["form_over_line"] = w
        score += w
        edge_parts.append(f"avg {rolling_avg:.1f} > line {pp_line}")

    if hit_rate is not None:
        if hit_rate >= 0.70:
            w = weights.get("form_hit_rate_70", 0)
            components["form_hit_rate_70"] = w
            score += w
            edge_parts.append(f"hit rate {hit_rate:.0%}")
        elif hit_rate >= 0.60:
            w = weights.get("form_hit_rate_60", 0)
            components["form_hit_rate_60"] = w
            score += w
            edge_parts.append(f"hit rate {hit_rate:.0%}")
    return score


def _apply_demon_bonus(score, is_demon, components, edge_parts, weights) -> float:
    if is_demon and score >= DEMON_QUALIFY_SCORE_MIN:
        w = weights.get("demon_bonus", 20)
        components["demon_bonus"] = w
        score += w
        edge_parts.append(f"🔴 DEMON +{w}pts")
    return score


# ── Per-category Scoring ──────────────────────────────────────────────────────

def _score_rebs_asts(row, components, edge_parts) -> float:
    W = SCORE_WEIGHTS
    score = _shared_pace(row, components, edge_parts, W)

    opp_reb_rank = _si(row.get("opp_reb_rank"))
    if opp_reb_rank is not None and opp_reb_rank >= OPP_REB_RANK_THRESHOLD:
        w = W["opp_reb_weak"]
        components["opp_reb_weak"] = w
        score += w
        edge_parts.append(f"opp reb rank #{opp_reb_rank}")
        if opp_reb_rank >= OPP_REB_RANK_ELITE:
            w2 = W["opp_reb_very_weak"]
            components["opp_reb_very_weak"] = w2
            score += w2
            edge_parts.append("(very weak reb D)")

    opp_fg_pct = _sf(row.get("opp_fg_pct"))
    if opp_fg_pct is not None and opp_fg_pct < OPP_FG_PCT_WEAK:
        w = W["opp_fg_pct_weak"]
        components["opp_fg_pct_weak"] = w
        score += w
        edge_parts.append(f"opp FG% {opp_fg_pct:.1%}")
        if opp_fg_pct < OPP_FG_PCT_POOR:
            w2 = W["opp_fg_pct_poor"]
            components["opp_fg_pct_poor"] = w2
            score += w2
            edge_parts.append("(very poor shooting)")

    score += _shared_line_gap(row, components, edge_parts, W)
    score += _shared_form(row, components, edge_parts, W,
                          avg_col="rolling_ra_avg", hr_col="hit_rate")
    return score


def _score_points(row, components, edge_parts) -> float:
    W = SCORE_WEIGHTS_POINTS
    score = _shared_pace(row, components, edge_parts, W)

    opp_def_rank = _si(row.get("opp_def_rank"))
    if opp_def_rank is not None and opp_def_rank >= OPP_DEF_RANK_WEAK:
        w = W["opp_def_weak"]
        components["opp_def_weak"] = w
        score += w
        edge_parts.append(f"opp def rank #{opp_def_rank}")
        if opp_def_rank >= OPP_DEF_RANK_POOR:
            w2 = W["opp_def_poor"]
            components["opp_def_poor"] = w2
            score += w2
            edge_parts.append("(very weak D)")

    score += _shared_line_gap(row, components, edge_parts, W)
    score += _shared_form(row, components, edge_parts, W)
    return score


def _score_rebounds(row, components, edge_parts) -> float:
    W = SCORE_WEIGHTS_REBOUNDS
    score = _shared_pace(row, components, edge_parts, W)

    opp_reb_rank = _si(row.get("opp_reb_rank"))
    if opp_reb_rank is not None and opp_reb_rank >= OPP_REB_RANK_THRESHOLD:
        w = W["opp_reb_weak"]
        components["opp_reb_weak"] = w
        score += w
        edge_parts.append(f"opp reb rank #{opp_reb_rank}")
        if opp_reb_rank >= OPP_REB_RANK_ELITE:
            w2 = W["opp_reb_very_weak"]
            components["opp_reb_very_weak"] = w2
            score += w2
            edge_parts.append("(very weak reb D)")

    opp_fg_pct = _sf(row.get("opp_fg_pct"))
    if opp_fg_pct is not None and opp_fg_pct < OPP_FG_PCT_WEAK:
        w = W["opp_fg_pct_weak"]
        components["opp_fg_pct_weak"] = w
        score += w
        edge_parts.append(f"opp FG% {opp_fg_pct:.1%}")
        if opp_fg_pct < OPP_FG_PCT_POOR:
            w2 = W["opp_fg_pct_poor"]
            components["opp_fg_pct_poor"] = w2
            score += w2
            edge_parts.append("(very poor shooting)")

    score += _shared_line_gap(row, components, edge_parts, W)
    score += _shared_form(row, components, edge_parts, W)
    return score


def _score_assists(row, components, edge_parts) -> float:
    W = SCORE_WEIGHTS_ASSISTS
    score = _shared_pace(row, components, edge_parts, W)

    opp_ast_rank = _si(row.get("opp_ast_rank"))
    if opp_ast_rank is not None and opp_ast_rank >= OPP_AST_RANK_WEAK:
        w = W["opp_ast_weak"]
        components["opp_ast_weak"] = w
        score += w
        edge_parts.append(f"opp AST rank #{opp_ast_rank}")

    score += _shared_line_gap(row, components, edge_parts, W)
    score += _shared_form(row, components, edge_parts, W)
    return score


def _score_3pm(row, components, edge_parts) -> float:
    W = SCORE_WEIGHTS_3PM

    # Volume qualifier — only score volume shooters
    fg3a = _sf(row.get("fg3a_per_game"))
    if fg3a is not None and fg3a < MIN_3PA_RATE:
        return 0.0  # disqualify low-volume 3PT shooters

    score = _shared_pace(row, components, edge_parts, W)

    opp_3pa_rank = _si(row.get("opp_3pa_rank"))
    if opp_3pa_rank is not None and opp_3pa_rank >= OPP_3PA_RANK_THRESHOLD:
        w = W["opp_3pa_rank_weak"]
        components["opp_3pa_rank_weak"] = w
        score += w
        edge_parts.append(f"opp 3PA rank #{opp_3pa_rank}")
        if opp_3pa_rank >= OPP_3PA_RANK_ELITE:
            w2 = W["opp_3pa_rank_elite"]
            components["opp_3pa_rank_elite"] = w2
            score += w2
            edge_parts.append("(elite 3PT vulnerability)")

    # Hot streak: recent 3PT% must clear floor; extra bonus if confirmed hot
    recent_pct = _sf(row.get("recent_3pt_pct"))
    if recent_pct is not None and recent_pct >= MIN_3PT_PCT_RECENT:
        if recent_pct >= HOT_STREAK_3PT_PCT:
            w = W["hot_streak"]
            components["hot_streak"] = w
            score += w
            edge_parts.append(f"hot streak {recent_pct:.1%}")
    elif recent_pct is not None and recent_pct < MIN_3PT_PCT_RECENT:
        # Cold shooter — disqualify regardless of other signals
        return 0.0

    score += _shared_line_gap(row, components, edge_parts, W)
    score += _shared_form(row, components, edge_parts, W)
    return score


# ── Dispatch Table ────────────────────────────────────────────────────────────

_SCORERS = {
    "Rebs+Asts": _score_rebs_asts,
    "Points":    _score_points,
    "Rebounds":  _score_rebounds,
    "Assists":   _score_assists,
    "3PM":       _score_3pm,
}

_WEIGHTS_BY_STAT = {
    "Rebs+Asts": SCORE_WEIGHTS,
    "Points":    SCORE_WEIGHTS_POINTS,
    "Rebounds":  SCORE_WEIGHTS_REBOUNDS,
    "Assists":   SCORE_WEIGHTS_ASSISTS,
    "3PM":       SCORE_WEIGHTS_3PM,
}


def score_row(row: pd.Series) -> dict | None:
    """
    Scores a single prop row. Returns a dict of output fields, or None if
    the prop does not qualify.
    """
    stat_cat  = str(row.get("stat_category", "Rebs+Asts"))
    is_demon  = bool(row.get("is_demon", False))
    is_goblin = bool(row.get("is_goblin", False))

    components = {}
    edge_parts = []

    scoring_fn = _SCORERS.get(stat_cat, _score_rebs_asts)
    raw_score  = scoring_fn(row, components, edge_parts)
    weights    = _WEIGHTS_BY_STAT.get(stat_cat, SCORE_WEIGHTS)

    # Demon bonus (only when base score already qualifies)
    final_score = _apply_demon_bonus(raw_score, is_demon, components, edge_parts, weights)

    # Qualify check
    threshold = DEMON_QUALIFY_SCORE_MIN if is_demon else QUALIFY_SCORE_MIN
    if final_score < threshold or final_score == 0:
        return None

    # EV calculation
    tier      = _prop_tier(is_goblin, is_demon)
    hit_rate  = _sf(row.get("stat_hit_rate")) or _sf(row.get("hit_rate"))
    ev        = _compute_ev(hit_rate, tier)
    req_hr    = _required_hit_rate(tier)
    hr_margin = round(hit_rate - req_hr, 3) if hit_rate is not None else None

    ev_tier = (
        "strong +EV" if (ev is not None and ev > 0.15) else
        "+EV"        if (ev is not None and ev > 0) else
        "-EV"
    )

    return {
        "player_name":     str(row.get("player_name", "")),
        "team":            str(row.get("team", "")),
        "vs":              str(row.get("opponent_team", "")),
        "position":        str(row.get("position", "")),
        "game":            str(row.get("game_label", "")),
        "start_time":      str(row.get("start_time", "")),
        "stat_type":       str(row.get("stat_type", "")),
        "stat_category":   stat_cat,
        "variance_tier":   VARIANCE_TIER.get(stat_cat, "medium"),
        "pp_line":         _sf(row.get("line", 0)) or 0.0,
        "is_demon":        is_demon,
        "is_goblin":       is_goblin,
        "prop_tier":       tier,
        # Game-level features
        "projected_pace":  _sf(row.get("projected_game_pace")),
        "opp_reb_rank":    _si(row.get("opp_reb_rank")),
        "opp_reb_allowed": _sf(row.get("opp_reb_allowed")),
        "opp_fg_pct":      _sf(row.get("opp_fg_pct")),
        "opp_fg_pct_rank": _si(row.get("opp_fg_pct_rank")),
        "opp_def_rank":    _si(row.get("opp_def_rank")),
        "opp_pts_allowed": _sf(row.get("opp_pts_allowed")),
        "opp_3pa_rank":    _si(row.get("opp_3pa_rank")),
        "opp_3pt_pct_allowed": _sf(row.get("opp_3pt_pct_allowed")),
        "opp_ast_rank":    _si(row.get("opp_ast_rank")),
        # Sportsbook
        "consensus_line":  _sf(row.get("consensus_line")),
        "line_gap":        _sf(row.get("line_gap")),
        "num_books":       _si(row.get("num_books")),
        # Rolling form
        "rolling_stat_avg":  _sf(row.get("rolling_stat_avg")),
        "rolling_stat_std":  _sf(row.get("rolling_stat_std")),
        "stat_hit_rate":     hit_rate,
        "avg_minutes":       _sf(row.get("avg_minutes")),
        "games_sampled":     _si(row.get("games_sampled")),
        "recent_3pt_pct":    _sf(row.get("recent_3pt_pct")),
        "fg3a_per_game":     _sf(row.get("fg3a_per_game")),
        "usg_pct":           _sf(row.get("usg_pct")),
        "ast_pct":           _sf(row.get("ast_pct")),
        # Scoring
        "raw_score":         raw_score,
        "final_score":       final_score,
        "score_components":  str(components),
        "edge_summary":      " | ".join(edge_parts) if edge_parts else "no edge detected",
        # EV
        "prop_tier":         tier,
        "required_hit_rate": req_hr,
        "hit_rate_margin":   hr_margin,
        "ev_estimate":       ev,
        "ev_tier":           ev_tier,
        # Correlation warning populated later
        "correlation_warning": "",
    }


# ── Correlation Warnings ──────────────────────────────────────────────────────

def _add_correlation_warnings(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # 1. Two players from same team both on rebounds
    reb = df[df["stat_category"] == "Rebounds"]
    if len(reb) > 0:
        reb_team_counts = reb.groupby("team")["player_name"].transform("count")
        dupe_teams = set(reb[reb_team_counts > 1]["team"])
        mask = (df["stat_category"] == "Rebounds") & df["team"].isin(dupe_teams)
        df.loc[mask, "correlation_warning"] += "⚠️ Stacked REB (same team) "

    # 2. Player has both PTS and AST props
    pts_p = set(df[df["stat_category"] == "Points"]["player_name"])
    ast_p = set(df[df["stat_category"] == "Assists"]["player_name"])
    both = pts_p & ast_p
    if both:
        mask = df["player_name"].isin(both) & df["stat_category"].isin(["Points", "Assists"])
        df.loc[mask, "correlation_warning"] += "⚠️ PTS+AST anti-corr "

    # 3. Multiple overs on same slow-paced game
    slow_mask = df["projected_pace"].notna() & (df["projected_pace"] < 98.0)
    slow_games = set(df[slow_mask]["game"])
    if slow_games:
        game_counts = df[df["game"].isin(slow_games)].groupby("game")["player_name"].transform("count")
        slow_multi  = df["game"].isin(slow_games) & (game_counts > 1)
        df.loc[slow_multi, "correlation_warning"] += "⚠️ Slow-game stacking "

    df["correlation_warning"] = df["correlation_warning"].str.strip()
    return df


# ── score_all ─────────────────────────────────────────────────────────────────

def score_all(feature_df: pd.DataFrame) -> pd.DataFrame:
    """
    Scores all props, filters to qualifying, adds EV and correlation warnings.
    Sorted by ev_estimate descending, then final_score descending.
    """
    if feature_df.empty:
        logger.error("Feature DataFrame is empty — nothing to score.")
        return pd.DataFrame()

    logger.info("Scoring %d props across all stat categories...", len(feature_df))

    rows = []
    for _, row in feature_df.iterrows():
        result = score_row(row)
        if result is not None:
            rows.append(result)

    if not rows:
        logger.warning("No props met the minimum score threshold.")
        return pd.DataFrame()

    result_df = pd.DataFrame(rows)
    result_df = _add_correlation_warnings(result_df)

    result_df = (
        result_df
        .sort_values(
            ["ev_estimate", "final_score"],
            ascending=[False, False],
            na_position="last",
        )
        .reset_index(drop=True)
    )

    by_cat = result_df["stat_category"].value_counts().to_dict()
    ev_pos = (result_df["ev_estimate"] > 0).sum()
    ev_strong = (result_df["ev_estimate"] > 0.15).sum()
    logger.info(
        "%d props qualify — by category: %s | +EV: %d | strong +EV: %d",
        len(result_df), by_cat, ev_pos, ev_strong,
    )
    return result_df
