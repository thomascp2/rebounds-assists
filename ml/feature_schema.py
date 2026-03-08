"""
ml/feature_schema.py — Canonical feature definitions for the ML model.

These are the columns that get passed to the model for training/inference.
All features come from the daily scored_df and are already saved in the CSV.

Usage:
    from ml.feature_schema import FEATURE_COLS, TARGET_COL, META_COLS, CAT_COLS
"""

# ── Target ─────────────────────────────────────────────────────────────────────
TARGET_COL = "hit"          # 1 = over cleared, 0 = missed, None = not yet filled

# ── Metadata (not model inputs) ────────────────────────────────────────────────
META_COLS = [
    "player_name",
    "team",
    "vs",
    "game",
    "start_time",
    "stat_type",
    "picks_date",           # added by build_dataset.py
]

# ── Categorical features (need encoding before XGBoost) ────────────────────────
CAT_COLS = [
    "stat_category",        # Points / Rebounds / Assists / Rebs+Asts / 3PM
    "variance_tier",        # low / low_medium / medium / high
    "prop_tier",            # goblin / standard / demon
    "trend_direction",      # up / down / flat / mixed / unknown
]

# ── Binary / flag features ──────────────────────────────────────────────────────
FLAG_COLS = [
    "is_demon",
    "is_goblin",
    "trend_is_valid",
]

# ── Numeric features ────────────────────────────────────────────────────────────
NUMERIC_COLS = [
    # Prop
    "pp_line",

    # Game context
    "projected_pace",

    # Opponent defense (filled per stat category — others will be NaN)
    "opp_reb_rank",         # Rebs+Asts, Rebounds
    "opp_fg_pct",           # Rebs+Asts, Rebounds
    "opp_def_rank",         # Points
    "opp_pts_allowed",      # Points
    "opp_3pa_rank",         # 3PM
    "opp_ast_rank",         # Assists

    # Sportsbook
    "consensus_line",
    "line_gap",
    "num_books",

    # Season baseline
    "season_avg",

    # Rolling form (variance-aware primary window)
    "rolling_stat_avg",
    "rolling_stat_std",
    "stat_hit_rate",
    "avg_minutes",
    "games_sampled",

    # Multi-window trend
    "l5_avg",
    "l5_hit_rate",
    "l10_avg",
    "l10_hit_rate",
    "l15_avg",
    "l15_hit_rate",
    "trend_pct",

    # Player advanced
    "usg_pct",
    "ast_pct",
    "fg3a_per_game",
    "recent_3pt_pct",

    # Scorer output (useful as model features too)
    "ev_estimate",
    "final_score",
    "hit_rate_margin",
    "required_hit_rate",
]

# ── All ML feature columns ──────────────────────────────────────────────────────
FEATURE_COLS = CAT_COLS + FLAG_COLS + NUMERIC_COLS

# ── Derived features to compute at training time ───────────────────────────────
# These are not stored in the CSV but can be computed from existing columns:
#
#   avg_vs_line        = rolling_stat_avg / pp_line        (how far over/under the line)
#   season_vs_line     = season_avg / pp_line              (season baseline vs line)
#   l5_vs_l15          = l5_avg / l15_avg - 1              (same as trend_pct)
#   ev_margin          = ev_estimate - required_ev()       (cushion above break-even)
#
# Add these in build_dataset.py before fitting the model.
DERIVED_FEATURE_FORMULAS = {
    "avg_vs_line":    "rolling_stat_avg / pp_line",
    "season_vs_line": "season_avg / pp_line",
}
