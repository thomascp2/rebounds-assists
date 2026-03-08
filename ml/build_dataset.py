"""
ml/build_dataset.py — Stacks all labeled picks CSVs into a clean training dataset.

Reads every output/YYYY-MM-DD_nba_picks.csv that has a filled 'hit' column,
selects canonical ML feature columns, adds derived features, and saves
to ml/training_data.csv.

Usage:
    python -m ml.build_dataset
    python -m ml.build_dataset --output ml/training_data.csv --min-rows 10
"""

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ml.feature_schema import FEATURE_COLS, TARGET_COL, META_COLS, NUMERIC_COLS

logger = logging.getLogger(__name__)


def _add_derived_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute derived ratio features that the base CSV doesn't store."""
    df = df.copy()

    # How far the rolling avg clears (or trails) the PP line — key signal
    if "rolling_stat_avg" in df.columns and "pp_line" in df.columns:
        df["avg_vs_line"] = df["rolling_stat_avg"] / df["pp_line"].replace(0, np.nan)

    # Season baseline vs PP line — is the line a good value historically?
    if "season_avg" in df.columns and "pp_line" in df.columns:
        df["season_vs_line"] = df["season_avg"] / df["pp_line"].replace(0, np.nan)

    # How much the L5 avg diverges from the PP line
    if "l5_avg" in df.columns and "pp_line" in df.columns:
        df["l5_vs_line"] = df["l5_avg"] / df["pp_line"].replace(0, np.nan)

    return df


def build_dataset(
    output_dir: str = "output",
    out_path: str = "ml/training_data.csv",
    min_rows: int = 1,
) -> pd.DataFrame:
    """
    Scans output_dir for dated picks CSVs with labeled outcomes,
    stacks them, adds derived features, and saves to out_path.

    Args:
        output_dir:  Directory containing dated _nba_picks.csv files.
        out_path:    Destination CSV for the combined training data.
        min_rows:    Minimum labeled rows required to include a CSV.

    Returns:
        Combined training DataFrame.
    """
    picks_dir = Path(output_dir)
    csvs = sorted(picks_dir.glob("*_nba_picks.csv"))

    if not csvs:
        logger.error("No picks CSVs found in %s", picks_dir)
        return pd.DataFrame()

    frames = []
    for csv_path in csvs:
        try:
            df = pd.read_csv(csv_path, low_memory=False)
        except Exception as exc:
            logger.warning("Could not read %s: %s", csv_path.name, exc)
            continue

        if TARGET_COL not in df.columns:
            logger.debug("Skipping %s — no '%s' column", csv_path.name, TARGET_COL)
            continue

        labeled = df[df[TARGET_COL].notna()].copy()
        if len(labeled) < min_rows:
            logger.debug("Skipping %s — only %d labeled rows", csv_path.name, len(labeled))
            continue

        # Parse date from filename: YYYY-MM-DD_nba_picks.csv
        date_str = csv_path.stem.split("_")[0]
        labeled["picks_date"] = date_str

        frames.append(labeled)
        logger.info("  %-35s  %d labeled rows", csv_path.name, len(labeled))

    if not frames:
        logger.warning(
            "No labeled data found across %d CSVs. "
            "Run ml/backfill_outcomes.py first.",
            len(csvs),
        )
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)

    # Add derived features
    combined = _add_derived_features(combined)

    # Derived cols to include in the output
    derived_cols = [c for c in ["avg_vs_line", "season_vs_line", "l5_vs_line"]
                    if c in combined.columns]

    # Build final column set
    all_cols = ["picks_date"] + META_COLS + FEATURE_COLS + derived_cols + [TARGET_COL]
    available = [c for c in all_cols if c in combined.columns]
    result = combined[available].copy()

    # Coerce target to nullable int
    result[TARGET_COL] = pd.to_numeric(result[TARGET_COL], errors="coerce").astype("Int64")

    # Save
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(out_path, index=False)

    # Summary
    n_features = len([c for c in FEATURE_COLS + derived_cols if c in result.columns])
    logger.info(
        "Training dataset: %d rows × %d features → %s",
        len(result), n_features, out_path,
    )

    _print_summary(result)
    return result


def _print_summary(df: pd.DataFrame) -> None:
    print("\n" + "═" * 60)
    print("  ML TRAINING DATASET SUMMARY")
    print("═" * 60)
    print(f"  Total labeled rows : {len(df)}")

    if TARGET_COL in df.columns:
        overall_hr = df[TARGET_COL].mean()
        print(f"  Overall hit rate   : {overall_hr:.1%}")

    if "picks_date" in df.columns:
        dates = sorted(df["picks_date"].unique())
        print(f"  Date range         : {dates[0]} → {dates[-1]}  ({len(dates)} days)")

    if "stat_category" in df.columns and TARGET_COL in df.columns:
        print("\n  Hit rate by category:")
        by_cat = (
            df.groupby("stat_category")[TARGET_COL]
            .agg(count="count", hit_rate="mean")
            .sort_values("count", ascending=False)
        )
        for cat, row in by_cat.iterrows():
            print(f"    {cat:<15}  {int(row['count']):>4} rows  {row['hit_rate']:.1%} HR")

    if "prop_tier" in df.columns and TARGET_COL in df.columns:
        print("\n  Hit rate by tier:")
        by_tier = (
            df.groupby("prop_tier")[TARGET_COL]
            .agg(count="count", hit_rate="mean")
            .sort_values("hit_rate", ascending=False)
        )
        for tier, row in by_tier.iterrows():
            print(f"    {tier:<10}  {int(row['count']):>4} rows  {row['hit_rate']:.1%} HR")

    print("═" * 60 + "\n")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Build ML training dataset from labeled picks CSVs."
    )
    parser.add_argument(
        "--output", type=str, default="ml/training_data.csv",
        help="Output path for the training CSV.",
    )
    parser.add_argument(
        "--min-rows", type=int, default=1,
        help="Minimum labeled rows to include a picks CSV (default: 1).",
    )
    args = parser.parse_args()

    build_dataset(out_path=args.output, min_rows=args.min_rows)
