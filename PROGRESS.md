# PrizePicks Pipeline — Progress Log

---

## Session 1 — Initial Build
**Status:** Complete

### What Was Built
- Full end-to-end pipeline from scratch: PrizePicks board → NBA Stats → Odds API → features → score → report
- Started as Rebs+Asts only; expanded to all 5 stat categories (Points, Rebounds, Assists, Rebs+Asts, 3PM)
- Per-category scoring engines with category-specific weights and thresholds
- EV framework: goblin / standard / demon tiers with break-even hit rate requirements
- Correlation warnings: stacked REB same team, PTS+AST anti-corr, slow-game stacking
- Optimal lineup builder: 5x2, 4x3, 3x4, 3x5, 3x6 Power Play + Flex lineups
- Unicode name normalization (Dončić, Jokić), suffix stripping (Trey Murphy III)
- GitHub repo initialized: https://github.com/thomascp2/rebounds-assists
- Removed exposed Discord webhook from shared/smart_pick_selector.py
- CLAUDE.md created for persistent project context

### Key Config Values
- Qualify score: 40 (standard), 35 (demon)
- Form windows by variance: low=7g, low_medium=10g, medium=10g, high=15g
- EV multipliers: goblin=1.5x, standard=3.0x, demon=5.0x

---

## Session 2 — Trend Detection + Season Averages + ML Infrastructure
**Date:** 2026-03-08
**Status:** Complete

### What Was Added

#### L5/L10/L15 Trend Detection
- `_window_stats()`: computes avg + hit rate for any N-game window
- `_compute_trend()`: classifies trend as up/down/flat/mixed based on L5 vs L10 vs L15
- Trend bonus: **+10 pts** when L5 >= 15% above L15 AND L5 avg >= PP line AND L5 HR >= 60%
- Trend penalty: **-15 pts** when L5 >= 15% below L15 (unconditional — declining player is declining)
- Validity gate prevents rewarding a rising trend that is still below the line
- Report shows: `Trend: 📈 UP +18% ✅` with L5/L10/L15 avg and HR side-by-side

#### Season Averages Feature
- Extended `fetch_player_advanced_stats()` to pull full-season PTS/REB/AST/FG3M per game
  from the same `LeagueDashPlayerStats` Base call — no extra API hit
- New `attach_season_averages()` in engineer.py maps season avg per stat category
  (Rebs+Asts = season_avg_reb + season_avg_ast)
- `season_avg` column now in every output row and shown in report features block
- Addresses gap where rolling 7-10g window had no full-season baseline to compare against

#### ML Infrastructure (`ml/` directory)
| File | Purpose |
|---|---|
| `feature_schema.py` | 39 canonical ML features, target col (`hit`), derived feature formulas |
| `backfill_outcomes.py` | Post-game outcome grading: fetches game logs, fills `hit=1/0` per prop |
| `build_dataset.py` | Stacks all labeled CSVs into `ml/training_data.csv`; adds derived features |

- `hit` column (None by default) added to every daily picks CSV from this session forward
- `actual_stat` column added alongside `hit` for auditing
- Skips DNPs (< 22 min played)

#### Windows Task Scheduler
- Task: **"PrizePicks Backfill Outcomes"** — runs daily at 10:00 AM
- Launcher: `run_backfill.bat` (handles working directory, logs to `logs/backfill.log`)
- Grades previous day's picks automatically every morning
- Manual override: `python -m ml.backfill_outcomes --date YYYY-MM-DD`

### Pipeline Output (2026-03-08 run)
- 2,151 props on board (672 PTS / 534 R+A / 520 REB / 425 AST)
- **882 qualifying props** | 239 demon | 548 goblin
- 809 +EV props | 798 strong +EV | avg EV: +1.06
- 9 NBA games today across all time slots
- Top pick: Bub Carrington REB o2.5 (DEMON, EV +5.00, 100% HR, 📈 UP +47%)

### Commits This Session
- `f0ef087` — Add L5/L10/L15 trend detection with bonus/penalty scoring
- `8dc3654` — Add season avg feature and ML training infrastructure

---

## Next Steps / Roadmap

### Short Term
- [ ] Let backfill accumulate labeled data daily (target: ~200 rows / ~30 game days)
- [ ] Monitor `logs/backfill.log` to verify grading is working correctly
- [ ] Review hit rates by category after ~2 weeks to identify which scoring rules are weakest

### Medium Term
- [ ] Add `hit` outcome tracking to the lineup builder CSV as well
- [ ] Build a simple performance dashboard (hit rate by tier, category, score band)
- [ ] Consider adding opponent pace-adjusted DREB% as a stronger rebounding signal

### Long Term (ML)
- [ ] Train XGBoost binary classifier once 200+ labeled rows exist
- [ ] Replace `stat_hit_rate` (historical) with model-predicted probability
- [ ] Backtest rule-based vs ML-based EV rankings
- [ ] Add `hit` column to CSV for backtesting (already done)
