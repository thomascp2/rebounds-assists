# Rebs+Asts PrizePicks Pipeline

A rule-based prop scoring system targeting NBA **Rebounds + Assists** lines on PrizePicks. Combines PrizePicks board data, NBA Stats, and live sportsbook consensus lines to identify +EV overs — with special handling for Demon and Goblin lines.

---

## Quick Start

```bash
pip install -r requirements.txt
```

Set your Odds API key in `config.py`:
```python
ODDS_API_KEY = "your_key_here"   # https://the-odds-api.com (free tier: 500 req/month)
```

Run daily, 30–60 min before first tip-off:
```bash
python main.py
```

---

## Architecture

```
prizepicks_pipeline/
├── config.py               ← All thresholds, weights, API keys
├── main.py                 ← Pipeline orchestrator (6-step flow)
├── requirements.txt
│
├── data/
│   ├── prizepicks.py       ← PrizePicks partner + public API (no auth)
│   ├── nba_stats.py        ← nba_api: pace, opp rebounding, shooting, player logs
│   └── odds_api.py         ← The Odds API: sportsbook consensus lines
│
├── features/
│   └── engineer.py         ← Merges all sources, computes all features
│
├── model/
│   └── scorer.py           ← Rule-based scoring with demon/goblin boost
│
├── output/
│   └── report.py           ← Console display + CSV export
│
└── shared/
    ├── prizepicks_client.py ← Reusable PrizePicks API client (multi-sport)
    └── api_health_monitor.py← API schema validator + Claude-powered self-healer
```

---

## Pipeline Flow

```
[1] PrizePicks board   → fetch_rebs_asts_board()
[2] NBA Stats          → team pace, opp rebounding, opp shooting%, player logs
[3] Sportsbook lines   → The Odds API consensus (DK, FD, BetMGM, Caesars)
[4] Feature engineer   → merge all sources, compute all scoring features
[5] Score & rank       → rule-based weighted scoring, demon bonus applied
[6] Output             → console report + dated CSV
```

---

## Scoring Logic

Each qualifying feature awards weighted points toward a **final score (0–130+)**:

| Feature | Points |
|---|---|
| Game pace ≥ 100.5 | +10 |
| Game pace ≥ 103.0 (elite) | +10 |
| Opp reb rank ≥ 20th worst | +15 |
| Opp reb rank ≥ 25th worst (very weak) | +10 |
| Opp FG% < 46% (misses shots = more boards) | +8 |
| Opp FG% < 44% (very poor shooting) | +7 |
| PP line gap vs. books ≥ 0.5 | +15 |
| PP line gap vs. books ≥ 1.5 (strong) | +15 |
| Rolling avg RA ≥ PP line | +15 |
| Hit rate ≥ 60% (last 10g) | +5 |
| Hit rate ≥ 70% (last 10g) | +5 |
| **Demon bonus** (confirmed edge) | **+20** |

**Score tiers:**
- ⭐⭐⭐ ELITE: 80+
- ⭐⭐ STRONG: 60–79
- ⭐ SOLID: 40–59

**Minimum qualifying scores:** 40 (standard) / 35 (demon, before bonus)

---

## Features Explained

### Game Pace
Possessions per 48 minutes, averaged across both teams in the matchup. More possessions = more shots = more rebound and assist opportunities. Fetched from NBA Stats Advanced dashboard via `nba_api`.

### Opponent Rebounding Rank
How many total rebounds per game the opposing team allows. Ranked 1–30; rank 20+ = bad rebounding defense. Targeting teams ranked 20–30 means our player faces a team that systematically gives up boards. Fetched from NBA Stats Opponent dashboard.

### Opponent FG%
The opposing team's offensive field goal percentage. Lower FG% = they miss more shots = more defensive rebound opportunities for our player. Ranked 1–30; rank 20+ = poor shooters who create more boards. Fetched from NBA Stats Base dashboard.

### Line Gap (PP vs. Books)
`consensus_line - PP_line`. Positive gap means PrizePicks has set an easier-to-beat line than the sportsbooks. A +1.5 gap means books think the player will hit 1.5 more RA than PP is requiring. Strong signal.

### Rolling Form
Last 10 qualifying games (≥ 22 min played):
- `rolling_ra_avg`: average Rebs+Asts
- `hit_rate`: % of games where RA exceeded the current PP line
- `rolling_ra_std`: consistency (lower std = more reliable)

---

## PrizePicks API Notes

PrizePicks does not have an official public API. The pipeline uses two discovered endpoints, tried in order:

1. **Primary:** `https://partner-api.prizepicks.com/projections` — less Cloudflare friction
2. **Fallback:** `https://api.prizepicks.com/projections` — public endpoint, may 403

On 403, the fetcher immediately tries the next endpoint. No cloudscraper needed when using the partner API first.

The `shared/prizepicks_client.py` contains a reusable multi-sport client used across other prediction models (NHL, MLB, etc.).

---

## Sportsbook Lines

Uses **The Odds API** (the-odds-api.com). Free tier: 500 requests/month, sufficient for daily NBA use.

Two-pass strategy per player:
1. Uses `player_rebounds_assists` combined market if available
2. Falls back to summing `player_rebounds` + `player_assists` Over lines from the same bookmaker

Books: DraftKings, FanDuel, BetMGM, Caesars. Consensus line = median across all books.

---

## Configuration (`config.py`)

All thresholds and weights live here — no code changes needed for tuning:

```python
PACE_THRESHOLD = 100.5          # minimum pace to qualify
PACE_BOOST_THRESHOLD = 103.0    # elite pace bonus threshold
OPP_REB_RANK_THRESHOLD = 20     # target opponents ranked 20th+ worst
OPP_FG_PCT_WEAK = 0.46          # opponent FG% below this = bonus
OPP_FG_PCT_POOR = 0.44          # very poor shooting = extra bonus
LINE_GAP_MIN = 0.5              # minimum PP vs. books gap
LINE_GAP_STRONG = 1.5           # strong gap threshold
QUALIFY_SCORE_MIN = 40          # minimum score to appear in output
```

---

## Output

**Console:** Ranked list of top 20 qualifying props with full feature breakdown.

**CSV:** Saved to `output/YYYY-MM-DD_rebs_asts_picks.csv` for historical tracking and eventual hit rate analysis.

---

## Why Rebs+Asts?

- **Lower variance than points** — doesn't depend on shot-making efficiency
- **Pace amplifies both stats** — more possessions = more rebound and assist opps
- **Opponent reb defense is exploitable** — bad rebounding teams give up boards systematically
- **Opponent FG% is exploitable** — bad shooting teams create more missed shots = more boards
- **PrizePicks sets lines conservatively** — when underlying edge exists, the books vs. PP gap is often significant
- **Assists and rebounds don't anti-correlate** — unlike PRA, no drag from points stealing possessions

---

## Upgrading to ML

Once you have 50+ historical rows with outcomes, swap `model/scorer.py` for a logistic regression or XGBoost model. The feature DataFrame shape stays identical — no pipeline changes needed.

Suggested target: `hit` (1 if final RA ≥ PP line, 0 otherwise).

---

## Requirements

```
requests
pandas
cloudscraper
nba_api
```

Install: `pip install -r requirements.txt`
