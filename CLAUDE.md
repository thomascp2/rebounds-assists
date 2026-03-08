# CLAUDE.md — Project Context for prizepicks_pipeline

This file gives Claude Code persistent context about this project so future sessions start informed.

---

## What This Project Does

Rule-based prop scoring pipeline targeting NBA **Rebs+Asts** lines on PrizePicks. Pulls live data from 3 sources, engineers features, scores/ranks props, and outputs a console report + CSV.

**Run command:**
```bash
python main.py
```
Run 30–60 minutes before first tip-off.

---

## Directory Structure

```
prizepicks_pipeline/
├── config.py               ← ALL thresholds, weights, API keys — tune here
├── main.py                 ← Pipeline orchestrator
├── data/
│   ├── prizepicks.py       ← PrizePicks board fetcher
│   ├── nba_stats.py        ← nba_api wrapper (pace, reb, shooting, logs)
│   └── odds_api.py         ← The Odds API sportsbook lines
├── features/
│   └── engineer.py         ← Feature computation and merging
├── model/
│   └── scorer.py           ← Weighted rule-based scorer
├── output/
│   └── report.py           ← Console + CSV output
└── shared/
    ├── prizepicks_client.py ← Reusable PrizePicks client from other projects
    └── api_health_monitor.py← Claude-powered API self-healer
```

Flat copies of `data/`, `features/`, `model/`, `output/` files also exist at the root — these are originals, the subdirectory versions are what `main.py` imports.

---

## Key Technical Decisions

### PrizePicks API
- **No official API.** Uses two discovered endpoints.
- **Primary:** `https://partner-api.prizepicks.com/projections` — try this first, much less Cloudflare friction
- **Fallback:** `https://api.prizepicks.com/projections` — public endpoint, may 403 under load
- On 403, move to next endpoint immediately (don't retry same URL)
- Use plain `requests.Session()` with `app.prizepicks.com` as Referer/Origin
- **Do NOT use cloudscraper** — it's installed but not needed when partner API works
- 2-second rate limit between requests
- Game team data is at: `attributes.metadata.game_info.teams.away/home.abbreviation` (changed in 2026)

### NBA Stats API
- Use **`nba_api` library** — NOT raw requests to stats.nba.com (gets rate-limited/blocked)
- `LeagueDashTeamStats` parameter name is `per_mode_detailed` (not `per_mode_simple`)
- `_team_abbrev_map()` in `nba_stats.py` adds abbreviations via `nba_api.stats.static.teams` since the endpoint doesn't return them
- Sleep 0.6s between calls

### The Odds API
- Key in `config.py` as `ODDS_API_KEY`
- Free tier: 500 requests/month — sufficient for daily NBA use
- Fetches combined `player_rebounds_assists` market first; falls back to summing splits
- `FutureWarning` on `groupby.apply` — known pandas deprecation, non-breaking

### Windows Console
- `main.py` reconfigures stdout/stderr to UTF-8 at startup to handle Unicode (emojis, box chars)

---

## Data Flow Summary

```
PP board (partner API) ──┐
NBA pace (nba_api)     ──┤
NBA opp rebounding     ──┤─→ engineer.py ──→ scorer.py ──→ report.py
NBA opp shooting %     ──┤
NBA player logs        ──┤
Odds API consensus     ──┘
```

---

## Scoring Features (all weights in `config.py → SCORE_WEIGHTS`)

| Feature | Source | Weight |
|---|---|---|
| Pace ≥ 100.5 | NBA Advanced | +10 |
| Pace ≥ 103.0 | NBA Advanced | +10 |
| Opp reb rank ≥ 20 | NBA Opponent | +15 |
| Opp reb rank ≥ 25 | NBA Opponent | +10 |
| Opp FG% < 0.46 | NBA Base | +8 |
| Opp FG% < 0.44 | NBA Base | +7 |
| Line gap ≥ 0.5 | Odds API | +15 |
| Line gap ≥ 1.5 | Odds API | +15 |
| Avg RA ≥ PP line | NBA logs | +15 |
| Hit rate ≥ 60% | NBA logs | +5 |
| Hit rate ≥ 70% | NBA logs | +5 |
| Demon bonus | PrizePicks | +20 |

---

## Known Issues / Watch Points

- **Trey Murphy** — PrizePicks sends "Trey Murphy", NBA has "Trey Murphy III". The engineer now strips suffixes (Jr, Sr, II, III, IV, V) before matching.
- **PrizePicks 403 storms** — if we hit the API too frequently in testing, the public endpoint gets temp-blocked (~10–30 min). Partner API is more resilient.
- **PrizePicks game object structure** — changed in 2026. Teams now at `metadata.game_info.teams.away/home.abbreviation`, not flat fields.
- **Combo props** — PrizePicks sends combo player entries (e.g. "Mitchell + White"). These currently pass through `_is_target_stat()` but won't match NBA player IDs, so form stats will be null. They can still score on pace/reb/shooting/line gap.

---

## API Keys

- `ODDS_API_KEY` in `config.py` — The Odds API key (hardcoded, consider env var for production)
- No PrizePicks key needed
- No NBA Stats key needed

---

## GitHub

- Repo: https://github.com/thomascp2/rebounds-assists
- Branch: main

---

## Future Improvements

- Swap `model/scorer.py` for XGBoost once 50+ historical rows exist
- Add `hit` outcome column to CSV for backtesting
- Add opponent pace-adjusted reb% (DREB%) as a stronger rebounding signal than raw OPP_REB_RANK
- Consider `nba_api` `LeagueDashPlayerStats` for per-player RA/game vs. team-level features
