#!/usr/bin/env python3
"""
Smart Pick Selector - ONLY shows plays that are ACTUALLY available on PrizePicks

Key Features:
1. Fetches REAL PrizePicks lines (not just stored data)
2. Recalculates probability for PP's ACTUAL line (using our Poisson model)
3. Calculates Expected Value based on parlay payouts
4. Filters to high-edge plays only

This solves the problem of showing predictions for lines that don't exist.
"""

import sqlite3
import json
import math
import argparse
import requests
import os
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from collections import defaultdict
from fuzzywuzzy import fuzz

# Discord webhook URL
DISCORD_WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_URL',
    "https://discord.com/api/webhooks/1435509138687004672/YSOXw9z6gtGj9wSRAABiGLa-7P2eBhFgPRoAQp1vdV5f2_5YCmy1fYkj2EQpb-XIPnBQ")


@dataclass
class SmartPick:
    """A pick that matches an actual PrizePicks line"""
    player_name: str        # PrizePicks display name (full name, e.g. "Sam Bennett")
    local_player_name: str  # Local DB name (may be abbreviated, e.g. "S. Bennett")
    team: str
    opponent: str
    prop_type: str

    # Our prediction
    our_line: float
    our_probability: float
    our_lambda: float  # Expected value (shots/points) - recent form

    # PrizePicks actual line
    pp_line: float
    pp_odds_type: str  # 'standard', 'goblin', 'demon'

    # Recalculated for PP line
    pp_probability: float  # Probability for PP's actual line
    prediction: str  # 'OVER' or 'UNDER'
    edge: float  # pp_probability - break_even

    # ML vs Baseline comparison
    season_avg: float = 0.0  # Naive baseline (season average)
    recent_avg: float = 0.0  # What ML uses (L10 or L5)
    baseline_prob: float = 0.0  # What a naive model would predict
    ml_adjustment: float = 0.0  # pp_probability - baseline_prob (positive = ML likes it more)

    # Expected Value for different parlay sizes
    ev_2leg: float = 0.0
    ev_3leg: float = 0.0
    ev_4leg: float = 0.0
    ev_5leg: float = 0.0
    ev_6leg: float = 0.0

    # Confidence tier
    tier: str = 'T5-FADE'

    def __post_init__(self):
        # Cap probability at 95% - no model can be 100% certain
        if self.pp_probability > 0.95:
            self.pp_probability = 0.95
        self.tier = self._get_tier()
        self._calculate_ev()

    def _get_tier(self) -> str:
        prob = self.pp_probability
        if prob >= 0.75:
            return 'T1-ELITE'
        elif prob >= 0.70:
            return 'T2-STRONG'
        elif prob >= 0.65:
            return 'T3-GOOD'
        elif prob >= 0.55:
            return 'T4-LEAN'
        else:
            return 'T5-FADE'

    def _calculate_ev(self):
        """Calculate expected value for parlay payouts"""
        # PrizePicks payouts by total "leg value" (as of Jan 2026)
        # Standard picks = 1.0 leg, Goblin = 0.5 leg, Demon = 0.25 leg
        PAYOUTS = {
            2: 3.0,   # 2-leg: 3x
            3: 5.0,   # 3-leg: 5x
            4: 10.0,  # 4-leg: 10x
            5: 20.0,  # 5-leg: 20x
            6: 25.0,  # 6-leg: 25x
        }

        # Leg value based on odds type
        # Goblin = easier, less payout | Demon = harder, more payout
        LEG_VALUES = {
            'standard': 1.0,
            'goblin': 0.5,   # Easier line, less payout
            'demon': 1.5,    # Harder line, more payout
        }

        self.leg_value = LEG_VALUES.get(self.pp_odds_type, 1.0)

        p = self.pp_probability
        # EV = (probability^legs * payout) - 1
        # For single pick contribution, we use geometric mean approach
        self.ev_2leg = (p ** 2) * PAYOUTS[2] - 1
        self.ev_3leg = (p ** 3) * PAYOUTS[3] - 1
        self.ev_4leg = (p ** 4) * PAYOUTS[4] - 1
        self.ev_5leg = (p ** 5) * PAYOUTS[5] - 1
        self.ev_6leg = (p ** 6) * PAYOUTS[6] - 1


class SmartPickSelector:
    """
    Selects picks based on ACTUAL PrizePicks availability.

    Unlike edge_calculator.py which shows predictions for lines that might not exist,
    this starts from PP's actual lines and recalculates our predictions for them.
    """

    # Break-even rates by odds type (based on leg values and parlay payouts)
    # Goblin (0.5x leg): 4 picks = 2 legs = 3x payout → need 76% per pick
    # Standard (1.0x leg): 4 picks = 4 legs = 10x payout → need 56% per pick
    # Demon (1.5x leg): 4 picks = 6 legs = 25x payout → need 45% per pick
    BREAK_EVEN = {
        'standard': 0.56,
        'goblin': 0.76,   # Higher break-even (easier line, less payout)
        'demon': 0.45,    # Lower break-even (harder line, more payout)
    }

    def __init__(self, sport: str = 'nhl'):
        self.sport = sport.upper()
        self.root = Path(__file__).parent.parent

        # Database paths
        if sport.lower() == 'nhl':
            self.pred_db_path = self.root / 'nhl' / 'database' / 'nhl_predictions_v2.db'
        else:
            self.pred_db_path = self.root / 'nba' / 'database' / 'nba_predictions.db'

        self.pp_db_path = self.root / 'shared' / 'prizepicks_lines.db'

    def poisson_prob_over(self, lambda_param: float, line: float) -> float:
        """
        Calculate P(X > line) using Poisson distribution (for points).

        For line = k.5, we want P(X >= k+1) = 1 - P(X <= k)
        """
        k = int(line)  # 0.5 -> 0, 1.5 -> 1, 2.5 -> 2, etc.

        # P(X <= k) = sum of P(X = i) for i = 0 to k
        cumulative = 0
        for i in range(k + 1):
            cumulative += (lambda_param ** i) * math.exp(-lambda_param) / math.factorial(i)

        return 1 - cumulative

    def normal_prob_over(self, mean: float, std_dev: float, line: float) -> float:
        """
        Calculate P(X > line) using Normal distribution (for shots).

        Uses error function approximation for CDF.
        """
        if std_dev <= 0:
            return 0.5

        z_score = (line - mean) / std_dev
        # P(X > line) = 1 - CDF(z_score) = 0.5 * (1 - erf(z/sqrt(2)))
        return 0.5 * (1 - self._erf(z_score / math.sqrt(2)))

    def _erf(self, x: float) -> float:
        """Approximation of error function"""
        # Abramowitz and Stegun approximation
        a1, a2, a3, a4, a5 = 0.254829592, -0.284496736, 1.421413741, -1.453152027, 1.061405429
        p = 0.3275911

        sign = 1 if x >= 0 else -1
        x = abs(x)

        t = 1.0 / (1.0 + p * x)
        y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * math.exp(-x * x)

        return sign * y

    def fetch_fresh_lines(self) -> int:
        """Fetch fresh lines from PrizePicks API"""
        try:
            from prizepicks_client import PrizePicksIngestion
            ingestion = PrizePicksIngestion()
            result = ingestion.run_ingestion([self.sport])
            return result.get('total_lines', 0)
        except Exception as e:
            print(f"Warning: Could not fetch fresh lines: {e}")
            return 0

    def _is_initial_match(self, full_name: str, abbrev_name: str) -> bool:
        """
        Check if abbrev_name ('a. fox') matches full_name ('adam fox').
        Handles the NHL prediction DB abbreviation convention.
        """
        if '. ' not in abbrev_name:
            return False
        parts = abbrev_name.split('. ', 1)
        if len(parts[0]) != 1:
            return False
        initial = parts[0]          # 'a'
        abbrev_last = parts[1]      # 'fox'

        full_parts = full_name.lower().split()
        if len(full_parts) < 2:
            return False
        full_initial = full_parts[0][0]   # 'a' from 'adam'
        full_last = full_parts[-1]        # 'fox'

        return initial == full_initial and abbrev_last == full_last

    def get_smart_picks(
        self,
        game_date: Optional[str] = None,
        min_edge: float = 5.0,
        min_prob: float = 0.55,
        odds_types: List[str] = None,
        refresh_lines: bool = True,
        overs_only: bool = False
    ) -> List[SmartPick]:
        """
        Get smart picks that match ACTUAL PrizePicks lines.

        Args:
            game_date: Date to get picks for (default: today)
            min_edge: Minimum edge percentage (default: 5%)
            min_prob: Minimum probability (default: 55%)
            odds_types: List of odds types to include ['standard', 'goblin', 'demon']
            refresh_lines: Whether to fetch fresh lines first
            overs_only: Only show OVER predictions

        Returns:
            List of SmartPick objects sorted by edge
        """
        if game_date is None:
            game_date = date.today().isoformat()

        if odds_types is None:
            odds_types = ['standard', 'goblin']  # Skip demon by default (too risky)

        # Optionally refresh lines
        if refresh_lines:
            print(f"[{self.sport}] Fetching fresh PrizePicks lines...")
            count = self.fetch_fresh_lines()
            print(f"[{self.sport}] Fetched {count} lines")

        # Get PrizePicks lines for today
        pp_lines = self._get_pp_lines(game_date, odds_types)
        print(f"[{self.sport}] Found {len(pp_lines)} PP lines")

        # Get our predictions with lambda values
        predictions = self._get_predictions_with_params(game_date)
        print(f"[{self.sport}] Found {len(predictions)} predictions with lambda values")

        # Build lookup by player + prop.
        # Sort each group by probability descending so [0] is always the most
        # confident prediction. This prevents probability inversion when multiple
        # prediction rows exist for the same player+prop (e.g. duplicate STD lines).
        pred_lookup = {}
        for pred in predictions:
            key = (pred['player_name'].lower(), pred['prop_type'])
            if key not in pred_lookup:
                pred_lookup[key] = []
            pred_lookup[key].append(pred)
        for key in pred_lookup:
            pred_lookup[key].sort(key=lambda p: p.get('probability', 0), reverse=True)

        # Match PP lines to our predictions
        smart_picks = []
        matched = 0

        for pp in pp_lines:
            # Try to find our prediction for this player+prop
            key = (pp['player_name'].lower(), pp['prop_type'])

            # If no exact match, try proper fuzzy matching with high threshold
            if key not in pred_lookup:
                best_match_key = None
                best_match_score = 0
                pp_name_lower = pp['player_name'].lower()

                for pred_key in pred_lookup.keys():
                    # Only consider same prop type
                    if pred_key[1] != pp['prop_type']:
                        continue

                    pred_name = pred_key[0]

                    # Initial-name match: handles NHL abbreviated names
                    # e.g. PrizePicks 'Adam Fox' ↔ our DB 'a. fox'
                    if self._is_initial_match(pp_name_lower, pred_name):
                        best_match_key = pred_key
                        best_match_score = 100
                        break

                    # Fall back to fuzzy match for other cases
                    score = fuzz.ratio(pp_name_lower, pred_name)
                    if score > best_match_score and score >= 85:
                        best_match_score = score
                        best_match_key = pred_key

                if best_match_key:
                    key = best_match_key

            if key not in pred_lookup:
                continue

            # Get the prediction with features
            # Use the first one (they should all have same parameters for same player+prop)
            pred = pred_lookup[key][0]
            prop_type = pp['prop_type']

            # Team verification - skip if player changed teams (trade, etc.)
            # Normalize abbreviations first: NYK=NY, SAS=SA, NOP=NO, GSW=GS, UTA=UTAH
            _TEAM_ALIASES = {
                'NYK': 'NY', 'NOP': 'NO', 'SAS': 'SA', 'GSW': 'GS', 'UTAH': 'UTA',
            }
            def _canonical(t):
                t = t.upper()
                return _TEAM_ALIASES.get(t, t)
            pp_team = _canonical(pp.get('team', ''))
            pred_team = _canonical(pred.get('team', ''))
            if pp_team and pred_team and pp_team != pred_team:
                # Player was traded — PP is authoritative, log and continue.
                # Probability is still valid (based on player stats, not team).
                # PP team is used at SmartPick creation below (pp.get('team')).
                print(f"[INFO] Trade detected: {pp['player_name']} local={pred_team} → PP={pp_team}")

            # Get season average for baseline comparison
            season_avg = pred.get('f_season_avg') or pred.get('season_avg') or 0
            season_std = pred.get('f_season_std') or pred.get('season_std') or 1.0
            recent_avg = 0
            recent_std = 1.0

            # Recalculate probability based on sport and prop type
            if self.sport == 'NHL' and prop_type in ['points', 'goals', 'assists', 'pp_points']:
                # NHL: Points-based props use Poisson distribution
                lambda_param = pred.get('lambda_param')
                if lambda_param is None or lambda_param <= 0:
                    continue
                pp_prob_over = self.poisson_prob_over(lambda_param, pp['line'])
                our_param = lambda_param
                recent_avg = lambda_param
                # Baseline: use season average lambda if available
                baseline_prob_over = self.poisson_prob_over(season_avg, pp['line']) if season_avg > 0 else pp_prob_over
            elif self.sport == 'NHL':
                # NHL: Shots and other continuous props use Normal distribution
                mean_shots = pred.get('mean_shots') or pred.get('sog_l10')
                std_dev = pred.get('std_dev') or pred.get('sog_std_l10') or 1.5
                if mean_shots is None or mean_shots <= 0:
                    continue
                pp_prob_over = self.normal_prob_over(mean_shots, std_dev, pp['line'])
                our_param = mean_shots
                recent_avg = mean_shots
                # Baseline: use season average
                baseline_prob_over = self.normal_prob_over(season_avg, season_std, pp['line']) if season_avg > 0 else pp_prob_over
            else:
                # NBA: All props use Normal distribution
                mean = pred.get('mean') or pred.get('f_l10_avg')
                std_dev = pred.get('std_dev') or pred.get('f_l10_std') or 1.0
                recent_avg = mean or 0
                recent_std = std_dev
                if mean is None or mean <= 0:
                    continue
                pp_prob_over = self.normal_prob_over(mean, std_dev, pp['line'])
                our_param = mean
                # Baseline: use season average with season std
                if season_avg > 0:
                    baseline_prob_over = self.normal_prob_over(season_avg, season_std, pp['line'])
                else:
                    baseline_prob_over = pp_prob_over

            matched += 1

            pp_prob_under = 1 - pp_prob_over
            baseline_prob_under = 1 - baseline_prob_over

            # PP platform rule: goblin and demon lines ONLY offer OVER bets.
            # UNDER is not available for non-standard lines on PrizePicks.
            if pp['odds_type'] in ('goblin', 'demon'):
                prediction = 'OVER'
                probability = pp_prob_over
                baseline_prob = baseline_prob_over
            elif pp_prob_over >= pp_prob_under:
                prediction = 'OVER'
                probability = pp_prob_over
                baseline_prob = baseline_prob_over
            else:
                prediction = 'UNDER'
                probability = pp_prob_under
                baseline_prob = baseline_prob_under

            # Calculate ML adjustment (how much better/worse than naive baseline)
            ml_adjustment = (probability - baseline_prob) * 100

            # Skip if overs_only and this is an UNDER
            if overs_only and prediction != 'OVER':
                continue

            # Calculate edge
            break_even = self.BREAK_EVEN.get(pp['odds_type'], 0.50)
            edge = (probability - break_even) * 100

            # Filter by minimum edge and probability
            if edge < min_edge or probability < min_prob:
                continue

            # Create SmartPick - PP team is authoritative (handles recent trades)
            pick = SmartPick(
                player_name=pp['player_name'],
                local_player_name=pred['player_name'],
                team=pp.get('team', '') or pred.get('team', ''),
                opponent=pred.get('opponent', ''),
                prop_type=pp['prop_type'],
                our_line=pred['line'],
                our_probability=pred['probability'],
                our_lambda=our_param,
                pp_line=pp['line'],
                pp_odds_type=pp['odds_type'],
                pp_probability=probability,
                prediction=prediction,
                edge=edge,
                season_avg=season_avg,
                recent_avg=recent_avg,
                baseline_prob=baseline_prob,
                ml_adjustment=ml_adjustment
            )

            smart_picks.append(pick)

        print(f"[{self.sport}] Matched {matched} PP lines to predictions")
        print(f"[{self.sport}] Found {len(smart_picks)} picks with edge >= {min_edge}%")

        # Sort by edge descending
        smart_picks.sort(key=lambda x: x.edge, reverse=True)

        return smart_picks

    def _get_pp_lines(self, game_date: str, odds_types: List[str]) -> List[Dict]:
        """Get PrizePicks lines for a date"""
        conn = sqlite3.connect(self.pp_db_path)
        conn.row_factory = sqlite3.Row

        placeholders = ','.join(['?' for _ in odds_types])

        # Sport-specific prop types
        if self.sport == 'NHL':
            props = ('shots', 'points', 'goals', 'assists', 'pp_points')
        else:
            # NBA has many more props
            props = ('points', 'rebounds', 'assists', 'threes', 'pra',
                     'pts_rebs', 'pts_asts', 'rebs_asts', 'steals',
                     'blocked_shots', 'turnovers', 'blks+stls', 'fantasy')

        prop_placeholders = ','.join(['?' for _ in props])

        # Match by game date (from start_time) rather than fetch_date
        # This handles cases where lines are fetched day before the game
        query = f'''
            SELECT DISTINCT player_name, prop_type, line, odds_type, team
            FROM prizepicks_lines
            WHERE substr(start_time, 1, 10) = ?
            AND league = ?
            AND odds_type IN ({placeholders})
            AND prop_type IN ({prop_placeholders})
        '''

        params = [game_date, self.sport] + odds_types + list(props)
        rows = conn.execute(query, params).fetchall()
        conn.close()

        all_rows = [dict(row) for row in rows]

        # Enforce platform rule: max 1 standard line per (player, prop).
        # PP's API sometimes returns multiple projections all labeled 'standard'
        # for the same player+prop (alt lines). Keep the median standard line —
        # it's most likely to be the real board line; outliers are de-facto goblin/demon.
        from collections import defaultdict
        std_by_key = defaultdict(list)
        for r in all_rows:
            if r['odds_type'] == 'standard':
                std_by_key[(r['player_name'], r['prop_type'])].append(r)

        seen_std = set()
        deduped = []
        for r in all_rows:
            if r['odds_type'] != 'standard':
                deduped.append(r)
                continue
            key = (r['player_name'], r['prop_type'])
            if key in seen_std:
                continue
            candidates = sorted(std_by_key[key], key=lambda x: x['line'])
            if len(candidates) > 1:
                print(f"[PP] Multiple STD lines for {r['player_name']} {r['prop_type']}: "
                      f"{[c['line'] for c in candidates]} — keeping median")
            keeper = candidates[len(candidates) // 2]   # median (rounds down for even count)
            deduped.append(keeper)
            seen_std.add(key)

        return deduped

    def _get_predictions_with_params(self, game_date: str) -> List[Dict]:
        """Get our predictions with statistical parameters extracted from features"""
        conn = sqlite3.connect(self.pred_db_path)
        conn.row_factory = sqlite3.Row

        # Different query based on sport (NHL vs NBA have different schemas)
        if self.sport == 'NHL':
            rows = conn.execute('''
                SELECT player_name, team, opponent, prop_type, line,
                       prediction, probability, features_json
                FROM predictions
                WHERE game_date = ?
            ''', (game_date,)).fetchall()
        else:
            # NBA has f_l10_avg, f_l10_std as columns
            rows = conn.execute('''
                SELECT player_name, team, opponent, prop_type, line,
                       prediction, probability, features_json,
                       f_l10_avg, f_l10_std, f_season_avg, f_season_std
                FROM predictions
                WHERE game_date = ?
            ''', (game_date,)).fetchall()

        predictions = []
        for row in rows:
            pred = dict(row)

            if self.sport == 'NHL':
                # NHL: Extract parameters from features_json
                try:
                    features = json.loads(row['features_json'])
                    # For points (Poisson)
                    pred['lambda_param'] = features.get('lambda_param')
                    # For shots (Normal)
                    pred['mean_shots'] = features.get('mean_shots') or features.get('sog_l10')
                    pred['std_dev'] = features.get('std_dev') or features.get('sog_std_l10')
                    pred['sog_l10'] = features.get('sog_l10')
                    pred['sog_std_l10'] = features.get('sog_std_l10')
                    # Season averages for baseline comparison
                    pred['f_season_avg'] = features.get('season_avg') or features.get('pts_l20') or features.get('sog_season')
                    pred['f_season_std'] = features.get('season_std') or features.get('sog_std_season') or 1.0
                except:
                    pred['lambda_param'] = None
                    pred['mean_shots'] = None
                    pred['std_dev'] = None
                    pred['f_season_avg'] = None
                    pred['f_season_std'] = 1.0
            else:
                # NBA: Use columns directly (all props use Normal distribution)
                pred['mean'] = row['f_l10_avg'] or row['f_season_avg']
                pred['std_dev'] = row['f_l10_std'] or row['f_season_std'] or 1.0
                pred['f_season_avg'] = row['f_season_avg']
                pred['f_season_std'] = row['f_season_std'] or 1.0
                pred['lambda_param'] = None  # NBA doesn't use Poisson

                # Also try features_json as backup
                try:
                    features = json.loads(row['features_json']) if row['features_json'] else {}
                    if not pred['mean']:
                        pred['mean'] = features.get('f_l10_avg') or features.get('f_season_avg')
                    if not pred['std_dev'] or pred['std_dev'] == 1.0:
                        pred['std_dev'] = features.get('f_l10_std') or features.get('f_season_std') or 1.0
                    if not pred['f_season_avg']:
                        pred['f_season_avg'] = features.get('f_season_avg')
                    if not pred['f_season_std'] or pred['f_season_std'] == 1.0:
                        pred['f_season_std'] = features.get('f_season_std') or 1.0
                except:
                    pass

            predictions.append(pred)

        conn.close()
        return predictions

    def generate_report(
        self,
        picks: List[SmartPick],
        show_ev: bool = True
    ) -> str:
        """Generate a formatted report of smart picks"""
        lines = []
        lines.append("=" * 130)
        lines.append(f"  {self.sport} SMART PICKS - {date.today().isoformat()}")
        lines.append("  Only showing plays ACTUALLY AVAILABLE on PrizePicks")
        lines.append("=" * 130)
        lines.append(f"  Total Smart Picks: {len(picks)}")
        lines.append("")
        lines.append("  COLUMNS: Prob=Model probability | Edge=vs breakeven | ML Adj=vs season avg (^=hot, v=cold) | Avg=Season->Recent")
        lines.append("")

        # Group by prop type
        by_prop = defaultdict(list)
        for pick in picks:
            by_prop[pick.prop_type].append(pick)

        for prop_type, prop_picks in sorted(by_prop.items()):
            lines.append(f"  {prop_type.upper()} ({len(prop_picks)} plays)")
            lines.append("-" * 130)

            header = f"  {'Player':<18} {'Matchup':<12} {'Line':^16} {'Prob':>6} {'Edge':>7} {'ML Adj':>8} {'Avg':>12} {'Tier':<10}"
            lines.append(header)

            for pick in prop_picks[:10]:  # Top 10 per prop
                line_str = f"{pick.prediction} {pick.pp_line:.1f}"
                if pick.pp_odds_type != 'standard':
                    line_str += f" ({pick.pp_odds_type[:3]})"

                # Format matchup as "TEAM vs OPP"
                matchup = f"{pick.team} vs {pick.opponent}" if pick.team and pick.opponent else ""
                matchup = matchup[:12]  # Truncate if too long

                # ML adjustment with direction indicator
                if pick.ml_adjustment > 0.5:
                    ml_adj_str = f"+{pick.ml_adjustment:4.1f}% ^"  # Hot - recent form better than season
                elif pick.ml_adjustment < -0.5:
                    ml_adj_str = f"{pick.ml_adjustment:5.1f}% v"  # Cold - recent form worse than season
                else:
                    ml_adj_str = f"{pick.ml_adjustment:5.1f}%  "  # Neutral

                # Show season vs recent average
                avg_str = f"{pick.season_avg:4.1f}->{pick.recent_avg:4.1f}" if pick.season_avg > 0 else ""

                row = f"  {pick.player_name:<18} {matchup:<12} {line_str:^16} {pick.pp_probability*100:5.1f}% {pick.edge:+6.1f}% {ml_adj_str:>8} {avg_str:>12} {pick.tier:<10}"
                lines.append(row)

            lines.append("")

        # ML adjustment explanation
        lines.append("=" * 130)
        lines.append("  ML ADJUSTMENT EXPLAINED")
        lines.append("-" * 130)
        lines.append("  ML Adj shows how much BETTER (^) or WORSE (v) our model is vs a naive season-average approach:")
        lines.append("    +10% ^ = Player's recent form is HOT - L10 avg much higher than season avg, model sees 10% more edge")
        lines.append("    -5% v  = Player's recent form is COLD - L10 avg lower than season avg, model is more conservative")
        lines.append("    0%     = Recent form matches season average - no adjustment needed")
        lines.append("")
        lines.append("  WHY THIS MATTERS: A casual bettor using season averages would miss these edges.")
        lines.append("  Our model captures recent form, trends, and momentum that season averages ignore.")
        lines.append("")

        # Parlay building tips
        lines.append("=" * 130)
        lines.append("  PRIZEPICKS PAYOUT GUIDE")
        lines.append("-" * 130)
        lines.append("  Total Leg Value  Payout  Required Win Rate")
        lines.append("  2.0 legs         3x      58.5% per pick")
        lines.append("  3.0 legs         5x      58.5% per pick")
        lines.append("  4.0 legs         10x     56.2% per pick")
        lines.append("  5.0 legs         20x     54.9% per pick")
        lines.append("  6.0 legs         25x     55.1% per pick")
        lines.append("")
        lines.append("  LEG VALUES: Goblin=0.5x (easier) | Standard=1.0x | Demon=1.5x (harder)")
        lines.append("=" * 130)

        return "\n".join(lines)

    def generate_discord_message(self, picks: List[SmartPick], game_date: str) -> str:
        """Generate a Discord-formatted message with picks"""
        lines = []

        # Header (Windows-safe, no emojis in code - Discord will render them)
        sport_label = "[NBA]" if self.sport == "NBA" else "[NHL]"
        lines.append(f"```")
        lines.append(f"{sport_label} SMART PICKS - {game_date}")
        lines.append(f"{'='*50}")

        if not picks:
            lines.append("No high-edge picks found for this date.")
            lines.append("```")
            return "\n".join(lines)

        lines.append(f"Found {len(picks)} verified picks (edge >= 5%)")
        lines.append("")

        # Group by tier for easier reading
        elite_picks = [p for p in picks if p.tier == 'T1-ELITE']
        strong_picks = [p for p in picks if p.tier == 'T2-STRONG']
        good_picks = [p for p in picks if p.tier == 'T3-GOOD']

        if elite_picks:
            lines.append("[FIRE] ELITE TIER (75%+ probability)")
            lines.append("-" * 50)
            for p in elite_picks[:8]:
                trend = "[HOT]" if p.ml_adjustment > 5 else ("[COLD]" if p.ml_adjustment < -5 else "[--]")
                lines.append(f"{trend} {p.player_name}")
                lines.append(f"   {p.prediction} {p.pp_line} {p.prop_type}")
                lines.append(f"   {p.team} vs {p.opponent} | {p.pp_probability*100:.0f}% | +{p.edge:.0f}% edge")
                lines.append("")

        if strong_picks:
            lines.append("[STRONG] STRONG TIER (70-74% probability)")
            lines.append("-" * 50)
            for p in strong_picks[:6]:
                trend = "[HOT]" if p.ml_adjustment > 5 else ("[COLD]" if p.ml_adjustment < -5 else "[--]")
                lines.append(f"{trend} {p.player_name}")
                lines.append(f"   {p.prediction} {p.pp_line} {p.prop_type}")
                lines.append(f"   {p.team} vs {p.opponent} | {p.pp_probability*100:.0f}% | +{p.edge:.0f}% edge")
                lines.append("")

        if good_picks:
            lines.append("[GOOD] GOOD TIER (65-69% probability)")
            lines.append("-" * 50)
            for p in good_picks[:4]:
                lines.append(f"* {p.player_name}: {p.prediction} {p.pp_line} {p.prop_type} ({p.pp_probability*100:.0f}%)")

        lines.append("")
        lines.append("=" * 50)
        lines.append("[HOT] = Recent form > season avg")
        lines.append("[COLD] = Recent form < season avg")
        lines.append("```")

        return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description='Smart Pick Selector - Only actual PP lines')
    parser.add_argument('--sport', choices=['nhl', 'nba'], default='nhl', help='Sport')
    parser.add_argument('--date', help='Game date (YYYY-MM-DD), defaults to today')
    parser.add_argument('--min-edge', type=float, default=5.0, help='Minimum edge %%')
    parser.add_argument('--min-prob', type=float, default=0.55, help='Minimum probability')
    parser.add_argument('--include-demon', action='store_true', help='Include demon odds')
    parser.add_argument('--no-refresh', action='store_true', help='Skip fetching fresh lines')
    parser.add_argument('--overs-only', action='store_true', help='Only show OVER predictions')
    parser.add_argument('--show-ev', action='store_true', help='Show EV calculations')
    parser.add_argument('--discord', action='store_true', help='Output Discord-formatted message')
    parser.add_argument('--post-discord', action='store_true', help='Post picks to Discord webhook')

    args = parser.parse_args()

    game_date = args.date or date.today().isoformat()

    selector = SmartPickSelector(args.sport)

    odds_types = ['standard', 'goblin']
    if args.include_demon:
        odds_types.append('demon')

    picks = selector.get_smart_picks(
        game_date=game_date,
        min_edge=args.min_edge,
        min_prob=args.min_prob,
        odds_types=odds_types,
        refresh_lines=not args.no_refresh,
        overs_only=args.overs_only
    )

    if args.post_discord:
        # Post to Discord webhook
        message = selector.generate_discord_message(picks, game_date)
        if DISCORD_WEBHOOK_URL:
            try:
                response = requests.post(
                    DISCORD_WEBHOOK_URL,
                    json={"content": message},
                    timeout=10
                )
                if response.status_code == 204:
                    print(f"[OK] Posted {len(picks)} picks to Discord!")
                else:
                    print(f"[WARN] Discord returned status {response.status_code}")
            except Exception as e:
                print(f"[ERROR] Failed to post to Discord: {e}")
        else:
            print("[WARN] No Discord webhook configured")
    elif args.discord:
        # Discord-formatted output (print only)
        print(selector.generate_discord_message(picks, game_date))
    else:
        report = selector.generate_report(picks, show_ev=args.show_ev)
        print(report)


if __name__ == '__main__':
    main()
