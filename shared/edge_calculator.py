"""
Edge Calculator & Pick Favorability System
===========================================

Compares our model predictions against PrizePicks lines to calculate edge
and rank picks by favorability.

Key Concepts:
- Edge = Our Probability - Break-Even Probability
- For PrizePicks standard picks (no juice): Break-even ~50%
- For goblin/demon picks: Break-even varies by payout

Usage:
    python edge_calculator.py --sport nhl --date 2026-01-16
    python edge_calculator.py --sport nba --top 20
    python edge_calculator.py --sport all --min-edge 5
"""

import sqlite3
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
import json


@dataclass
class EdgePick:
    """A single pick with edge calculation"""
    # Player/Game info
    player_name: str
    team: str
    opponent: str
    game_date: str

    # Prop info
    prop_type: str
    our_line: float
    prizepicks_line: float
    prediction: str  # OVER or UNDER

    # Probabilities
    our_probability: float
    model_version: str
    confidence_tier: str

    # Edge calculations
    edge: float  # Our probability - 50% (break-even)
    edge_pct: float  # Edge as percentage points

    # Favorability score (composite)
    favorability_score: float

    # Line comparison
    line_difference: float  # PP line - our line
    line_favorable: bool  # Is PP line favorable for our prediction?

    # Availability
    available_on_prizepicks: bool
    prizepicks_prop_type: str


class EdgeCalculator:
    """
    Calculate edge by comparing predictions to PrizePicks lines.

    Edge Formula:
        edge = our_probability - break_even_probability

    For PrizePicks standard picks:
        break_even = 0.50 (no juice on standard)

    Favorability Score:
        score = edge * confidence_weight * availability_bonus
    """

    # Break-even probabilities for different pick types
    BREAK_EVEN = {
        'standard': 0.50,  # Standard picks (no juice)
        'goblin': 0.545,   # Goblin picks (need ~54.5% to break even)
        'demon': 0.60,     # Demon picks (higher variance)
    }

    # Confidence tier weights
    CONFIDENCE_WEIGHTS = {
        'T1-ELITE': 1.5,
        'T2-STRONG': 1.3,
        'T3-GOOD': 1.1,
        'T4-LEAN': 0.9,
        'T5-FADE': 0.7,
    }

    def __init__(self, sport: str):
        """
        Initialize edge calculator.

        Args:
            sport: 'nhl' or 'nba'
        """
        self.sport = sport.upper()
        self.root = Path(__file__).parent.parent

        # Database paths
        if self.sport == 'NHL':
            self.predictions_db = self.root / "nhl" / "database" / "nhl_predictions_v2.db"
        else:
            self.predictions_db = self.root / "nba" / "database" / "nba_predictions.db"

        self.prizepicks_db = self.root / "shared" / "prizepicks_lines.db"

        # Prop type mapping (our types -> PrizePicks types)
        self.prop_type_map = self._get_prop_type_map()

    def _get_prop_type_map(self) -> Dict[str, List[str]]:
        """Map our prop types to possible PrizePicks stat types"""
        if self.sport == 'NHL':
            return {
                'points': ['points', 'pts'],
                'shots': ['shots', 'shots on goal', 'sog'],
                'goals': ['goals'],
                'assists': ['assists'],
            }
        else:  # NBA
            return {
                'points': ['points', 'pts'],
                'rebounds': ['rebounds', 'rebs'],
                'assists': ['assists', 'asts'],
                'threes': ['threes', '3-pt made', '3-pointers made', '3-pt_made'],
                'stocks': ['stocks', 'blks+stls', 'stls+blks', 'steals+blocks'],
                'pra': ['pra', 'pts+rebs+asts'],
                'minutes': ['minutes', 'mins'],  # Note: PP may not have minutes
            }

    def get_predictions_for_date(self, game_date: str) -> List[Dict]:
        """
        Get all predictions for a specific date.

        Args:
            game_date: Date string (YYYY-MM-DD)

        Returns:
            List of prediction dicts
        """
        conn = sqlite3.connect(self.predictions_db)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Different schemas for NHL vs NBA
        if self.sport == 'NHL':
            cursor.execute('''
                SELECT
                    player_name, team, opponent, game_date,
                    prop_type, line, prediction, probability,
                    confidence_tier, model_version, expected_value
                FROM predictions
                WHERE game_date = ?
                ORDER BY probability DESC
            ''', (game_date,))
        else:  # NBA - different schema
            cursor.execute('''
                SELECT
                    player_name, team, opponent, game_date,
                    prop_type, line, prediction, probability,
                    model_version
                FROM predictions
                WHERE game_date = ?
                ORDER BY probability DESC
            ''', (game_date,))

        rows = cursor.fetchall()
        conn.close()

        # Normalize results
        results = []
        for row in rows:
            d = dict(row)
            # Add missing fields for NBA
            if 'confidence_tier' not in d:
                # Assign tier based on probability
                prob = d.get('probability', 0.5)
                if prob >= 0.75:
                    d['confidence_tier'] = 'T1-ELITE'
                elif prob >= 0.70:
                    d['confidence_tier'] = 'T2-STRONG'
                elif prob >= 0.65:
                    d['confidence_tier'] = 'T3-GOOD'
                elif prob >= 0.55:
                    d['confidence_tier'] = 'T4-LEAN'
                else:
                    d['confidence_tier'] = 'T5-FADE'
            if 'expected_value' not in d:
                d['expected_value'] = d.get('line', 0)
            results.append(d)

        return results

    def get_prizepicks_lines(self, fetch_date: str) -> List[Dict]:
        """
        Get PrizePicks lines for a specific date.

        Args:
            fetch_date: Date string (YYYY-MM-DD)

        Returns:
            List of line dicts
        """
        if not self.prizepicks_db.exists():
            print(f"[WARN] PrizePicks database not found: {self.prizepicks_db}")
            return []

        conn = sqlite3.connect(self.prizepicks_db)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('''
            SELECT
                player_name, team, prop_type, line,
                stat_type_raw, odds_type, is_promo
            FROM prizepicks_lines
            WHERE fetch_date = ?
              AND UPPER(league) = ?
        ''', (fetch_date, self.sport))

        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    def match_prediction_to_line(
        self,
        prediction: Dict,
        pp_lines: List[Dict]
    ) -> Optional[Dict]:
        """
        Find matching PrizePicks line for a prediction.

        Uses fuzzy matching on player name and prop type.

        Args:
            prediction: Our prediction dict
            pp_lines: List of PrizePicks lines

        Returns:
            Matching PP line or None
        """
        pred_player = prediction['player_name'].lower()
        pred_prop = prediction['prop_type'].lower()
        pred_line = prediction['line']

        # Get possible PP prop type names
        possible_props = self.prop_type_map.get(pred_prop, [pred_prop])

        # First pass: find all lines for this player with matching prop
        candidates = []

        for line in pp_lines:
            pp_player = line['player_name'].lower()
            pp_prop = line['prop_type'].lower()

            # STRICT prop type match first
            prop_matches = pp_prop in possible_props or pred_prop in pp_prop
            if not prop_matches:
                continue

            # Check player name match
            player_matches = False

            # Exact match
            if pred_player == pp_player:
                player_matches = True
            else:
                # Try partial match (last name)
                pred_parts = pred_player.split()
                pp_parts = pp_player.split()

                # Match on last name
                if len(pred_parts) > 0 and len(pp_parts) > 0:
                    if pred_parts[-1] == pp_parts[-1]:
                        player_matches = True

                # Match if one contains the other
                if not player_matches:
                    if pred_player in pp_player or pp_player in pred_player:
                        player_matches = True

            if player_matches:
                candidates.append(line)

        if not candidates:
            return None

        # Find best match - prefer closest line value
        best_match = min(candidates, key=lambda x: abs(x['line'] - pred_line))

        # Sanity check: line should be somewhat close (within 50% or 10 points)
        line_diff = abs(best_match['line'] - pred_line)
        if line_diff > max(pred_line * 0.5, 10):
            # Line too different - probably wrong match
            return None

        return best_match

    def calculate_edge(
        self,
        prediction: Dict,
        pp_line: Optional[Dict] = None,
        pick_type: str = 'standard'
    ) -> EdgePick:
        """
        Calculate edge for a single prediction.

        Args:
            prediction: Our prediction dict
            pp_line: Matching PrizePicks line (or None)
            pick_type: 'standard', 'goblin', or 'demon'

        Returns:
            EdgePick with all calculations
        """
        our_prob = prediction['probability']
        our_line = prediction['line']
        our_pred = prediction['prediction']
        confidence_tier = prediction.get('confidence_tier', 'T4-LEAN')

        # Get break-even probability
        break_even = self.BREAK_EVEN.get(pick_type, 0.50)

        # Calculate raw edge
        edge = our_prob - break_even
        edge_pct = edge * 100

        # Determine if PP line is available and favorable
        available = pp_line is not None
        pp_line_value = pp_line['line'] if pp_line else our_line
        pp_prop_type = pp_line['prop_type'] if pp_line else prediction['prop_type']

        line_diff = pp_line_value - our_line

        # Line is favorable if:
        # - OVER prediction and PP line is LOWER than ours
        # - UNDER prediction and PP line is HIGHER than ours
        if our_pred == 'OVER':
            line_favorable = line_diff <= 0
        else:
            line_favorable = line_diff >= 0

        # Calculate favorability score
        conf_weight = self.CONFIDENCE_WEIGHTS.get(confidence_tier, 1.0)
        availability_bonus = 1.2 if available else 1.0
        line_bonus = 1.1 if line_favorable else 0.9

        favorability_score = edge_pct * conf_weight * availability_bonus * line_bonus

        return EdgePick(
            player_name=prediction['player_name'],
            team=prediction['team'],
            opponent=prediction['opponent'],
            game_date=prediction['game_date'],
            prop_type=prediction['prop_type'],
            our_line=our_line,
            prizepicks_line=pp_line_value,
            prediction=our_pred,
            our_probability=our_prob,
            model_version=prediction.get('model_version', 'unknown'),
            confidence_tier=confidence_tier,
            edge=edge,
            edge_pct=edge_pct,
            favorability_score=favorability_score,
            line_difference=line_diff,
            line_favorable=line_favorable,
            available_on_prizepicks=available,
            prizepicks_prop_type=pp_prop_type,
        )

    def calculate_all_edges(
        self,
        game_date: str,
        min_edge: float = 0.0,
        only_available: bool = False
    ) -> List[EdgePick]:
        """
        Calculate edges for all predictions on a date.

        Args:
            game_date: Date string (YYYY-MM-DD)
            min_edge: Minimum edge percentage to include
            only_available: Only include picks available on PrizePicks

        Returns:
            List of EdgePick sorted by favorability
        """
        # Get predictions and PP lines
        predictions = self.get_predictions_for_date(game_date)
        pp_lines = self.get_prizepicks_lines(game_date)

        print(f"[{self.sport}] Found {len(predictions)} predictions for {game_date}")
        print(f"[{self.sport}] Found {len(pp_lines)} PrizePicks lines")

        # Calculate edges
        edge_picks = []
        matched_count = 0

        for pred in predictions:
            pp_line = self.match_prediction_to_line(pred, pp_lines)
            if pp_line:
                matched_count += 1

            edge_pick = self.calculate_edge(pred, pp_line)

            # Apply filters
            if edge_pick.edge_pct < min_edge:
                continue
            if only_available and not edge_pick.available_on_prizepicks:
                continue

            edge_picks.append(edge_pick)

        print(f"[{self.sport}] Matched {matched_count} predictions to PP lines")

        # Sort by favorability score (descending)
        edge_picks.sort(key=lambda x: x.favorability_score, reverse=True)

        return edge_picks

    def get_top_picks(
        self,
        game_date: str,
        top_n: int = 20,
        min_edge: float = 3.0,
        only_available: bool = True
    ) -> List[EdgePick]:
        """
        Get top picks for the day.

        Args:
            game_date: Date string (YYYY-MM-DD)
            top_n: Number of top picks to return
            min_edge: Minimum edge percentage
            only_available: Only picks available on PrizePicks

        Returns:
            Top N picks by favorability
        """
        all_picks = self.calculate_all_edges(
            game_date,
            min_edge=min_edge,
            only_available=only_available
        )

        return all_picks[:top_n]


class DailyPicksReport:
    """Generate formatted daily picks report."""

    def __init__(self, sport: str):
        self.sport = sport.upper()
        self.calculator = EdgeCalculator(sport)

    def generate_report(
        self,
        game_date: str = None,
        top_n: int = 25,
        min_edge: float = 2.0,
        only_available: bool = False
    ) -> str:
        """
        Generate a formatted daily picks report.

        Args:
            game_date: Date (defaults to today)
            top_n: Number of picks to show
            min_edge: Minimum edge to include
            only_available: Only show available picks

        Returns:
            Formatted report string
        """
        if game_date is None:
            game_date = datetime.now().strftime('%Y-%m-%d')

        picks = self.calculator.get_top_picks(
            game_date,
            top_n=top_n,
            min_edge=min_edge,
            only_available=only_available
        )

        lines = []
        lines.append("=" * 80)
        lines.append(f"  {self.sport} DAILY PICKS REPORT - {game_date}")
        lines.append(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 80)
        lines.append("")

        if not picks:
            lines.append("  No picks meet the criteria.")
            lines.append(f"  (min_edge={min_edge}%, only_available={only_available})")
            return "\n".join(lines)

        # Summary stats
        available_count = sum(1 for p in picks if p.available_on_prizepicks)
        avg_edge = sum(p.edge_pct for p in picks) / len(picks)

        lines.append(f"  Total Picks: {len(picks)}")
        lines.append(f"  Available on PrizePicks: {available_count}")
        lines.append(f"  Average Edge: {avg_edge:.1f}%")
        lines.append("")
        lines.append("-" * 80)
        lines.append("")

        # Group by confidence tier
        by_tier = {}
        for pick in picks:
            tier = pick.confidence_tier
            if tier not in by_tier:
                by_tier[tier] = []
            by_tier[tier].append(pick)

        # Display picks
        for tier in ['T1-ELITE', 'T2-STRONG', 'T3-GOOD', 'T4-LEAN', 'T5-FADE']:
            if tier not in by_tier:
                continue

            tier_picks = by_tier[tier]
            lines.append(f"  [{tier}] - {len(tier_picks)} picks")
            lines.append("-" * 80)

            for pick in tier_picks:
                avail = "[PP]" if pick.available_on_prizepicks else "[--]"
                fav = "[FAV]" if pick.line_favorable else ""

                lines.append(
                    f"  {avail} {pick.player_name} ({pick.team} vs {pick.opponent})"
                )
                lines.append(
                    f"       {pick.prop_type.upper()} {pick.prediction} {pick.our_line} "
                    f"@ {pick.our_probability:.1%} | Edge: {pick.edge_pct:+.1f}% "
                    f"| Score: {pick.favorability_score:.1f} {fav}"
                )

                if pick.available_on_prizepicks and pick.line_difference != 0:
                    direction = "lower" if pick.line_difference < 0 else "higher"
                    lines.append(
                        f"       PP Line: {pick.prizepicks_line} ({direction} by {abs(pick.line_difference):.1f})"
                    )

                lines.append("")

            lines.append("")

        lines.append("=" * 80)
        lines.append("  LEGEND:")
        lines.append("  [PP] = Available on PrizePicks")
        lines.append("  [FAV] = PrizePicks line favorable for our prediction")
        lines.append("  Edge = Our Probability - 50% (break-even)")
        lines.append("  Score = Edge * Confidence * Availability * Line Favorability")
        lines.append("=" * 80)

        return "\n".join(lines)

    def generate_csv(
        self,
        game_date: str = None,
        output_path: str = None,
        min_edge: float = 0.0
    ) -> str:
        """
        Generate CSV export of picks.

        Args:
            game_date: Date (defaults to today)
            output_path: Output file path
            min_edge: Minimum edge to include

        Returns:
            Path to generated CSV
        """
        if game_date is None:
            game_date = datetime.now().strftime('%Y-%m-%d')

        picks = self.calculator.calculate_all_edges(game_date, min_edge=min_edge)

        if output_path is None:
            output_path = f"{self.sport.lower()}_picks_{game_date}.csv"

        # Write CSV
        with open(output_path, 'w') as f:
            # Header
            f.write("player_name,team,opponent,prop_type,prediction,our_line,")
            f.write("pp_line,probability,edge_pct,favorability,confidence_tier,")
            f.write("available_on_pp,line_favorable,model_version\n")

            for pick in picks:
                f.write(f"{pick.player_name},{pick.team},{pick.opponent},")
                f.write(f"{pick.prop_type},{pick.prediction},{pick.our_line},")
                f.write(f"{pick.prizepicks_line},{pick.our_probability:.4f},")
                f.write(f"{pick.edge_pct:.2f},{pick.favorability_score:.2f},")
                f.write(f"{pick.confidence_tier},{pick.available_on_prizepicks},")
                f.write(f"{pick.line_favorable},{pick.model_version}\n")

        print(f"Exported {len(picks)} picks to {output_path}")
        return output_path

    def get_best_plays(
        self,
        game_date: str = None,
        top_n: int = 10,
        min_edge: float = 5.0
    ) -> List[Dict]:
        """
        Get the absolute best plays for the day.

        Filters for:
        - Available on PrizePicks
        - Favorable line
        - High edge

        Args:
            game_date: Date (defaults to today)
            top_n: Number of plays
            min_edge: Minimum edge percentage

        Returns:
            List of best play dicts
        """
        if game_date is None:
            game_date = datetime.now().strftime('%Y-%m-%d')

        all_picks = self.calculator.calculate_all_edges(game_date, min_edge=min_edge)

        # Filter for best plays
        best = [
            p for p in all_picks
            if p.available_on_prizepicks and p.line_favorable
        ]

        return [asdict(p) for p in best[:top_n]]

    def generate_parlay_report(
        self,
        game_date: str = None,
        min_edge: float = 5.0,
        plays_per_prop: int = 8,
        overs_only: bool = False
    ) -> str:
        """
        Generate parlay-focused report grouped by prop type.

        Shows plays organized for building diverse parlays with
        mix of probability levels.

        Args:
            game_date: Date (defaults to today)
            min_edge: Minimum edge percentage
            plays_per_prop: Number of plays to show per prop type
            overs_only: If True, only show OVER predictions

        Returns:
            Formatted report string
        """
        if game_date is None:
            game_date = datetime.now().strftime('%Y-%m-%d')

        all_picks = self.calculator.calculate_all_edges(game_date, min_edge=0)

        # Filter for available on PP
        available_picks = [p for p in all_picks if p.available_on_prizepicks]

        # Filter for OVERS only if requested
        if overs_only:
            available_picks = [p for p in available_picks if p.prediction == 'OVER']

        if not available_picks:
            return f"No picks available on PrizePicks for {game_date}"

        # Group by prop type
        by_prop = {}
        for pick in available_picks:
            pt = pick.prop_type
            if pt not in by_prop:
                by_prop[pt] = []
            by_prop[pt].append(pick)

        # Sort each group by edge
        for pt in by_prop:
            by_prop[pt].sort(key=lambda x: -x.edge_pct)

        lines = []
        lines.append("=" * 80)
        title = f"  {self.sport.upper()} PARLAY BUILDER - {game_date}"
        if overs_only:
            title += " (OVERS ONLY)"
        lines.append(title)
        lines.append("=" * 80)
        lines.append(f"  Total Available Picks: {len(available_picks)}")
        lines.append("")

        # Define prop order by sport
        if self.sport.lower() == 'nhl':
            prop_order = ['shots', 'points', 'goals', 'assists', 'saves', 'blocked_shots']
        else:
            prop_order = ['points', 'rebounds', 'assists', 'pra', 'threes', 'stocks', 'pts_rebs', 'pts_asts']

        # Show each prop type
        for pt in prop_order:
            if pt not in by_prop:
                continue

            prop_picks = by_prop[pt][:plays_per_prop]
            total = len(by_prop[pt])

            lines.append(f"  {pt.upper()} ({total} plays available)")
            lines.append("  " + "-" * 76)
            lines.append(f"  {'Player':<20} {'Line':<14} {'Prob':>7} {'Edge':>8}   {'Tier'}")

            for pick in prop_picks:
                line_str = f"{pick.prediction} {pick.our_line}"
                lines.append(
                    f"  {pick.player_name:<20} {line_str:<14} {pick.our_probability*100:>5.1f}%  "
                    f"{pick.edge_pct:>+6.1f}%   {pick.confidence_tier}"
                )

            lines.append("")

        # Parlay building tips
        lines.append("=" * 80)
        lines.append("  PARLAY BUILDING STRATEGY")
        lines.append("=" * 80)
        lines.append("")
        lines.append("  Tier Guide:")
        lines.append("    T1-ELITE (75%+)  : Anchor legs - high probability, lower payout")
        lines.append("    T2-STRONG (70-75%): Core legs - good balance of prob and value")
        lines.append("    T3-GOOD (65-70%)  : Value legs - solid edge, reasonable risk")
        lines.append("    T4-LEAN (55-65%)  : Risk legs - use sparingly, higher reward")
        lines.append("")
        lines.append("  Suggested 4-Leg Parlay Structure:")
        lines.append("    1x T1-ELITE  (anchor)  +  2x T2-STRONG  +  1x T3-GOOD")
        lines.append("")
        lines.append("  Suggested 6-Leg Parlay Structure:")
        lines.append("    2x T1-ELITE  +  2x T2-STRONG  +  2x T3-GOOD")
        lines.append("")
        lines.append("  Tips:")
        lines.append("    - Mix prop types (don't stack all shots)")
        lines.append("    - Spread across different games when possible")
        lines.append("    - Higher edge = more value, not just higher probability")
        lines.append("=" * 80)

        return "\n".join(lines)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Edge Calculator & Daily Picks Report',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python edge_calculator.py --sport nhl --date 2026-01-16
    python edge_calculator.py --sport nba --top 20
    python edge_calculator.py --sport nba --min-edge 5 --available-only
    python edge_calculator.py --sport nhl --csv
        """
    )
    parser.add_argument(
        '--sport',
        choices=['nhl', 'nba'],
        required=True,
        help='Sport to analyze'
    )
    parser.add_argument(
        '--date',
        help='Date to analyze (YYYY-MM-DD, defaults to today)'
    )
    parser.add_argument(
        '--top',
        type=int,
        default=25,
        help='Number of top picks to show (default: 25)'
    )
    parser.add_argument(
        '--min-edge',
        type=float,
        default=2.0,
        help='Minimum edge percentage (default: 2.0)'
    )
    parser.add_argument(
        '--available-only',
        action='store_true',
        help='Only show picks available on PrizePicks'
    )
    parser.add_argument(
        '--csv',
        action='store_true',
        help='Export to CSV instead of report'
    )
    parser.add_argument(
        '--best',
        action='store_true',
        help='Show only absolute best plays (available + favorable)'
    )
    parser.add_argument(
        '--parlay',
        action='store_true',
        help='Show plays grouped by prop type for parlay building'
    )
    parser.add_argument(
        '--overs-only',
        action='store_true',
        help='Only show OVER predictions (for parlay building)'
    )

    args = parser.parse_args()

    report = DailyPicksReport(args.sport)

    if args.csv:
        report.generate_csv(args.date, min_edge=args.min_edge)
    elif args.best:
        best_plays = report.get_best_plays(args.date, top_n=args.top, min_edge=args.min_edge)
        print(f"\n{'='*60}")
        print(f"  BEST PLAYS - {args.sport.upper()}")
        print(f"{'='*60}\n")

        if not best_plays:
            print("  No plays meet criteria (available + favorable + edge >= {args.min_edge}%)")
        else:
            for i, play in enumerate(best_plays, 1):
                print(f"  {i}. {play['player_name']} ({play['team']})")
                print(f"     {play['prop_type'].upper()} {play['prediction']} {play['our_line']}")
                print(f"     Edge: {play['edge_pct']:+.1f}% | Prob: {play['our_probability']:.1%}")
                print(f"     PP Line: {play['prizepicks_line']} | Score: {play['favorability_score']:.1f}")
                print()
    elif args.parlay:
        output = report.generate_parlay_report(
            args.date,
            min_edge=args.min_edge,
            overs_only=args.overs_only
        )
        print(output)
    else:
        output = report.generate_report(
            args.date,
            top_n=args.top,
            min_edge=args.min_edge,
            only_available=args.available_only
        )
        print(output)


if __name__ == '__main__':
    import sys
    import io
    # Fix Windows encoding issues
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    main()
