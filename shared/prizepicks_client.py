#!/usr/bin/env python3
"""
PrizePicks Line Ingestion Script
=================================

Fetches available player props/lines from PrizePicks API.
Stores in database for comparison with our predictions.

Run daily before prediction generation to have current lines available.

Usage:
    python prizepicks_ingestion.py --sport nhl
    python prizepicks_ingestion.py --sport nba
    python prizepicks_ingestion.py --sport all
"""

import requests
import sqlite3
import json
import time
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import traceback

# Optional: pandas for data processing
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    print("NOTE: pandas not installed. Basic functionality only.")


class PrizePicksAPI:
    """
    PrizePicks API client for fetching player prop lines.

    Uses the unofficial API endpoints discovered through reverse engineering.
    Note: PrizePicks doesn't have an official public API.
    """

    # API endpoints (try partner-api first, fallback to main)
    ENDPOINTS = [
        "https://partner-api.prizepicks.com/projections",
        "https://api.prizepicks.com/projections"
    ]

    # League IDs (verified Jan 2026)
    LEAGUE_IDS = {
        'NHL': 8,
        'NBA': 7,
        'NFL': 9,
        'MLB': 3,  # Verify when MLB season starts
        'CFB': 15,
        'CBB': 20,
    }

    # Map PrizePicks stat types to our prop types
    STAT_TYPE_MAP = {
        # NHL
        'Points': 'points',
        'Goals': 'goals',
        'Assists': 'assists',
        'Shots': 'shots',
        'Shots On Goal': 'shots',
        'SOG': 'shots',
        'Saves': 'saves',
        'Goals Against': 'goals_against',
        'Blocked Shots': 'blocked_shots',
        'Hits': 'hits',
        'Power Play Points': 'pp_points',

        # NBA
        'Pts': 'points',
        'Rebs': 'rebounds',
        'Asts': 'assists',
        'Pts+Rebs': 'pts_rebs',
        'Pts+Asts': 'pts_asts',
        'Rebs+Asts': 'rebs_asts',
        'Pts+Rebs+Asts': 'pra',
        '3-PT Made': 'threes',
        '3-Pointers Made': 'threes',
        'Steals': 'steals',
        'Blocks': 'blocks',
        'Stls+Blks': 'stocks',
        'Turnovers': 'turnovers',
        'Fantasy Score': 'fantasy',
    }

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json; charset=UTF-8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Dest': 'empty',
            'Referer': 'https://app.prizepicks.com/',
            'Origin': 'https://app.prizepicks.com',
        })
        self.last_request_time = 0
        self.min_request_interval = 2.0  # seconds between requests

    def _rate_limit(self):
        """Enforce rate limiting between requests"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_request_interval:
            time.sleep(self.min_request_interval - elapsed)
        self.last_request_time = time.time()

    def fetch_projections(self, league: str = None, per_page: int = 1000, max_retries: int = 3) -> Optional[Dict]:
        """
        Fetch projections from PrizePicks API.

        Args:
            league: League name (NHL, NBA, etc.) or None for all
            per_page: Number of results per page
            max_retries: Number of retry attempts per endpoint

        Returns:
            Raw API response as dict, or None on error
        """
        params = {
            'per_page': per_page,
            'single_stat': 'true',
        }

        # Add league filter if specified
        if league and league.upper() in self.LEAGUE_IDS:
            params['league_id'] = self.LEAGUE_IDS[league.upper()]

        # Try each endpoint with retries (API can be intermittent)
        for endpoint in self.ENDPOINTS:
            for attempt in range(max_retries):
                self._rate_limit()
                try:
                    response = self.session.get(
                        endpoint,
                        params=params,
                        timeout=30
                    )

                    if response.status_code == 200:
                        content_type = response.headers.get('Content-Type', '')
                        if 'application/json' in content_type:
                            return response.json()
                        else:
                            print(f"   [WARN] Got non-JSON response from {endpoint}")
                            break  # Try next endpoint
                    elif response.status_code == 403:
                        print(f"   [WARN] 403 Forbidden from {endpoint}")
                        break  # Try next endpoint (bot protection)
                    elif response.status_code in [500, 502, 503, 521]:
                        # Server error - retry
                        if attempt < max_retries - 1:
                            print(f"   [WARN] Status {response.status_code}, retrying...")
                            time.sleep(2 ** attempt)  # Exponential backoff
                            continue
                    else:
                        print(f"   [WARN] Status {response.status_code} from {endpoint}")
                        break

                except requests.exceptions.RequestException as e:
                    if attempt < max_retries - 1:
                        print(f"   [WARN] Request failed, retrying: {e}")
                        time.sleep(2 ** attempt)
                        continue
                    print(f"   [ERROR] Request failed for {endpoint}: {e}")
                    break

        return None

    def parse_projections(self, response: Dict) -> List[Dict]:
        """
        Parse API response into structured projection data.

        Args:
            response: Raw API response

        Returns:
            List of parsed projection dicts
        """
        if not response:
            return []

        # Build player lookup from 'included' array
        players = {}
        leagues = {}

        for item in response.get('included', []):
            item_type = item.get('type', '')
            item_id = item.get('id', '')
            attrs = item.get('attributes', {})

            if item_type == 'new_player':
                players[item_id] = {
                    'name': attrs.get('name', ''),
                    'team': attrs.get('team', ''),
                    'team_name': attrs.get('team_name', ''),
                    'position': attrs.get('position', ''),
                    'image_url': attrs.get('image_url', ''),
                }
            elif item_type == 'league':
                leagues[item_id] = {
                    'name': attrs.get('name', ''),
                }

        # Parse projections from 'data' array
        projections = []

        for item in response.get('data', []):
            if item.get('type') != 'projection':
                continue

            attrs = item.get('attributes', {})
            relationships = item.get('relationships', {})

            # Get player info
            player_data = relationships.get('new_player', {}).get('data', {})
            player_id = player_data.get('id', '')
            player_info = players.get(player_id, {})

            # Get league info
            league_data = relationships.get('league', {}).get('data', {})
            league_id = league_data.get('id', '')
            league_info = leagues.get(league_id, {})

            # Map stat type to our format
            raw_stat_type = attrs.get('stat_type', '')
            mapped_prop_type = self.STAT_TYPE_MAP.get(raw_stat_type, raw_stat_type.lower().replace(' ', '_'))

            projection = {
                'projection_id': item.get('id', ''),
                'player_name': player_info.get('name', ''),
                'team': player_info.get('team', ''),
                'team_name': player_info.get('team_name', ''),
                'position': player_info.get('position', ''),
                'league': league_info.get('name', attrs.get('league', '')),
                'stat_type_raw': raw_stat_type,
                'prop_type': mapped_prop_type,
                'line': float(attrs.get('line_score', 0)),
                'description': attrs.get('description', ''),
                'odds_type': attrs.get('odds_type', ''),
                'is_promo': attrs.get('is_promo', False),
                'flash_sale_line_score': attrs.get('flash_sale_line_score'),
                'game_id': attrs.get('game_id', ''),
                'start_time': attrs.get('start_time', ''),
                'end_time': attrs.get('end_time', ''),
                'status': attrs.get('status', ''),
                'refundable': attrs.get('refundable', False),
                'tv_channel': attrs.get('tv_channel', ''),
                'updated_at': attrs.get('updated_at', ''),
                'fetched_at': datetime.now().isoformat(),
            }

            projections.append(projection)

        return projections

    def get_lines_for_sport(self, sport: str) -> List[Dict]:
        """
        Get all available lines for a specific sport.

        Args:
            sport: Sport name (NHL, NBA, etc.)

        Returns:
            List of projection dicts
        """
        print(f"   Fetching {sport} lines from PrizePicks...")

        response = self.fetch_projections(league=sport)

        if not response:
            print(f"   [WARN] No response from API for {sport}")
            return []

        projections = self.parse_projections(response)

        # Filter by sport (in case API returned mixed results)
        sport_projections = [
            p for p in projections
            if p['league'].upper() == sport.upper()
        ]

        print(f"   [OK] Found {len(sport_projections)} {sport} lines")
        return sport_projections


class PrizePicksDatabase:
    """Database manager for PrizePicks lines storage."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_database()

    def _init_database(self):
        """Initialize database tables if they don't exist."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Main lines table - stores current/historical lines
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS prizepicks_lines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                projection_id TEXT,
                fetch_date DATE,
                player_name TEXT,
                team TEXT,
                league TEXT,
                prop_type TEXT,
                stat_type_raw TEXT,
                line REAL,
                description TEXT,
                odds_type TEXT,
                is_promo BOOLEAN,
                flash_sale_line REAL,
                game_id TEXT,
                start_time TEXT,
                status TEXT,
                fetched_at TEXT,
                UNIQUE(projection_id, fetch_date)
            )
        ''')

        # Index for fast lookups
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_pp_lines_date
            ON prizepicks_lines(fetch_date)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_pp_lines_player
            ON prizepicks_lines(player_name, prop_type)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_pp_lines_league
            ON prizepicks_lines(league, fetch_date)
        ''')

        # View for today's lines (convenience)
        cursor.execute('''
            CREATE VIEW IF NOT EXISTS prizepicks_today AS
            SELECT * FROM prizepicks_lines
            WHERE fetch_date = DATE('now')
        ''')

        conn.commit()
        conn.close()

    def save_lines(self, projections: List[Dict]) -> int:
        """
        Save projections to database.

        Args:
            projections: List of projection dicts

        Returns:
            Number of rows saved
        """
        if not projections:
            return 0

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        today = datetime.now().strftime('%Y-%m-%d')
        saved_count = 0

        for proj in projections:
            try:
                cursor.execute('''
                    INSERT OR REPLACE INTO prizepicks_lines
                    (projection_id, fetch_date, player_name, team, league,
                     prop_type, stat_type_raw, line, description, odds_type,
                     is_promo, flash_sale_line, game_id, start_time, status, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    proj['projection_id'],
                    today,
                    proj['player_name'],
                    proj['team'],
                    proj['league'],
                    proj['prop_type'],
                    proj['stat_type_raw'],
                    proj['line'],
                    proj['description'],
                    proj['odds_type'],
                    proj['is_promo'],
                    proj.get('flash_sale_line_score'),
                    proj['game_id'],
                    proj['start_time'],
                    proj['status'],
                    proj['fetched_at'],
                ))
                saved_count += 1
            except sqlite3.Error as e:
                print(f"   [WARN] Error saving {proj['player_name']}: {e}")

        conn.commit()
        conn.close()

        return saved_count

    def get_lines_for_date(self, date: str, league: str = None) -> List[Dict]:
        """
        Get all lines for a specific date.

        Args:
            date: Date string (YYYY-MM-DD)
            league: Optional league filter

        Returns:
            List of line dicts
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        if league:
            cursor.execute('''
                SELECT * FROM prizepicks_lines
                WHERE fetch_date = ? AND league = ?
            ''', (date, league.upper()))
        else:
            cursor.execute('''
                SELECT * FROM prizepicks_lines WHERE fetch_date = ?
            ''', (date,))

        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    def get_player_line(self, player_name: str, prop_type: str, date: str = None) -> Optional[Dict]:
        """
        Get a specific player's line for a prop type.

        Args:
            player_name: Player name (fuzzy match supported)
            prop_type: Prop type (points, shots, etc.)
            date: Date string (defaults to today)

        Returns:
            Line dict or None
        """
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Exact match first
        cursor.execute('''
            SELECT * FROM prizepicks_lines
            WHERE fetch_date = ?
              AND LOWER(player_name) = LOWER(?)
              AND prop_type = ?
            LIMIT 1
        ''', (date, player_name, prop_type))

        row = cursor.fetchone()

        # Try fuzzy match if exact fails
        if not row:
            cursor.execute('''
                SELECT * FROM prizepicks_lines
                WHERE fetch_date = ?
                  AND LOWER(player_name) LIKE ?
                  AND prop_type = ?
                LIMIT 1
            ''', (date, f'%{player_name.lower()}%', prop_type))
            row = cursor.fetchone()

        conn.close()

        return dict(row) if row else None

    def is_line_available(self, player_name: str, prop_type: str, line: float,
                         prediction: str, date: str = None) -> Tuple[bool, Optional[Dict]]:
        """
        Check if a specific line is available on PrizePicks.

        Args:
            player_name: Player name
            prop_type: Prop type
            line: Line value
            prediction: OVER or UNDER
            date: Date string (defaults to today)

        Returns:
            Tuple of (is_available, line_info)
        """
        pp_line = self.get_player_line(player_name, prop_type, date)

        if not pp_line:
            return (False, None)

        # Check if the line matches
        # PrizePicks typically offers exact lines
        if abs(pp_line['line'] - line) < 0.01:
            return (True, pp_line)

        # Different line - might still be usable depending on direction
        # If PP line is lower and we want OVER, our prediction might still be valid
        # If PP line is higher and we want UNDER, our prediction might still be valid
        return (False, pp_line)


class PrizePicksIngestion:
    """Main ingestion orchestrator."""

    def __init__(self):
        self.api = PrizePicksAPI()

        # Database paths - store in parent user folder
        self.root = Path(__file__).parent
        self.db_path = self.root / "prizepicks_lines.db"
        self.db = PrizePicksDatabase(str(self.db_path))

    def run_ingestion(self, sports: List[str] = None) -> Dict:
        """
        Run the full ingestion pipeline.

        Args:
            sports: List of sports to ingest (default: ['NHL', 'NBA'])

        Returns:
            Summary dict with results
        """
        if sports is None:
            sports = ['NHL', 'NBA']

        print("\n" + "=" * 60)
        print("  PRIZEPICKS LINE INGESTION")
        print("=" * 60)
        print(f"  Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Sports: {', '.join(sports)}")
        print("=" * 60 + "\n")

        results = {
            'timestamp': datetime.now().isoformat(),
            'sports': {},
            'total_lines': 0,
            'errors': []
        }

        for sport in sports:
            print(f"\n[{sport}] Processing {sport}...")

            try:
                # Fetch lines from API
                lines = self.api.get_lines_for_sport(sport)

                if lines:
                    # Save to database
                    saved = self.db.save_lines(lines)

                    # Generate summary
                    prop_types = {}
                    for line in lines:
                        pt = line['prop_type']
                        if pt not in prop_types:
                            prop_types[pt] = 0
                        prop_types[pt] += 1

                    results['sports'][sport] = {
                        'lines_fetched': len(lines),
                        'lines_saved': saved,
                        'prop_types': prop_types
                    }
                    results['total_lines'] += saved

                    print(f"   [OK] Saved {saved} lines")
                    print(f"   Prop breakdown: {prop_types}")
                else:
                    results['sports'][sport] = {
                        'lines_fetched': 0,
                        'lines_saved': 0,
                        'error': 'No data returned'
                    }
                    results['errors'].append(f"{sport}: No data returned from API")
                    print(f"   [WARN] No lines available")

            except Exception as e:
                error_msg = f"{sport}: {str(e)}"
                results['sports'][sport] = {'error': str(e)}
                results['errors'].append(error_msg)
                print(f"   [ERROR] {e}")
                traceback.print_exc()

        # Summary
        print("\n" + "=" * 60)
        print("  INGESTION COMPLETE")
        print("=" * 60)
        print(f"  Total lines saved: {results['total_lines']}")

        if results['errors']:
            print(f"  Errors: {len(results['errors'])}")
            for err in results['errors']:
                print(f"    - {err}")

        print("=" * 60 + "\n")

        return results

    def export_to_csv(self, date: str = None, output_path: str = None):
        """Export lines to CSV file for analysis."""
        if not PANDAS_AVAILABLE:
            print("pandas required for CSV export")
            return

        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')

        lines = self.db.get_lines_for_date(date)

        if not lines:
            print(f"No lines found for {date}")
            return

        df = pd.DataFrame(lines)

        if output_path is None:
            output_path = self.root / f"prizepicks_lines_{date}.csv"

        df.to_csv(output_path, index=False)
        print(f"Exported {len(lines)} lines to {output_path}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='PrizePicks Line Ingestion Script',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python prizepicks_ingestion.py --sport nhl
    python prizepicks_ingestion.py --sport nba
    python prizepicks_ingestion.py --sport all
    python prizepicks_ingestion.py --export
        """
    )
    parser.add_argument(
        '--sport',
        choices=['nhl', 'nba', 'all'],
        default='all',
        help='Sport to fetch (default: all)'
    )
    parser.add_argument(
        '--export',
        action='store_true',
        help='Export to CSV after ingestion'
    )
    parser.add_argument(
        '--date',
        help='Date for export (YYYY-MM-DD)'
    )

    args = parser.parse_args()

    ingestion = PrizePicksIngestion()

    # Determine sports
    if args.sport == 'all':
        sports = ['NHL', 'NBA']
    else:
        sports = [args.sport.upper()]

    # Run ingestion
    results = ingestion.run_ingestion(sports)

    # Export if requested
    if args.export:
        ingestion.export_to_csv(args.date)

    return results


if __name__ == '__main__':
    main()
