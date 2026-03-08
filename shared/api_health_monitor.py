"""
API Health Monitor & Self-Healing System
=========================================

Monitors API structure changes and automatically repairs broken scripts
using Claude API for intelligent code fixes.

Key Features:
- Validates API response structures against known schemas
- Detects when APIs change their data format
- Uses Claude API to analyze broken APIs and generate fixes
- Automatically applies fixes or alerts for manual review
- Maintains version history of API structures

Usage:
    from api_health_monitor import APIHealthMonitor

    monitor = APIHealthMonitor()
    monitor.validate_espn_api("2025-12-08")
    monitor.validate_nhl_api("2025-12-08")
"""

import os
import json
import requests
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
import traceback
import shutil

# Claude API
try:
    from anthropic import Anthropic
    CLAUDE_AVAILABLE = True
except ImportError:
    CLAUDE_AVAILABLE = False
    print("WARNING: anthropic package not installed. Self-healing disabled.")


@dataclass
class APISchema:
    """Expected API response schema"""
    api_name: str
    endpoint: str
    version: str
    expected_structure: Dict
    sample_data: Dict
    last_validated: str
    validation_count: int = 0


@dataclass
class APIValidationResult:
    """Result of API validation"""
    api_name: str
    is_valid: bool
    expected_structure: Dict
    actual_structure: Dict
    differences: List[str]
    timestamp: str
    raw_response_sample: Optional[Dict] = None


@dataclass
class SelfHealingResult:
    """Result of self-healing attempt"""
    success: bool
    api_name: str
    script_path: str
    fix_description: str
    code_changes: str
    timestamp: str
    backup_path: Optional[str] = None
    errors: Optional[List[str]] = None


class APIHealthMonitor:
    """
    Monitors API health and automatically heals broken scripts.

    This class validates API responses against known schemas,
    detects structural changes, and uses Claude API to intelligently
    fix broken code when APIs change.
    """

    def __init__(self, config_dir: Path = None):
        """Initialize the API health monitor."""
        if config_dir is None:
            config_dir = Path(__file__).parent.parent / "data" / "api_schemas"

        self.config_dir = Path(config_dir)
        self.config_dir.mkdir(parents=True, exist_ok=True)

        self.schema_file = self.config_dir / "api_schemas.json"
        self.history_file = self.config_dir / "validation_history.jsonl"

        # Load known schemas
        self.schemas = self._load_schemas()

        # Initialize Claude API
        if CLAUDE_AVAILABLE and os.getenv('ANTHROPIC_API_KEY'):
            api_key = os.getenv('ANTHROPIC_API_KEY').strip()
            self.claude = Anthropic(api_key=api_key)
            self.claude_enabled = True
        else:
            self.claude = None
            self.claude_enabled = False

        print(f"[API Monitor] Initialized (Claude: {'Enabled' if self.claude_enabled else 'Disabled'})")

    def _load_schemas(self) -> Dict[str, APISchema]:
        """Load known API schemas from disk."""
        if not self.schema_file.exists():
            return self._initialize_default_schemas()

        try:
            with open(self.schema_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                schemas = {}
                for name, schema_dict in data.items():
                    schemas[name] = APISchema(**schema_dict)
                return schemas
        except Exception as e:
            print(f"[WARN] Failed to load schemas: {e}")
            return self._initialize_default_schemas()

    def _initialize_default_schemas(self) -> Dict[str, APISchema]:
        """Initialize default API schemas based on known working structures."""

        schemas = {
            'espn_nba_summary': APISchema(
                api_name='ESPN NBA Summary API',
                endpoint='https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary',
                version='2025-12-10',  # Date this schema was confirmed working
                expected_structure={
                    'boxscore': {
                        'players': [
                            {
                                'team': {'abbreviation': 'str'},
                                'statistics': [
                                    {
                                        'keys': ['list'],
                                        'athletes': [
                                            {
                                                'athlete': {'displayName': 'str'},
                                                'stats': ['list'],
                                                'didNotPlay': 'bool'
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                },
                sample_data={},
                last_validated=datetime.now().isoformat(),
                validation_count=0
            ),

            'espn_nba_scoreboard': APISchema(
                api_name='ESPN NBA Scoreboard API',
                endpoint='https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard',
                version='2025-12-10',
                expected_structure={
                    'events': [
                        {
                            'id': 'str',
                            'competitions': [
                                {
                                    'competitors': [
                                        {
                                            'homeAway': 'str',
                                            'team': {'abbreviation': 'str'},
                                            'score': 'str'
                                        }
                                    ],
                                    'status': {
                                        'type': {'name': 'str'}
                                    }
                                }
                            ]
                        }
                    ]
                },
                sample_data={},
                last_validated=datetime.now().isoformat(),
                validation_count=0
            ),

            'nhl_api_gamecenter': APISchema(
                api_name='NHL API Game Center',
                endpoint='https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore',
                version='2025-12-10',
                expected_structure={
                    'playerByGameStats': {
                        'homeTeam': {
                            'forwards': [{'playerId': 'int', 'name': {'default': 'str'}}],
                            'defense': [{'playerId': 'int', 'name': {'default': 'str'}}]
                        },
                        'awayTeam': {
                            'forwards': [{'playerId': 'int', 'name': {'default': 'str'}}],
                            'defense': [{'playerId': 'int', 'name': {'default': 'str'}}]
                        }
                    }
                },
                sample_data={},
                last_validated=datetime.now().isoformat(),
                validation_count=0
            )
        }

        self._save_schemas(schemas)
        return schemas

    def _save_schemas(self, schemas: Dict[str, APISchema] = None):
        """Save API schemas to disk."""
        if schemas is None:
            schemas = self.schemas

        try:
            data = {name: asdict(schema) for name, schema in schemas.items()}
            with open(self.schema_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"[ERROR] Failed to save schemas: {e}")

    def _log_validation(self, result: APIValidationResult):
        """Log validation result to history."""
        try:
            with open(self.history_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(asdict(result)) + '\n')
        except Exception as e:
            print(f"[WARN] Failed to log validation: {e}")

    def _get_structure_signature(self, obj: any, max_depth: int = 5, depth: int = 0) -> any:
        """
        Extract the structure signature of an object (types and keys, not values).

        This allows us to compare API structures without being affected by
        actual data values.
        """
        if depth > max_depth:
            return "..."

        if isinstance(obj, dict):
            return {k: self._get_structure_signature(v, max_depth, depth + 1) for k, v in obj.items()}
        elif isinstance(obj, list):
            if len(obj) == 0:
                return []
            # For lists, just analyze first element (assume homogeneous)
            return [self._get_structure_signature(obj[0], max_depth, depth + 1)]
        else:
            return type(obj).__name__

    def _compare_structures(self, expected: Dict, actual: Dict) -> List[str]:
        """
        Compare two structure signatures and return differences.

        Returns list of human-readable differences.
        """
        differences = []

        def compare_recursive(exp, act, path=""):
            if isinstance(exp, dict) and isinstance(act, dict):
                # Check for missing keys
                expected_keys = set(exp.keys())
                actual_keys = set(act.keys())

                missing = expected_keys - actual_keys
                extra = actual_keys - expected_keys

                if missing:
                    differences.append(f"Missing keys at {path}: {missing}")
                if extra:
                    differences.append(f"Extra keys at {path}: {extra}")

                # Recurse into common keys
                for key in expected_keys & actual_keys:
                    compare_recursive(exp[key], act[key], f"{path}.{key}" if path else key)

            elif isinstance(exp, list) and isinstance(act, list):
                if len(exp) > 0 and len(act) > 0:
                    compare_recursive(exp[0], act[0], f"{path}[0]")
                # Empty expected list = schema allows empty (e.g. events on off-days) — don't flag
                # Non-empty expected but empty actual = only flag if schema explicitly requires items

            elif exp != act:
                differences.append(f"Type mismatch at {path}: expected {exp}, got {act}")

        compare_recursive(expected, actual)
        return differences

    def validate_api(self, api_name: str, actual_response: Dict) -> APIValidationResult:
        """
        Validate an API response against known schema.

        Args:
            api_name: Name of the API schema to validate against
            actual_response: The actual API response to validate

        Returns:
            APIValidationResult with validation details
        """
        if api_name not in self.schemas:
            return APIValidationResult(
                api_name=api_name,
                is_valid=False,
                expected_structure={},
                actual_structure={},
                differences=[f"Unknown API: {api_name}"],
                timestamp=datetime.now().isoformat()
            )

        schema = self.schemas[api_name]

        # Get structure signatures
        expected_sig = schema.expected_structure
        actual_sig = self._get_structure_signature(actual_response)

        # Compare structures
        differences = self._compare_structures(expected_sig, actual_sig)

        # Create result
        result = APIValidationResult(
            api_name=api_name,
            is_valid=len(differences) == 0,
            expected_structure=expected_sig,
            actual_structure=actual_sig,
            differences=differences,
            timestamp=datetime.now().isoformat(),
            raw_response_sample=actual_response if not len(differences) == 0 else None
        )

        # Update schema if valid
        if result.is_valid:
            schema.last_validated = result.timestamp
            schema.validation_count += 1
            self._save_schemas()

        # Log result
        self._log_validation(result)

        return result

    def validate_espn_nba_summary(self, game_id: str) -> APIValidationResult:
        """Validate ESPN NBA summary API for a specific game."""
        try:
            url = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary"
            response = requests.get(url, params={'event': game_id}, timeout=30)
            response.raise_for_status()
            data = response.json()

            return self.validate_api('espn_nba_summary', data)

        except Exception as e:
            return APIValidationResult(
                api_name='espn_nba_summary',
                is_valid=False,
                expected_structure=self.schemas['espn_nba_summary'].expected_structure,
                actual_structure={},
                differences=[f"API request failed: {str(e)}"],
                timestamp=datetime.now().isoformat()
            )

    def validate_espn_nba_scoreboard(self, game_date: str) -> APIValidationResult:
        """Validate ESPN NBA scoreboard API for a specific date."""
        try:
            url = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
            espn_date = game_date.replace('-', '')
            response = requests.get(url, params={'dates': espn_date}, timeout=30)
            response.raise_for_status()
            data = response.json()

            return self.validate_api('espn_nba_scoreboard', data)

        except Exception as e:
            return APIValidationResult(
                api_name='espn_nba_scoreboard',
                is_valid=False,
                expected_structure=self.schemas['espn_nba_scoreboard'].expected_structure,
                actual_structure={},
                differences=[f"API request failed: {str(e)}"],
                timestamp=datetime.now().isoformat()
            )

    def self_heal_api_script(
        self,
        api_name: str,
        validation_result: APIValidationResult,
        script_path: Path
    ) -> SelfHealingResult:
        """
        Automatically fix a broken API script using Claude API.

        This method:
        1. Reads the broken script
        2. Analyzes the validation failure
        3. Uses Claude to generate a fix
        4. Creates a backup
        5. Applies the fix (or returns it for manual review)

        Args:
            api_name: Name of the API
            validation_result: The validation failure details
            script_path: Path to the script that needs fixing

        Returns:
            SelfHealingResult with fix details
        """
        if not self.claude_enabled:
            return SelfHealingResult(
                success=False,
                api_name=api_name,
                script_path=str(script_path),
                fix_description="Claude API not available",
                code_changes="",
                timestamp=datetime.now().isoformat(),
                errors=["Claude API not enabled or API key not set"]
            )

        try:
            # Read the broken script
            with open(script_path, 'r', encoding='utf-8') as f:
                broken_code = f.read()

            # Build prompt for Claude
            prompt = self._build_healing_prompt(
                api_name=api_name,
                broken_code=broken_code,
                validation_result=validation_result
            )

            # Call Claude API
            print(f"[HEAL] Analyzing broken script with Claude...")
            response = self.claude.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=8000,
                messages=[{
                    "role": "user",
                    "content": prompt
                }]
            )

            analysis = response.content[0].text

            # Extract the fixed code from Claude's response
            fixed_code = self._extract_code_from_response(analysis)

            if not fixed_code:
                return SelfHealingResult(
                    success=False,
                    api_name=api_name,
                    script_path=str(script_path),
                    fix_description=analysis,
                    code_changes="",
                    timestamp=datetime.now().isoformat(),
                    errors=["Could not extract code from Claude's response"]
                )

            # Create backup
            backup_path = self._create_backup(script_path)

            # Apply the fix
            with open(script_path, 'w', encoding='utf-8') as f:
                f.write(fixed_code)

            print(f"[HEAL] [OK] Script fixed and saved")
            print(f"[HEAL] Backup: {backup_path}")

            return SelfHealingResult(
                success=True,
                api_name=api_name,
                script_path=str(script_path),
                fix_description=analysis,
                code_changes=f"Backup: {backup_path}\n\nClaude's analysis:\n{analysis}",
                backup_path=str(backup_path),
                timestamp=datetime.now().isoformat()
            )

        except Exception as e:
            return SelfHealingResult(
                success=False,
                api_name=api_name,
                script_path=str(script_path),
                fix_description=str(e),
                code_changes="",
                timestamp=datetime.now().isoformat(),
                errors=[traceback.format_exc()]
            )

    def _build_healing_prompt(
        self,
        api_name: str,
        broken_code: str,
        validation_result: APIValidationResult
    ) -> str:
        """Build a prompt for Claude to fix the broken API script."""

        return f"""You are a self-healing code repair system. An API has changed its structure and broken a script.

**API THAT CHANGED:** {api_name}

**VALIDATION FAILURE:**
The API response structure has changed. Here are the differences detected:
{chr(10).join('- ' + d for d in validation_result.differences)}

**EXPECTED STRUCTURE (what the code expects):**
```json
{json.dumps(validation_result.expected_structure, indent=2)}
```

**ACTUAL STRUCTURE (what the API now returns):**
```json
{json.dumps(validation_result.actual_structure, indent=2)}
```

**SAMPLE ACTUAL RESPONSE (partial):**
```json
{json.dumps(validation_result.raw_response_sample, indent=2)[:1000] if validation_result.raw_response_sample else 'N/A'}
```

**BROKEN CODE:**
```python
{broken_code}
```

**YOUR TASK:**
1. Analyze the structural differences between expected and actual API responses
2. Identify the exact lines in the code that are broken due to the API change
3. Fix the code to work with the new API structure
4. Ensure the fixed code maintains the same functionality and return values
5. Add comments explaining what changed and why

**IMPORTANT:**
- Return the COMPLETE fixed script (not just the changes)
- Preserve all existing functionality
- Add a comment at the top: "# AUTO-FIXED: [date] - [brief description of fix]"
- Ensure the code is production-ready

Return your response in this format:

ANALYSIS:
[Explain what changed in the API and what needs to be fixed]

FIXED CODE:
```python
[Complete fixed code here]
```

TESTING RECOMMENDATION:
[Suggest how to verify the fix works]
"""

    def _extract_code_from_response(self, response: str) -> Optional[str]:
        """Extract Python code from Claude's response."""

        # Look for code block
        if "```python" in response:
            start = response.find("```python") + len("```python")
            end = response.find("```", start)
            if end != -1:
                return response[start:end].strip()

        # Look for FIXED CODE section
        if "FIXED CODE:" in response:
            start = response.find("FIXED CODE:") + len("FIXED CODE:")
            # Find the code block after this
            if "```python" in response[start:]:
                start = response.find("```python", start) + len("```python")
                end = response.find("```", start)
                if end != -1:
                    return response[start:end].strip()

        return None

    def _create_backup(self, file_path: Path) -> Path:
        """Create a backup of a file before modifying it."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = file_path.parent / "backups"
        backup_dir.mkdir(exist_ok=True)

        backup_name = f"{file_path.stem}_backup_{timestamp}{file_path.suffix}"
        backup_path = backup_dir / backup_name

        shutil.copy2(file_path, backup_path)
        return backup_path

    def run_full_health_check(self, test_date: str = "2025-12-08") -> Dict[str, APIValidationResult]:
        """
        Run health check on all known APIs.

        Args:
            test_date: Date to use for testing (YYYY-MM-DD)

        Returns:
            Dict mapping API names to validation results
        """
        results = {}

        print(f"\n{'='*70}")
        print(f"  API HEALTH CHECK - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*70}\n")

        # Test ESPN NBA Scoreboard
        print("[1/2] Testing ESPN NBA Scoreboard API...")
        results['espn_nba_scoreboard'] = self.validate_espn_nba_scoreboard(test_date)
        status = "[PASS]" if results['espn_nba_scoreboard'].is_valid else "[FAIL]"
        print(f"      {status}")

        if not results['espn_nba_scoreboard'].is_valid:
            print(f"      Issues: {len(results['espn_nba_scoreboard'].differences)}")
            for diff in results['espn_nba_scoreboard'].differences[:3]:
                print(f"        - {diff}")

        # Test ESPN NBA Summary (need a game ID)
        print("\n[2/2] Testing ESPN NBA Summary API...")
        try:
            # Get a game ID from scoreboard first
            scoreboard_test = self.validate_espn_nba_scoreboard(test_date)
            if scoreboard_test.raw_response_sample and 'events' in scoreboard_test.raw_response_sample:
                game_id = scoreboard_test.raw_response_sample['events'][0]['id']
                results['espn_nba_summary'] = self.validate_espn_nba_summary(game_id)
                status = "[PASS]" if results['espn_nba_summary'].is_valid else "[FAIL]"
                print(f"      {status}")

                if not results['espn_nba_summary'].is_valid:
                    print(f"      Issues: {len(results['espn_nba_summary'].differences)}")
                    for diff in results['espn_nba_summary'].differences[:3]:
                        print(f"        - {diff}")
            else:
                print("      [SKIP] (no games available)")
        except Exception as e:
            print(f"      [ERROR]: {e}")

        print(f"\n{'='*70}")
        print(f"  HEALTH CHECK COMPLETE")
        print(f"{'='*70}\n")

        return results


# CLI Interface
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="API Health Monitor & Self-Healing System")
    parser.add_argument('--check', action='store_true', help='Run full health check')
    parser.add_argument('--heal', type=str, help='Attempt to heal a specific script (path)')
    parser.add_argument('--api', type=str, help='API name for healing (required with --heal)')
    parser.add_argument('--date', type=str, default='2025-12-08', help='Test date (YYYY-MM-DD)')

    args = parser.parse_args()

    monitor = APIHealthMonitor()

    if args.check:
        results = monitor.run_full_health_check(args.date)

        # Check if any APIs failed
        failed = [name for name, result in results.items() if not result.is_valid]

        if failed:
            print(f"\n[WARN] {len(failed)} API(s) failed validation:")
            for api_name in failed:
                print(f"   - {api_name}")

            if monitor.claude_enabled:
                print("\n[INFO] Run with --heal to attempt automatic fixes")
        else:
            print("\n[OK] All APIs passed validation!")

    elif args.heal:
        if not args.api:
            print("ERROR: --api is required when using --heal")
            exit(1)

        script_path = Path(args.heal)
        if not script_path.exists():
            print(f"ERROR: Script not found: {script_path}")
            exit(1)

        # First validate the API to get failure details
        print(f"Validating {args.api}...")
        if args.api == 'espn_nba_summary':
            # Need a game ID - get from scoreboard
            scoreboard = monitor.validate_espn_nba_scoreboard(args.date)
            if scoreboard.raw_response_sample and 'events' in scoreboard.raw_response_sample:
                game_id = scoreboard.raw_response_sample['events'][0]['id']
                validation = monitor.validate_espn_nba_summary(game_id)
            else:
                print("ERROR: Could not get game ID for testing")
                exit(1)
        else:
            validation = monitor.validate_espn_nba_scoreboard(args.date)

        if validation.is_valid:
            print(f"[OK] API is working correctly. No healing needed.")
        else:
            print(f"[FAIL] API validation failed. Attempting to heal script...")
            result = monitor.self_heal_api_script(args.api, validation, script_path)

            if result.success:
                print(f"\n[SUCCESS] HEALING SUCCESSFUL!")
                print(f"   Backup: {result.backup_path}")
                print(f"\n{result.fix_description}")
            else:
                print(f"\n[FAIL] HEALING FAILED")
                print(f"   {result.fix_description}")
                if result.errors:
                    print(f"\n   Errors:")
                    for error in result.errors:
                        print(f"   {error}")

    else:
        parser.print_help()
