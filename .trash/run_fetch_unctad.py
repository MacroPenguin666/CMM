"""
Fetch UNCTAD bilateral merchandise trade data (HS-2 chapters, China-centric).

Writes to data/unctad_trade.db.

Usage:
    python run_fetch_unctad.py              # incremental (missing years only)
    python run_fetch_unctad.py --full       # all years (2015-2023)
    python run_fetch_unctad.py --year 2022  # specific year
    python run_fetch_unctad.py --force      # re-fetch already-stored years
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "scripts"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)

from policy_monitor.unctad import fetch, HISTORY_YEARS, MAX_YEAR
from datetime import datetime, timezone

ap = argparse.ArgumentParser(description="Fetch UNCTAD bilateral trade data")
ap.add_argument("--full",  action="store_true", help=f"Fetch all {HISTORY_YEARS} history years")
ap.add_argument("--year",  type=int, help="Fetch a specific year (e.g. 2022)")
ap.add_argument("--force", action="store_true", help="Re-fetch even if already stored")
args = ap.parse_args()

if args.year:
    years = [args.year]
elif args.full:
    current = datetime.now(timezone.utc).year
    years = list(range(current - HISTORY_YEARS, MAX_YEAR + 1))
else:
    years = None   # incremental

result = fetch(years=years, force=args.force)

print(f"\n{'='*50}")
print(f"  Years fetched : {result['years_fetched']}")
print(f"  Rows added    : {result['rows_added']}")
if result["errors"]:
    print(f"  Errors ({len(result['errors'])}):")
    for e in result["errors"]:
        print(f"    - {e}")
print(f"{'='*50}\n")
