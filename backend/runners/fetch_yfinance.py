"""
Yahoo Finance daily market data runner.
Fetches China indices, ETFs, FX rates, commodities, and rates into data/yfinance.db.

Requires: pip install yfinance  (auto-installed on first run if missing)
"""

import argparse
import logging
from datetime import datetime
from pathlib import Path

from backend.storage import LOG_DIR
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_DIR / f"yfinance_{datetime.now():%Y-%m-%d}.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("fetch_yfinance")


def run(force_full: bool = False, tickers: list[str] | None = None):
    from backend.fetchers.yfinance_data import get_yfinance_db, fetch_all, TICKERS

    conn = get_yfinance_db()
    log.info(f"Yahoo Finance fetch — {len(tickers or TICKERS)} tickers, full={force_full}")

    results = fetch_all(conn, force_full=force_full, tickers=tickers)

    ok = sum(1 for v in results.values() if v > 0)
    empty = sum(1 for v in results.values() if v == 0)
    total_rows = sum(results.values())
    log.info(f"Done: {ok} tickers fetched, {empty} empty | {total_rows:,} rows → data/yfinance.db")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch Yahoo Finance daily market data")
    parser.add_argument("--full", action="store_true", help=f"Full history ({5} years)")
    parser.add_argument("--tickers", nargs="+", help="Specific tickers to fetch")
    args = parser.parse_args()
    run(force_full=args.full, tickers=args.tickers)
