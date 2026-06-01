"""
Entry point for the China Customs export data scraper.

Usage:
    python -m customs_scraper.main                          # scrape previous month
    python -m customs_scraper.main --year 2024 --month 1   # backfill a specific month
    python -m customs_scraper.main --schedule               # start monthly scheduler (blocks)
    python -m customs_scraper.main --resume <run_id>        # resume a partial/failed run
    python -m customs_scraper.main --debug-browser          # open browser for site inspection
    python -m customs_scraper.main --bootstrap-hs-codes     # fetch HS code + country lists
"""
import argparse
import logging
import sys
from datetime import date

from .config import DB_PATH
from .db import init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("customs_scraper.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


def _parse_args() -> argparse.Namespace:
    today = date.today()
    # When run on the 15th or later, default to the previous month (published data)
    if today.day >= 15:
        default_month = today.month - 1 if today.month > 1 else 12
        default_year = today.year if today.month > 1 else today.year - 1
    else:
        default_month = today.month - 2 if today.month > 2 else (12 + today.month - 2)
        default_year = today.year if today.month > 2 else today.year - 1

    p = argparse.ArgumentParser(
        description="China Customs (GACC) monthly export data scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--year", type=int, default=default_year,
                   help=f"Year to scrape (default: {default_year})")
    p.add_argument("--month", type=int, default=default_month,
                   help=f"Month to scrape 1-12 (default: {default_month})")
    p.add_argument("--schedule", action="store_true",
                   help="Start APScheduler daemon — runs monthly on the 15th (blocks)")
    p.add_argument("--resume", metavar="RUN_ID",
                   help="Resume a partial/failed run by its run_id UUID")
    p.add_argument("--debug-browser", action="store_true",
                   help="Open non-headless browser at query page for manual inspection")
    p.add_argument("--bootstrap-hs-codes", action="store_true",
                   help="Fetch HS8 code + country lists from site and save to data/ CSVs")
    p.add_argument("--db-path", default=DB_PATH,
                   help=f"Override SQLite database path (default: {DB_PATH})")
    return p.parse_args()


def main() -> None:
    args = _parse_args()

    if not (1 <= args.month <= 12):
        logger.error(f"Invalid month: {args.month}. Must be 1-12.")
        sys.exit(1)

    init_db(args.db_path)
    logger.info(f"Database: {args.db_path}")

    if args.schedule:
        from .scheduler import start_scheduler
        start_scheduler(db_path=args.db_path)
        return  # blocks until killed

    if args.debug_browser:
        from .debug import run_debug_browser
        run_debug_browser(args.year, args.month)
        return

    if args.bootstrap_hs_codes:
        from .bootstrap import bootstrap_hs_codes, bootstrap_countries
        bootstrap_hs_codes()
        bootstrap_countries()
        return

    from .orchestrator import ScrapeOrchestrator
    orchestrator = ScrapeOrchestrator(
        year=args.year,
        month=args.month,
        resume_run_id=args.resume,
        db_path=args.db_path,
    )
    logger.info(f"Scraping {args.year}-{args.month:02d}")
    status = orchestrator.run()
    sys.exit(0 if status in ("success", "partial") else 1)


if __name__ == "__main__":
    main()
