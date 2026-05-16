"""
Ministry HTML scraper runner.

Fetches announcement pages from Chinese ministry sites directly (no RSSHub needed).
Stores results in two places:
  - data/ministries/{slug}.db  — dedicated per-ministry DB (full history)
  - data/feeds.db items table  — shared DB for dashboard news tab

First run (DB empty / < 50 articles): fetches all pages (up to max_pages_full).
Subsequent runs: fetches only recent pages (max_pages_incr) and stops when
                 articles already known are encountered.

Pass --full to force a complete re-fetch regardless of DB state.
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

LOG_DIR = Path(__file__).parent.parent.parent / "data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_DIR / f"ministries_{datetime.now():%Y-%m-%d}.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("fetch_ministries")

MAX_PAGES_FULL = 100   # initial bulk fetch
MAX_PAGES_INCR = 5     # incremental (stop early when known articles found)
FULL_THRESHOLD = 50    # DB articles below this → treat as first run


def run(force_full: bool = False):
    log.info("MINISTRY SCRAPER — paginated fetch")

    from policy_monitor.ministry_scraper import TARGETS, scrape_all
    from policy_monitor.storage import (
        get_db, get_item_count, store_feed_result,
        get_ministry_db, get_ministry_known_links,
        get_ministry_article_count, store_ministry_result,
    )

    # ------------------------------------------------------------------
    # Phase 1: determine fetch mode per ministry and collect known links
    # ------------------------------------------------------------------
    ministry_dbs = {}         # source_name → sqlite connection
    known_links_by_source = {}
    max_pages_by_source = {}

    for t in TARGETS:
        name = t["name"]
        mdb = get_ministry_db(name)
        ministry_dbs[name] = mdb
        count = get_ministry_article_count(mdb)
        if force_full or count < FULL_THRESHOLD:
            max_pages_by_source[name] = MAX_PAGES_FULL
            known_links_by_source[name] = None  # no stop — fetch everything
            log.info(f"  {name}: FULL fetch (currently {count} articles)")
        else:
            max_pages_by_source[name] = MAX_PAGES_INCR
            known_links_by_source[name] = get_ministry_known_links(mdb)
            log.info(f"  {name}: incremental fetch (currently {count} articles)")

    # Use the smallest max_pages across all as pool-wide setting, but pass
    # per-target known_links so each can stop independently.
    # We drive pagination per-target anyway via known_links_by_source.
    overall_max_pages = max(max_pages_by_source.values())

    log.info(f"Scraping {len(TARGETS)} ministries (max {overall_max_pages} pages each)...")

    # ------------------------------------------------------------------
    # Phase 2: paginated scrape
    # ------------------------------------------------------------------
    results = scrape_all(
        TARGETS,
        max_workers=4,       # keep concurrent connections low to be polite
        timeout=25,
        paginate=True,
        max_pages=overall_max_pages,
        known_links_by_source=known_links_by_source,
        page_delay=1.0,
    )

    # ------------------------------------------------------------------
    # Phase 3: store to per-ministry DB and to shared feeds.db
    # ------------------------------------------------------------------
    shared_db = get_db()
    total_new_ministry = 0
    total_new_shared = 0

    for r in results:
        name = r["source"]
        status = "OK" if r["ok"] else "FAIL"
        pages = r.get("pages_fetched", 1)
        n_entries = len(r.get("entries", []))

        msg = f"[{status}] {name} — {n_entries} new items across {pages} page(s)"
        if not r["ok"]:
            msg += f"  ({r.get('error', '')})"
        log.info(msg)

        if r["ok"] or n_entries:
            # Per-ministry DB — stores full history
            mdb = ministry_dbs.get(name)
            if mdb:
                new_min = store_ministry_result(mdb, r)
                total_new_ministry += new_min

            # Shared feeds.db — for dashboard news tab (deduplication via UNIQUE)
            store_feed_result(shared_db, r)
            total_new_shared += n_entries

    ok = sum(1 for r in results if r["ok"])
    fail = len(results) - ok
    log.info(
        f"Done: {ok} OK, {fail} failed | "
        f"{total_new_ministry} new in ministry DBs | "
        f"{get_item_count(shared_db)} total in feeds.db"
    )

    for mdb in ministry_dbs.values():
        mdb.close()
    shared_db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch Chinese ministry news and policies")
    parser.add_argument("--full", action="store_true",
                        help="Force full re-fetch of all pages regardless of DB state")
    args = parser.parse_args()
    run(force_full=args.full)
