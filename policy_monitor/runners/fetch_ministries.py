"""
Ministry HTML scraper runner.

Fetches announcement pages from Chinese ministry sites directly (no RSSHub needed).
Stores results in two places:
  - data/ministries.db (per-ministry table, e.g. ndrc, mofcom) — full history
  - data/feeds.db items table  — shared DB for dashboard news tab

First run (table empty / < 50 articles): fetches all pages (up to max_pages_full).
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
        get_ministries_db, ensure_ministry_table,
        get_ministry_known_links_t, get_ministry_article_count_by_source,
        store_ministry_result_t,
    )

    # ------------------------------------------------------------------
    # Phase 1: determine fetch mode per ministry and collect known links
    # ------------------------------------------------------------------
    ministry_conn = get_ministries_db()
    ministry_tables = {}      # source_name → table slug
    known_links_by_source = {}
    max_pages_by_source = {}

    for t in TARGETS:
        name = t["name"]
        table = ensure_ministry_table(ministry_conn, name)
        ministry_tables[name] = table
        count = get_ministry_article_count_by_source(ministry_conn, table, name)
        if force_full or count < FULL_THRESHOLD:
            max_pages_by_source[name] = MAX_PAGES_FULL
            known_links_by_source[name] = None  # no stop — fetch everything
            log.info(f"  {name}: FULL fetch (currently {count} articles)")
        else:
            max_pages_by_source[name] = MAX_PAGES_INCR
            known_links_by_source[name] = get_ministry_known_links_t(ministry_conn, table)
            log.info(f"  {name}: incremental fetch (currently {count} articles)")

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
        max_pages=MAX_PAGES_FULL,          # fallback; real limit comes from dict
        max_pages_by_source=max_pages_by_source,
        known_links_by_source=known_links_by_source,
        page_delay=1.0,
    )

    # ------------------------------------------------------------------
    # Phase 3: store exclusively to ministries.db (per-ministry tables)
    # ------------------------------------------------------------------
    total_new = 0

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
            table = ministry_tables.get(name)
            if table:
                new_n = store_ministry_result_t(ministry_conn, table, r)
                total_new += new_n

    ok = sum(1 for r in results if r["ok"])
    fail = len(results) - ok
    log.info(f"Done: {ok} OK, {fail} failed | {total_new} new rows in ministries.db")

    ministry_conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch Chinese ministry news and policies")
    parser.add_argument("--full", action="store_true",
                        help="Force full re-fetch of all pages regardless of DB state")
    args = parser.parse_args()
    run(force_full=args.full)
