"""
Ministry HTML scraper runner.
Fetches announcement pages from Chinese ministry sites directly (no RSSHub needed).
"""

import logging
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


def run():
    log.info("MINISTRY SCRAPER — direct HTML fetch")
    try:
        from policy_monitor.ministry_scraper import TARGETS, scrape_all
        from policy_monitor.storage import get_db, get_item_count, store_feed_result

        log.info(f"Scraping {len(TARGETS)} ministry pages...")
        results = scrape_all(TARGETS, timeout=20)

        db = get_db()
        total_new = 0
        for r in results:
            store_feed_result(db, r)
            status = "OK" if r["ok"] else "FAIL"
            count = len(r.get("entries", []))
            msg = f"[{status}] {r['source']} — {count} items"
            if not r["ok"]:
                msg += f" ({r.get('error', '')})"
            log.info(msg)
            total_new += count

        ok = sum(1 for r in results if r["ok"])
        fail = len(results) - ok
        log.info(f"Done: {ok} OK, {fail} failed, {total_new} items fetched, "
                 f"{get_item_count(db)} total in DB")
        db.close()
    except Exception as e:
        log.error(f"Ministry scrape error: {e}", exc_info=True)


if __name__ == "__main__":
    run()
