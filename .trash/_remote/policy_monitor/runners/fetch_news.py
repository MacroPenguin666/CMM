"""
Hourly news feed fetch.
Called by launchd every hour.
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
        logging.FileHandler(LOG_DIR / f"news_{datetime.now():%Y-%m-%d}.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("fetch_news")


def run():
    log.info("NEWS FEEDS — hourly fetch")
    try:
        from policy_monitor.sources.loader import get_direct_feeds, load_registry
        from policy_monitor.monitor import fetch_all_feeds
        from policy_monitor.storage import get_db, get_item_count, store_feed_result

        reg = load_registry()
        feeds = get_direct_feeds(reg)
        log.info(f"Fetching {len(feeds)} feeds...")
        results = fetch_all_feeds(feeds, timeout=30)

        db = get_db()
        for r in results:
            store_feed_result(db, r)

        ok = sum(1 for r in results if r["ok"])
        fail = len(results) - ok
        log.info(f"News: {ok} OK, {fail} failed, {get_item_count(db)} total items in DB")
        db.close()
    except Exception as e:
        log.error(f"News fetch error: {e}")


if __name__ == "__main__":
    run()
