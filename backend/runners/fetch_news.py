"""
Hourly news feed fetch.
Called by launchd every hour.
"""

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
        logging.FileHandler(LOG_DIR / f"news_{datetime.now():%Y-%m-%d}.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("fetch_news")


def run():
    log.warning("fetch_news.py is deprecated — news is now handled by fetch_realtime.py. "
                "This script still works for one-off manual runs.")
    log.info("NEWS FEEDS — hourly fetch")
    try:
        from backend.sources.loader import get_direct_feeds, load_registry
        from backend.fetchers.monitor import fetch_all_feeds
        from backend.storage import get_db, get_item_count, store_feed_result

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
