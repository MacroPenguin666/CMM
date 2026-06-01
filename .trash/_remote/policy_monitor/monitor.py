"""
Fetch latest policy items from all configured feeds and store to local DB.

Results are stored in data/feeds.db (SQLite) and optionally printed.

Usage:
    python monitor.py                       # fetch direct feeds, store to DB, print summary
    python monitor.py --all                 # fetch direct + RSSHub feeds (needs self-hosted RSSHub)
    python monitor.py --category regulator  # filter by source category
    python monitor.py --json                # output results as JSON
    python monitor.py --show                # show latest stored items from DB
"""

import argparse
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import feedparser
import requests

from policy_monitor.sources.loader import get_all_feeds, get_direct_feeds, load_registry
from policy_monitor.storage import get_db, get_fetch_stats, get_item_count, get_recent_items, store_feed_result

# ---------------------------------------------------------------------------
# Logging — writes to data/logs/monitor_YYYY-MM-DD.log + stdout
# ---------------------------------------------------------------------------
LOG_DIR = Path(__file__).parent.parent / "data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

log_file = LOG_DIR / f"monitor_{datetime.now():%Y-%m-%d}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("monitor")


def fetch_feed(feed: dict, timeout: int = 15) -> dict:
    """Fetch and parse a single RSS/Atom feed."""
    try:
        resp = requests.get(
            feed["url"],
            timeout=timeout,
            headers={"User-Agent": "Mozilla/5.0 (compatible; ChinaPolicyMonitor/1.0)"},
        )
        resp.raise_for_status()
        parsed = feedparser.parse(resp.text)
        entries = []
        for e in parsed.entries[:10]:  # latest 10 per feed
            entries.append(
                {
                    "title": e.get("title", ""),
                    "link": e.get("link", ""),
                    "published": e.get("published", ""),
                    "summary": e.get("summary", "")[:500],
                }
            )
        return {
            "source": feed["name"],
            "source_cn": feed.get("name_cn", ""),
            "category": feed.get("category", ""),
            "feed_url": feed["url"],
            "description": feed.get("description", ""),
            "entries": entries,
            "ok": True,
        }
    except Exception as exc:
        return {
            "source": feed["name"],
            "source_cn": feed.get("name_cn", ""),
            "category": feed.get("category", ""),
            "feed_url": feed["url"],
            "entries": [],
            "ok": False,
            "error": str(exc),
        }


def fetch_all_feeds(
    feeds: list[dict], max_workers: int = 8, timeout: int = 15
) -> list[dict]:
    """Fetch all feeds concurrently."""
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(fetch_feed, f, timeout): f for f in feeds}
        for fut in as_completed(futures):
            results.append(fut.result())
    return results


def main():
    parser = argparse.ArgumentParser(description="Fetch China policy feeds")
    parser.add_argument(
        "--all", action="store_true", dest="fetch_all",
        help="Fetch all feeds including RSSHub (needs self-hosted instance)",
    )
    parser.add_argument("--category", help="Filter feeds by source category")
    parser.add_argument(
        "--json", action="store_true", dest="as_json", help="Output as JSON"
    )
    parser.add_argument(
        "--show", action="store_true", help="Show latest items from DB (no fetch)"
    )
    parser.add_argument(
        "--show-limit", type=int, default=30, help="Number of items to show"
    )
    parser.add_argument(
        "--timeout", type=int, default=15, help="Request timeout in seconds"
    )
    args = parser.parse_args()

    db = get_db()

    # --show: just display what's in the DB
    if args.show:
        items = get_recent_items(db, limit=args.show_limit)
        stats = get_fetch_stats(db)
        print(f"DB: {get_item_count(db)} items, {stats['total_fetches']} fetches\n")
        if args.as_json:
            print(json.dumps(items, ensure_ascii=False, indent=2))
        else:
            for item in items:
                print(f"[{item['category']}] {item['source']}")
                print(f"  {item['title']}")
                if item["link"]:
                    print(f"  {item['link']}")
                print()
        db.close()
        return

    # Fetch feeds
    reg = load_registry()
    feeds = get_all_feeds(reg) if args.fetch_all else get_direct_feeds(reg)

    if args.category:
        feeds = [f for f in feeds if f.get("category") == args.category]

    log.info(f"Fetching {len(feeds)} feeds...")
    results = fetch_all_feeds(feeds, timeout=args.timeout)

    # Store to DB
    total_new = 0
    for r in results:
        store_feed_result(db, r)
        total_new += len(r.get("entries", []))

    ok_count = sum(1 for r in results if r["ok"])
    fail_count = len(results) - ok_count

    # Output
    if args.as_json:
        print(json.dumps(results, ensure_ascii=False, indent=2))
    else:
        for r in sorted(results, key=lambda x: (x["ok"], x["source"]), reverse=True):
            status = "OK" if r["ok"] else "FAIL"
            log.info(f"[{status}] {r['source']} — {r.get('description', '')}")
            if not r["ok"]:
                log.warning(f"  Error: {r.get('error', 'unknown')}")
                continue
            for entry in r["entries"][:3]:
                log.info(f"  • {entry['title']}")

    log.info(
        f"Done: {ok_count} OK, {fail_count} failed, {total_new} items fetched, "
        f"{get_item_count(db)} total in DB"
    )
    db.close()


if __name__ == "__main__":
    main()
