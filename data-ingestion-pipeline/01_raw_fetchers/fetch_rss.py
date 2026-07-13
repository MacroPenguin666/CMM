"""
Raw RSS/news fetcher — every entry from every configured feed, as it comes.

Polls all direct feeds in the source registry and stores **all** entries each poll
(not the latest 10), append-only to ``02_inputs/rss/items``. Re-runs accrete new
parts; deduplication to a "latest seen" set is a read-time concern. Designed to be
called on a short interval by ``_run.py --realtime``.

Counterpart to ``backend/fetchers/monitor.py`` (latest 10/feed into cmm.db). Reuses
the registry loader; the entry parse is reimplemented to drop the 10-item cap.
"""

from __future__ import annotations

import argparse
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import feedparser
import pandas as pd
import requests

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_HERE.parents[1]))

import _raw_store as store  # noqa: E402
from backend.sources.loader import get_direct_feeds, load_registry  # noqa: E402

log = logging.getLogger("raw_rss")
SOURCE = "rss"
UA = {"User-Agent": "Mozilla/5.0 (compatible; ChinaPolicyMonitor/1.0)"}


def _poll(feed: dict, timeout: int = 20) -> list[dict]:
    try:
        resp = requests.get(feed["url"], timeout=timeout, headers=UA)
        resp.raise_for_status()
        parsed = feedparser.parse(resp.text)
    except Exception as e:
        log.warning("  %-28s FAIL: %s", feed.get("name", "?"), str(e)[:80])
        return []
    rows = []
    for e in parsed.entries:                       # ALL entries, no cap
        rows.append({
            "source": feed["name"],
            "source_cn": feed.get("name_cn", ""),
            "category": feed.get("category", ""),
            "feed_url": feed["url"],
            "title": e.get("title", ""),
            "link": e.get("link", ""),
            "published": e.get("published", ""),
            "summary": e.get("summary", ""),
            "guid": e.get("id", e.get("link", "")),
        })
    log.info("  %-28s %4d entries", feed.get("name", "?"), len(rows))
    return rows


def run(run_id: str | None = None) -> None:
    run_id = run_id or store.new_run_id()
    datasets: dict[str, int] = {}
    try:
        feeds = get_direct_feeds(load_registry())
        rows: list[dict] = []
        with ThreadPoolExecutor(max_workers=8) as pool:
            futs = {pool.submit(_poll, f): f for f in feeds}
            for fut in as_completed(futs):
                rows.extend(fut.result())
        if rows:
            store.append(SOURCE, "items", pd.DataFrame(rows), run_id=run_id)
            datasets["items"] = len(rows)
        store.write_manifest(SOURCE, status="ok", datasets=datasets, run_id=run_id)
    except Exception as e:
        store.write_manifest(SOURCE, status="error", datasets=datasets,
                             run_id=run_id, error=str(e))
        raise


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s",
                        datefmt="%H:%M:%S")
    argparse.ArgumentParser(description="Raw RSS fetcher").parse_args()
    run()


if __name__ == "__main__":
    main()
