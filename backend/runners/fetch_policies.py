"""
Policy pipeline runner: ministry list discovery → full-text content swarm
→ policy_docs table in data/cmm.db.

Stage 1 (discovery): paginated scrape of ministry announcement list pages
    (backend.fetchers.ministry_scraper.TARGETS). New URLs are inserted into
    policy_docs with fetch_status='pending'. Incremental: stops paginating a
    source once a whole page of already-known URLs is seen.

Stage 2 (content): fetches every pending URL — parallel across ministry
    domains, serial + delayed within a domain — extracts the main document
    text and 文号, and stores them on the row.

Usage:
    cmm-fetch policies                      # discovery + content
    cmm-fetch policies --discover-only
    cmm-fetch policies --content-only --limit 500
    cmm-fetch policies --ministry ndrc
    cmm-fetch policies --full               # ignore known URLs, re-crawl all pages
    cmm-fetch policies --retry-errors       # also re-fetch rows that errored
"""

import argparse
import logging
import sys
from datetime import datetime

from backend.storage import LOG_DIR

LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_DIR / f"policies_{datetime.now():%Y-%m-%d}.log",
                            encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("fetch_policies")

MAX_PAGES_FULL = 100
MAX_PAGES_INCR = 5
FULL_THRESHOLD = 50   # fewer docs than this for a source → treat as first run


def discover(conn, ministry: str | None = None, force_full: bool = False) -> int:
    """Stage 1: scrape list pages, insert new metadata rows. Returns new-row count."""
    from backend.fetchers.ministry_scraper import SEED_DOCS, TARGETS, scrape_all
    from backend.storage import (domain_slug, get_policy_doc_count,
                                 get_policy_known_links, insert_policy_metadata)

    seeded = 0
    for doc in SEED_DOCS:
        if ministry and domain_slug(doc["feed_url"]) != ministry:
            continue
        seeded += insert_policy_metadata(conn, doc)
    if seeded:
        log.info(f"Seeded {seeded} landmark document(s) as pending")

    targets = [t for t in TARGETS
               if not ministry or domain_slug(t["url"]) == ministry]
    if not targets:
        log.error(f"No targets match ministry slug '{ministry}'")
        return seeded

    known_links_by_source = {}
    max_pages_by_source = {}
    for t in targets:
        name = t["name"]
        count = get_policy_doc_count(conn, source=name)
        if force_full or count < FULL_THRESHOLD:
            max_pages_by_source[name] = MAX_PAGES_FULL
            known_links_by_source[name] = None
        else:
            max_pages_by_source[name] = MAX_PAGES_INCR
            known_links_by_source[name] = get_policy_known_links(conn, source=name)

    log.info(f"DISCOVERY — {len(targets)} sources "
             f"({sum(1 for v in known_links_by_source.values() if v is None)} full, "
             f"{sum(1 for v in known_links_by_source.values() if v is not None)} incremental)")

    results = scrape_all(
        targets,
        max_workers=4,
        timeout=25,
        paginate=True,
        max_pages=MAX_PAGES_FULL,
        max_pages_by_source=max_pages_by_source,
        known_links_by_source=known_links_by_source,
        page_delay=1.0,
    )

    total_new = seeded
    for r in results:
        status = "OK" if r["ok"] else "FAIL"
        new_n = insert_policy_metadata(conn, r)
        total_new += new_n
        msg = (f"[{status}] {r['source']} — {new_n} new "
               f"({r.get('pages_fetched', 1)} pages)")
        if not r["ok"]:
            msg += f"  ({r.get('error', '')[:120]})"
        log.info(msg)

    log.info(f"Discovery done: {total_new} new documents queued")
    return total_new


def fetch_content(conn, limit: int | None = None, ministry: str | None = None,
                  retry_errors: bool = False, delay: float = 1.0) -> dict:
    """Stage 2: fetch full text for pending rows."""
    from backend.fetchers.policy_content import fetch_contents
    from backend.storage import get_pending_policy_docs, update_policy_content

    rows = get_pending_policy_docs(conn, limit=limit, ministry=ministry,
                                   retry_errors=retry_errors)
    if not rows:
        log.info("CONTENT — nothing pending")
        return {"done": 0, "by_status": {}}
    log.info(f"CONTENT — {len(rows)} documents to fetch")

    def on_result(row, result):
        update_policy_content(
            conn, row["id"],
            status=result["status"],
            full_text=result["text"],
            doc_number=result["doc_number"],
            published=result["published"],
            http_status=result["http_status"],
            error=result["error"],
        )
        conn.commit()

    stats = fetch_contents(rows, on_result, max_domains=6,
                           per_request_delay=delay, timeout=25)
    log.info(f"Content done: {stats['done']} fetched — {stats['by_status']}")
    return stats


def report(conn) -> None:
    from backend.storage import get_policy_fetch_stats
    log.info("policy_docs status by ministry:")
    totals: dict[str, int] = {}
    for row in get_policy_fetch_stats(conn):
        log.info(f"  {row['ministry']:<22s} {row['fetch_status']:<8s} "
                 f"{row['n']:>6} docs  {row['chars']:>12,} chars")
        totals[row["fetch_status"]] = totals.get(row["fetch_status"], 0) + row["n"]
    log.info(f"TOTAL: {sum(totals.values())} docs — {totals}")


def run(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(description="Fetch ministry policy documents (full text)")
    parser.add_argument("--discover-only", action="store_true")
    parser.add_argument("--content-only", action="store_true")
    parser.add_argument("--limit", type=int, default=None,
                        help="max documents to content-fetch this run")
    parser.add_argument("--ministry", type=str, default=None,
                        help="restrict to one ministry slug (e.g. ndrc, mofcom)")
    parser.add_argument("--full", action="store_true",
                        help="discovery: re-crawl all pages regardless of DB state")
    parser.add_argument("--retry-errors", action="store_true",
                        help="content: also retry rows whose last fetch errored")
    parser.add_argument("--delay", type=float, default=1.0,
                        help="content: seconds between requests to the same domain")
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])

    from backend.storage import get_policy_docs_db
    conn = get_policy_docs_db()

    log.info("POLICY PIPELINE start")
    if not args.content_only:
        discover(conn, ministry=args.ministry, force_full=args.full)
    if not args.discover_only:
        fetch_content(conn, limit=args.limit, ministry=args.ministry,
                      retry_errors=args.retry_errors, delay=args.delay)
    report(conn)
    conn.close()


if __name__ == "__main__":
    run()
