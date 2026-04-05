"""
Unified scheduled fetch — runs news feeds + financial data in one go.
Called by the launchd scheduler twice daily.
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
        logging.FileHandler(LOG_DIR / f"fetch_{datetime.now():%Y-%m-%d}.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("fetch_all")


def run():
    # --- News feeds ---
    log.info("=" * 60)
    log.info("NEWS FEEDS")
    log.info("=" * 60)
    try:
        from policy_monitor.sources.loader import get_direct_feeds, load_registry
        from policy_monitor.monitor import fetch_all_feeds
        from policy_monitor.storage import get_db, get_item_count, store_feed_result

        reg = load_registry()
        feeds = get_direct_feeds(reg)
        log.info(f"Fetching {len(feeds)} news feeds...")
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

    # --- Financial data ---
    log.info("")
    log.info("=" * 60)
    log.info("FINANCIAL DATA")
    log.info("=" * 60)
    try:
        from policy_monitor.financial import fetch_all_financial, get_financial_db, store_financial_data

        conn = get_financial_db()
        all_rows, all_snapshots, ok, fail = fetch_all_financial()
        inserted = store_financial_data(conn, all_rows, all_snapshots)

        total = conn.execute("SELECT COUNT(*) FROM financial_series").fetchone()[0]
        log.info(f"Financial: {ok} OK, {fail} failed, {inserted} new points, {total} total in DB")
        conn.close()
    except Exception as e:
        log.error(f"Financial fetch error: {e}")

    # --- Dissent events ---
    log.info("")
    log.info("=" * 60)
    log.info("DISSENT EVENTS (China Dissent Monitor)")
    log.info("=" * 60)
    try:
        from policy_monitor.dissent import (
            _get_session,
            fetch_events,
            fetch_provinces,
            get_dissent_db,
            store_events,
            store_provinces,
        )

        conn = get_dissent_db()
        session = _get_session()

        provinces = fetch_provinces(session)
        store_provinces(conn, provinces)

        events = fetch_events(session, max_pages=35)
        inserted = store_events(conn, events)
        total = conn.execute("SELECT COUNT(*) FROM dissent_events").fetchone()[0]
        log.info(f"Dissent: {len(events)} fetched, {inserted} new, {total} total in DB")
        conn.close()
    except Exception as e:
        log.error(f"Dissent fetch error: {e}")

    # --- Bruegel China Economic Database ---
    log.info("")
    log.info("=" * 60)
    log.info("BRUEGEL CHINA ECONOMIC DATABASE")
    log.info("=" * 60)
    try:
        from policy_monitor.bruegel import fetch_all_bruegel, get_bruegel_db, store_bruegel_data, store_provincial_data

        conn = get_bruegel_db()
        all_rows, all_snapshots, all_provincial, ok, fail = fetch_all_bruegel(conn)
        inserted = store_bruegel_data(conn, all_rows, all_snapshots)
        prov_inserted = store_provincial_data(conn, all_provincial) if all_provincial else 0

        total = conn.execute("SELECT COUNT(*) FROM bruegel_series").fetchone()[0]
        prov_total = conn.execute("SELECT COUNT(*) FROM bruegel_provincial").fetchone()[0]
        log.info(f"Bruegel: {ok} OK, {fail} failed, {inserted} new series, {prov_inserted} new provincial, {total}+{prov_total} total in DB")
        conn.close()
    except Exception as e:
        log.error(f"Bruegel fetch error: {e}")

    # --- Global Macro Database ---
    log.info("")
    log.info("=" * 60)
    log.info("GLOBAL MACRO DATABASE")
    log.info("=" * 60)
    try:
        from policy_monitor.macro import fetch_china_macro

        result = fetch_china_macro()
        log.info(f"Macro: {result['status']} (version {result['version']})")
    except Exception as e:
        log.error(f"Macro fetch error: {e}")

    # --- Academic publications ---
    log.info("")
    log.info("=" * 60)
    log.info("ACADEMIC PUBLICATIONS")
    log.info("=" * 60)
    try:
        from policy_monitor.academic import fetch_academic, get_academic_db, store_articles

        conn = get_academic_db()
        articles, ok, fail = fetch_academic()
        inserted = store_articles(conn, articles)
        total = conn.execute("SELECT COUNT(*) FROM academic_articles").fetchone()[0]
        log.info(f"Academic: {ok} OK, {fail} failed, {len(articles)} China-relevant, "
                 f"{inserted} new, {total} total in DB")
        conn.close()
    except Exception as e:
        log.error(f"Academic fetch error: {e}")

    log.info("")
    log.info("Fetch complete.")


if __name__ == "__main__":
    run()
