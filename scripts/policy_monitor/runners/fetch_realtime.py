"""
Continuous real-time fetcher for ship and flight position data.

Runs as a long-lived background process, separate from the twice-daily
fetch_all.py.  Polls OpenSky (flights) and AISStream (ships) on a loop.

Usage:
    python fetch_realtime.py
"""

import logging
import threading
import time
from datetime import datetime
from pathlib import Path

LOG_DIR = Path(__file__).parent.parent.parent / "data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(name)s]  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(
            LOG_DIR / f"realtime_{datetime.now():%Y-%m-%d}.log", encoding="utf-8"
        ),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("realtime")


def _has_opensky_credentials() -> bool:
    """Check if OpenSky credentials are configured (for rate limit decisions)."""
    from policy_monitor.flights import _load_credentials
    u, p = _load_credentials()
    return bool(u and p)


def _fetch_flights():
    """Fetch and store flight positions."""
    try:
        from policy_monitor.flights import fetch_flight_positions, get_flights_db, store_flight_positions
        conn = get_flights_db()
        positions = fetch_flight_positions()
        n = store_flight_positions(conn, positions)
        conn.close()
        log.info(f"Flights: {n} positions stored")
    except Exception as e:
        log.error(f"Flights error: {e}")


def _fetch_ships():
    """Collect AIS stream and store ship positions."""
    try:
        from policy_monitor.ships import (
            _load_api_key,
            cleanup_stale,
            get_ships_db,
            run_ais_stream,
            store_ship_positions,
        )
        api_key = _load_api_key()
        if not api_key:
            log.debug("Ships: no API key configured, skipping")
            return

        positions = run_ais_stream(api_key, duration_seconds=55)
        conn = get_ships_db()
        n = store_ship_positions(conn, positions)
        cleanup_stale(conn)
        conn.close()
        log.info(f"Ships: {n} positions stored")
    except Exception as e:
        log.error(f"Ships error: {e}")


def _fetch_news():
    """Fetch RSS news feeds."""
    try:
        from policy_monitor.sources.loader import get_direct_feeds, load_registry
        from policy_monitor.monitor import fetch_all_feeds
        from policy_monitor.storage import get_db, get_item_count, store_feed_result

        reg = load_registry()
        feeds = get_direct_feeds(reg)
        log.info(f"News: fetching {len(feeds)} feeds...")
        results = fetch_all_feeds(feeds, timeout=30)

        db = get_db()
        for r in results:
            store_feed_result(db, r)

        ok = sum(1 for r in results if r["ok"])
        fail = len(results) - ok
        log.info(f"News: {ok} OK, {fail} failed, {get_item_count(db)} total items in DB")
        db.close()
    except Exception as e:
        log.error(f"News error: {e}")


NEWS_INTERVAL = 14400  # every 4 hours


def run():
    """Main loop — polls flights, ships, and news continuously."""
    has_creds = _has_opensky_credentials()
    flight_interval = 60 if has_creds else 900  # 1min vs 15min
    log.info(f"Starting real-time fetcher "
             f"(flights every {flight_interval}s, ships every 60s, "
             f"news every {NEWS_INTERVAL // 3600}h)")

    last_flight_fetch = 0
    last_news_fetch = 0

    while True:
        now = time.time()

        threads = []

        # Flights: only fetch if enough time has passed
        if now - last_flight_fetch >= flight_interval:
            t = threading.Thread(target=_fetch_flights, daemon=True)
            t.start()
            threads.append(t)
            last_flight_fetch = now

        # Ships: always try (runs for ~55s internally)
        t = threading.Thread(target=_fetch_ships, daemon=True)
        t.start()
        threads.append(t)

        # News: fetch hourly
        if now - last_news_fetch >= NEWS_INTERVAL:
            t = threading.Thread(target=_fetch_news, daemon=True)
            t.start()
            threads.append(t)
            last_news_fetch = now

        # Wait for all threads to finish
        for t in threads:
            t.join(timeout=120)

        # Sleep briefly before next cycle
        time.sleep(5)


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        log.info("Shutting down real-time fetcher.")
