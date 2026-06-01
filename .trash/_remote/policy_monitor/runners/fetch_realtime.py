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


def run():
    """Main loop — polls flights and ships continuously."""
    has_creds = _has_opensky_credentials()
    flight_interval = 60 if has_creds else 900  # 1min vs 15min
    log.info(f"Starting real-time fetcher "
             f"(flights every {flight_interval}s, ships every 60s)")

    last_flight_fetch = 0

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
