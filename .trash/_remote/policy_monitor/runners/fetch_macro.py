"""
Daily macro data fetch with randomised timing.

Called by launchd once per day (e.g. at 03:00 local).
On startup, sleeps for a random 0–18 h offset so the actual fetch
lands at a different wall-clock time each day.  This covers:
  - Financial data (AKShare / SHIBOR)
  - Dissent events (China Dissent Monitor)
  - Bruegel China Economic Database
  - Global Macro Database
"""

import logging
import random
import time
from datetime import datetime
from pathlib import Path

LOG_DIR = Path(__file__).parent.parent.parent / "data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_DIR / f"macro_{datetime.now():%Y-%m-%d}.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("fetch_macro")

MAX_RANDOM_DELAY = 18 * 3600  # up to 18 hours


def run():
    delay = random.randint(0, MAX_RANDOM_DELAY)
    log.info(f"Macro fetch scheduled — sleeping {delay}s ({delay/3600:.1f}h) before starting")
    time.sleep(delay)
    log.info("Macro fetch starting now")

    # --- Financial data ---
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
        from policy_monitor.bruegel import fetch_all_bruegel, get_bruegel_db, store_bruegel_data

        conn = get_bruegel_db()
        all_rows, all_snapshots, ok, fail = fetch_all_bruegel(conn)
        inserted = store_bruegel_data(conn, all_rows, all_snapshots)

        total = conn.execute("SELECT COUNT(*) FROM bruegel_series").fetchone()[0]
        log.info(f"Bruegel: {ok} OK, {fail} failed, {inserted} new points, {total} total in DB")
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

    # --- BIS ---
    log.info("")
    log.info("=" * 60)
    log.info("BIS DATA")
    log.info("=" * 60)
    try:
        from policy_monitor.bis import fetch_all_bis, get_bis_db

        conn = get_bis_db()
        ok, fail = fetch_all_bis(conn)
        log.info(f"BIS: {ok} datasets OK, {fail} failed")
        conn.close()
    except Exception as e:
        log.error(f"BIS fetch error: {e}")

    # --- ECB ---
    log.info("")
    log.info("=" * 60)
    log.info("ECB DATA")
    log.info("=" * 60)
    try:
        from policy_monitor.ecb import fetch_all_ecb, get_ecb_db

        conn = get_ecb_db()
        ok, fail = fetch_all_ecb(conn)
        log.info(f"ECB: {ok} datasets OK, {fail} failed")
        conn.close()
    except Exception as e:
        log.error(f"ECB fetch error: {e}")

    # --- Destatis ---
    log.info("")
    log.info("=" * 60)
    log.info("DESTATIS DATA")
    log.info("=" * 60)
    try:
        from policy_monitor.destatis import fetch_all_destatis, get_destatis_db

        conn = get_destatis_db()
        ok, fail = fetch_all_destatis(conn)
        log.info(f"Destatis: {ok} datasets OK, {fail} failed")
        conn.close()
    except Exception as e:
        log.error(f"Destatis fetch error: {e}")

    # --- IMF + World Bank Global Macro ---
    log.info("")
    log.info("=" * 60)
    log.info("IMF + WORLD BANK GLOBAL MACRO")
    log.info("=" * 60)
    try:
        from policy_monitor.global_macro import fetch_all_global_macro, get_global_macro_db

        conn = get_global_macro_db()
        ok, fail = fetch_all_global_macro(conn)
        log.info(f"Global macro: {ok} indicators OK, {fail} failed")
        conn.close()
    except Exception as e:
        log.error(f"Global macro fetch error: {e}")

    # --- IMF Fiscal Monitor ---
    log.info("")
    log.info("=" * 60)
    log.info("IMF FISCAL MONITOR")
    log.info("=" * 60)
    try:
        from policy_monitor.imf_fiscal import fetch_all_imf_fiscal, get_imf_fiscal_db

        conn = get_imf_fiscal_db()
        ok, fail = fetch_all_imf_fiscal(conn)
        log.info(f"IMF Fiscal: {ok} indicators OK, {fail} failed")
        conn.close()
    except Exception as e:
        log.error(f"IMF Fiscal fetch error: {e}")

    log.info("")
    log.info("Macro fetch complete.")


if __name__ == "__main__":
    run()
