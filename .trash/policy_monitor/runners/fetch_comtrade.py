"""
UN Comtrade HS4 + bilateral trade data runner.

First run  : last HISTORY_YEARS of annual data for all ~200 reporters
Subsequent : most-recent completed year only

Flags:
  --full       Force complete re-fetch regardless of DB state
  --bilateral  Also fetch reporter × partner totals (larger; run separately)
  --freq M     Monthly data instead of annual (sparser country coverage)
"""

import argparse
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
        logging.FileHandler(LOG_DIR / f"comtrade_{datetime.now():%Y-%m-%d}.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("fetch_comtrade")


def run(bilateral: bool = False, freq: str = "A", force_full: bool = False):
    from policy_monitor.comtrade import (
        get_comtrade_db, load_reporters,
        HISTORY_YEARS, MAX_YEAR,
        stored_years, fetch_hs4, fetch_bilateral,
    )
    from policy_monitor.config import COMTRADE_API_KEY

    if not COMTRADE_API_KEY:
        log.error("COMTRADE_API_KEY not set — add it to .env (comtradedeveloper.un.org)")
        return

    conn = get_comtrade_db()

    log.info("Loading Comtrade reporter list …")
    reporters = load_reporters()
    if not reporters:
        log.error("No reporters loaded — check API key or network")
        conn.close()
        return
    log.info(f"  {len(reporters)} reporters")

    # ── HS4 years ────────────────────────────────────────────────────────────
    already_hs4 = stored_years(conn, "comtrade_hs4")
    target_years = [MAX_YEAR - i for i in range(HISTORY_YEARS)]

    if force_full or not already_hs4:
        years_hs4 = target_years
        log.info(f"HS4: FULL fetch — {years_hs4}")
    else:
        years_hs4 = [MAX_YEAR]
        log.info(f"HS4: incremental — refreshing {MAX_YEAR} (already have {sorted(already_hs4)})")

    # ── HS4 fetch ────────────────────────────────────────────────────────────
    est_calls = len(years_hs4) * (len(reporters) // 10 + 1)
    log.info(
        f"Fetching HS4 data: {len(reporters)} reporters × {len(years_hs4)} year(s) "
        f"freq={freq}  (~{est_calls} API calls)"
    )
    n_hs4 = fetch_hs4(conn, reporters, years_hs4, freq=freq)
    log.info(f"HS4 complete: {n_hs4:,} rows stored → data/comtrade.db")

    # ── Bilateral (optional) ─────────────────────────────────────────────────
    if bilateral:
        already_bil = stored_years(conn, "comtrade_bilateral")
        if force_full or not already_bil:
            years_bil = target_years
            log.info(f"Bilateral: FULL fetch — {years_bil}")
        else:
            years_bil = [MAX_YEAR]
            log.info(f"Bilateral: incremental — refreshing {MAX_YEAR}")

        est_bil = len(years_bil) * (len(reporters) // 20 + 1)
        log.info(
            f"Fetching bilateral data: {len(reporters)} reporters × {len(years_bil)} year(s) "
            f"  (~{est_bil} API calls)"
        )
        n_bil = fetch_bilateral(conn, reporters, years_bil, freq=freq)
        log.info(f"Bilateral complete: {n_bil:,} rows stored")

    conn.close()
    log.info("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch UN Comtrade HS4 trade data")
    parser.add_argument("--bilateral", action="store_true",
                        help="Also fetch reporter × partner totals")
    parser.add_argument("--freq", default="A", choices=["A", "M"],
                        help="A=annual (default), M=monthly")
    parser.add_argument("--full", action="store_true",
                        help="Force full re-fetch of all years")
    args = parser.parse_args()
    run(bilateral=args.bilateral, freq=args.freq, force_full=args.full)
