"""
Trade & labour statistics runner.
Orchestrates: WITS/UNCTAD tariff, WTO timeseries, ILO labour, OECD STAN + FDI.

All sources write to data/trade_stats.db.

Sources that work without any API key:
  - WITS World Bank tariff fallback (WDI aggregate tariff indicators)
  - ILO World Bank employment fallback (WDI employment indicators)
  - OECD STAN08BIS (production & value added by industry)
  - OECD FDI_FLOW_PARTNER (FDI flows by partner)

Sources that require keys (gracefully skipped if not set):
  - WITS HS-level tariff (WITS_API_KEY — free at wits.worldbank.org)
  - WTO timeseries + disputes (WTO_API_KEY — free at api.wto.org)
  - ILO SDMX dataflows (no key, but API has intermittent availability)
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
        logging.FileHandler(LOG_DIR / f"trade_stats_{datetime.now():%Y-%m-%d}.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("fetch_trade_stats")


def run(
    sources: list[str] | None = None,
    force_full: bool = False,
):
    """
    Run trade stats fetchers. sources: subset of ['wits','wto','ilo','oecd'].
    Default (None) runs all four.
    """
    from policy_monitor.wits import get_trade_stats_db, fetch_all as wits_fetch
    from policy_monitor.wto import fetch_all as wto_fetch
    from policy_monitor.ilo import fetch_all as ilo_fetch
    from policy_monitor.oecd_tiva import fetch_all as oecd_fetch

    run_all = sources is None
    enabled = set(sources or ["wits", "wto", "ilo", "oecd"])

    conn = get_trade_stats_db()
    totals: dict[str, int] = {}

    if "wits" in enabled:
        log.info("── WITS / UNCTAD tariff ─────────────────────────────")
        n = wits_fetch(conn, force_full=force_full)
        totals["wits"] = n
        log.info(f"   WITS total: {n:,} rows")

    if "wto" in enabled:
        log.info("── WTO Data Portal ──────────────────────────────────")
        n = wto_fetch(conn, force_full=force_full)
        totals["wto"] = n
        log.info(f"   WTO total: {n:,} rows")

    if "ilo" in enabled:
        log.info("── ILO STAT labour data ─────────────────────────────")
        n = ilo_fetch(conn, force_full=force_full)
        totals["ilo"] = n
        log.info(f"   ILO total: {n:,} rows")

    if "oecd" in enabled:
        log.info("── OECD STAN + FDI ──────────────────────────────────")
        n = oecd_fetch(conn, force_full=force_full)
        totals["oecd"] = n
        log.info(f"   OECD total: {n:,} rows")

    conn.close()

    grand_total = sum(totals.values())
    log.info(f"Done — {grand_total:,} total rows stored in data/trade_stats.db")
    for src, n in totals.items():
        log.info(f"  {src:6s}: {n:,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch trade & labour statistics")
    parser.add_argument("--sources", nargs="+", choices=["wits", "wto", "ilo", "oecd"],
                        help="Which sources to fetch (default: all)")
    parser.add_argument("--full", action="store_true", help="Force full re-fetch")
    args = parser.parse_args()
    run(sources=args.sources, force_full=args.full)
