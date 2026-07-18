"""
Fiscal-capacity data runner — MOF releases, curves, monetary context,
LGB registry, final accounts, curated reference seed.

Usage:
    python -m backend.runners.fetch_fiscal            # incremental pass
    python -m backend.runners.fetch_fiscal --full     # refetch all releases
    python -m backend.runners.fetch_fiscal --registry-backfill  # walk all pages
"""

import argparse
import logging
import socket
from datetime import datetime

from backend.storage import LOG_DIR

LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_DIR / f"fiscal_{datetime.now():%Y-%m-%d}.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("fetch_fiscal")

# AKShare issues HTTP calls without timeouts; don't let one hang the runner
socket.setdefaulttimeout(60)


REGISTRY_COMPLETE_THRESHOLD = 17000  # ~18.1k LGBs listed on ChinaMoney (2026-07)


def _registry_complete(conn) -> bool:
    n = conn.execute("SELECT COUNT(*) FROM fiscal_lgb_bonds").fetchone()[0]
    return n >= REGISTRY_COMPLETE_THRESHOLD


def run(full: bool = False, registry_backfill: bool = False):
    from backend.fetchers.fiscal_china import (
        fetch_all_mof, fetch_curves, fetch_final_accounts, fetch_lgb_registry,
        fetch_monetary, get_fiscal_db, rebuild_maturity, seed_fiscal_reference,
    )

    conn = get_fiscal_db()
    steps = [
        ("reference seed", lambda: seed_fiscal_reference(conn)),
        ("MOF releases", lambda: fetch_all_mof(full=full)),
        ("final accounts", lambda: fetch_final_accounts(conn)),
        ("monetary", lambda: fetch_monetary(conn)),
        ("curves", lambda: fetch_curves(conn)),
        # incremental mode early-exits on known pages — correct once complete,
        # but while the backfill is partial the NEWEST pages are the known ones,
        # so keep walking the full list until coverage is essentially done
        ("LGB registry", lambda: fetch_lgb_registry(
            conn, incremental=not registry_backfill and _registry_complete(conn))),
        ("maturity rebuild", lambda: rebuild_maturity(conn)),
    ]
    ok = fail = 0
    for name, step in steps:
        try:
            result = step()
            log.info(f"{name}: {result}")
            ok += 1
        except Exception as e:
            log.error(f"{name} failed: {type(e).__name__}: {e}")
            fail += 1
    conn.close()
    log.info(f"Fiscal run complete: {ok} steps OK, {fail} failed")
    return ok, fail


def main():
    parser = argparse.ArgumentParser(description="CMM fiscal-capacity fetcher")
    parser.add_argument("--full", action="store_true", help="refetch all MOF releases")
    parser.add_argument("--registry-backfill", action="store_true",
                        help="walk the full ChinaMoney registry (not just newest pages)")
    args = parser.parse_args()
    run(full=args.full, registry_backfill=args.registry_backfill)


if __name__ == "__main__":
    main()
