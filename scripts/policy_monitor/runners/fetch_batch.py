"""
Batch data ingestion pipeline.

Fetches all non-realtime data sources and writes to feeds.db.
Realtime sources (flights, ships, RSS news) are handled by fetch_realtime.py.

Usage:
    python -m policy_monitor.runners.fetch_batch
    python -m policy_monitor.runners.fetch_batch --sources financial,macro
    python -m policy_monitor.runners.fetch_batch --random-delay 64800
"""

import argparse
import json
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
        logging.FileHandler(LOG_DIR / f"batch_{datetime.now():%Y-%m-%d}.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("fetch_batch")

ALL_SOURCES = [
    "financial", "dissent", "bruegel", "macro", "academic", "customs", "regulations",
    "bis", "ecb", "destatis", "global_macro", "imf_fiscal",
    "eurostat", "ministries", "trade_stats", "yfinance", "comtrade",
]


def _fetch_financial():
    from policy_monitor.financial import fetch_all_financial, get_financial_db, store_financial_data

    conn = get_financial_db()
    all_rows, all_snapshots, ok, fail = fetch_all_financial()
    inserted = store_financial_data(conn, all_rows, all_snapshots)
    total = conn.execute("SELECT COUNT(*) FROM financial_series").fetchone()[0]
    log.info(f"Financial: {ok} OK, {fail} failed, {inserted} new points, {total} total in DB")
    conn.close()


def _fetch_dissent():
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


def _fetch_bruegel():
    from policy_monitor.bruegel import fetch_all_bruegel, get_bruegel_db, store_bruegel_data, store_provincial_data

    conn = get_bruegel_db()
    all_rows, all_snapshots, all_provincial, ok, fail = fetch_all_bruegel(conn)
    inserted = store_bruegel_data(conn, all_rows, all_snapshots)
    prov_inserted = store_provincial_data(conn, all_provincial) if all_provincial else 0
    total = conn.execute("SELECT COUNT(*) FROM bruegel_series").fetchone()[0]
    prov_total = conn.execute("SELECT COUNT(*) FROM bruegel_provincial").fetchone()[0]
    log.info(f"Bruegel: {ok} OK, {fail} failed, {inserted} new series, "
             f"{prov_inserted} new provincial, {total}+{prov_total} total in DB")
    conn.close()


def _fetch_macro():
    from policy_monitor.macro import fetch_china_macro

    result = fetch_china_macro()
    log.info(f"Macro: {result['status']} (version {result['version']})")


def _fetch_academic():
    from policy_monitor.academic import fetch_academic, get_academic_db, store_articles

    conn = get_academic_db()
    articles, ok, fail = fetch_academic()
    inserted = store_articles(conn, articles)
    total = conn.execute("SELECT COUNT(*) FROM academic_articles").fetchone()[0]
    log.info(f"Academic: {ok} OK, {fail} failed, {len(articles)} China-relevant, "
             f"{inserted} new, {total} total in DB")
    conn.close()


def _fetch_customs():
    from datetime import date

    from customs_scraper.db import init_db
    from customs_scraper.orchestrator import ScrapeOrchestrator

    today = date.today()
    if today.day >= 15:
        month = today.month - 1 if today.month > 1 else 12
        year = today.year if today.month > 1 else today.year - 1
    else:
        month = today.month - 2 if today.month > 2 else (12 + today.month - 2)
        year = today.year if today.month > 2 else today.year - 1

    from policy_monitor.storage import DB_PATH
    db_path = str(DB_PATH)
    init_db(db_path)
    orchestrator = ScrapeOrchestrator(year=year, month=month, db_path=db_path)
    status = orchestrator.run()
    log.info(f"Customs: {year}-{month:02d} finished with status={status}")


def _fetch_regulations():
    from policy_monitor.regulations import get_regulations_db, store_mofcom_docs, store_npc_bill
    from policy_monitor.scrapers.mofcom import fetch_mofcom
    from policy_monitor.scrapers.npc_observer import fetch_npc_bills

    conn = get_regulations_db()

    docs = fetch_mofcom()
    inserted_mofcom = store_mofcom_docs(conn, docs)
    mofcom_total = conn.execute("SELECT COUNT(*) FROM mofcom_docs").fetchone()[0]
    log.info("Regulations/MOFCOM: %d fetched, %d new, %d total in DB", len(docs), inserted_mofcom, mofcom_total)

    bills = fetch_npc_bills()
    for bill, events in bills:
        store_npc_bill(conn, bill, events)
    npc_total = conn.execute("SELECT COUNT(*) FROM npc_bills").fetchone()[0]
    log.info("Regulations/NPC: %d bills fetched, %d total in DB", len(bills), npc_total)

    conn.close()


def _fetch_bis():
    from policy_monitor.bis import fetch_all_bis, get_bis_db
    conn = get_bis_db()
    ok, fail = fetch_all_bis(conn)
    log.info(f"BIS: {ok} datasets OK, {fail} failed")
    conn.close()


def _fetch_ecb():
    from policy_monitor.ecb import fetch_all_ecb, get_ecb_db
    conn = get_ecb_db()
    ok, fail = fetch_all_ecb(conn)
    log.info(f"ECB: {ok} datasets OK, {fail} failed")
    conn.close()


def _fetch_destatis():
    from policy_monitor.destatis import fetch_all_destatis, get_destatis_db
    conn = get_destatis_db()
    ok, fail = fetch_all_destatis(conn)
    log.info(f"Destatis: {ok} datasets OK, {fail} failed")
    conn.close()


def _fetch_global_macro():
    from policy_monitor.global_macro import fetch_all_global_macro, get_global_macro_db
    conn = get_global_macro_db()
    ok, fail = fetch_all_global_macro(conn)
    log.info(f"Global macro: {ok} indicators OK, {fail} failed")
    conn.close()


def _fetch_imf_fiscal():
    from policy_monitor.imf_fiscal import fetch_all_imf_fiscal, get_imf_fiscal_db
    conn = get_imf_fiscal_db()
    ok, fail = fetch_all_imf_fiscal(conn)
    log.info(f"IMF Fiscal: {ok} indicators OK, {fail} failed")
    conn.close()


def _fetch_eurostat():
    from policy_monitor.runners.fetch_eurostat import run as run_eurostat
    run_eurostat()


def _fetch_ministries():
    from policy_monitor.runners.fetch_ministries import run as run_ministries
    run_ministries()


def _fetch_trade_stats():
    from policy_monitor.runners.fetch_trade_stats import run as run_trade_stats
    run_trade_stats()


def _fetch_yfinance():
    from policy_monitor.runners.fetch_yfinance import run as run_yfinance
    run_yfinance()


def _fetch_comtrade():
    from policy_monitor.runners.fetch_comtrade import run as run_comtrade
    run_comtrade()


FETCHERS = {
    "financial": _fetch_financial,
    "dissent": _fetch_dissent,
    "bruegel": _fetch_bruegel,
    "macro": _fetch_macro,
    "academic": _fetch_academic,
    "customs": _fetch_customs,
    "regulations": _fetch_regulations,
    "bis": _fetch_bis,
    "ecb": _fetch_ecb,
    "destatis": _fetch_destatis,
    "global_macro": _fetch_global_macro,
    "imf_fiscal": _fetch_imf_fiscal,
    "eurostat": _fetch_eurostat,
    "ministries": _fetch_ministries,
    "trade_stats": _fetch_trade_stats,
    "yfinance": _fetch_yfinance,
    "comtrade": _fetch_comtrade,
}


def run(sources=None, random_delay=0):
    """Run the batch ingestion pipeline.

    Args:
        sources: list of source names to fetch, or None for all.
        random_delay: max random delay in seconds before starting (0 = no delay).
    """
    if random_delay > 0:
        delay = random.randint(0, random_delay)
        log.info(f"Batch fetch scheduled — sleeping {delay}s ({delay / 3600:.1f}h) before starting")
        time.sleep(delay)

    targets = sources or ALL_SOURCES
    log.info(f"Batch pipeline starting — sources: {', '.join(targets)}")

    from policy_monitor.storage import get_db
    db = get_db()
    started_at = datetime.utcnow().isoformat()
    cur = db.execute(
        "INSERT INTO batch_runs (started_at, sources_run, status) VALUES (?, ?, 'running')",
        (started_at, json.dumps(targets)),
    )
    run_id = cur.lastrowid
    db.commit()

    ok_sources = []
    failed_sources = []

    for name in targets:
        fetcher = FETCHERS.get(name)
        if not fetcher:
            log.warning(f"Unknown source: {name}, skipping")
            failed_sources.append(name)
            continue

        log.info("")
        log.info("=" * 60)
        log.info(name.upper())
        log.info("=" * 60)
        try:
            fetcher()
            ok_sources.append(name)
        except Exception as e:
            log.error(f"{name} fetch error: {e}")
            failed_sources.append(name)

    completed_at = datetime.utcnow().isoformat()
    status = "completed" if not failed_sources else "completed_with_errors"
    db.execute(
        "UPDATE batch_runs SET completed_at=?, sources_ok=?, sources_failed=?, status=? WHERE id=?",
        (completed_at, json.dumps(ok_sources), json.dumps(failed_sources), status, run_id),
    )
    db.commit()
    db.close()

    log.info("")
    log.info(f"Batch pipeline {status}: {len(ok_sources)} OK, {len(failed_sources)} failed")


def main():
    parser = argparse.ArgumentParser(description="CMM batch data ingestion pipeline")
    parser.add_argument("--sources", type=str, default=None,
                        help="Comma-separated list of sources to fetch (default: all)")
    parser.add_argument("--random-delay", type=int, default=0,
                        help="Max random delay in seconds before starting (default: 0)")
    args = parser.parse_args()

    sources = [s.strip() for s in args.sources.split(",")] if args.sources else None
    run(sources=sources, random_delay=args.random_delay)


if __name__ == "__main__":
    main()
