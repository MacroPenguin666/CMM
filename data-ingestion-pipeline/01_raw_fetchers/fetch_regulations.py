"""
Raw regulations fetcher — MOFCOM active laws + NPC bills, full.

Stores the complete MOFCOM active-laws set and the full NPC Observer bill list
(with their event timelines) raw to ``02_inputs/regulations/{mofcom,npc_bills,
npc_bill_events}``, append-only.

Counterpart to ``backend/fetchers/regulations.py`` (into cmm.db). Reuses the
existing scrapers ``scrapers.mofcom.fetch_mofcom`` and
``scrapers.npc_observer.fetch_npc_bills``.

NOTE: MOFCOM is unreachable outside China; that half is logged and skipped when it
fails. NPC Observer (WordPress) is reachable globally.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_HERE.parents[1]))

import _raw_store as store  # noqa: E402

log = logging.getLogger("raw_regulations")
SOURCE = "regulations"


def run(run_id: str | None = None) -> None:
    run_id = run_id or store.new_run_id()
    datasets: dict[str, int] = {}
    status, err = "ok", None

    # --- MOFCOM active laws ---
    try:
        from backend.fetchers.scrapers.mofcom import fetch_mofcom
        docs = fetch_mofcom()
        if docs:
            store.append(SOURCE, "mofcom", pd.DataFrame(docs), run_id=run_id)
            datasets["mofcom"] = len(docs)
        log.info("  MOFCOM: %d docs", len(docs))
    except Exception as e:
        status, err = "partial", str(e)
        log.warning("  MOFCOM FAIL: %s", str(e)[:100])

    # --- NPC Observer bills + events ---
    try:
        from backend.fetchers.scrapers.npc_observer import fetch_npc_bills
        bills = fetch_npc_bills()
        bill_rows, event_rows = [], []
        for bill, events in bills:
            bill_rows.append(dict(bill))
            for ev in events:
                rec = dict(ev)
                rec["bill_id"] = bill.get("bill_id")
                event_rows.append(rec)
        if bill_rows:
            store.append(SOURCE, "npc_bills", pd.DataFrame(bill_rows), run_id=run_id)
            datasets["npc_bills"] = len(bill_rows)
        if event_rows:
            store.append(SOURCE, "npc_bill_events", pd.DataFrame(event_rows), run_id=run_id)
            datasets["npc_bill_events"] = len(event_rows)
        log.info("  NPC: %d bills, %d events", len(bill_rows), len(event_rows))
    except Exception as e:
        status, err = "partial", str(e)
        log.warning("  NPC FAIL: %s", str(e)[:100])

    store.write_manifest(SOURCE, status=status, datasets=datasets, run_id=run_id, error=err)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s",
                        datefmt="%H:%M:%S")
    argparse.ArgumentParser(description="Raw regulations fetcher").parse_args()
    run()


if __name__ == "__main__":
    main()
