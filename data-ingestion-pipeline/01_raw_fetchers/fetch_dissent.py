"""
Raw dissent fetcher — China Dissent Monitor, full event database.

Pulls the complete event list (deep pagination) plus the province reference table
and stores them raw to ``02_inputs/dissent/{events,provinces}``, append-only.

Counterpart to ``backend/fetchers/dissent.py`` (into cmm.db). Reuses that module's
session + ``fetch_provinces`` / ``fetch_events`` (with a high page ceiling).
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
from backend.fetchers.dissent import _get_session, fetch_provinces, fetch_events  # noqa: E402

log = logging.getLogger("raw_dissent")
SOURCE = "dissent"


def run(run_id: str | None = None, *, max_pages: int = 500) -> None:
    run_id = run_id or store.new_run_id()
    datasets: dict[str, int] = {}
    try:
        session = _get_session()
        provinces = fetch_provinces(session)
        if provinces:
            store.append(SOURCE, "provinces", pd.DataFrame(provinces), run_id=run_id)
            datasets["provinces"] = len(provinces)
        events = fetch_events(session, max_pages=max_pages)
        if events:
            store.append(SOURCE, "events", pd.DataFrame(events), run_id=run_id)
            datasets["events"] = len(events)
        log.info("  %d provinces, %d events", len(provinces), len(events))
        store.write_manifest(SOURCE, status="ok", datasets=datasets, run_id=run_id)
    except Exception as e:
        store.write_manifest(SOURCE, status="error", datasets=datasets,
                             run_id=run_id, error=str(e))
        raise


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s",
                        datefmt="%H:%M:%S")
    ap = argparse.ArgumentParser(description="Raw dissent fetcher")
    ap.add_argument("--max-pages", type=int, default=500)
    args = ap.parse_args()
    run(max_pages=args.max_pages)


if __name__ == "__main__":
    main()
