"""
Raw flights fetcher — OpenSky live aircraft over China, snapshot per poll.

Each call appends one raw snapshot of all aircraft state-vectors inside the China
bounding box to ``02_inputs/flights/positions``. Append-only — the dataset is the
full time-stamped track of every poll. Designed for ``_run.py --realtime``.

Counterpart to ``backend/fetchers/flights.py`` (current+history into cmm.db).
Reuses ``fetch_flight_positions`` and ``_load_credentials``.
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
from backend.fetchers.flights import fetch_flight_positions, _load_credentials, OPENSKY_URL  # noqa: E402

log = logging.getLogger("raw_flights")
SOURCE = "flights"


def run(run_id: str | None = None) -> None:
    run_id = run_id or store.new_run_id()
    datasets: dict[str, int] = {}
    try:
        try:
            user, pwd = _load_credentials()
        except Exception:
            user = pwd = None
        positions = fetch_flight_positions(user, pwd)
        if positions:
            store.append(SOURCE, "positions", pd.DataFrame(positions), run_id=run_id,
                         endpoint=OPENSKY_URL)
            datasets["positions"] = len(positions)
        log.info("  %d aircraft", len(positions))
        store.write_manifest(SOURCE, status="ok", datasets=datasets, run_id=run_id)
    except Exception as e:
        store.write_manifest(SOURCE, status="error", datasets=datasets,
                             run_id=run_id, error=str(e))
        raise


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s",
                        datefmt="%H:%M:%S")
    argparse.ArgumentParser(description="Raw flights fetcher").parse_args()
    run()


if __name__ == "__main__":
    main()
