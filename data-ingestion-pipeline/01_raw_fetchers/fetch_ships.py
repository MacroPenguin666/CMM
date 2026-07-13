"""
Raw ships fetcher — AIS vessel positions in Chinese waters, snapshot per poll.

Each call appends one raw batch of vessel positions inside the China maritime
bounding box to ``02_inputs/ships/positions``, append-only. Designed for
``_run.py --realtime``.

Counterpart to ``backend/fetchers/ships.py`` (current+history into cmm.db). Reuses
``fetch_ship_positions``.
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
from backend.fetchers.ships import fetch_ship_positions  # noqa: E402

log = logging.getLogger("raw_ships")
SOURCE = "ships"


def run(run_id: str | None = None, *, duration_seconds: int = 55) -> None:
    run_id = run_id or store.new_run_id()
    datasets: dict[str, int] = {}
    try:
        positions = fetch_ship_positions(duration_seconds=duration_seconds)
        if positions:
            store.append(SOURCE, "positions", pd.DataFrame(positions), run_id=run_id)
            datasets["positions"] = len(positions)
        log.info("  %d vessels", len(positions))
        store.write_manifest(SOURCE, status="ok", datasets=datasets, run_id=run_id)
    except Exception as e:
        store.write_manifest(SOURCE, status="error", datasets=datasets,
                             run_id=run_id, error=str(e))
        raise


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s",
                        datefmt="%H:%M:%S")
    ap = argparse.ArgumentParser(description="Raw ships fetcher")
    ap.add_argument("--duration", type=int, default=55, help="AIS stream listen seconds")
    args = ap.parse_args()
    run(duration_seconds=args.duration)


if __name__ == "__main__":
    main()
