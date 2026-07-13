"""
Raw BIS fetcher — China across all major BIS statistical dataflows.

Pulls every China series (REF_AREA / borrower country = CN) from the BIS SDMX REST
API for the dataflows below, raw CSV stored as-is per dataflow, full history,
append-only. Keys are China-pinned with the rest of the SDMX key wildcarded, so
all sectors / baskets / measures for China come back in one call each.

    WS_TC     credit-to-GDP & total credit (all borrower/lender/valuation combos)
    WS_EER    effective exchange rates (nominal + real, narrow + broad)
    WS_CBPOL  central bank policy rate
    WS_DSR    debt service ratios
    WS_SPP    selected (residential) property prices

Counterpart to ``backend/fetchers/bis.py`` (5 curated series into cmm.db). Reuses
that module's ``_fetch_bis`` CSV helper. Keys verified against the live API.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_HERE.parents[1]))

import _raw_store as store  # noqa: E402
from backend.fetchers.bis import _fetch_bis, BIS_BASE  # noqa: E402

log = logging.getLogger("raw_bis")
SOURCE = "bis"

# (dataflow, China-pinned SDMX key, startPeriod) — verified non-empty against API.
TARGETS = [
    ("WS_TC",    "Q.CN.....", "1940-Q1"),
    ("WS_EER",   "M...CN",    "1990-01"),
    ("WS_CBPOL", "M.CN",      "1940-01"),
    ("WS_DSR",   "Q.CN...",   "1990-Q1"),
    ("WS_SPP",   "Q.CN....",  "1940-Q1"),
]


def run(run_id: str | None = None) -> None:
    run_id = run_id or store.new_run_id()
    datasets: dict[str, int] = {}
    status = "ok"
    err = None
    for dataflow, key, start in TARGETS:
        ds = dataflow.lower()
        try:
            df = _fetch_bis(dataflow, key, start=start)
            df = df.copy()
            df["bis_dataflow"] = dataflow
            store.append(SOURCE, ds, df, run_id=run_id,
                         endpoint=f"{BIS_BASE}/{dataflow}/{key}")
            datasets[ds] = len(df)
            log.info("  %-9s %-10s %5d rows", dataflow, key, len(df))
        except Exception as e:
            status = "partial"
            err = str(e)
            log.warning("  %-9s %-10s FAIL: %s", dataflow, key, str(e)[:90])
    store.write_manifest(SOURCE, status=status, datasets=datasets, run_id=run_id, error=err)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s",
                        datefmt="%H:%M:%S")
    argparse.ArgumentParser(description="Raw BIS China fetcher").parse_args()
    run()


if __name__ == "__main__":
    main()
