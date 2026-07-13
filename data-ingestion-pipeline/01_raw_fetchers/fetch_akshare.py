"""
Raw AKShare fetcher — comprehensive China macro interface sweep.

AKShare exposes ~85 ``macro_china_*`` interfaces (plus China bond/FX/index ones).
``backend/fetchers/financial.py`` calls just 7 of them with row caps. This fetcher
**auto-discovers** every no-argument ``macro_china_*`` interface (plus a curated
list of other China interfaces), calls each, and stores the **full** returned
DataFrame raw to ``02_inputs/akshare/<interface>``, append-only.

Interfaces that require arguments are skipped (logged); failures (AKShare throttles
non-China IPs heavily) are caught per interface so the sweep always completes.
Run from a China-capable network for full coverage.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

import pandas as pd

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_HERE.parents[1]))

import _raw_store as store  # noqa: E402

log = logging.getLogger("raw_akshare")
SOURCE = "akshare"

# China interfaces that take no required args but aren't under the macro_china_ prefix.
EXTRA_INTERFACES = [
    "bond_china_yield", "bond_zh_us_rate",
    "currency_boc_safe", "currency_boc_sina",
    "stock_zh_index_spot_em",
]


def _discover(ak) -> list[str]:
    names = [n for n in dir(ak) if n.startswith("macro_china_")]
    names += [n for n in EXTRA_INTERFACES if hasattr(ak, n)]
    return sorted(set(names))


def run(run_id: str | None = None, *, delay: float = 0.4) -> None:
    run_id = run_id or store.new_run_id()
    datasets: dict[str, int] = {}
    skipped = 0
    try:
        import akshare as ak
        interfaces = _discover(ak)
        log.info("AKShare: %d China interfaces discovered", len(interfaces))
        for i, name in enumerate(interfaces, 1):
            fn = getattr(ak, name)
            try:
                df = fn()                      # no-arg call; TypeError => needs args
            except TypeError as e:
                if "positional argument" in str(e) or "required" in str(e):
                    skipped += 1
                    continue
                log.warning("  [%d/%d] %s FAIL: %s", i, len(interfaces), name, str(e)[:70])
                continue
            except Exception as e:
                log.warning("  [%d/%d] %s FAIL: %s", i, len(interfaces), name, str(e)[:70])
                continue
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue
            df = df.copy()
            df.columns = [str(c) for c in df.columns]   # ensure string col names for parquet
            store.append(SOURCE, name, df, run_id=run_id, endpoint=f"akshare.{name}()")
            datasets[name] = len(df)
            log.info("  [%d/%d] %-40s %5d rows", i, len(interfaces), name, len(df))
            time.sleep(delay)
        log.info("Done: %d interfaces stored, %d needed args (skipped)", len(datasets), skipped)
        store.write_manifest(SOURCE, status="ok", datasets=datasets, run_id=run_id)
    except Exception as e:
        store.write_manifest(SOURCE, status="error", datasets=datasets,
                             run_id=run_id, error=str(e))
        raise


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s",
                        datefmt="%H:%M:%S")
    argparse.ArgumentParser(description="Raw AKShare China sweep").parse_args()
    run()


if __name__ == "__main__":
    main()
