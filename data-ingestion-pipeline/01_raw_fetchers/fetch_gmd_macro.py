"""
Raw GMD fetcher — Global Macro Database (Müller, Xu, Lehbih & Chen 2025).

Pulls the **entire** China row set: all ~75 annual macro variables, full history
(back to 1640) plus IMF/WB projections to 2030. Stored raw (the wide GMD frame
as-is) to ``02_inputs/gmd_macro/series``, append-only with the GMD version stamped
as a column so every data vintage is retained.

Counterpart to ``backend/fetchers/macro.py`` (which reshapes a curated subset into
cmm.db). Reuses the ``global_macro_data`` package directly.
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

log = logging.getLogger("raw_gmd")
SOURCE = "gmd_macro"


def run(run_id: str | None = None) -> None:
    run_id = run_id or store.new_run_id()
    datasets: dict[str, int] = {}
    try:
        from global_macro_data import gmd, get_current_version
        version = get_current_version()
        log.info("GMD version %s — downloading full China frame...", version)
        df: pd.DataFrame = gmd(country="CHN", fast="yes")
        df = df.copy()
        df["gmd_version"] = version          # vintage marker, kept in the raw row
        store.append(SOURCE, "series", df, run_id=run_id,
                     endpoint=f"global_macro_data:gmd(country=CHN,v={version})")
        datasets["series"] = len(df)
        store.write_manifest(SOURCE, status="ok", datasets=datasets, run_id=run_id)
        log.info("Done: %d rows, %d columns", len(df), df.shape[1])
    except Exception as e:
        store.write_manifest(SOURCE, status="error", datasets=datasets,
                             run_id=run_id, error=str(e))
        raise


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s",
                        datefmt="%H:%M:%S")
    argparse.ArgumentParser(description="Raw GMD China fetcher").parse_args()
    run()


if __name__ == "__main__":
    main()
