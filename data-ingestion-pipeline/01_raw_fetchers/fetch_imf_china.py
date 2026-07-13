"""
Raw IMF DataMapper fetcher — every indicator, China slice, full history.

The IMF DataMapper exposes ~50 macro/fiscal/external indicators (WEO + Fiscal
Monitor + others). This fetcher enumerates **all** of them via ``/indicators`` and
keeps the full China (``CHN``) series for each — every year, including projections.
Stored raw to ``02_inputs/imf/series`` plus an indicator catalog, append-only.

Counterpart to ``backend/fetchers/global_macro.py`` + ``imf_fiscal.py`` (curated
~20 indicators, year-capped, all-country, into cmm.db). Reuses the same base URL.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

import pandas as pd
import requests

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_HERE.parents[1]))

import _raw_store as store  # noqa: E402
from backend.config import IMF_BASE_URL  # noqa: E402

log = logging.getLogger("raw_imf")
SOURCE = "imf"
COUNTRY = "CHN"


def _get(path: str, retries: int = 3) -> dict:
    for attempt in range(retries):
        try:
            r = requests.get(f"{IMF_BASE_URL}/{path}", timeout=45)
            if r.status_code in (429, 503):
                time.sleep(15 * (attempt + 1))
                continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            if attempt == retries - 1:
                raise
            time.sleep(10 * (attempt + 1))
    return {}


def _list_indicators() -> list[dict]:
    payload = _get("indicators")
    inds = payload.get("indicators", {})
    return [{"indicator": code,
             "label": info.get("label", ""),
             "unit": info.get("unit", ""),
             "source": info.get("source", ""),
             "dataset": info.get("dataset", "")}
            for code, info in inds.items()]


def run(run_id: str | None = None) -> None:
    run_id = run_id or store.new_run_id()
    datasets: dict[str, int] = {}
    try:
        catalog = _list_indicators()
        store.append(SOURCE, "catalog", pd.DataFrame(catalog), run_id=run_id,
                     endpoint=f"{IMF_BASE_URL}/indicators")
        datasets["catalog"] = len(catalog)
        log.info("IMF DataMapper: %d indicators", len(catalog))

        rows: list[dict] = []
        for i, ind in enumerate(catalog, 1):
            code = ind["indicator"]
            try:
                data = _get(code).get("values", {}).get(code, {})
            except Exception as e:
                log.warning("  [%d/%d] %s FAIL: %s", i, len(catalog), code, str(e)[:80])
                continue
            chn = data.get(COUNTRY, {})
            for year_str, val in chn.items():
                try:
                    rows.append({"indicator": code, "year": int(year_str),
                                 "value": float(val)})
                except (ValueError, TypeError):
                    continue
            if i % 10 == 0:
                log.info("  [%d/%d] indicators scanned, %d CHN points", i, len(catalog), len(rows))
            time.sleep(0.2)

        if rows:
            store.append(SOURCE, "series", pd.DataFrame(rows), run_id=run_id,
                         endpoint=f"{IMF_BASE_URL}/<indicator>")
            datasets["series"] = len(rows)
        store.write_manifest(SOURCE, status="ok", datasets=datasets, run_id=run_id)
        log.info("Done: %d CHN observations across %d indicators", len(rows), len(catalog))
    except Exception as e:
        store.write_manifest(SOURCE, status="error", datasets=datasets,
                             run_id=run_id, error=str(e))
        raise


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s",
                        datefmt="%H:%M:%S")
    argparse.ArgumentParser(description="Raw IMF DataMapper China fetcher").parse_args()
    run()


if __name__ == "__main__":
    main()
