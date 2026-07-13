"""
Raw Yahoo Finance fetcher — China-relevant markets, full history.

Pulls the **maximum available** daily history (``period="max"``) for every
China-relevant ticker — indices, A-share/HK names, China ETFs, CNY/CNH FX,
commodities China dominates, and US-rate context — stored raw OHLCV to
``02_inputs/yfinance/ohlcv``, append-only.

Counterpart to ``backend/fetchers/yfinance_data.py`` (5-year window into cmm.db).
Reuses that module's curated ``TICKERS`` list.
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
from backend.fetchers.yfinance_data import TICKERS  # noqa: E402

log = logging.getLogger("raw_yfinance")
SOURCE = "yfinance"


def run(run_id: str | None = None) -> None:
    run_id = run_id or store.new_run_id()
    datasets: dict[str, int] = {}
    try:
        import yfinance as yf
        all_rows: list[pd.DataFrame] = []
        for ticker, cat, desc in TICKERS:
            try:
                hist = yf.Ticker(ticker).history(period="max", interval="1d",
                                                 auto_adjust=True)
                if hist.empty:
                    log.info("  %-12s no data", ticker)
                    continue
                h = hist.reset_index()
                h.columns = [str(c).lower().replace(" ", "_") for c in h.columns]
                h["date"] = h[h.columns[0]].astype(str).str.slice(0, 10)
                h["ticker"] = ticker
                h["category"] = cat
                h["description"] = desc
                all_rows.append(h)
                log.info("  %-12s %5d rows", ticker, len(h))
                time.sleep(0.3)
            except Exception as e:
                log.warning("  %-12s FAIL: %s", ticker, str(e)[:80])
        if all_rows:
            df = pd.concat(all_rows, ignore_index=True)
            store.append(SOURCE, "ohlcv", df, run_id=run_id, endpoint="yfinance:history(max)")
            datasets["ohlcv"] = len(df)
        store.write_manifest(SOURCE, status="ok", datasets=datasets, run_id=run_id)
    except Exception as e:
        store.write_manifest(SOURCE, status="error", datasets=datasets,
                             run_id=run_id, error=str(e))
        raise


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s",
                        datefmt="%H:%M:%S")
    argparse.ArgumentParser(description="Raw yfinance China fetcher").parse_args()
    run()


if __name__ == "__main__":
    main()
