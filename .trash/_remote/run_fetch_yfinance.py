"""Fetch Yahoo Finance daily market data (China indices, FX, commodities, rates).

Usage:
    python run_fetch_yfinance.py           # incremental (last 35 days)
    python run_fetch_yfinance.py --full    # full 5-year history
    python run_fetch_yfinance.py --tickers USDCNY=X FXI  # specific tickers
"""
import sys
from policy_monitor.runners.fetch_yfinance import run

force_full = "--full" in sys.argv
tickers = None
if "--tickers" in sys.argv:
    idx = sys.argv.index("--tickers")
    tickers = [s for s in sys.argv[idx + 1:] if not s.startswith("--")]

run(force_full=force_full, tickers=tickers)
