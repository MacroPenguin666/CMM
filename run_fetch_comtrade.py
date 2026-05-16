"""Fetch UN Comtrade HS4 trade data for all countries.

Usage:
    python run_fetch_comtrade.py                # HS4 incremental (latest year)
    python run_fetch_comtrade.py --full         # HS4 full history (HISTORY_YEARS)
    python run_fetch_comtrade.py --bilateral    # also fetch reporter × partner totals
    python run_fetch_comtrade.py --freq M       # monthly instead of annual
"""
import sys
from policy_monitor.runners.fetch_comtrade import run

bilateral  = "--bilateral" in sys.argv
force_full = "--full" in sys.argv
freq = "M" if ("--freq" in sys.argv and sys.argv[sys.argv.index("--freq") + 1] == "M") else "A"
run(bilateral=bilateral, freq=freq, force_full=force_full)
