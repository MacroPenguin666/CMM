"""
CMM unified fetch CLI — entry point for the `cmm-fetch` command.

Usage:
    cmm-fetch news          # RSS feeds + ministry scrapers
    cmm-fetch realtime      # flights + ships (runs continuously)
    cmm-fetch financial     # AKShare + Yahoo Finance
    cmm-fetch macro         # GMD, IMF WEO, IMF Fiscal, NBS, Bruegel
    cmm-fetch batch         # all non-realtime sources
    cmm-fetch comtrade      # UN Comtrade bilateral trade
    cmm-fetch eurostat      # Eurostat datasets
    cmm-fetch ministries    # ministry HTML scrapers only
    cmm-fetch trade-stats   # WITS, WTO, USITC, ILO, OECD
    cmm-fetch yfinance      # Yahoo Finance OHLCV
    cmm-fetch ccp-elites    # import CCP elite leadership xlsx
"""

import argparse
import sys


def main():
    parser = argparse.ArgumentParser(prog="cmm-fetch", description="CMM data fetcher")
    parser.add_argument(
        "command",
        choices=[
            "news", "realtime", "financial", "macro", "batch",
            "comtrade", "eurostat", "ministries", "trade-stats",
            "yfinance", "ccp-elites",
        ],
    )
    args, extra = parser.parse_known_args()

    sys.argv = [sys.argv[0]] + extra

    if args.command == "news":
        from backend.runners.fetch_news import run
    elif args.command == "realtime":
        from backend.runners.fetch_realtime import run
    elif args.command == "financial":
        from backend.runners.fetch_batch import run
    elif args.command == "macro":
        from backend.runners.fetch_macro import run
    elif args.command == "batch":
        from backend.runners.fetch_batch import run
    elif args.command == "comtrade":
        from backend.runners.fetch_comtrade import run
    elif args.command == "eurostat":
        from backend.runners.fetch_eurostat import run
    elif args.command == "ministries":
        from backend.runners.fetch_ministries import run
    elif args.command == "trade-stats":
        from backend.runners.fetch_trade_stats import run
    elif args.command == "yfinance":
        from backend.runners.fetch_yfinance import run
    elif args.command == "ccp-elites":
        from backend.runners.import_ccp_elites import run

    run()


if __name__ == "__main__":
    main()
