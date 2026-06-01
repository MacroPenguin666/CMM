"""Fetch trade & labour statistics: WITS/UNCTAD, WTO, ILO, OECD.

Usage:
    python run_fetch_trade_stats.py                     # all sources, incremental
    python run_fetch_trade_stats.py --full              # all sources, full history
    python run_fetch_trade_stats.py --sources oecd ilo  # specific sources only

Keys needed for full coverage (add to .env):
    WITS_API_KEY   — free at wits.worldbank.org → My Account → API Access
    WTO_API_KEY    — free at https://api.wto.org (developer tier)

Sources that work with NO keys:
    WITS fallback  — World Bank WDI aggregate tariff indicators
    ILO fallback   — World Bank WDI employment indicators (~12 series, 200 countries)
    OECD STAN      — Production & value added by industry (no auth needed)
    OECD FDI       — FDI flows by partner country (no auth needed)
"""
import sys
from policy_monitor.runners.fetch_trade_stats import run

sources    = None
force_full = "--full" in sys.argv

if "--sources" in sys.argv:
    idx = sys.argv.index("--sources")
    sources = []
    for s in sys.argv[idx + 1:]:
        if s.startswith("--"):
            break
        sources.append(s)

run(sources=sources, force_full=force_full)
