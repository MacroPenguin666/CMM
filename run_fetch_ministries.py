"""Fetch latest (or full history) announcements from Chinese ministry websites.

Usage:
    python run_fetch_ministries.py           # incremental — recent pages only
    python run_fetch_ministries.py --full    # bulk fetch all pages (first run)
"""
import sys
from policy_monitor.runners.fetch_ministries import run

force_full = "--full" in sys.argv
run(force_full=force_full)
