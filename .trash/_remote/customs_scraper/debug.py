"""
Debug browser mode: opens a non-headless Playwright browser at the query page,
pauses for manual inspection, then prints captured HTML.

Usage:
    python -m customs_scraper.main --debug-browser --year 2024 --month 1

Steps:
  1. Set CUSTOMS_PROXY_URL in .env (needs Chinese IP to access the site)
  2. Run the above command
  3. A browser window will open. Interact with it:
     - Open DevTools → Network tab → filter: Fetch/XHR
     - Submit the export query form for any HS code
     - If you see a JSON API call: note the URL, method, and params
       → implement it in fetcher.py (much faster than browser automation)
     - If HTML form only: inspect the selectors for each field
       → update page_actions.py and parser.py accordingly
  4. Press Enter in this terminal to capture the page HTML
  5. Copy the HTML to tests/fixtures/sample_table.html
"""
import logging
from pathlib import Path

from .config import QUERY_ENDPOINT
from .fetcher import CustomsFetcher
from .page_actions import debug_inspect_action

logger = logging.getLogger(__name__)

_FIXTURE_PATH = Path(__file__).parent.parent / "tests" / "fixtures" / "sample_table.html"


def run_debug_browser(year: int, month: int) -> None:
    """Open non-headless browser at query page and pause for inspection."""
    print(f"\n[DEBUG] Opening browser at: {QUERY_ENDPOINT}")
    print("[DEBUG] Remember to check the Network tab for XHR/API calls!\n")

    fetcher = CustomsFetcher(use_dynamic=True, headless=False)
    fetcher._open_session()
    try:
        page = fetcher.fetch(
            url=QUERY_ENDPOINT,
            page_action=debug_inspect_action(),
        )
        html = getattr(page, "html", None) or str(page)
        print(f"\n[DEBUG] Captured {len(html)} bytes of HTML")
        print("[DEBUG] First 2000 chars:\n")
        print(html[:2000])

        save = input("\n[DEBUG] Save HTML to tests/fixtures/sample_table.html? [y/N] ").strip().lower()
        if save == "y":
            _FIXTURE_PATH.parent.mkdir(parents=True, exist_ok=True)
            _FIXTURE_PATH.write_text(html, encoding="utf-8")
            print(f"[DEBUG] Saved to {_FIXTURE_PATH}")
    finally:
        fetcher._close_session()
