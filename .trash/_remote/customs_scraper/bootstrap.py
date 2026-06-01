"""
Bootstrap: fetch the HS8 code list and country list from the site itself.
Run once before the first full scrape (requires proxy / Chinese IP).

    python -m customs_scraper.main --bootstrap-hs-codes

This writes:
  data/hs8_codes.csv   (columns: code, description)
  data/countries.csv   (columns: code, name)

*** STUB — implement after site inspection ***

How to implement:
  1. Run --debug-browser and inspect the HS code dropdown in DevTools.
  2. Look for XHR calls in the Network tab (filter: Fetch/XHR).
     Many GACC-style portals have an endpoint like:
       GET /api/hscode/list?level=8   → returns JSON array of {code, name}
       GET /api/country/list          → returns JSON array of {code, name}
  3. If JSON API found: implement _fetch_hs_codes_json() and _fetch_countries_json()
  4. If dropdown only: implement Playwright-based extraction via DynamicFetcher
"""
import csv
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

_DATA_DIR = Path(__file__).parent.parent / "data"


def bootstrap_hs_codes() -> None:
    """
    Fetch HS 8-digit codes from the site and write to data/hs8_codes.csv.

    *** NOT YET IMPLEMENTED — requires proxy + site inspection ***
    """
    raise NotImplementedError(
        "bootstrap_hs_codes() is not yet implemented.\n\n"
        "To populate data/hs8_codes.csv manually:\n"
        "  1. Download China's annual tariff schedule XLS from:\n"
        "     http://www.customs.gov.cn/customs/302249/302266/index.html\n"
        "  2. Extract 8-digit codes and English descriptions.\n"
        "  3. Save as data/hs8_codes.csv with columns: code, description\n\n"
        "Or implement this function after inspecting the site with --debug-browser."
    )


def bootstrap_countries() -> None:
    """
    Fetch GACC country codes from the site and write to data/countries.csv.

    *** NOT YET IMPLEMENTED — requires proxy + site inspection ***
    """
    raise NotImplementedError(
        "bootstrap_countries() is not yet implemented.\n\n"
        "To populate data/countries.csv manually:\n"
        "  1. Find the GACC country/region code list in their explanatory notes,\n"
        "     published alongside monthly bulletins at http://stats.customs.gov.cn/\n"
        "  2. Save as data/countries.csv with columns: code, name\n\n"
        "Or implement this function after inspecting the site with --debug-browser."
    )


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f"Wrote {len(rows)} rows to {path}")
