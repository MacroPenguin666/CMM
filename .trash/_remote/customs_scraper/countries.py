"""
Loads the list of GACC destination country codes to iterate over.

Seed file: data/countries.csv  (columns: code, name)

How to populate countries.csv:
  Option A — Bootstrap from site (preferred once proxy is available):
    python -m customs_scraper.main --bootstrap-hs-codes
    (also bootstraps countries)

  Option B — From GACC published country/region code list:
    GACC publishes country codes used in their statistics.
    A standard starting point is the UN M49 country list or the
    GACC-specific list from their explanatory notes, which is available
    alongside monthly bulletins at http://stats.customs.gov.cn/
"""
import csv
from pathlib import Path

_DATA_DIR = Path(__file__).parent.parent / "data"
COUNTRIES_CSV = _DATA_DIR / "countries.csv"


def load_countries() -> list[dict]:
    """
    Returns list of {"code": "502", "name": "United States"}.
    Raises FileNotFoundError with instructions if CSV is missing.
    """
    if not COUNTRIES_CSV.exists():
        raise FileNotFoundError(
            f"Country seed file not found: {COUNTRIES_CSV}\n"
            "Run `python -m customs_scraper.main --bootstrap-hs-codes` "
            "to fetch from the site, or manually create the CSV from the "
            "GACC country/region code list."
        )
    countries = []
    with open(COUNTRIES_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            code = row.get("code", "").strip()
            if code:
                countries.append({"code": code, "name": row.get("name", "").strip()})
    if not countries:
        raise ValueError(f"No country entries found in {COUNTRIES_CSV}")
    return countries
