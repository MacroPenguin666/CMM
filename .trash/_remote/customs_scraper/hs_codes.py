"""
Loads the list of HS 8-digit codes to iterate over during scraping.

Seed file: data/hs8_codes.csv  (columns: code, description)

How to populate hs8_codes.csv:
  Option A — Bootstrap from site (preferred once proxy is available):
    python -m customs_scraper.main --bootstrap-hs-codes

  Option B — From China's published tariff schedule (no proxy needed):
    The GACC publishes the full tariff schedule as XLS/PDF each year.
    Download from http://www.customs.gov.cn/customs/302249/302266/index.html
    and convert to CSV with columns: code (8-digit), description.
"""
import csv
from pathlib import Path

_DATA_DIR = Path(__file__).parent.parent / "data"
HS8_CSV = _DATA_DIR / "hs8_codes.csv"


def load_hs_codes() -> list[dict]:
    """
    Returns list of {"code": "84713000", "description": "..."}.
    Raises FileNotFoundError with instructions if CSV is missing.
    """
    if not HS8_CSV.exists():
        raise FileNotFoundError(
            f"HS code seed file not found: {HS8_CSV}\n"
            "Run `python -m customs_scraper.main --bootstrap-hs-codes` "
            "to fetch from the site, or manually create the CSV from the "
            "GACC annual tariff schedule."
        )
    codes = []
    with open(HS8_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            code = row.get("code", "").strip()
            if code and len(code) == 8 and code.isdigit():
                codes.append({"code": code, "description": row.get("description", "").strip()})
    if not codes:
        raise ValueError(f"No valid 8-digit HS codes found in {HS8_CSV}")
    return codes
