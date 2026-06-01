"""
HTML parser: converts scrapling Page objects → list of export row dicts.

*** HOW TO UPDATE THIS FILE after site inspection ***
1. Run: python -m customs_scraper.main --debug-browser
2. Submit a query for any HS code and let the results load.
3. In the browser, right-click the results table → "Inspect"
4. Find the table element and note its CSS class or id → update TABLE_SELECTOR
5. Find a data row (<tr>) → note its parent → update DATA_ROW_SELECTOR
6. Note which column index maps to which field → update COLUMN_MAP
7. Find the "next page" button → update NEXT_PAGE_SELECTOR
8. Save a sample result page: File > Save Page As → tests/fixtures/sample_table.html
9. Run: python -m pytest tests/test_parser.py
"""
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

# ── Selectors (stubs — update after site inspection) ──────────────────────────

TABLE_SELECTOR      = "table.data-table, table#result-table"
DATA_ROW_SELECTOR   = "table.data-table tbody tr, table#result-table tbody tr"
NEXT_PAGE_SELECTOR  = "a.next-page, .pagination-next, button.next"
TOTAL_ROWS_SELECTOR = ".total-count, #totalCount, span.total"

# Maps column index → field name in the exports table.
# *** Update after inspecting real column headers ***
COLUMN_MAP: dict[int, str] = {
    0: "hs8_code",
    1: "hs_description",
    2: "country_code",
    3: "country_name",
    4: "export_value_usd",
    5: "export_value_cny",
    6: "export_qty",
    7: "export_qty_unit",
}

# ── Public functions ──────────────────────────────────────────────────────────

def parse_results_page(page: Any, year: int, month: int) -> list[dict]:
    """
    Extract all export data rows from a scrapling Page object.

    Args:
        page:  scrapling Page returned by CustomsFetcher.fetch()
        year:  query year (injected, site may not include it in row data)
        month: query month

    Returns:
        List of dicts with keys matching the `exports` table columns.
    """
    rows: list[dict] = []
    try:
        data_rows = page.css(DATA_ROW_SELECTOR)
    except Exception as exc:
        logger.error(f"Failed to locate data rows with selector '{DATA_ROW_SELECTOR}': {exc}")
        return rows

    if not data_rows:
        logger.warning(f"No rows found with selector '{DATA_ROW_SELECTOR}' — check selectors")
        return rows

    for tr in data_rows:
        try:
            cells = tr.css("td")
            if not cells:
                continue
            row = _extract_row(cells, year, month)
            if row is not None:
                rows.append(row)
        except Exception as exc:
            logger.warning(f"Skipping malformed row: {exc}")

    logger.debug(f"Parsed {len(rows)} rows from page")
    return rows


def has_next_page(page: Any) -> bool:
    """
    Return True if a 'next page' control exists and is not disabled.

    *** Update the disabled-state check after site inspection ***
    """
    try:
        elements = page.css(NEXT_PAGE_SELECTOR)
    except Exception:
        return False
    if not elements:
        return False
    # Take the first match and check for common disabled patterns
    first = elements[0] if isinstance(elements, list) else elements
    try:
        css_class = first.attrib.get("class", "") if hasattr(first, "attrib") else ""
        aria_disabled = first.attrib.get("aria-disabled", "") if hasattr(first, "attrib") else ""
        if "disabled" in css_class or aria_disabled == "true":
            return False
    except Exception:
        pass
    return True


def get_total_row_count(page: Any) -> int | None:
    """Extract the total result count shown on the page, if available."""
    try:
        el = page.css(TOTAL_ROWS_SELECTOR)
        if not el:
            return None
        text = el[0].css("::text").get("") if isinstance(el, list) else el.css("::text").get("")
        return _parse_int(text)
    except Exception:
        return None

# ── Internal helpers ──────────────────────────────────────────────────────────

def _extract_row(cells: Any, year: int, month: int) -> dict | None:
    """Map table cells to an exports-table dict. Returns None for empty/header rows."""
    raw: dict[str, str] = {}
    for idx, field_name in COLUMN_MAP.items():
        try:
            text = cells[idx].css("::text").get("") if hasattr(cells[idx], "css") else ""
            raw[field_name] = text.strip()
        except (IndexError, Exception):
            raw[field_name] = ""

    # Skip rows with no HS code and no value — likely header or spacer rows
    if not raw.get("hs8_code") and not raw.get("export_value_usd"):
        return None

    return {
        "year":             year,
        "month":            month,
        "hs8_code":         _clean_hs_code(raw.get("hs8_code", "")),
        "hs_description":   raw.get("hs_description") or None,
        "country_code":     raw.get("country_code", "").strip() or None,
        "country_name":     raw.get("country_name") or None,
        "export_value_usd": _parse_number(raw.get("export_value_usd")),
        "export_value_cny": _parse_number(raw.get("export_value_cny")),
        "export_qty":       _parse_number(raw.get("export_qty")),
        "export_qty_unit":  raw.get("export_qty_unit") or None,
    }


def _clean_hs_code(raw: str) -> str:
    return re.sub(r"\s+", "", raw)


def _parse_number(raw: str | None) -> float | None:
    if not raw:
        return None
    cleaned = re.sub(r"[^\d.\-]", "", raw)
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def _parse_int(raw: str | None) -> int | None:
    if not raw:
        return None
    cleaned = re.sub(r"[^\d]", "", raw)
    try:
        return int(cleaned) if cleaned else None
    except ValueError:
        return None
