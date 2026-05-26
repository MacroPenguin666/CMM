"""
Bootstrap: fetch the HS8 code list and country list from stats.customs.gov.cn.
Run once before the first full scrape.

    python -m customs_scraper.main --bootstrap-hs-codes
    python -m customs_scraper.main --bootstrap-hs-codes --from-xls tariff.xlsx

This writes:
  data/hs8_codes.csv   (columns: code, description)
  data/countries.csv   (columns: code, name)

Strategies (tried in order):
  0. Local XLS   — if --from-xls given, parse a downloaded tariff schedule
  1. API probe   — hit common REST endpoints, scan page HTML for embedded JSON
  2. Playwright  — load the query page, intercept XHR, extract from DOM
  3. Fail        — clear error with manual alternatives
"""
import csv
import json
import logging
import re
from pathlib import Path
from typing import Any

import requests

from .config import BASE_URL, CUSTOMS_PROXY_URL

logger = logging.getLogger(__name__)

_DATA_DIR = Path(__file__).parent.parent / "data"
_HS_CSV = _DATA_DIR / "hs8_codes.csv"
_COUNTRIES_CSV = _DATA_DIR / "countries.csv"
_EN_QUERY_URL = f"{BASE_URL}/indexEn"

# ── Candidate API endpoints ──────────────────────────────────────────────────

_HS_API_CANDIDATES = [
    f"{BASE_URL}/api/hscode/list",
    f"{BASE_URL}/api/commodity/list",
    f"{BASE_URL}/api/commodity/query",
    f"{BASE_URL}/api/hscode/tree",
    f"{BASE_URL}/api/customs/commodity",
    f"{BASE_URL}/indexEn/api/hscode",
    f"{BASE_URL}/indexEn/api/commodity",
]

_COUNTRY_API_CANDIDATES = [
    f"{BASE_URL}/api/country/list",
    f"{BASE_URL}/api/region/list",
    f"{BASE_URL}/api/customs/country",
    f"{BASE_URL}/indexEn/api/country",
    f"{BASE_URL}/indexEn/api/region",
]

# ── Shared utilities ─────────────────────────────────────────────────────────


def _requests_session() -> requests.Session:
    """Build a requests.Session with proxy from config."""
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/html, */*",
        "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
    })
    if CUSTOMS_PROXY_URL:
        s.proxies = {"http": CUSTOMS_PROXY_URL, "https": CUSTOMS_PROXY_URL}
    return s


_CODE_KEYS = ("code", "hsCode", "hscode", "hs_code", "commodityCode",
              "commodity_code", "value", "id", "Code", "HSCode")
_NAME_KEYS = ("name", "nameEn", "name_en", "description", "label", "text",
              "desc", "Name", "NameEn", "Description")
_COUNTRY_CODE_KEYS = ("code", "countryCode", "country_code", "regionCode",
                      "region_code", "value", "id", "Code")
_COUNTRY_NAME_KEYS = ("name", "nameEn", "name_en", "countryName",
                      "country_name", "label", "text", "Name", "NameEn")


def _extract_code_entries(
    data: Any,
    code_keys: tuple[str, ...] = _CODE_KEYS,
    name_keys: tuple[str, ...] = _NAME_KEYS,
) -> list[dict]:
    """Recursively extract code/name pairs from arbitrary JSON structures."""
    results: list[dict] = []
    if isinstance(data, list):
        for item in data:
            results.extend(_extract_code_entries(item, code_keys, name_keys))
    elif isinstance(data, dict):
        code = None
        name = None
        for k in code_keys:
            if k in data and data[k]:
                code = str(data[k]).strip()
                break
        for k in name_keys:
            if k in data and data[k]:
                name = str(data[k]).strip()
                break
        if code:
            results.append({"code": code, "description": name or ""})
        # Recurse into child collections
        for child_key in ("children", "items", "data", "list", "rows",
                          "records", "result", "results", "body"):
            if child_key in data and isinstance(data[child_key], (list, dict)):
                results.extend(_extract_code_entries(
                    data[child_key], code_keys, name_keys))
    return results


def _scan_page_scripts(html: str, code_keys: tuple[str, ...],
                       name_keys: tuple[str, ...]) -> list[dict]:
    """Scan <script> tags for embedded JSON data blobs."""
    results: list[dict] = []
    # Find JSON arrays/objects in script tags
    for match in re.finditer(
        r'<script[^>]*>(.*?)</script>', html, re.DOTALL | re.IGNORECASE
    ):
        script = match.group(1)
        # Look for variable assignments containing JSON arrays
        for var_match in re.finditer(
            r'(?:var|let|const)\s+\w+\s*=\s*(\[.*?\]);', script, re.DOTALL
        ):
            try:
                data = json.loads(var_match.group(1))
                entries = _extract_code_entries(data, code_keys, name_keys)
                if entries:
                    results.extend(entries)
            except (json.JSONDecodeError, ValueError):
                continue
        # Look for JSON.parse('...')
        for parse_match in re.finditer(
            r"""JSON\.parse\(\s*['"](.+?)['"]\s*\)""", script
        ):
            try:
                raw = parse_match.group(1).replace("\\'", "'").replace('\\"', '"')
                data = json.loads(raw)
                entries = _extract_code_entries(data, code_keys, name_keys)
                if entries:
                    results.extend(entries)
            except (json.JSONDecodeError, ValueError):
                continue
    return results


# ── Strategy 1: API probe ────────────────────────────────────────────────────


def _probe_api_endpoints(
    session: requests.Session,
    candidates: list[str],
    code_keys: tuple[str, ...],
    name_keys: tuple[str, ...],
    label: str,
) -> list[dict] | None:
    """Try GET/POST on candidate URLs, return extracted entries or None."""
    for url in candidates:
        for method in ("GET", "POST"):
            try:
                logger.debug(f"Probing {method} {url}")
                if method == "GET":
                    resp = session.get(url, timeout=15)
                else:
                    resp = session.post(url, json={}, timeout=15)
                if resp.status_code != 200:
                    continue
                ct = resp.headers.get("content-type", "")
                if "json" in ct or resp.text.strip().startswith(("[", "{")):
                    try:
                        data = resp.json()
                    except ValueError:
                        continue
                    entries = _extract_code_entries(data, code_keys, name_keys)
                    if len(entries) >= 10:
                        logger.info(
                            f"API probe success: {method} {url} "
                            f"-> {len(entries)} {label} entries"
                        )
                        return entries
            except requests.RequestException:
                continue
    return None


def _probe_page_html(
    session: requests.Session,
    code_keys: tuple[str, ...],
    name_keys: tuple[str, ...],
    label: str,
) -> list[dict] | None:
    """Load the English query page and scan for embedded JSON in scripts."""
    try:
        resp = session.get(_EN_QUERY_URL, timeout=20)
        if resp.status_code != 200:
            logger.debug(f"Query page returned {resp.status_code}")
            return None
        entries = _scan_page_scripts(resp.text, code_keys, name_keys)
        if entries:
            logger.info(
                f"Embedded JSON scan: found {len(entries)} {label} entries"
            )
            return entries
        # Also try extracting from <select>/<option> elements via regex
        options = _extract_options_from_html(resp.text)
        if options:
            logger.info(
                f"HTML option scan: found {len(options)} {label} entries"
            )
            return options
    except requests.RequestException as exc:
        logger.debug(f"Could not load query page: {exc}")
    return None


def _extract_options_from_html(html: str) -> list[dict]:
    """Extract code/description pairs from <option> tags."""
    results: list[dict] = []
    for match in re.finditer(
        r'<option[^>]*value=["\'](\d{4,8})["\'][^>]*>(.*?)</option>',
        html, re.IGNORECASE
    ):
        code = match.group(1).strip()
        desc = re.sub(r'<[^>]+>', '', match.group(2)).strip()
        results.append({"code": code, "description": desc})
    return results


# ── Strategy 2: Playwright DOM extraction ────────────────────────────────────


def _extract_hs_via_playwright() -> list[dict] | None:
    """Load query page with Playwright, intercept XHR, extract HS codes."""
    captured: list[dict] = []

    async def page_action(page: Any) -> None:
        # Intercept XHR responses that might contain code data
        async def on_response(response: Any) -> None:
            try:
                ct = response.headers.get("content-type", "")
                if response.ok and ("json" in ct):
                    body = await response.json()
                    entries = _extract_code_entries(body, _CODE_KEYS, _NAME_KEYS)
                    if entries:
                        captured.extend(entries)
                        logger.debug(
                            f"XHR intercept: {response.url} -> {len(entries)} entries"
                        )
            except Exception:
                pass

        page.on("response", on_response)

        # Wait for page to settle
        await page.wait_for_load_state("networkidle")
        import asyncio
        await asyncio.sleep(2)

        # Try extracting from select/option elements
        options = await page.query_selector_all("select option")
        for opt in options:
            val = await opt.get_attribute("value") or ""
            txt = (await opt.inner_text()).strip()
            if val and val.isdigit() and len(val) == 8:
                captured.append({"code": val, "description": txt})

        # Try tree widget nodes (common in Chinese gov portals)
        tree_nodes = await page.query_selector_all(
            ".el-tree-node__label, .tree-node, "
            "[class*='tree'] [class*='leaf'], "
            "[class*='cascader'] [class*='node']"
        )
        for node in tree_nodes:
            txt = (await node.inner_text()).strip()
            # Pattern: "84713000 - Portable computers" or "84713000 Portable..."
            m = re.match(r'(\d{8})\s*[-\s]*(.*)', txt)
            if m:
                captured.append({"code": m.group(1), "description": m.group(2).strip()})

        # Try clicking chapter-level dropdowns to load sub-codes
        chapter_opts = await page.query_selector_all(
            "select option, .el-cascader-node, [class*='chapter']"
        )
        # Only attempt cascading if we found chapter-level items but no 8-digit codes yet
        hs8_count = sum(1 for c in captured if len(c["code"]) == 8 and c["code"].isdigit())
        if hs8_count < 50 and len(chapter_opts) > 0:
            logger.info(
                f"Found {len(chapter_opts)} chapter-level elements, "
                "attempting cascading expansion..."
            )
            for i, opt in enumerate(chapter_opts[:100]):  # cap to avoid infinite loops
                try:
                    await opt.click()
                    await asyncio.sleep(0.5)
                    if (i + 1) % 10 == 0:
                        logger.debug(f"Expanded {i + 1} chapter nodes...")
                except Exception:
                    continue

        # Final sweep: grab any dynamically loaded options
        all_opts = await page.query_selector_all("select option, [role='option']")
        for opt in all_opts:
            val = await opt.get_attribute("value") or ""
            txt = (await opt.inner_text()).strip()
            if val and val.isdigit() and len(val) == 8:
                captured.append({"code": val, "description": txt})

    try:
        from scrapling.fetchers import DynamicFetcher
        proxy_kw = {"proxy": CUSTOMS_PROXY_URL} if CUSTOMS_PROXY_URL else {}
        DynamicFetcher().fetch(
            _EN_QUERY_URL,
            headless=True,
            network_idle=True,
            page_action=page_action,
            **proxy_kw,
        )
    except Exception as exc:
        logger.warning(f"Playwright extraction failed: {exc}")
        return None

    if captured:
        logger.info(f"Playwright extraction: {len(captured)} raw entries")
        return captured
    return None


def _extract_countries_via_playwright() -> list[dict] | None:
    """Load query page with Playwright, extract country codes from DOM."""
    captured: list[dict] = []

    async def page_action(page: Any) -> None:
        async def on_response(response: Any) -> None:
            try:
                ct = response.headers.get("content-type", "")
                if response.ok and ("json" in ct):
                    body = await response.json()
                    entries = _extract_code_entries(
                        body, _COUNTRY_CODE_KEYS, _COUNTRY_NAME_KEYS)
                    if entries:
                        captured.extend(entries)
            except Exception:
                pass

        page.on("response", on_response)
        await page.wait_for_load_state("networkidle")
        import asyncio
        await asyncio.sleep(2)

        # Extract from select/option or list elements
        options = await page.query_selector_all("select option, [role='option']")
        for opt in options:
            val = await opt.get_attribute("value") or ""
            txt = (await opt.inner_text()).strip()
            # Country codes are typically 3-digit or 3-letter
            if val and txt and not val.isdigit():
                # Skip if it looks like an HS code
                continue
            if val and txt and len(val) <= 5:
                captured.append({"code": val, "description": txt})

    try:
        from scrapling.fetchers import DynamicFetcher
        proxy_kw = {"proxy": CUSTOMS_PROXY_URL} if CUSTOMS_PROXY_URL else {}
        DynamicFetcher().fetch(
            _EN_QUERY_URL,
            headless=True,
            network_idle=True,
            page_action=page_action,
            **proxy_kw,
        )
    except Exception as exc:
        logger.warning(f"Playwright country extraction failed: {exc}")
        return None

    if captured:
        logger.info(f"Playwright country extraction: {len(captured)} raw entries")
        return captured
    return None


# ── Strategy 0: Local XLS/XLSX parsing ────────────��───────────────────────────


def _hs_codes_from_xls(xls_path: Path) -> list[dict]:
    """
    Parse HS 8-digit codes from a downloaded GACC tariff schedule XLS/XLSX.

    Auto-detects the code column (first column with 8-digit numeric values)
    and the description column (first text column after the code column).
    Supports both .xls (via xlrd) and .xlsx (via openpyxl).
    """
    suffix = xls_path.suffix.lower()
    if suffix == ".xls":
        return _parse_xls_legacy(xls_path)
    elif suffix in (".xlsx", ".xlsm"):
        return _parse_xlsx(xls_path)
    elif suffix == ".csv":
        return _parse_csv_seed(xls_path)
    else:
        raise ValueError(
            f"Unsupported file format: {suffix}. "
            "Expected .xls, .xlsx, .xlsm, or .csv"
        )


def _parse_xlsx(path: Path) -> list[dict]:
    """Parse HS codes from .xlsx using openpyxl."""
    import openpyxl

    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb.active
    if ws is None:
        raise ValueError(f"No active sheet in {path}")

    rows = list(ws.iter_rows(values_only=True))
    wb.close()
    return _extract_codes_from_rows(rows, str(path))


def _parse_xls_legacy(path: Path) -> list[dict]:
    """Parse HS codes from .xls using xlrd (if available) or fallback."""
    try:
        import xlrd
    except ImportError:
        raise ImportError(
            "xlrd is required to read .xls files. "
            "Install with: pip install xlrd\n"
            "Or convert the file to .xlsx and retry."
        )
    wb = xlrd.open_workbook(str(path))
    ws = wb.sheet_by_index(0)
    rows = [ws.row_values(i) for i in range(ws.nrows)]
    return _extract_codes_from_rows(rows, str(path))


def _parse_csv_seed(path: Path) -> list[dict]:
    """Parse HS codes from a CSV file (code, description columns)."""
    codes: list[dict] = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = (row.get("code") or "").strip()
            desc = (row.get("description") or row.get("name") or "").strip()
            if code:
                codes.append({"code": code, "description": desc})
    return codes


def _extract_codes_from_rows(rows: list, source: str) -> list[dict]:
    """
    Auto-detect code and description columns from tabular data.

    Scans the first 20 rows to find:
      - code_col: first column with 8-digit numeric values
      - desc_col: first text column after code_col (English preferred)
    """
    if not rows:
        raise ValueError(f"No rows found in {source}")

    # Sample first 20 data rows (skip obvious header rows)
    sample_start = 0
    for i, row in enumerate(rows[:5]):
        # Skip rows that look like headers (all strings, no 8-digit codes)
        if any(_normalize_code(v) for v in row):
            sample_start = i
            break
        sample_start = i + 1

    sample = rows[sample_start:sample_start + 20]
    if not sample:
        raise ValueError(f"No data rows found in {source}")

    # Find column indices
    code_col = _find_code_column(sample)
    if code_col is None:
        raise ValueError(
            f"Could not auto-detect HS code column in {source}. "
            "Expected a column with 8-digit numeric values."
        )

    desc_col = _find_desc_column(sample, code_col)
    logger.info(
        f"Detected columns: code={code_col}, description={desc_col} "
        f"(from row {sample_start})"
    )

    # Extract all codes
    codes: list[dict] = []
    for row in rows[sample_start:]:
        if len(row) <= code_col:
            continue
        code = _normalize_code(row[code_col])
        if not code:
            continue

        desc = ""
        if desc_col is not None and len(row) > desc_col and row[desc_col] is not None:
            desc = str(row[desc_col]).strip()

        codes.append({"code": code, "description": desc})

    logger.info(f"Parsed {len(codes)} HS8 codes from {source}")
    return codes


def _normalize_code(raw: Any) -> str:
    """Normalize a raw cell value to an 8-digit HS code string, or return ''."""
    if raw is None:
        return ""
    val = str(raw).strip()
    # Handle float (e.g., 84713000.0)
    if "." in val:
        try:
            val = str(int(float(val)))
        except (ValueError, OverflowError):
            return ""
    # Zero-pad 7-digit codes (leading 0 lost by Excel)
    if val.isdigit() and len(val) == 7:
        val = "0" + val
    if len(val) == 8 and val.isdigit():
        return val
    return ""


def _find_code_column(sample: list) -> int | None:
    """Find the column index most likely to contain 8-digit HS codes."""
    if not sample:
        return None
    num_cols = max(len(row) for row in sample)
    best_col = None
    best_score = 0
    for col in range(num_cols):
        score = 0
        for row in sample:
            if col >= len(row):
                continue
            if _normalize_code(row[col]):
                score += 1
        if score > best_score:
            best_score = score
            best_col = col
    # Require at least 30% of sample rows or 2 matches, whichever is lower
    min_score = max(2, len(sample) * 3 // 10)
    return best_col if best_score >= min(min_score, len(sample)) else None


def _find_desc_column(sample: list, code_col: int) -> int | None:
    """Find the description column — prefer English text after the code column."""
    if not sample:
        return None
    num_cols = max(len(row) for row in sample)
    # Score each column by how much text it has (prefer columns with ASCII/English)
    best_col = None
    best_score = 0
    for col in range(num_cols):
        if col == code_col:
            continue
        score = 0
        for row in sample:
            if col >= len(row) or row[col] is None:
                continue
            val = str(row[col]).strip()
            if len(val) > 3 and not val.isdigit():
                score += 1
                # Bonus for English text
                if val.isascii():
                    score += 1
        if score > best_score:
            best_score = score
            best_col = col
    return best_col


# ── Deduplication ─────────────���──────────────────────────────────────────────


def _dedup(entries: list[dict], key: str = "code") -> list[dict]:
    """Remove duplicate entries by key, keeping the first with a description."""
    seen: dict[str, dict] = {}
    for entry in entries:
        code = entry[key]
        if code not in seen or (not seen[code].get("description") and entry.get("description")):
            seen[code] = entry
    return list(seen.values())


# ── Public API ───────────────────────────────────────────────────────────────


def bootstrap_hs_codes(from_xls: str | None = None) -> None:
    """
    Fetch HS 8-digit codes and write to data/hs8_codes.csv.

    If from_xls is provided, parses the local file (XLS/XLSX/CSV).
    Otherwise tries: API probe -> page HTML scan -> Playwright DOM extraction.
    """
    codes: list[dict] | None = None

    # Strategy 0: Local file
    if from_xls:
        xls_path = Path(from_xls)
        if not xls_path.exists():
            raise FileNotFoundError(f"File not found: {xls_path}")
        logger.info(f"Parsing HS codes from local file: {xls_path}")
        codes = _hs_codes_from_xls(xls_path)
    else:
        logger.info("Bootstrapping HS8 codes from stats.customs.gov.cn ...")
        session = _requests_session()

        # Strategy 1a: API endpoint probe
        logger.info("Strategy 1: Probing API endpoints ...")
        codes = _probe_api_endpoints(
            session, _HS_API_CANDIDATES, _CODE_KEYS, _NAME_KEYS, "HS code")

        # Strategy 1b: Embedded JSON in page HTML
        if codes is None:
            logger.info("No API found, scanning page HTML for embedded data ...")
            codes = _probe_page_html(session, _CODE_KEYS, _NAME_KEYS, "HS code")

        # Strategy 2: Playwright
        if codes is None:
            logger.info("HTML scan failed, trying Playwright DOM extraction ...")
            codes = _extract_hs_via_playwright()

    # Validate
    if not codes:
        raise RuntimeError(
            "Could not bootstrap HS codes from stats.customs.gov.cn.\n\n"
            "Possible causes:\n"
            "  - No proxy configured (set CUSTOMS_PROXY_URL in .env)\n"
            "  - Site structure has changed\n"
            "  - Site is temporarily unreachable\n\n"
            "Manual alternatives:\n"
            "  1. Run --debug-browser to inspect the site manually\n"
            "  2. Download tariff schedule XLS and run:\n"
            "     python -m customs_scraper.main --bootstrap-hs-codes --from-xls <file>\n"
            "  3. Save as data/hs8_codes.csv with columns: code, description"
        )

    # Filter to valid 8-digit numeric codes and deduplicate
    valid = [c for c in codes if len(c["code"]) == 8 and c["code"].isdigit()]
    valid = _dedup(valid)

    if not valid:
        raise RuntimeError(
            f"Found {len(codes)} entries but none were valid 8-digit codes.\n"
            "The site may use a different code format. "
            "Run --debug-browser to inspect."
        )

    if len(valid) < 100:
        logger.warning(
            f"Only {len(valid)} HS8 codes found (expected ~9000). "
            "The bootstrap may have only captured a partial set."
        )

    _write_csv(_HS_CSV, valid, ["code", "description"])
    logger.info(f"Bootstrap complete: {len(valid)} HS8 codes -> {_HS_CSV}")


def bootstrap_countries() -> None:
    """
    Fetch GACC country/region codes from stats.customs.gov.cn
    and write to data/countries.csv.
    """
    logger.info("Bootstrapping country codes from stats.customs.gov.cn ...")
    session = _requests_session()
    countries: list[dict] | None = None

    # Strategy 1a: API endpoint probe
    logger.info("Strategy 1: Probing API endpoints ...")
    countries = _probe_api_endpoints(
        session, _COUNTRY_API_CANDIDATES,
        _COUNTRY_CODE_KEYS, _COUNTRY_NAME_KEYS, "country")

    # Strategy 1b: Embedded JSON in page HTML
    if countries is None:
        logger.info("No API found, scanning page HTML for embedded data ...")
        countries = _probe_page_html(
            session, _COUNTRY_CODE_KEYS, _COUNTRY_NAME_KEYS, "country")

    # Strategy 2: Playwright
    if countries is None:
        logger.info("HTML scan failed, trying Playwright DOM extraction ...")
        countries = _extract_countries_via_playwright()

    if not countries:
        raise RuntimeError(
            "Could not bootstrap country codes from stats.customs.gov.cn.\n\n"
            "Possible causes:\n"
            "  - No proxy configured (set CUSTOMS_PROXY_URL in .env)\n"
            "  - Site structure has changed\n"
            "  - Site is temporarily unreachable\n\n"
            "Manual alternatives:\n"
            "  1. Run --debug-browser to inspect the site manually\n"
            "  2. Find the GACC country code list in their published bulletins\n"
            "  3. Save as data/countries.csv with columns: code, name"
        )

    # Remap 'description' -> 'name' for countries CSV
    for c in countries:
        if "description" in c and "name" not in c:
            c["name"] = c.pop("description")

    countries = _dedup(countries)
    _write_csv(_COUNTRIES_CSV, countries, ["code", "name"])
    logger.info(f"Bootstrap complete: {len(countries)} countries -> {_COUNTRIES_CSV}")


# ── CSV writer ───────────────────────────────────────────────────────────────


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f"Wrote {len(rows)} rows to {path}")
