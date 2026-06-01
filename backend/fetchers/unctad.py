"""
China bilateral merchandise trade fetcher (UN Comtrade+ public preview).

Downloads China's bilateral trade flows by HS-2 chapter from the UN Comtrade+
public preview API (https://comtradeapi.un.org/public/v1/preview/).
No API key required.

Reporter: CHN (UN M49 code 156)
Partners: ~200 economies
Flows: X (exports) and M (imports)

Stores in data/unctad_trade.db.

Usage:
    python -m backend.fetchers.unctad                # incremental (missing years only)
    python -m backend.fetchers.unctad --full         # all HISTORY_YEARS
    python -m backend.fetchers.unctad --year 2022    # specific year
    python -m backend.fetchers.unctad --force        # re-fetch already-stored years
"""

import argparse
import logging
import sqlite3
import time
from datetime import datetime, timezone

import requests

from backend.storage import DB_DIR

log = logging.getLogger("unctad")

_BASE     = "https://comtradeapi.un.org/public/v1/preview/C/A/HS"
_REF_PARTNERS = "https://comtradeapi.un.org/files/v1/app/reference/partnerAreas.json"
_HEADERS  = {"User-Agent": "CMM/1.0", "Accept": "application/json"}
_CHN_CODE = 156   # UN M49 code for China mainland

UNCTAD_DB    = DB_DIR / "unctad_trade.db"
MAX_YEAR     = 2023
HISTORY_YEARS = 8
DELAY_SEC     = 1.0    # base pause between requests; backs off on 429
_MAX_RETRIES  = 4

# HS-2 chapter descriptions (for display)
HS2_DESCRIPTIONS: dict[str, str] = {
    "01": "Live animals", "02": "Meat & offal", "03": "Fish & crustaceans",
    "04": "Dairy products", "05": "Other animal products", "06": "Trees & plants",
    "07": "Vegetables", "08": "Fruit & nuts", "09": "Coffee, tea, spices",
    "10": "Cereals", "11": "Milling products", "12": "Oil seeds",
    "13": "Lac & gums", "14": "Vegetable plaiting materials",
    "15": "Animal & vegetable fats", "16": "Meat & fish preparations",
    "17": "Sugars", "18": "Cocoa", "19": "Food preparations",
    "20": "Vegetable & fruit preparations", "21": "Miscellaneous foods",
    "22": "Beverages & vinegar", "23": "Food industry residues",
    "24": "Tobacco", "25": "Salt & cement minerals", "26": "Ores & slag",
    "27": "Mineral fuels & oils", "28": "Inorganic chemicals",
    "29": "Organic chemicals", "30": "Pharmaceuticals",
    "31": "Fertilizers", "32": "Tanning & dyeing extracts",
    "33": "Cosmetics & essential oils", "34": "Soaps & lubricants",
    "35": "Albuminoidal substances", "36": "Explosives",
    "37": "Photographic goods", "38": "Miscellaneous chemicals",
    "39": "Plastics", "40": "Rubber", "41": "Hides & skins",
    "42": "Leather articles", "43": "Furskins",
    "44": "Wood & articles", "45": "Cork", "46": "Straw articles",
    "47": "Pulp of wood", "48": "Paper & paperboard",
    "49": "Books & printed matter",
    "50": "Silk", "51": "Wool", "52": "Cotton",
    "53": "Vegetable textile fibres", "54": "Man-made filaments",
    "55": "Man-made staple fibres", "56": "Wadding & felt",
    "57": "Carpets", "58": "Special woven fabrics",
    "59": "Coated textile fabrics", "60": "Knitted fabrics",
    "61": "Knitted apparel", "62": "Woven apparel",
    "63": "Miscellaneous textiles", "64": "Footwear",
    "65": "Headgear", "66": "Umbrellas", "67": "Feathers & flowers",
    "68": "Stone & plaster articles", "69": "Ceramic products",
    "70": "Glass", "71": "Precious stones & metals",
    "72": "Iron & steel", "73": "Iron & steel articles",
    "74": "Copper", "75": "Nickel", "76": "Aluminium",
    "78": "Lead", "79": "Zinc", "80": "Tin",
    "81": "Other base metals", "82": "Tools & cutlery",
    "83": "Miscellaneous metal articles",
    "84": "Machinery & mechanical appliances",
    "85": "Electrical machinery & equipment",
    "86": "Railway locomotives",
    "87": "Motor vehicles & parts",
    "88": "Aircraft & spacecraft",
    "89": "Ships & boats",
    "90": "Optical & medical instruments",
    "91": "Clocks & watches", "92": "Musical instruments",
    "93": "Arms & ammunition",
    "94": "Furniture & bedding",
    "95": "Toys, games & sports",
    "96": "Miscellaneous manufactures",
    "97": "Works of art",
    "99": "Special classifications",
}

# HS-2 section labels (for dropdown grouping in the UI)
HS2_SECTIONS: list[dict] = [
    {"range": "01-05", "label": "I — Live animals & products"},
    {"range": "06-14", "label": "II — Vegetable products"},
    {"range": "15",    "label": "III — Animal & vegetable fats"},
    {"range": "16-24", "label": "IV — Foodstuffs, beverages, tobacco"},
    {"range": "25-27", "label": "V — Mineral products & fuels"},
    {"range": "28-38", "label": "VI — Chemicals"},
    {"range": "39-40", "label": "VII — Plastics & rubber"},
    {"range": "41-43", "label": "VIII — Hides & leather"},
    {"range": "44-46", "label": "IX — Wood & cork"},
    {"range": "47-49", "label": "X — Paper & pulp"},
    {"range": "50-63", "label": "XI — Textiles & apparel"},
    {"range": "64-67", "label": "XII — Footwear & headgear"},
    {"range": "68-71", "label": "XIII — Stone, glass & precious"},
    {"range": "72-83", "label": "XIV-XV — Metals"},
    {"range": "84-85", "label": "XVI — Machinery & electronics"},
    {"range": "86-89", "label": "XVII — Transport equipment"},
    {"range": "90-97", "label": "XVIII-XXI — Instruments & misc."},
]

# All valid HS-2 chapters (01-97 excluding 77 which is unassigned, plus 99)
_HS2_CHAPTERS = [f"{i:02d}" for i in range(1, 98) if i != 77] + ["99"]

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
_SCHEMA = """
CREATE TABLE IF NOT EXISTS unctad_trade (
    year         INTEGER NOT NULL,
    reporter_iso TEXT    NOT NULL,
    partner_iso  TEXT    NOT NULL,
    partner_name TEXT,
    hs2          TEXT    NOT NULL,
    hs2_desc     TEXT,
    flow         TEXT    NOT NULL,
    value_usd    REAL,
    fetched_at   TEXT    NOT NULL,
    PRIMARY KEY (year, reporter_iso, partner_iso, hs2, flow)
);
CREATE INDEX IF NOT EXISTS idx_unctad_year    ON unctad_trade(year);
CREATE INDEX IF NOT EXISTS idx_unctad_hs2     ON unctad_trade(hs2);
CREATE INDEX IF NOT EXISTS idx_unctad_flow    ON unctad_trade(flow);
CREATE INDEX IF NOT EXISTS idx_unctad_partner ON unctad_trade(partner_iso);

CREATE TABLE IF NOT EXISTS unctad_fetch_log (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at     TEXT    NOT NULL,
    year       INTEGER NOT NULL,
    rows_added INTEGER NOT NULL DEFAULT 0,
    ok         INTEGER NOT NULL DEFAULT 0,
    msg        TEXT
);
"""


def get_db() -> sqlite3.Connection:
    from backend.storage import get_conn as _storage_get_conn
    conn = _storage_get_conn()
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA)
    conn.commit()
    return conn


def _stored_chapters(conn: sqlite3.Connection) -> set[tuple[int, str, str]]:
    """Return set of (year, hs2, flow) combinations already present in the DB."""
    rows = conn.execute(
        "SELECT DISTINCT year, hs2, flow FROM unctad_trade WHERE reporter_iso='CHN'"
    ).fetchall()
    return {(r[0], r[1], r[2]) for r in rows}


# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------

def _load_partner_map() -> dict[int, tuple[str, str]]:
    """Return {m49_code → (iso3, name)} from UN Comtrade partner reference."""
    try:
        r = requests.get(_REF_PARTNERS, headers=_HEADERS, timeout=30)
        r.raise_for_status()
        items = r.json().get("results", [])
        result = {}
        for it in items:
            code = it.get("PartnerCode")
            iso3 = it.get("PartnerCodeIsoAlpha3", "")
            name = it.get("PartnerDesc", "")
            if code is not None and iso3 and not iso3.startswith("_"):
                result[int(code)] = (iso3, name)
        log.info(f"Loaded {len(result)} partner ISO mappings")
        return result
    except Exception as exc:
        log.warning(f"Partner map load failed: {exc}")
        return {}


# ---------------------------------------------------------------------------
# UN Comtrade public preview API
# ---------------------------------------------------------------------------

def _fetch_chapter(year: int, hs2: str, flow: str) -> list[dict]:
    """
    Fetch all bilateral flows for CHN as reporter for one HS-2 chapter and one flow.
    Returns list of dicts with keys: partner_code, value_usd.
    Retries on 429 / DNS / timeout with exponential backoff.
    """
    params = {
        "reporterCode": _CHN_CODE,
        "period":       str(year),
        "cmdCode":      hs2,
        "flowCode":     flow,
        "maxRecords":   500,
    }
    for attempt in range(_MAX_RETRIES):
        try:
            r = requests.get(_BASE, params=params, headers=_HEADERS, timeout=60)
            if r.status_code == 429:
                wait = 60 * (2 ** attempt)
                log.warning(f"    429 rate-limit → sleeping {wait}s")
                time.sleep(wait)
                continue
            if r.status_code == 404:
                return []
            r.raise_for_status()
            data = r.json()
            rows = []
            for item in data.get("data") or []:
                partner_code = item.get("partnerCode")
                if partner_code is None or partner_code == 0:
                    continue
                val = item.get("primaryValue") or item.get("fobvalue") or item.get("cifvalue")
                try:
                    val = float(val) if val is not None else None
                except (ValueError, TypeError):
                    val = None
                rows.append({"partner_code": int(partner_code), "value_usd": val})
            return rows
        except requests.exceptions.Timeout:
            wait = 30 * (attempt + 1)
            log.warning(f"    timeout (attempt {attempt+1}/{_MAX_RETRIES}) → sleep {wait}s")
            time.sleep(wait)
        except Exception as exc:
            if attempt < _MAX_RETRIES - 1:
                wait = 15 * (attempt + 1)
                log.warning(f"    error (attempt {attempt+1}): {exc} → sleep {wait}s")
                time.sleep(wait)
            else:
                raise
    return []


def _fetch_year(year: int, partner_map: dict[int, tuple[str, str]],
               conn: sqlite3.Connection, now: str,
               skip: set[tuple[int, str, str]] | None = None) -> int:
    """
    Fetch HS-2 bilateral flows for CHN as reporter for one year.
    Skips (year, hs2, flow) tuples already present in `skip`.
    Upserts to DB after each chapter so partial progress is preserved.
    Returns total rows inserted.
    """
    total = 0
    for flow in ("X", "M"):
        for hs2 in _HS2_CHAPTERS:
            if skip and (year, hs2, flow) in skip:
                continue
            try:
                raw = _fetch_chapter(year, hs2, flow)
                chapter_rows = []
                for r in raw:
                    code = r["partner_code"]
                    iso3, name = partner_map.get(code, (f"M49_{code}", f"Country {code}"))
                    chapter_rows.append({
                        "year":         year,
                        "reporter_iso": "CHN",
                        "partner_iso":  iso3,
                        "partner_name": name,
                        "hs2":          hs2,
                        "hs2_desc":     HS2_DESCRIPTIONS.get(hs2, ""),
                        "flow":         flow,
                        "value_usd":    r["value_usd"],
                        "fetched_at":   now,
                    })
                if chapter_rows:
                    conn.executemany(
                        "INSERT OR REPLACE INTO unctad_trade "
                        "(year, reporter_iso, partner_iso, partner_name, hs2, hs2_desc, flow, value_usd, fetched_at) "
                        "VALUES (:year,:reporter_iso,:partner_iso,:partner_name,:hs2,:hs2_desc,:flow,:value_usd,:fetched_at)",
                        chapter_rows,
                    )
                    conn.commit()
                    total += len(chapter_rows)
                log.debug(f"  hs2={hs2} flow={flow}: {len(chapter_rows)} rows")
                time.sleep(DELAY_SEC)
            except Exception as exc:
                log.warning(f"  chapter={hs2} flow={flow}: {exc}")
                time.sleep(DELAY_SEC)
    return total


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def fetch(years: list[int] | None = None, force: bool = False) -> dict:
    """
    Fetch UN Comtrade bilateral HS-2 trade data for CHN.

    Args:
        years: list of years to fetch; default = all HISTORY_YEARS
        force: if True, re-fetch all chapters regardless of DB state

    Incremental behaviour (default, no --force):
        - Skips individual (year, hs2, flow) combinations already in the DB.
        - A full reload only happens when the DB has no data at all.

    Returns:
        dict with keys: years_fetched, rows_added, errors
    """
    conn = get_db()
    stored = _stored_chapters(conn) if not force else set()

    if years is None:
        current_year = datetime.now(timezone.utc).year
        years = list(range(current_year - HISTORY_YEARS, MAX_YEAR + 1))

    # Keep only years that have at least one missing (hs2, flow) combination.
    all_combos = {(y, hs2, flow) for y in years for hs2 in _HS2_CHAPTERS for flow in ("X", "M")}
    missing = all_combos - stored
    to_fetch = sorted({y for y, _, _ in missing})

    if not to_fetch:
        log.info("UNCTAD — nothing to fetch; all chapters already stored")
        conn.close()
        return {"years_fetched": 0, "rows_added": 0, "errors": []}

    partner_map = _load_partner_map()
    log.info(f"UNCTAD — {len(missing)} missing chapter/flow combos across {len(to_fetch)} year(s): {to_fetch}")
    total_rows = 0
    errors: list[str] = []
    now = datetime.now(timezone.utc).isoformat()

    for yr in to_fetch:
        yr_missing = sum(1 for y, _, _ in missing if y == yr)
        try:
            log.info(f"  year={yr}: {yr_missing} chapter/flow combos to fetch …")
            added = _fetch_year(yr, partner_map, conn, now, skip=stored)
            total_rows += added
            conn.execute(
                "INSERT INTO unctad_fetch_log (run_at, year, rows_added, ok) VALUES (?,?,?,1)",
                (now, yr, added),
            )
            conn.commit()
            log.info(f"  year={yr}  rows={added}")
        except Exception as exc:
            msg = str(exc)
            errors.append(f"{yr}: {msg}")
            log.error(f"  year={yr}  ERROR: {msg}")
            conn.execute(
                "INSERT INTO unctad_fetch_log (run_at, year, rows_added, ok, msg) VALUES (?,?,0,0,?)",
                (now, yr, msg),
            )
            conn.commit()

    conn.close()
    return {"years_fetched": len(to_fetch), "rows_added": total_rows, "errors": errors}


# ---------------------------------------------------------------------------
# Read helpers (called by live/api.py)
# ---------------------------------------------------------------------------

def get_unctad_db() -> sqlite3.Connection:
    return get_db()


def get_available_years(conn: sqlite3.Connection) -> list[int]:
    rows = conn.execute(
        "SELECT DISTINCT year FROM unctad_trade WHERE reporter_iso='CHN' ORDER BY year"
    ).fetchall()
    return [r[0] for r in rows]


def get_hs2_codes(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT DISTINCT hs2, hs2_desc FROM unctad_trade WHERE reporter_iso='CHN' ORDER BY hs2"
    ).fetchall()
    return [{"hs2": r[0], "label": f"HS {r[0]} — {r[1]}"} for r in rows]


def get_trade_map(conn: sqlite3.Connection, year: int, flow: str, hs2: str | None) -> list[dict]:
    """
    Returns per-partner trade value for choropleth map.

    flow: 'X' (exports from China) or 'M' (imports to China)
    hs2:  '01'–'99' for a specific chapter, or None/'all' for totals
    """
    if hs2 and hs2.lower() != "all":
        rows = conn.execute(
            """SELECT partner_iso, partner_name, SUM(value_usd) AS value
               FROM unctad_trade
               WHERE year=? AND reporter_iso='CHN' AND flow=? AND hs2=?
                 AND value_usd IS NOT NULL
               GROUP BY partner_iso, partner_name""",
            (year, flow.upper(), hs2.zfill(2)),
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT partner_iso, partner_name, SUM(value_usd) AS value
               FROM unctad_trade
               WHERE year=? AND reporter_iso='CHN' AND flow=?
                 AND value_usd IS NOT NULL
               GROUP BY partner_iso, partner_name""",
            (year, flow.upper()),
        ).fetchall()
    return [{"iso3": r[0], "name": r[1], "value": r[2]} for r in rows]


def get_top_partners(conn: sqlite3.Connection, year: int, flow: str, hs2: str | None, n: int = 20) -> list[dict]:
    """Top N partners by trade value for the bar chart."""
    data = get_trade_map(conn, year, flow, hs2)
    return sorted(data, key=lambda x: x["value"] or 0, reverse=True)[:n]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")
    ap = argparse.ArgumentParser(description="Fetch UN Comtrade bilateral HS-2 trade data for China")
    ap.add_argument("--full",  action="store_true", help=f"Fetch last {HISTORY_YEARS} years")
    ap.add_argument("--year",  type=int, help="Fetch specific year")
    ap.add_argument("--force", action="store_true", help="Re-fetch even if already stored")
    args = ap.parse_args()

    if args.year:
        result = fetch(years=[args.year], force=args.force)
    elif args.full:
        current = datetime.now(timezone.utc).year
        all_years = list(range(current - HISTORY_YEARS, MAX_YEAR + 1))
        result = fetch(years=all_years, force=args.force)
    else:
        result = fetch(force=args.force)

    print(f"Done: {result}")