"""
WITS / UNCTAD TRAINS tariff data fetcher.

Primary:  WITS SDMX API (requires WITS_API_KEY — free at wits.worldbank.org)
Fallback: World Bank WDI tariff aggregate indicators (no key required)

Table: unctad_series in data/trade_stats.db

WITS datasets fetched (when key available):
  MFN tariff — simple average by reporter × HS2 chapter × year
  Applied tariff — simple average by reporter × HS2 chapter × year

WB fallback (always fetched):
  TM.TAX.MRCH.SM.AR.ZS   applied tariff, simple mean, all products
  TM.TAX.MRCH.WM.AR.ZS   applied tariff, weighted mean, all products
  TM.TAX.MANP.SM.AR.ZS   applied tariff, simple mean, manufactures
  TM.TAX.TCOM.SM.AR.ZS   applied tariff, ICT/capital goods
  TM.TAX.MRCH.BC.ZS      WTO binding coverage (% of tariff lines bound)
"""

import logging
import sqlite3
import time
from datetime import datetime, timezone

import requests

from policy_monitor.config import WITS_API_KEY, WITS_BASE_URL, WB_BASE_URL
from policy_monitor.storage import DB_DIR

log = logging.getLogger("wits")

TRADE_STATS_DB = DB_DIR / "trade_stats.db"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS unctad_series (
    reporter_iso  TEXT    NOT NULL,
    hs_code       TEXT    NOT NULL DEFAULT '',
    year          INTEGER NOT NULL,
    indicator     TEXT    NOT NULL,
    value         REAL,
    source        TEXT    NOT NULL DEFAULT 'WITS',
    fetched_at    TEXT    NOT NULL,
    PRIMARY KEY (reporter_iso, hs_code, year, indicator, source)
);
CREATE INDEX IF NOT EXISTS idx_unctad_reporter ON unctad_series(reporter_iso);
CREATE INDEX IF NOT EXISTS idx_unctad_year     ON unctad_series(year);
CREATE INDEX IF NOT EXISTS idx_unctad_hs       ON unctad_series(hs_code);
"""

# HS2 chapters 01–97
_HS2_CODES = [f"{i:02d}" for i in range(1, 98)]

# WITS tariff indicators
_WITS_INDICATORS = {
    "MFN-SMPL-AVRG":  "mfn_tariff_simple_avg",
    "AHS-SMPL-AVRG":  "applied_tariff_simple_avg",
    "MFN-WGHTD-AVRG": "mfn_tariff_weighted_avg",
}

# World Bank WDI fallback indicators (no auth needed)
_WB_INDICATORS = {
    "TM.TAX.MRCH.SM.AR.ZS": "applied_tariff_simple_avg",
    "TM.TAX.MRCH.WM.AR.ZS": "applied_tariff_weighted_avg",
    "TM.TAX.MANP.SM.AR.ZS": "applied_tariff_mfg_simple_avg",
    "TM.TAX.TCOM.SM.AR.ZS": "applied_tariff_ict_simple_avg",
    "TM.TAX.MRCH.BC.ZS":    "wto_binding_coverage_pct",
}

HISTORY_YEARS = 10
MAX_YEAR = 2023


def get_trade_stats_db() -> sqlite3.Connection:
    DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(TRADE_STATS_DB))
    conn.executescript(_SCHEMA)
    return conn


def _stored_years_unctad(conn: sqlite3.Connection) -> set[int]:
    try:
        cur = conn.execute("SELECT DISTINCT year FROM unctad_series WHERE source='WITS'")
        return {r[0] for r in cur.fetchall()}
    except sqlite3.OperationalError:
        return set()


def _get_json(url: str, params: dict | None = None, headers: dict | None = None,
              retries: int = 3) -> dict | None:
    hdrs = {"User-Agent": "CMM/1.0 ha.boehm@web.de", **(headers or {})}
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=hdrs, timeout=30)
            if r.status_code in (401, 403):
                log.warning(f"  [HTTP {r.status_code}] auth required for {url[:60]}")
                return None
            if r.status_code == 404:
                return None
            if r.status_code in (429, 503):
                wait = 30 * (2 ** attempt)
                log.warning(f"  [{r.status_code}] retrying in {wait}s")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.Timeout:
            time.sleep(10 * (attempt + 1))
        except Exception as exc:
            if attempt == retries - 1:
                log.warning(f"  [error] {exc}")
    return None


def _parse_sdmx_json(j: dict) -> list[dict]:
    """Parse OECD/WITS-style SDMX-JSON (structures plural) into flat rows."""
    data = j.get("data", j)
    datasets = data.get("dataSets", [])
    structs = data.get("structures", data.get("structure", {}))
    struct = structs[0] if isinstance(structs, list) and structs else structs
    dims_s = (struct.get("dimensions") or {}).get("series", [])
    dims_o = (struct.get("dimensions") or {}).get("observation", [])

    rows: list[dict] = []
    for ds in datasets:
        for s_key, s_data in (ds.get("series") or {}).items():
            parts = s_key.split(":")
            dim_vals: dict[str, str] = {}
            for i, dim in enumerate(dims_s):
                if i < len(parts):
                    vals = dim.get("values", [])
                    idx = int(parts[i])
                    if idx < len(vals):
                        dim_vals[dim["id"]] = vals[idx].get("id", "")
            for o_key, o_data in (s_data.get("observations") or {}).items():
                o_parts = o_key.split(":")
                time_val = ""
                for i, dim in enumerate(dims_o):
                    if i < len(o_parts):
                        vals = dim.get("values", [])
                        idx = int(o_parts[i])
                        if idx < len(vals):
                            time_val = vals[idx].get("id", "")
                value = o_data[0] if o_data else None
                if value is not None:
                    rows.append({**dim_vals, "TIME_PERIOD": time_val, "value": float(value)})
    return rows


# ---------------------------------------------------------------------------
# WITS tariff fetch (requires WITS_API_KEY)
# ---------------------------------------------------------------------------

def fetch_wits_tariff(conn: sqlite3.Connection, years: list[int]) -> int:
    if not WITS_API_KEY:
        log.info("  WITS_API_KEY not set — skipping WITS HS-level tariffs")
        log.info("  Register at wits.worldbank.org → My Account → API Access")
        return 0

    now = datetime.now(timezone.utc).isoformat()
    total = 0
    auth_hdr = {"Authorization": f"Bearer {WITS_API_KEY}"}

    for year in years:
        for wits_ind, col_name in _WITS_INDICATORS.items():
            url = f"{WITS_BASE_URL}/tariff/reporter/ALL/year/{year}/partner/WLD/product/AG2/indicator/{wits_ind}"
            j = _get_json(url, params={"format": "JSON"}, headers=auth_hdr)
            if not j:
                continue
            rows = _parse_sdmx_json(j)
            records = []
            for row in rows:
                reporter = row.get("reporter", row.get("REPORTER", ""))
                hs = row.get("product", row.get("PRODUCT", ""))
                val = row.get("value")
                tp = row.get("TIME_PERIOD", str(year))
                if reporter and val is not None:
                    records.append((reporter, hs, int(tp[:4]), col_name, float(val), "WITS", now))

            conn.executemany(
                "INSERT OR REPLACE INTO unctad_series "
                "(reporter_iso, hs_code, year, indicator, value, source, fetched_at) "
                "VALUES (?,?,?,?,?,?,?)",
                records,
            )
            conn.commit()
            total += len(records)
            log.info(f"  WITS {year} {wits_ind}: {len(records)} records")
            time.sleep(1.0)

    return total


# ---------------------------------------------------------------------------
# World Bank tariff fallback (always runs, no key needed)
# ---------------------------------------------------------------------------

def fetch_wb_tariff(conn: sqlite3.Connection, years: list[int]) -> int:
    now = datetime.now(timezone.utc).isoformat()
    total = 0
    start_year = min(years)
    end_year   = max(years)

    for wb_code, col_name in _WB_INDICATORS.items():
        log.info(f"  WB tariff: {wb_code} → {col_name}")
        page = 1
        rows_all: list[tuple] = []
        while True:
            j = _get_json(
                f"{WB_BASE_URL}/{wb_code}",
                params={"format": "json", "per_page": 1000,
                        "date": f"{start_year}:{end_year}", "page": page},
            )
            if not j or len(j) < 2:
                break
            meta, data = j[0], j[1]
            for item in (data or []):
                iso3 = item.get("countryiso3code", "")
                yr   = item.get("date", "")
                val  = item.get("value")
                if iso3 and yr and val is not None:
                    try:
                        rows_all.append((iso3, "", int(yr), col_name, float(val), "WB", now))
                    except (ValueError, TypeError):
                        pass
            if page >= meta.get("pages", 1):
                break
            page += 1
            time.sleep(0.3)

        conn.executemany(
            "INSERT OR REPLACE INTO unctad_series "
            "(reporter_iso, hs_code, year, indicator, value, source, fetched_at) "
            "VALUES (?,?,?,?,?,?,?)",
            rows_all,
        )
        conn.commit()
        total += len(rows_all)

    return total


def fetch_all(conn: sqlite3.Connection, force_full: bool = False) -> int:
    stored = _stored_years_unctad(conn)
    target = [MAX_YEAR - i for i in range(HISTORY_YEARS)]
    years = target if (force_full or not stored) else [MAX_YEAR]

    log.info(f"WITS/UNCTAD tariff — years: {years}")
    n1 = fetch_wits_tariff(conn, years)
    n2 = fetch_wb_tariff(conn, years)
    return n1 + n2
