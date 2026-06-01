"""
OECD trade, production, and FDI data fetcher.
Uses the stats.oecd.org legacy SDMX-JSON API (no auth required).

Table: oecd_series in data/trade_stats.db

Datasets fetched:
  STAN08BIS           OECD STAN structural analysis — production, value added,
                      exports, imports by industry (ISIC rev.4, ~40 OECD + G20)
  FDI_FLOW_PARTNER    FDI inflows and outflows by reporter × partner country (USD)

Note on TiVA:
  The TIVA_2023 dataset was removed from stats.oecd.org. A dedicated
  TiVA module can be added once OECD migrates it to their new SDMX endpoint
  (https://sdmx.oecd.org). The STAN data provides the underlying gross
  production and value-added figures that TiVA is derived from.
"""

import logging
import sqlite3
import time
from datetime import datetime, timezone

import requests

from policy_monitor.wits import get_trade_stats_db  # reuse shared DB

log = logging.getLogger("oecd_tiva")

_OECD_BASE = "https://stats.oecd.org/SDMX-JSON/data"

_SCHEMA_EXTRA = """
CREATE TABLE IF NOT EXISTS oecd_series (
    reporter     TEXT    NOT NULL,
    partner      TEXT    NOT NULL DEFAULT '',
    dataset      TEXT    NOT NULL,
    variable     TEXT    NOT NULL,
    industry     TEXT    NOT NULL DEFAULT '',
    year         INTEGER NOT NULL,
    value        REAL,
    fetched_at   TEXT    NOT NULL,
    PRIMARY KEY (reporter, partner, dataset, variable, industry, year)
);
CREATE INDEX IF NOT EXISTS idx_oecd_reporter ON oecd_series(reporter);
CREATE INDEX IF NOT EXISTS idx_oecd_dataset  ON oecd_series(dataset);
CREATE INDEX IF NOT EXISTS idx_oecd_year     ON oecd_series(year);
"""

# OECD STAN variables of interest
# PROD=Production, VALU=Value Added, EXPO=Exports, IMPO=Imports,
# GFCF=Gross Fixed Capital Formation, INTI=Intermediate Inputs
_STAN_VARIABLES = ["PROD", "VALU", "EXPO", "IMPO", "GFCF"]

# ISIC Rev.4 sectors (total + key sectors)
_STAN_INDUSTRIES = [
    "DTOTAL",  # Total economy
    "D",       # Manufacturing total (ISIC C)
    "C",       # Manufacturing total (alt code)
    "G",       # Wholesale & retail trade
    "H",       # Transport & storage
    "J",       # ICT / information & communication
    "K",       # Financial activities
    "M",       # Professional & scientific
    "D26",     # Computer, electronic, optical products
    "D29",     # Motor vehicles
    "D30",     # Other transport equipment
]

# Countries to request from STAN
_STAN_COUNTRIES = [
    "CHN", "USA", "DEU", "JPN", "KOR", "GBR", "FRA", "ITA", "CAN",
    "AUS", "NLD", "ESP", "MEX", "TUR", "POL", "IND", "BRA", "RUS",
    "IDN", "SAU", "SWE", "CHE", "NOR", "DNK", "FIN", "AUT", "BEL",
    "NZL", "ISR", "CZE", "HUN", "SVK", "PRT", "GRC", "IRL", "LUX",
]

# FDI flows: reporter = investee country, partner = investor country
# FLOW: IN (inward/liabilities), OUT (outward/assets)
_FDI_FLOWS = ["IN", "OUT"]
_FDI_CURRENCIES = ["USD"]  # USD values

HISTORY_YEARS = 10
MAX_YEAR = 2023
START_YEAR = MAX_YEAR - HISTORY_YEARS


def _ensure_schema(conn: sqlite3.Connection):
    conn.executescript(_SCHEMA_EXTRA)


def _get_json(url: str, params: dict | None = None, retries: int = 3) -> dict | None:
    hdrs = {"User-Agent": "CMM/1.0 ha.boehm@web.de", "Accept": "application/json"}
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=hdrs, timeout=60)
            if r.status_code == 404:
                return None
            if r.status_code in (429, 503):
                time.sleep(30 * (2 ** attempt))
                continue
            if r.status_code >= 400:
                log.debug(f"  HTTP {r.status_code}: {r.text[:60]}")
                return None
            r.raise_for_status()
            return r.json()
        except requests.exceptions.Timeout:
            time.sleep(20 * (attempt + 1))
        except Exception as exc:
            if attempt == retries - 1:
                log.warning(f"  [error] {exc}")
    return None


def _parse_sdmx_json(j: dict) -> list[dict]:
    """Parse stats.oecd.org SDMX-JSON (data.structures plural) into flat rows."""
    data = j.get("data", j)
    datasets  = data.get("dataSets", [])
    structures = data.get("structures", [])
    struct = structures[0] if structures else {}
    dims_s = struct.get("dimensions", {}).get("series", [])
    dims_o = struct.get("dimensions", {}).get("observation", [])

    rows: list[dict] = []
    for ds in datasets:
        for s_key, s_data in (ds.get("series") or {}).items():
            parts = s_key.split(":")
            dim_vals: dict[str, str] = {}
            for i, dim in enumerate(dims_s):
                if i < len(parts):
                    vals = dim.get("values", [])
                    idx  = int(parts[i])
                    if idx < len(vals):
                        dim_vals[dim["id"]] = vals[idx].get("id", "")
            for o_key, o_data in (s_data.get("observations") or {}).items():
                o_parts = o_key.split(":")
                time_val = ""
                for i, dim in enumerate(dims_o):
                    if i < len(o_parts):
                        vals = dim.get("values", [])
                        idx  = int(o_parts[i])
                        if idx < len(vals):
                            time_val = vals[idx].get("id", "")
                value = o_data[0] if o_data else None
                if value is not None and time_val:
                    rows.append({**dim_vals, "TIME_PERIOD": time_val, "value": float(value)})
    return rows


# ---------------------------------------------------------------------------
# OECD STAN — production and value added by industry
# ---------------------------------------------------------------------------

def fetch_stan(conn: sqlite3.Connection, years: list[int]) -> int:
    """Fetch OECD STAN08BIS. The API ignores filter dimensions and returns the
    full dataset; we apply country/variable/industry filters in Python."""
    now = datetime.now(timezone.utc).isoformat()
    start = min(years)
    end   = max(years)
    total = 0

    _country_set  = set(_STAN_COUNTRIES)
    _variable_set = set(_STAN_VARIABLES)
    _industry_set = set(_STAN_INDUSTRIES)

    log.info(f"  OECD STAN08BIS: fetching full dataset (filter in Python)")

    # Single call returns all countries/vars/industries — filter below
    j = _get_json(
        f"{_OECD_BASE}/STAN08BIS/all/OECD",
        params={"startTime": str(start), "endTime": str(end), "contentType": "json"},
    )
    if not j:
        log.warning("  STAN08BIS: no data returned")
        return 0

    rows = _parse_sdmx_json(j)
    records = []
    for row in rows:
        reporter = row.get("COU", "")
        variable = row.get("VAR", "")
        industry = row.get("IND", "")
        time_val = row.get("TIME_PERIOD", "")
        value    = row.get("value")
        # Filter to countries and variables we care about
        if reporter not in _country_set:
            continue
        if variable not in _variable_set:
            continue
        if time_val and value is not None:
            try:
                records.append((reporter, "", "STAN08BIS", variable, industry,
                                int(time_val[:4]), float(value), now))
            except (ValueError, TypeError):
                pass

    conn.executemany(
        "INSERT OR REPLACE INTO oecd_series "
        "(reporter, partner, dataset, variable, industry, year, value, fetched_at) "
        "VALUES (?,?,?,?,?,?,?,?)",
        records,
    )
    conn.commit()
    total = len(records)
    log.info(f"  STAN: {total} records ({len(_country_set)} countries × {len(_variable_set)} vars filtered)")
    return total


# ---------------------------------------------------------------------------
# OECD FDI flows by partner country
# ---------------------------------------------------------------------------

def fetch_fdi(conn: sqlite3.Connection, years: list[int]) -> int:
    now = datetime.now(timezone.utc).isoformat()
    start = min(years)
    end   = max(years)
    total = 0

    log.info(f"  OECD FDI_FLOW_PARTNER: {len(_FDI_FLOWS)} flow directions …")

    _country_set = set(_STAN_COUNTRIES)

    # API ignores filters — fetch all and filter in Python (USD currency only)
    j = _get_json(
        f"{_OECD_BASE}/FDI_FLOW_PARTNER/all/OECD",
        params={"startTime": str(start), "endTime": str(end), "contentType": "json"},
    )
    if not j:
        log.warning("  FDI_FLOW_PARTNER: no data")
        return 0

    rows = _parse_sdmx_json(j)
    records = []
    for row in rows:
        # Dims: FLOW (IN/OUT), PC (partner country), CUR (currency), COU (reporter)
        reporter = row.get("COU", "")
        partner  = row.get("PC", "")
        flow_dir = row.get("FLOW", "")
        currency = row.get("CUR", "")
        time_val = row.get("TIME_PERIOD", "")
        value    = row.get("value")
        if reporter not in _country_set:
            continue
        if currency != "USD":
            continue
        if time_val and value is not None:
            variable = f"fdi_{flow_dir.lower()}_usd"
            try:
                records.append((reporter, partner, "FDI_FLOW_PARTNER", variable, "",
                                int(time_val[:4]), float(value), now))
            except (ValueError, TypeError):
                pass

    conn.executemany(
        "INSERT OR REPLACE INTO oecd_series "
        "(reporter, partner, dataset, variable, industry, year, value, fetched_at) "
        "VALUES (?,?,?,?,?,?,?,?)",
        records,
    )
    conn.commit()
    total = len(records)
    log.info(f"  FDI: {total} records ({len(_country_set)} reporters filtered, USD only)")
    return total


def fetch_all(conn: sqlite3.Connection, force_full: bool = False) -> int:
    _ensure_schema(conn)
    try:
        stored = {r[0] for r in conn.execute("SELECT DISTINCT year FROM oecd_series").fetchall()}
    except sqlite3.OperationalError:
        stored = set()

    target = [MAX_YEAR - i for i in range(HISTORY_YEARS)]
    years = target if (force_full or not stored) else [MAX_YEAR]

    log.info(f"OECD data — years: {years}")
    n1 = fetch_stan(conn, years)
    n2 = fetch_fdi(conn, years)
    log.info(f"  OECD total: {n1 + n2} rows")
    return n1 + n2
