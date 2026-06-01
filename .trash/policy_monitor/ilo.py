"""
ILO STAT labour data fetcher.

Primary:  ILO SDMX REST API (https://sdmx.ilo.org/rest/)  — no auth needed
Fallback: World Bank WDI employment indicators            — no auth needed

Table: ilo_series in data/trade_stats.db

ILO dataflows fetched (when SDMX is available):
  DF_EMP_TEMP_SEX_ECO_NB   Employment by sex and economic activity (thousands)
  DF_UNE_TUNE_SEX_AGE_NB   Unemployment by sex and age (thousands)
  DF_GDP_211P_NOC_NB        GDP at constant prices (national currency)

World Bank fallback (always fetched):
  SL.EMP.TOTL.SP.ZS        Employment to population ratio, 15+ (%)
  SL.IND.EMPL.ZS           Employment in industry (% of total)
  SL.SRV.EMPL.ZS           Employment in services (% of total)
  SL.AGR.EMPL.ZS           Employment in agriculture (% of total)
  SL.UEM.TOTL.ZS           Unemployment (% of labour force)
  SL.EMP.MPYR.ZS           Employers (% of employment)
  SL.GDP.PCAP.EM.KD        GDP per person employed (constant 2017 PPP USD)
  SL.EMP.VULN.ZS           Vulnerable employment (% of total)
"""

import logging
import sqlite3
import time
from datetime import datetime, timezone

import requests

from policy_monitor.config import WB_BASE_URL
from policy_monitor.wits import get_trade_stats_db  # reuse shared DB

log = logging.getLogger("ilo")

_ILO_BASE = "https://sdmx.ilo.org/rest/data"

_SCHEMA_EXTRA = """
CREATE TABLE IF NOT EXISTS ilo_series (
    country       TEXT    NOT NULL,
    dataflow      TEXT    NOT NULL,
    indicator     TEXT    NOT NULL,
    sex           TEXT    NOT NULL DEFAULT 'T',
    year          INTEGER NOT NULL,
    value         REAL,
    unit          TEXT,
    source        TEXT    NOT NULL DEFAULT 'ILO',
    fetched_at    TEXT    NOT NULL,
    PRIMARY KEY (country, dataflow, indicator, sex, year, source)
);
CREATE INDEX IF NOT EXISTS idx_ilo_country   ON ilo_series(country);
CREATE INDEX IF NOT EXISTS idx_ilo_indicator ON ilo_series(indicator);
CREATE INDEX IF NOT EXISTS idx_ilo_year      ON ilo_series(year);
"""

# ILO dataflows: (flow_id, key_pattern, description)
# Key: {freq}.{country}.{sex}.{classif1}.{source}  — trailing components optional
_ILO_FLOWS = [
    ("DF_EMP_TEMP_SEX_ECO_NB", "A.{country}.SEX_T.ECO_AGGREGATE_TOTAL.BA_363", "employment_total_thousands"),
    ("DF_UNE_TUNE_SEX_AGE_NB", "A.{country}.SEX_T.AGE_AGGREGATE_TOTAL.BA_363", "unemployment_total_thousands"),
]

# Countries to fetch from ILO SDMX (major economies; ILO has country data for ~200+)
_ILO_COUNTRIES = [
    "CHN", "USA", "DEU", "JPN", "IND", "GBR", "FRA", "KOR", "ITA", "BRA",
    "CAN", "AUS", "ESP", "MEX", "IDN", "NLD", "SAU", "TUR", "RUS", "ZAF",
    "ARG", "POL", "THA", "MYS", "VNM", "BGD", "PAK", "NGA", "ETH", "EGY",
]

# World Bank employment indicators (reliable fallback for all countries)
_WB_INDICATORS = {
    "SL.EMP.TOTL.SP.ZS":   "employment_to_pop_ratio_pct",
    "SL.IND.EMPL.ZS":      "employment_industry_pct",
    "SL.SRV.EMPL.ZS":      "employment_services_pct",
    "SL.AGR.EMPL.ZS":      "employment_agriculture_pct",
    "SL.UEM.TOTL.ZS":      "unemployment_rate_pct",
    "SL.GDP.PCAP.EM.KD":   "gdp_per_worker_ppp_usd",
    "SL.EMP.VULN.ZS":      "vulnerable_employment_pct",
    "SL.EMP.MPYR.ZS":      "employers_pct_employment",
    "SL.TLF.CACT.ZS":      "labour_force_participation_rate_pct",
    "SL.TLF.CACT.FE.ZS":   "labour_force_participation_female_pct",
    "SL.EMP.SELF.ZS":      "self_employed_pct",
    "SL.EMP.WORK.ZS":      "wage_salary_workers_pct",
}

HISTORY_YEARS = 15
MAX_YEAR = 2024
START_YEAR = MAX_YEAR - HISTORY_YEARS


def _ensure_schema(conn: sqlite3.Connection):
    conn.executescript(_SCHEMA_EXTRA)


def _get_json(url: str, params: dict | None = None, retries: int = 3) -> dict | list | None:
    hdrs = {"User-Agent": "CMM/1.0 ha.boehm@web.de", "Accept": "application/json"}
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=hdrs, timeout=30)
            if r.status_code == 404:
                return None
            if r.status_code in (429, 503):
                time.sleep(30 * (2 ** attempt))
                continue
            if r.status_code >= 400:
                log.debug(f"  HTTP {r.status_code} for {url[:80]}: {r.text[:60]}")
                return None
            r.raise_for_status()
            return r.json()
        except requests.exceptions.Timeout:
            time.sleep(10 * (attempt + 1))
        except Exception as exc:
            if attempt == retries - 1:
                log.debug(f"  error: {exc}")
    return None


def _parse_ilo_sdmx(j: dict, flow_id: str, indicator: str) -> list[dict]:
    """Parse ILO SDMX-JSON (data.structure singular format)."""
    data = j.get("data", j)
    datasets = data.get("dataSets", [])
    struct = data.get("structure", {})
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
                    rows.append({
                        "country":   dim_vals.get("REF_AREA", ""),
                        "dataflow":  flow_id,
                        "indicator": indicator,
                        "sex":       dim_vals.get("SEX", "T"),
                        "year":      int(time_val[:4]),
                        "value":     float(value),
                    })
    return rows


# ---------------------------------------------------------------------------
# ILO SDMX fetch
# ---------------------------------------------------------------------------

def fetch_ilo_sdmx(conn: sqlite3.Connection, years: list[int]) -> int:
    now = datetime.now(timezone.utc).isoformat()
    total = 0
    start = min(years)
    end   = max(years)

    for flow_id, key_tpl, indicator in _ILO_FLOWS:
        log.info(f"  ILO SDMX {flow_id} ({indicator}) …")
        n_flow = 0
        for country in _ILO_COUNTRIES:
            key = key_tpl.format(country=country)
            j = _get_json(
                f"{_ILO_BASE}/ILO,{flow_id}/{key}",
                params={
                    "format": "jsondata",
                    "detail": "dataonly",
                    "startPeriod": str(start),
                    "endPeriod":   str(end),
                },
            )
            if not j:
                continue
            rows = _parse_ilo_sdmx(j, flow_id, indicator)
            if not rows:
                continue
            records = [
                (r["country"], r["dataflow"], r["indicator"], r.get("sex", "T"),
                 r["year"], r["value"], "", "ILO", now)
                for r in rows
            ]
            conn.executemany(
                "INSERT OR REPLACE INTO ilo_series "
                "(country, dataflow, indicator, sex, year, value, unit, source, fetched_at) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                records,
            )
            conn.commit()
            n_flow += len(records)
            time.sleep(0.2)

        log.info(f"    {n_flow} records stored")
        total += n_flow

    return total


# ---------------------------------------------------------------------------
# World Bank employment fallback
# ---------------------------------------------------------------------------

def fetch_wb_employment(conn: sqlite3.Connection, years: list[int]) -> int:
    now = datetime.now(timezone.utc).isoformat()
    total = 0

    for wb_code, col_name in _WB_INDICATORS.items():
        log.info(f"  WB employment: {wb_code} → {col_name}")
        page = 1
        records: list[tuple] = []
        while True:
            j = _get_json(
                f"{WB_BASE_URL}/{wb_code}",
                params={"format": "json", "per_page": 1000,
                        "date": f"{START_YEAR}:{MAX_YEAR}", "page": page},
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
                        records.append((iso3, "WB_WDI", col_name, "T",
                                        int(yr), float(val), "%", "WB", now))
                    except (ValueError, TypeError):
                        pass
            if page >= meta.get("pages", 1):
                break
            page += 1
            time.sleep(0.3)

        conn.executemany(
            "INSERT OR REPLACE INTO ilo_series "
            "(country, dataflow, indicator, sex, year, value, unit, source, fetched_at) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            records,
        )
        conn.commit()
        total += len(records)

    return total


def fetch_all(conn: sqlite3.Connection, force_full: bool = False) -> int:
    _ensure_schema(conn)
    try:
        stored = {r[0] for r in conn.execute("SELECT DISTINCT year FROM ilo_series WHERE source='ILO'").fetchall()}
    except sqlite3.OperationalError:
        stored = set()

    target = [MAX_YEAR - i for i in range(HISTORY_YEARS)]
    years = target if (force_full or not stored) else [MAX_YEAR]

    log.info(f"ILO labour data — years: {years}")
    n1 = fetch_ilo_sdmx(conn, years)
    n2 = fetch_wb_employment(conn, years)
    log.info(f"  ILO total: {n1} SDMX rows + {n2} WB rows")
    return n1 + n2
