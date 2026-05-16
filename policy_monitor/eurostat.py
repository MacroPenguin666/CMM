"""
Eurostat fetcher for EU-China competitive intelligence.

Two core use cases:
  1. European SMEs — trade exposure, cost gap, innovation gap vs China
  2. Policy / Regulatory — strategic dependencies, tech transfer, import competition

Datasets fetched (all verified against live Eurostat API 2025-05):
  ext_lt_maineu  Annual EU-China trade by SITC sector (exports, imports, balance)
  sts_inpr_a     EU industrial production index by NACE (sector competitiveness)
  rd_e_gerdtot   EU gross R&D expenditure (% GDP, by performing sector)
  pat_ep_ntec    EPO patent applications by technology class
  lc_lci_r2_a   EU labour cost index by NACE sector

API: https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{dataset}
Format: JSON-STAT 2.0

Usage:
    python -m policy_monitor.eurostat             # fetch all datasets
    python -m policy_monitor.eurostat --show      # list stored datasets
    python -m policy_monitor.eurostat --dataset ext_lt_maineu
    python -m policy_monitor.eurostat --indicators rd_e_gerdtot
"""

import argparse
import logging
import sqlite3
from datetime import datetime
from pathlib import Path

import requests

from policy_monitor.storage import DB_DIR

log = logging.getLogger("eurostat")

EUROSTAT_BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
HEADERS = {"Accept": "application/json", "User-Agent": "CMM/1.0 ha.boehm@web.de"}
TIMEOUT = 120

DB_PATH = DB_DIR / "eurostat.db"

# ---------------------------------------------------------------------------
# DB schema
# ---------------------------------------------------------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS eurostat_series (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset     TEXT NOT NULL,
    indicator   TEXT NOT NULL,
    geo         TEXT NOT NULL DEFAULT '',
    partner     TEXT NOT NULL DEFAULT '',
    nace        TEXT NOT NULL DEFAULT '',
    unit        TEXT NOT NULL DEFAULT '',
    time_period TEXT NOT NULL,
    year        INTEGER NOT NULL,
    value       REAL NOT NULL,
    fetched_at  TEXT NOT NULL,
    UNIQUE(dataset, indicator, geo, partner, nace, unit, time_period)
);

CREATE INDEX IF NOT EXISTS idx_es_dataset_year ON eurostat_series(dataset, year);

CREATE TABLE IF NOT EXISTS eurostat_labels (
    dataset     TEXT NOT NULL,
    dim_name    TEXT NOT NULL,
    code        TEXT NOT NULL,
    label       TEXT,
    PRIMARY KEY (dataset, dim_name, code)
);

CREATE TABLE IF NOT EXISTS eurostat_fetch_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset      TEXT NOT NULL,
    ok           INTEGER NOT NULL,
    rows_fetched INTEGER DEFAULT 0,
    rows_stored  INTEGER DEFAULT 0,
    error        TEXT,
    fetched_at   TEXT NOT NULL
);
"""


def get_eurostat_db() -> sqlite3.Connection:
    DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.executescript(SCHEMA)
    return conn


# ---------------------------------------------------------------------------
# Dataset fetch configurations
#
# All dimension names and partner codes verified against live Eurostat API.
#
# indicator_dim : dimension whose values become the "indicator" key in our DB
# geo_dim       : geography dimension (almost always "geo")
# partner_dim   : bilateral partner dimension, if any
# nace_dim      : sector classification dimension, if any
# unit_dim      : unit-of-measure dimension, if any
# ---------------------------------------------------------------------------
FETCH_CONFIGS: list[dict] = [
    {
        "dataset": "ext_lt_maineu",
        "label": "EU-China Trade by Sector",
        "description": (
            "Annual EU bilateral trade with China (excl. Hong Kong) by SITC sector. "
            "Indicators: trade balance, exports, imports, EU import/export share. "
            "Sectors: total, food, raw materials, energy, chemicals, mfg goods, machinery, misc. "
            "Core metric for SME import-competition exposure and sector-level policy analysis."
        ),
        "use_cases": ["sme", "policy"],
        "params": {
            "freq": "A",
            "partner": "CN_X_HK",
            "geo": "EU27_2020",
            "sinceTimePeriod": "2010",
        },
        "indicator_dim": "indic_et",  # MIO_BAL_VAL, MIO_EXP_VAL, MIO_IMP_VAL, PC_IMP_PART, PC_EXP_PART
        "geo_dim": "geo",
        "partner_dim": "partner",
        "nace_dim": "sitc06",         # SITC sector breakdown
        "unit_dim": None,
    },
    {
        "dataset": "sts_inpr_a",
        "label": "EU Industrial Production Index by Sector",
        "description": (
            "Annual EU industrial production index by NACE sector. "
            "Tracks EU manufacturing output trends — identifies sectors where EU is losing "
            "ground to Chinese competition and those maintaining strength."
        ),
        "use_cases": ["sme", "policy"],
        "params": {
            "freq": "A",
            "geo": "EU27_2020",
            "indic_bt": "PRD",         # production index
            "s_adj": "CA",             # calendar adjusted
            "unit": "I15",             # index 2015=100
            "sinceTimePeriod": "2010",
        },
        "indicator_dim": "nace_r2",   # sector is the key differentiator here
        "geo_dim": "geo",
        "partner_dim": None,
        "nace_dim": None,
        "unit_dim": "unit",
    },
    {
        "dataset": "rd_e_gerdtot",
        "label": "EU Gross R&D Expenditure (GERD, % GDP)",
        "description": (
            "EU R&D investment as % of GDP by performing sector "
            "(total, business, government, higher education, private non-profit). "
            "Benchmarks EU innovation capacity; business-sector GERD is the key "
            "metric for private-sector competitiveness vs China."
        ),
        "use_cases": ["sme", "policy"],
        "params": {
            "freq": "A",
            "geo": "EU27_2020",
            "unit": "PC_GDP",
            "sinceTimePeriod": "2010",
        },
        "indicator_dim": "sectperf",  # TOTAL, BES, GOV, HES, PNP
        "geo_dim": "geo",
        "partner_dim": None,
        "nace_dim": None,
        "unit_dim": "unit",
    },
    {
        "dataset": "pat_ep_ntec",
        "label": "EU Patent Applications by Technology (EPO)",
        "description": (
            "Patent applications filed at the EPO by high-tech IPC class. "
            "Categories: high-tech total, aviation, computer/IT, communications, "
            "laser, biotech, semiconductors. "
            "Tracks EU innovation output in sectors where China is catching up fastest."
        ),
        "use_cases": ["sme", "policy"],
        "params": {
            "freq": "A",
            "geo": "EU27_2020",
            "sinceTimePeriod": "2010",
        },
        "indicator_dim": "ipc",       # HT, AVI, CAB, CTE, LSR, MGE, SMC
        "geo_dim": "geo",
        "partner_dim": None,
        "nace_dim": None,
        "unit_dim": "unit",
    },
    {
        "dataset": "lc_lci_r2_a",
        "label": "EU Labour Cost Index by Sector (NACE Rev.2)",
        "description": (
            "Annual EU labour cost index by NACE sector and cost component. "
            "Labour costs are the primary cost-competitiveness metric for EU SMEs "
            "vs Chinese manufacturers; trend data reveals where cost pressure is rising."
        ),
        "use_cases": ["sme"],
        "params": {
            "freq": "A",
            "geo": "EU27_2020",
            "sinceTimePeriod": "2010",
        },
        "indicator_dim": "lcstruct",  # D1_D4_MD5 (total LCI), D11 (wages), D12_D4_MD5 (other)
        "geo_dim": "geo",
        "partner_dim": None,
        "nace_dim": "nace_r2",
        "unit_dim": "unit",
    },
]

# Lookup dict keyed by dataset code
DATASET_META: dict[str, dict] = {cfg["dataset"]: cfg for cfg in FETCH_CONFIGS}


# ---------------------------------------------------------------------------
# JSON-STAT 2.0 parser
# ---------------------------------------------------------------------------
def _parse_jsonstat(raw: dict) -> tuple[list[dict], dict[str, dict[str, str]]]:
    """
    Parse a Eurostat JSON-STAT 2.0 response.

    Returns
    -------
    records : list of dicts — keys are dimension names + "value"
    labels  : {dim_name: {code: human_readable_label}}
    """
    dims: list[str] = raw["id"]
    sizes: list[int] = raw["size"]
    dim_data: dict = raw["dimension"]
    values_raw = raw.get("value", {})

    ordered_codes: dict[str, list[str]] = {}
    labels: dict[str, dict[str, str]] = {}

    for dim in dims:
        cat = dim_data[dim]["category"]
        index_map = cat.get("index", {})
        label_map = cat.get("label", {})

        if isinstance(index_map, dict):
            codes = sorted(index_map.keys(), key=lambda k: index_map[k])
        else:
            codes = list(index_map)

        ordered_codes[dim] = codes
        labels[dim] = label_map if isinstance(label_map, dict) else {}

    # Normalise values → {int_position: float_value}
    if isinstance(values_raw, list):
        values: dict[int, float] = {
            i: float(v) for i, v in enumerate(values_raw) if v is not None
        }
    elif isinstance(values_raw, dict):
        values = {int(k): float(v) for k, v in values_raw.items() if v is not None}
    else:
        values = {}

    # Strides: rightmost dimension changes fastest
    strides = [1] * len(dims)
    for i in range(len(dims) - 2, -1, -1):
        strides[i] = strides[i + 1] * sizes[i + 1]

    records: list[dict] = []
    for pos, val in values.items():
        rec: dict = {"value": val}
        remaining = pos
        for i, dim in enumerate(dims):
            idx = remaining // strides[i]
            remaining %= strides[i]
            if 0 <= idx < len(ordered_codes[dim]):
                rec[dim] = ordered_codes[dim][idx]
            else:
                rec[dim] = ""
        records.append(rec)

    return records, labels


# ---------------------------------------------------------------------------
# Fetch + store
# ---------------------------------------------------------------------------
def _fetch_raw(dataset: str, params: dict) -> dict:
    url = f"{EUROSTAT_BASE}/{dataset}"
    full_params = {"format": "JSON", "lang": "EN", **params}
    resp = requests.get(url, params=full_params, headers=HEADERS, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def _store_labels(conn: sqlite3.Connection, dataset: str,
                  labels: dict[str, dict[str, str]]) -> None:
    for dim_name, code_labels in labels.items():
        for code, label in code_labels.items():
            conn.execute(
                "INSERT OR REPLACE INTO eurostat_labels (dataset, dim_name, code, label) "
                "VALUES (?, ?, ?, ?)",
                (dataset, dim_name, code, label),
            )


def _store_records(conn: sqlite3.Connection, records: list[dict],
                   cfg: dict, now: str) -> int:
    dataset = cfg["dataset"]
    ind_dim = cfg.get("indicator_dim", "")
    geo_dim = cfg.get("geo_dim") or ""
    partner_dim = cfg.get("partner_dim") or ""
    nace_dim = cfg.get("nace_dim") or ""
    unit_dim = cfg.get("unit_dim") or ""

    stored = 0
    for rec in records:
        time_period = rec.get("time", "")
        if not time_period:
            continue
        try:
            year = int(str(time_period)[:4])
        except ValueError:
            continue

        indicator = rec.get(ind_dim, "") if ind_dim else ""
        geo = rec.get(geo_dim, "") if geo_dim else ""
        partner = rec.get(partner_dim, "") if partner_dim else ""
        nace = rec.get(nace_dim, "") if nace_dim else ""
        unit = rec.get(unit_dim, "") if unit_dim else ""

        try:
            conn.execute(
                "INSERT OR REPLACE INTO eurostat_series "
                "(dataset, indicator, geo, partner, nace, unit, time_period, year, value, fetched_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (dataset, indicator, geo, partner, nace, unit,
                 time_period, year, rec["value"], now),
            )
            stored += 1
        except sqlite3.Error as exc:
            log.debug("Insert skip %s/%s: %s", dataset, time_period, exc)

    return stored


def fetch_dataset(cfg: dict, conn: sqlite3.Connection) -> dict:
    """Fetch one configured dataset and persist results. Returns a summary dict."""
    now = datetime.utcnow().isoformat()
    try:
        raw = _fetch_raw(cfg["dataset"], cfg["params"])
        records, labels = _parse_jsonstat(raw)
        _store_labels(conn, cfg["dataset"], labels)
        stored = _store_records(conn, records, cfg, now)
        conn.execute(
            "INSERT INTO eurostat_fetch_log "
            "(dataset, ok, rows_fetched, rows_stored, fetched_at) VALUES (?, 1, ?, ?, ?)",
            (cfg["dataset"], len(records), stored, now),
        )
        conn.commit()
        log.info("  %-25s %5d fetched → %5d stored", cfg["dataset"], len(records), stored)
        return {"dataset": cfg["dataset"], "ok": True,
                "fetched": len(records), "stored": stored}
    except Exception as exc:
        log.warning("  %-25s FAILED: %s", cfg["dataset"], exc)
        try:
            conn.execute(
                "INSERT INTO eurostat_fetch_log (dataset, ok, error, fetched_at) "
                "VALUES (?, 0, ?, ?)",
                (cfg["dataset"], str(exc), now),
            )
            conn.commit()
        except Exception:
            pass
        return {"dataset": cfg["dataset"], "ok": False, "error": str(exc)}


def fetch_all_eurostat(force: bool = False) -> list[dict]:
    """Fetch all configured Eurostat datasets. Returns list of per-dataset result dicts."""
    conn = get_eurostat_db()
    results = []
    for cfg in FETCH_CONFIGS:
        log.info("Fetching: %s (%s)", cfg["dataset"], cfg["label"])
        results.append(fetch_dataset(cfg, conn))
    conn.close()
    return results


# ---------------------------------------------------------------------------
# Query helpers — used by dashboard
# ---------------------------------------------------------------------------
def get_eurostat_datasets(conn: sqlite3.Connection) -> list[dict]:
    """All stored datasets with row counts and year ranges."""
    cur = conn.execute(
        "SELECT dataset, COUNT(*) as rows, MIN(year), MAX(year) "
        "FROM eurostat_series GROUP BY dataset ORDER BY dataset"
    )
    results = []
    for dataset, rows, min_yr, max_yr in cur.fetchall():
        meta = DATASET_META.get(dataset, {})
        results.append({
            "dataset": dataset,
            "label": meta.get("label", dataset),
            "description": meta.get("description", ""),
            "use_cases": meta.get("use_cases", []),
            "rows": rows,
            "min_year": min_yr,
            "max_year": max_yr,
        })
    return results


def get_eurostat_series(
    conn: sqlite3.Connection,
    dataset: str,
    indicator: str | None = None,
    geo: str | None = None,
    partner: str | None = None,
    nace: str | None = None,
    unit: str | None = None,
    start_year: int | None = None,
) -> list[dict]:
    """Return time-series rows for one dataset, optionally filtered."""
    q = (
        "SELECT dataset, indicator, geo, partner, nace, unit, year, value "
        "FROM eurostat_series WHERE dataset=?"
    )
    p: list = [dataset]
    for col, val in [("indicator", indicator), ("geo", geo), ("partner", partner),
                     ("nace", nace), ("unit", unit)]:
        if val is not None:
            q += f" AND {col}=?"
            p.append(val)
    if start_year:
        q += " AND year>=?"
        p.append(start_year)
    q += " ORDER BY indicator, nace, year"
    cols = ["dataset", "indicator", "geo", "partner", "nace", "unit", "year", "value"]
    return [dict(zip(cols, r)) for r in conn.execute(q, p).fetchall()]


def get_eurostat_latest(conn: sqlite3.Connection, dataset: str) -> list[dict]:
    """Latest-year value for every (indicator, geo, partner, nace, unit) combination."""
    cfg = DATASET_META.get(dataset, {})
    indicator_dim = cfg.get("indicator_dim", "indicator")
    q = """
        SELECT e.dataset, e.indicator, e.geo, e.partner, e.nace, e.unit, e.year, e.value,
               l.label AS indicator_label
        FROM eurostat_series e
        LEFT JOIN eurostat_labels l
          ON l.dataset = e.dataset AND l.dim_name = ? AND l.code = e.indicator
        WHERE e.dataset = ?
          AND e.year = (
              SELECT MAX(e2.year) FROM eurostat_series e2
              WHERE e2.dataset = e.dataset AND e2.indicator = e.indicator
                AND e2.geo = e.geo AND e2.partner = e.partner
                AND e2.nace = e.nace AND e2.unit = e.unit
          )
        ORDER BY e.indicator, e.nace, e.geo
    """
    cols = ["dataset", "indicator", "geo", "partner", "nace", "unit", "year", "value",
            "indicator_label"]
    return [dict(zip(cols, r))
            for r in conn.execute(q, (indicator_dim, dataset)).fetchall()]


def get_eurostat_indicators(conn: sqlite3.Connection, dataset: str) -> list[dict]:
    """Distinct indicators stored for a dataset, with labels."""
    cfg = DATASET_META.get(dataset, {})
    indicator_dim = cfg.get("indicator_dim", "indicator")
    q = """
        SELECT DISTINCT e.indicator, l.label,
               MIN(e.year) as min_yr, MAX(e.year) as max_yr, COUNT(*) as rows
        FROM eurostat_series e
        LEFT JOIN eurostat_labels l
          ON l.dataset = e.dataset AND l.dim_name = ? AND l.code = e.indicator
        WHERE e.dataset = ?
        GROUP BY e.indicator
        ORDER BY e.indicator
    """
    cols = ["indicator", "label", "min_year", "max_year", "rows"]
    return [dict(zip(cols, r)) for r in conn.execute(q, (indicator_dim, dataset)).fetchall()]


def get_sme_scorecard(conn: sqlite3.Connection) -> dict:
    """
    SME competitive-intelligence scorecard.

    Returns latest values and time series for:
      - EU-China trade balance (ext_lt_maineu / MIO_BAL_VAL)
      - EU exports/imports to China (ext_lt_maineu)
      - EU total R&D as % GDP (rd_e_gerdtot / TOTAL)
      - EU business R&D as % GDP (rd_e_gerdtot / BES)
      - EU high-tech patent applications (pat_ep_ntec / HT)
      - EU total manufacturing production index (sts_inpr_a / B_C)
      - EU labour cost index — total (lc_lci_r2_a / D1_D4_MD5)
    """
    def _latest(dataset: str, indicator: str, nace: str = "", unit: str = "",
                partner: str = "") -> dict | None:
        q = "SELECT year, value FROM eurostat_series WHERE dataset=? AND indicator=?"
        p: list = [dataset, indicator]
        if nace:
            q += " AND nace=?"
            p.append(nace)
        if unit:
            q += " AND unit=?"
            p.append(unit)
        if partner:
            q += " AND partner=?"
            p.append(partner)
        q += " ORDER BY year DESC LIMIT 1"
        row = conn.execute(q, p).fetchone()
        return {"year": row[0], "value": row[1]} if row else None

    def _series(dataset: str, indicator: str, nace: str = "", unit: str = "",
                partner: str = "") -> list[dict]:
        q = "SELECT year, value FROM eurostat_series WHERE dataset=? AND indicator=?"
        p: list = [dataset, indicator]
        if nace:
            q += " AND nace=?"
            p.append(nace)
        if unit:
            q += " AND unit=?"
            p.append(unit)
        if partner:
            q += " AND partner=?"
            p.append(partner)
        q += " ORDER BY year"
        return [{"year": r[0], "value": r[1]} for r in conn.execute(q, p).fetchall()]

    return {
        "trade_balance": {
            "label": "EU-China Trade Balance (EUR mn)",
            "note": "Negative = EU trade deficit with China; SITC Total",
            "latest": _latest("ext_lt_maineu", "MIO_BAL_VAL", nace="TOTAL",
                               partner="CN_X_HK"),
            "series": _series("ext_lt_maineu", "MIO_BAL_VAL", nace="TOTAL",
                               partner="CN_X_HK"),
        },
        "exports_to_china": {
            "label": "EU Exports to China (EUR mn)",
            "latest": _latest("ext_lt_maineu", "MIO_EXP_VAL", nace="TOTAL",
                               partner="CN_X_HK"),
            "series": _series("ext_lt_maineu", "MIO_EXP_VAL", nace="TOTAL",
                               partner="CN_X_HK"),
        },
        "imports_from_china": {
            "label": "EU Imports from China (EUR mn)",
            "latest": _latest("ext_lt_maineu", "MIO_IMP_VAL", nace="TOTAL",
                               partner="CN_X_HK"),
            "series": _series("ext_lt_maineu", "MIO_IMP_VAL", nace="TOTAL",
                               partner="CN_X_HK"),
        },
        "china_import_share": {
            "label": "China's Share of EU Imports (%)",
            "note": "Share of total EU imports sourced from China; structural dependency indicator",
            "latest": _latest("ext_lt_maineu", "PC_IMP_PART", nace="TOTAL",
                               partner="CN_X_HK"),
            "series": _series("ext_lt_maineu", "PC_IMP_PART", nace="TOTAL",
                               partner="CN_X_HK"),
        },
        "rd_total_pct_gdp": {
            "label": "EU R&D Expenditure — Total (% GDP)",
            "latest": _latest("rd_e_gerdtot", "TOTAL", unit="PC_GDP"),
            "series": _series("rd_e_gerdtot", "TOTAL", unit="PC_GDP"),
        },
        "rd_business_pct_gdp": {
            "label": "EU R&D — Business Sector (% GDP)",
            "latest": _latest("rd_e_gerdtot", "BES", unit="PC_GDP"),
            "series": _series("rd_e_gerdtot", "BES", unit="PC_GDP"),
        },
        "patents_hightech": {
            "label": "EU High-Tech Patent Applications (EPO)",
            "latest": _latest("pat_ep_ntec", "HT"),
            "series": _series("pat_ep_ntec", "HT"),
        },
        "patents_semiconductors": {
            "label": "EU Semiconductor Patent Applications (EPO)",
            "latest": _latest("pat_ep_ntec", "SMC"),
            "series": _series("pat_ep_ntec", "SMC"),
        },
        "industrial_production_mfg": {
            "label": "EU Manufacturing Production Index (2015=100)",
            "note": "Mining + Manufacturing (B_C); calendar adjusted",
            "latest": _latest("sts_inpr_a", "B_C", unit="I15"),
            "series": _series("sts_inpr_a", "B_C", unit="I15"),
        },
        "labour_cost_total": {
            "label": "EU Labour Cost Index — Business Economy (index, 2020=100)",
            "note": "NACE B-N (business economy); total labour cost incl. taxes/subsidies; 2020=100",
            "latest": _latest("lc_lci_r2_a", "D1_D4_MD5", nace="B-N", unit="I20"),
            "series": _series("lc_lci_r2_a", "D1_D4_MD5", nace="B-N", unit="I20"),
        },
        "labour_cost_manufacturing": {
            "label": "EU Labour Cost Index — Manufacturing (index, 2020=100)",
            "note": "NACE C (manufacturing); most directly comparable to Chinese factory costs",
            "latest": _latest("lc_lci_r2_a", "D1_D4_MD5", nace="C", unit="I20"),
            "series": _series("lc_lci_r2_a", "D1_D4_MD5", nace="C", unit="I20"),
        },
    }


def get_policy_scorecard(conn: sqlite3.Connection) -> dict:
    """
    Regulatory / policy-maker scorecard.

    Returns latest values and time series for:
      - EU-China trade balance and trend
      - EU imports from China across SITC sectors (dependency mapping)
      - EU R&D investment gap
      - EU high-tech patent trends
      - EU manufacturing production trend (sector vulnerabilities)
    """
    def _latest(dataset: str, indicator: str, nace: str = "", unit: str = "",
                partner: str = "") -> dict | None:
        q = "SELECT year, value FROM eurostat_series WHERE dataset=? AND indicator=?"
        p: list = [dataset, indicator]
        if nace:
            q += " AND nace=?"
            p.append(nace)
        if unit:
            q += " AND unit=?"
            p.append(unit)
        if partner:
            q += " AND partner=?"
            p.append(partner)
        q += " ORDER BY year DESC LIMIT 1"
        row = conn.execute(q, p).fetchone()
        return {"year": row[0], "value": row[1]} if row else None

    def _series(dataset: str, indicator: str, nace: str = "", unit: str = "",
                partner: str = "") -> list[dict]:
        q = "SELECT year, value FROM eurostat_series WHERE dataset=? AND indicator=?"
        p: list = [dataset, indicator]
        if nace:
            q += " AND nace=?"
            p.append(nace)
        if unit:
            q += " AND unit=?"
            p.append(unit)
        if partner:
            q += " AND partner=?"
            p.append(partner)
        q += " ORDER BY year"
        return [{"year": r[0], "value": r[1]} for r in conn.execute(q, p).fetchall()]

    # Import dependency: total EU imports from China (sector breakdown limited to TOTAL)
    import_dependency = []
    for indic, label in [
        ("MIO_IMP_VAL", "EU Imports from China (EUR mn)"),
        ("MIO_EXP_VAL", "EU Exports to China (EUR mn)"),
        ("MIO_BAL_VAL", "EU-China Trade Balance (EUR mn)"),
        ("PC_IMP_PART", "China Share of EU Imports (%)"),
        ("PC_EXP_PART", "China Share of EU Exports (%)"),
    ]:
        row = _latest("ext_lt_maineu", indic, nace="TOTAL", partner="CN_X_HK")
        if row:
            import_dependency.append({"indicator": indic, "label": label, **row})

    return {
        "trade_balance_trend": {
            "label": "EU-China Trade Balance (EUR mn)",
            "note": "Negative values indicate EU deficit; measures structural trade exposure",
            "latest": _latest("ext_lt_maineu", "MIO_BAL_VAL", nace="TOTAL",
                               partner="CN_X_HK"),
            "series": _series("ext_lt_maineu", "MIO_BAL_VAL", nace="TOTAL",
                               partner="CN_X_HK"),
        },
        "import_share_of_china": {
            "label": "China's Share of EU Imports (%)",
            "latest": _latest("ext_lt_maineu", "PC_IMP_PART", nace="TOTAL",
                               partner="CN_X_HK"),
            "series": _series("ext_lt_maineu", "PC_IMP_PART", nace="TOTAL",
                               partner="CN_X_HK"),
        },
        "eu_china_trade_summary": {
            "label": "EU-China Trade Summary (latest year, EUR mn / %)",
            "note": "Trade balance, export/import volumes, and China's share in EU external trade",
            "data": import_dependency,
        },
        "rd_total_pct_gdp": {
            "label": "EU R&D Intensity — Total (% GDP)",
            "latest": _latest("rd_e_gerdtot", "TOTAL", unit="PC_GDP"),
            "series": _series("rd_e_gerdtot", "TOTAL", unit="PC_GDP"),
        },
        "patents_hightech": {
            "label": "EU High-Tech Patent Applications (EPO)",
            "note": "HT = all high-tech; decline signals erosion of technology leadership",
            "latest": _latest("pat_ep_ntec", "HT"),
            "series": _series("pat_ep_ntec", "HT"),
        },
        "patents_semiconductors": {
            "label": "EU Semiconductor Patent Applications (EPO)",
            "latest": _latest("pat_ep_ntec", "SMC"),
            "series": _series("pat_ep_ntec", "SMC"),
        },
        "patents_communications": {
            "label": "EU Communications Tech Patent Applications (EPO)",
            "latest": _latest("pat_ep_ntec", "CTE"),
            "series": _series("pat_ep_ntec", "CTE"),
        },
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    parser = argparse.ArgumentParser(
        description="Fetch Eurostat EU-China competitive-intelligence data"
    )
    parser.add_argument("--show", action="store_true",
                        help="List stored datasets with row counts")
    parser.add_argument("--dataset", type=str,
                        help="Fetch a single dataset by code")
    parser.add_argument("--indicators", type=str,
                        help="List indicators stored for a dataset")
    parser.add_argument("--force", action="store_true",
                        help="Re-fetch even if data already exists")
    args = parser.parse_args()

    conn = get_eurostat_db()

    if args.show:
        datasets = get_eurostat_datasets(conn)
        conn.close()
        if not datasets:
            print("No Eurostat data stored. Run: python -m policy_monitor.eurostat")
            return
        print(f"\n{'Dataset':<26} {'Label':<44} {'Years':<12} {'Rows':>7}")
        print("-" * 95)
        for d in datasets:
            yr = f"{d['min_year']}-{d['max_year']}"
            print(f"{d['dataset']:<26} {d['label']:<44} {yr:<12} {d['rows']:>7}")
        return

    if args.indicators:
        indicators = get_eurostat_indicators(conn, args.indicators)
        conn.close()
        if not indicators:
            print(f"No indicators for '{args.indicators}'. Fetch first.")
            return
        print(f"\n{'Code':<30} {'Label':<50} {'Years':<12} {'Rows':>6}")
        print("-" * 102)
        for ind in indicators:
            yr = f"{ind['min_year']}-{ind['max_year']}"
            label = (ind["label"] or "")[:48]
            print(f"{ind['indicator']:<30} {label:<50} {yr:<12} {ind['rows']:>6}")
        return

    conn.close()

    if args.dataset:
        cfg = next((c for c in FETCH_CONFIGS if c["dataset"] == args.dataset), None)
        if not cfg:
            print(f"Unknown dataset: {args.dataset}")
            print(f"Available: {[c['dataset'] for c in FETCH_CONFIGS]}")
            return
        conn = get_eurostat_db()
        result = fetch_dataset(cfg, conn)
        conn.close()
        print(f"\n{result}")
        return

    results = fetch_all_eurostat(force=args.force)
    ok = sum(1 for r in results if r["ok"])
    total_stored = sum(r.get("stored", 0) for r in results)
    print(f"\nFetched {ok}/{len(results)} datasets — {total_stored} rows stored")
    for r in results:
        status = "OK" if r["ok"] else "FAIL"
        detail = f"{r.get('stored', 0)} stored" if r["ok"] else r.get("error", "")
        print(f"  [{status}] {r['dataset']}: {detail}")


if __name__ == "__main__":
    main()
