"""
WTO Data Portal fetcher.
Requires WTO_API_KEY — free developer tier at https://api.wto.org

Table: wto_series, wto_disputes in data/trade_stats.db

Indicators fetched (when key available):
  TA_2_010   Bound tariff rates (simple avg, all products)
  TA_1_020   Applied MFN tariff rates
  TP_A010    Merchandise exports value (USD)
  TP_A020    Merchandise imports value (USD)
  TP_A050    Commercial services exports
  TP_A060    Commercial services imports

Dispute settlement:
  All WTO dispute cases — DS number, parties, status, agreements cited
"""

import logging
import sqlite3
import time
from datetime import datetime, timezone

import requests

from policy_monitor.config import WTO_API_KEY, WTO_BASE_URL
from policy_monitor.wits import get_trade_stats_db  # reuse shared DB

log = logging.getLogger("wto")

_SCHEMA_EXTRA = """
CREATE TABLE IF NOT EXISTS wto_series (
    reporter_iso  TEXT    NOT NULL,
    partner_iso   TEXT    NOT NULL DEFAULT '',
    indicator     TEXT    NOT NULL,
    year          INTEGER NOT NULL,
    value         REAL,
    unit          TEXT,
    fetched_at    TEXT    NOT NULL,
    PRIMARY KEY (reporter_iso, partner_iso, indicator, year)
);
CREATE INDEX IF NOT EXISTS idx_wto_reporter  ON wto_series(reporter_iso);
CREATE INDEX IF NOT EXISTS idx_wto_indicator ON wto_series(indicator);
CREATE INDEX IF NOT EXISTS idx_wto_year      ON wto_series(year);

CREATE TABLE IF NOT EXISTS wto_disputes (
    ds_number    TEXT PRIMARY KEY,
    title        TEXT,
    complainant  TEXT,
    respondent   TEXT,
    third_parties TEXT,
    agreement    TEXT,
    date_req     TEXT,
    status       TEXT,
    fetched_at   TEXT NOT NULL
);
"""

_WTO_INDICATORS = {
    "TA_2_010": "bound_tariff_simple_avg",
    "TA_1_020": "applied_mfn_tariff_avg",
    "TP_A010":  "merch_exports_usd",
    "TP_A020":  "merch_imports_usd",
    "TP_A050":  "services_exports_usd",
    "TP_A060":  "services_imports_usd",
}

HISTORY_YEARS = 10
MAX_YEAR = 2024

# China's WTO reporter code
_CHINA_CODE = "156"


def _ensure_schema(conn: sqlite3.Connection):
    conn.executescript(_SCHEMA_EXTRA)


def _headers() -> dict:
    return {
        "Ocp-Apim-Subscription-Key": WTO_API_KEY,
        "User-Agent": "CMM/1.0 ha.boehm@web.de",
        "Accept": "application/json",
    }


def _get_json(url: str, params: dict | None = None, retries: int = 3) -> dict | list | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=_headers(), timeout=30)
            if r.status_code in (401, 403):
                log.warning(f"  WTO auth error {r.status_code} — check WTO_API_KEY")
                return None
            if r.status_code == 404:
                return None
            if r.status_code == 429:
                wait = 60 * (2 ** min(attempt, 3))
                log.warning(f"  [429] sleeping {wait}s")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.Timeout:
            time.sleep(20 * (attempt + 1))
        except Exception as exc:
            if attempt == retries - 1:
                log.warning(f"  [error] {exc}")
    return None


def _stored_years_wto(conn: sqlite3.Connection) -> set[int]:
    try:
        cur = conn.execute("SELECT DISTINCT year FROM wto_series")
        return {r[0] for r in cur.fetchall()}
    except sqlite3.OperationalError:
        return set()


# ---------------------------------------------------------------------------
# Timeseries indicators
# ---------------------------------------------------------------------------

def fetch_indicators(conn: sqlite3.Connection, years: list[int]) -> int:
    if not WTO_API_KEY:
        log.info("  WTO_API_KEY not set — skipping WTO timeseries")
        log.info("  Register free at https://api.wto.org → Developer Portal")
        return 0

    now = datetime.now(timezone.utc).isoformat()
    total = 0
    years_str = ",".join(str(y) for y in years)

    for indicator, col_name in _WTO_INDICATORS.items():
        log.info(f"  WTO {indicator} ({col_name}) years={years_str}")
        j = _get_json(
            f"{WTO_BASE_URL}/data",
            params={
                "indicators": indicator,
                "years": years_str,
                "format": "json",
            },
        )
        if not j:
            continue

        dataset = j.get("Dataset") or j.get("data") or []
        records = []
        for row in (dataset if isinstance(dataset, list) else []):
            reporter = row.get("ReporterCode", row.get("reporterCode", ""))
            partner  = row.get("PartnerCode",  row.get("partnerCode", "")) or ""
            year     = row.get("Year",  row.get("year"))
            value    = row.get("Value", row.get("value"))
            unit     = row.get("Unit",  row.get("unit", ""))
            if reporter and year and value is not None:
                try:
                    records.append((
                        str(reporter), str(partner), col_name, int(year),
                        float(value), str(unit), now,
                    ))
                except (ValueError, TypeError):
                    pass

        conn.executemany(
            "INSERT OR REPLACE INTO wto_series "
            "(reporter_iso, partner_iso, indicator, year, value, unit, fetched_at) "
            "VALUES (?,?,?,?,?,?,?)",
            records,
        )
        conn.commit()
        total += len(records)
        time.sleep(0.5)

    return total


# ---------------------------------------------------------------------------
# Dispute settlement cases
# ---------------------------------------------------------------------------

def fetch_disputes(conn: sqlite3.Connection) -> int:
    if not WTO_API_KEY:
        return 0

    log.info("  Fetching WTO dispute settlement cases …")
    now = datetime.now(timezone.utc).isoformat()

    # Try the disputes endpoint (path may vary by API version)
    for path in ["/disputes/cases", "/dispu/cases", "/disputes"]:
        j = _get_json(f"{WTO_BASE_URL}{path}", params={"format": "json", "rows": 10000})
        if j:
            break
    else:
        log.warning("  WTO disputes endpoint not found — skipping")
        return 0

    cases = j.get("Dataset") or j.get("cases") or j.get("data") or []
    if not isinstance(cases, list):
        return 0

    records = []
    for c in cases:
        ds_num = str(c.get("CaseNumber") or c.get("ds_number") or c.get("number", ""))
        if not ds_num:
            continue
        records.append((
            ds_num,
            c.get("Title") or c.get("title", ""),
            c.get("Complainant") or c.get("complainant", ""),
            c.get("Respondent") or c.get("respondent", ""),
            c.get("ThirdParties") or c.get("third_parties", ""),
            c.get("Agreement") or c.get("agreement", ""),
            c.get("RequestDate") or c.get("date_req", ""),
            c.get("Status") or c.get("status", ""),
            now,
        ))

    conn.executemany(
        "INSERT OR REPLACE INTO wto_disputes "
        "(ds_number, title, complainant, respondent, third_parties, agreement, date_req, status, fetched_at) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        records,
    )
    conn.commit()
    log.info(f"  {len(records)} dispute cases stored")
    return len(records)


def fetch_all(conn: sqlite3.Connection, force_full: bool = False) -> int:
    _ensure_schema(conn)
    stored = _stored_years_wto(conn)
    target = [MAX_YEAR - i for i in range(HISTORY_YEARS)]
    years = target if (force_full or not stored) else [MAX_YEAR]

    n1 = fetch_indicators(conn, years)
    n2 = fetch_disputes(conn)
    return n1 + n2
