"""
WTO Timeseries API fetcher.
Requires WTO_API_KEY — free developer tier at https://api.wto.org

Table: wto_series in data/trade_stats.db

Indicators fetched:
  TP_A_0010   Simple average MFN applied tariff — all products
  TP_A_0030   Trade-weighted MFN applied tariff — all products
  TP_A_0160   Simple average MFN applied tariff — agricultural
  TP_A_0430   Simple average MFN applied tariff — non-agricultural
  ITS_MTV_AX  Merchandise exports by product group (annual)
  ITS_MTV_AM  Merchandise imports by product group (annual)
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
    product_code  TEXT    NOT NULL DEFAULT '',
    year          INTEGER NOT NULL,
    value         REAL,
    unit          TEXT,
    fetched_at    TEXT    NOT NULL,
    PRIMARY KEY (reporter_iso, partner_iso, indicator, product_code, year)
);
CREATE INDEX IF NOT EXISTS idx_wto_reporter  ON wto_series(reporter_iso);
CREATE INDEX IF NOT EXISTS idx_wto_indicator ON wto_series(indicator);
CREATE INDEX IF NOT EXISTS idx_wto_year      ON wto_series(year);
"""

# Indicator code → short column name stored in wto_series.indicator
# TP_B_0090 (bound tariff) omitted — it rejects year filters (static negotiated values)
_WTO_INDICATORS = {
    "TP_A_0010":  "mfn_applied_simple_avg",
    "TP_A_0030":  "mfn_applied_weighted_avg",
    "TP_A_0160":  "mfn_applied_agri",
    "TP_A_0430":  "mfn_applied_noagri",
    "ITS_MTV_AX": "merch_exports_usd",
    "ITS_MTV_AM": "merch_imports_usd",
}

HISTORY_YEARS = 10
MAX_YEAR = 2024


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


def fetch_indicators(conn: sqlite3.Connection, years: list[int]) -> int:
    if not WTO_API_KEY:
        log.info("  WTO_API_KEY not set — skipping WTO timeseries")
        log.info("  Register free at https://api.wto.org → Developer Portal")
        return 0

    now = datetime.now(timezone.utc).isoformat()
    total = 0
    years_str = ",".join(str(y) for y in sorted(years))

    for indicator, col_name in _WTO_INDICATORS.items():
        log.info(f"  WTO {indicator} ({col_name}) years={years_str}")
        j = _get_json(
            f"{WTO_BASE_URL}/data",
            params={
                "i":   indicator,
                "ps":  years_str,   # period(s), comma-separated years
                "fmt": "json",
                "max": 50000,
            },
        )
        if not j:
            continue

        dataset = j.get("Dataset") or []
        records = []
        for row in (dataset if isinstance(dataset, list) else []):
            reporter = row.get("ReportingEconomyCode") or ""
            partner  = row.get("PartnerEconomyCode")   or ""
            product  = row.get("ProductOrSectorCode")  or ""
            year     = row.get("Year")
            value    = row.get("Value")
            unit     = row.get("UnitCode") or row.get("Unit") or ""
            if reporter and year and value is not None:
                try:
                    records.append((
                        str(reporter), str(partner), col_name, str(product),
                        int(year), float(value), str(unit), now,
                    ))
                except (ValueError, TypeError):
                    pass

        conn.executemany(
            "INSERT OR REPLACE INTO wto_series "
            "(reporter_iso, partner_iso, indicator, product_code, year, value, unit, fetched_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            records,
        )
        conn.commit()
        total += len(records)
        log.info(f"    → {len(records)} rows")
        time.sleep(0.5)

    return total


def fetch_all(conn: sqlite3.Connection, force_full: bool = False) -> int:
    _ensure_schema(conn)
    stored = _stored_years_wto(conn)
    target = [MAX_YEAR - i for i in range(HISTORY_YEARS)]
    years = target if (force_full or not stored) else [MAX_YEAR]

    log.info(f"WTO timeseries — years: {years}")
    n = fetch_indicators(conn, years)
    log.info(f"  WTO total: {n} rows")
    return n
