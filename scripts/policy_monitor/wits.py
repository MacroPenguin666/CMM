"""
WITS tariff data fetcher — China-specific, no API key required.

Fetches from the WITS TradeStats Tariff SDMX API (World Bank, open access):
  https://wits.worldbank.org/API/V1/SDMX/V21/datasource/tradestats-tariff/...

Two directions:
  1. Tariffs BY China  — China as reporter, World as partner aggregate
  2. Tariffs AGAINST China — all countries as reporter, China as partner

Indicators fetched (all as simple and weighted averages):
  AHS-SMPL-AVRG   Applied tariff, simple average    (actual rate charged incl. preferential)
  MFN-SMPL-AVRG   MFN tariff, simple average        (standard WTO rate)
  MFN-WGHTD-AVRG  MFN tariff, trade-weighted average
  AHS-WGHTD-AVRG  Applied tariff, trade-weighted average

Table: china_tariffs in data/trade_stats.db

Product granularity: 29 WITS product groups (HS sections + UNCTAD categories).
Latest available year: 2023.

Usage:
    python wits.py          # fetch all
    python wits.py --show   # print latest snapshot
    python wits.py --force  # refetch all years
"""

import argparse
import logging
import sqlite3
import time
from datetime import datetime, timezone

import requests

from policy_monitor.storage import DB_DIR

log = logging.getLogger("wits")

TRADE_STATS_DB = DB_DIR / "trade_stats.db"

_BASE = "https://wits.worldbank.org/API/V1/SDMX/V21/datasource/tradestats-tariff"
_HEADERS = {"User-Agent": "CMM/1.0", "Accept": "application/json"}

_INDICATORS = [
    "AHS-SMPL-AVRG",
    "MFN-SMPL-AVRG",
    "MFN-WGHTD-AVRG",
    "AHS-WGHTD-AVRG",
]

HISTORY_YEARS = 5
MAX_YEAR = 2023

_SCHEMA = """
CREATE TABLE IF NOT EXISTS china_tariffs (
    reporter_iso  TEXT    NOT NULL,
    partner_iso   TEXT    NOT NULL,
    product_code  TEXT    NOT NULL,
    year          INTEGER NOT NULL,
    indicator     TEXT    NOT NULL,
    value         REAL,
    fetched_at    TEXT    NOT NULL,
    PRIMARY KEY (reporter_iso, partner_iso, product_code, year, indicator)
);
CREATE INDEX IF NOT EXISTS idx_ct_reporter  ON china_tariffs(reporter_iso);
CREATE INDEX IF NOT EXISTS idx_ct_partner   ON china_tariffs(partner_iso);
CREATE INDEX IF NOT EXISTS idx_ct_year      ON china_tariffs(year);
CREATE INDEX IF NOT EXISTS idx_ct_indicator ON china_tariffs(indicator);

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
"""


def get_trade_stats_db() -> sqlite3.Connection:
    DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(TRADE_STATS_DB))
    conn.executescript(_SCHEMA)
    return conn


def _fetch(reporter: str, partner: str, year: int, indicator: str) -> list[tuple]:
    """Fetch one (reporter, partner, year, indicator) slice. Returns list of row tuples."""
    url = f"{_BASE}/reporter/{reporter}/year/{year}/partner/{partner}/product/all/indicator/{indicator}"
    try:
        r = requests.get(url, headers=_HEADERS, params={"format": "JSON"}, timeout=30)
        if r.status_code == 404:
            return []
        if r.status_code in (429, 503):
            time.sleep(60)
            r = requests.get(url, headers=_HEADERS, params={"format": "JSON"}, timeout=30)
        r.raise_for_status()
    except Exception as exc:
        log.warning(f"  [{reporter}→{partner} {year} {indicator}] {exc}")
        return []

    j = r.json()
    ds = (j.get("dataSets") or [{}])[0]
    series = ds.get("series") or {}

    struct = j.get("structure") or {}
    dims_s = struct.get("dimensions", {}).get("series", [])
    dims_o = struct.get("dimensions", {}).get("observation", [])

    # Build lookup: dimension index → list of value IDs
    dim_vals_s = [d.get("values", []) for d in dims_s]
    dim_vals_o = [d.get("values", []) for d in dims_o]

    # Dimension positions in the series key
    dim_ids_s = [d["id"] for d in dims_s]

    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for s_key, s_data in series.items():
        parts = [int(x) for x in s_key.split(":")]
        dim_map = {}
        for i, dim_id in enumerate(dim_ids_s):
            if i < len(parts) and parts[i] < len(dim_vals_s[i]):
                dim_map[dim_id] = dim_vals_s[i][parts[i]].get("id", "")

        rep  = dim_map.get("REPORTER", reporter if reporter != "all" else "")
        part = dim_map.get("PARTNER", partner if partner != "all" else "")
        prod = dim_map.get("PRODUCTCODE", "")
        ind  = dim_map.get("INDICATOR", indicator)

        for o_key, o_data in (s_data.get("observations") or {}).items():
            o_parts = [int(x) for x in o_key.split(":")]
            tp = year
            for i, dim in enumerate(dims_o):
                if dim["id"] == "TIME_PERIOD" and i < len(o_parts) and o_parts[i] < len(dim_vals_o[i]):
                    try:
                        tp = int(dim_vals_o[i][o_parts[i]].get("id", str(year))[:4])
                    except ValueError:
                        pass
            value = o_data[0] if o_data else None
            if value is not None and rep and part and prod:
                rows.append((rep, part, prod, tp, ind, float(value), now))

    return rows


def fetch_china_imposes(conn: sqlite3.Connection, years: list[int]) -> int:
    """Tariffs BY China — China as reporter, World aggregate as partner."""
    log.info("[WITS] Tariffs imposed BY China (reporter=CHN, partner=WLD)")
    total = 0
    for year in years:
        for indicator in _INDICATORS:
            rows = _fetch("CHN", "WLD", year, indicator)
            if rows:
                conn.executemany(
                    "INSERT OR REPLACE INTO china_tariffs "
                    "(reporter_iso, partner_iso, product_code, year, indicator, value, fetched_at) "
                    "VALUES (?,?,?,?,?,?,?)",
                    rows,
                )
                conn.commit()
                total += len(rows)
                log.info(f"  CHN→WLD {year} {indicator}: {len(rows)} product groups")
            time.sleep(0.5)
    return total


def fetch_against_china(conn: sqlite3.Connection, years: list[int]) -> int:
    """Tariffs AGAINST China — all reporters, China as partner."""
    log.info("[WITS] Tariffs imposed AGAINST China (reporter=ALL, partner=CHN)")
    total = 0
    for year in years:
        for indicator in _INDICATORS:
            rows = _fetch("all", "CHN", year, indicator)
            if rows:
                conn.executemany(
                    "INSERT OR REPLACE INTO china_tariffs "
                    "(reporter_iso, partner_iso, product_code, year, indicator, value, fetched_at) "
                    "VALUES (?,?,?,?,?,?,?)",
                    rows,
                )
                conn.commit()
                total += len(rows)
                n_reporters = len({r[0] for r in rows})
                log.info(f"  ALL→CHN {year} {indicator}: {len(rows)} rows, {n_reporters} reporters")
            time.sleep(0.5)
    return total


def fetch_all(conn: sqlite3.Connection, force_full: bool = False) -> int:
    """Fetch both directions. Returns total rows stored."""
    try:
        stored_years = {r[0] for r in conn.execute("SELECT DISTINCT year FROM china_tariffs").fetchall()}
    except sqlite3.OperationalError:
        stored_years = set()

    target = [MAX_YEAR - i for i in range(HISTORY_YEARS)]
    years = target if (force_full or not stored_years) else [MAX_YEAR]

    log.info(f"WITS China tariffs — fetching years: {years}")
    n1 = fetch_china_imposes(conn, years)
    n2 = fetch_against_china(conn, years)
    log.info(f"WITS done: {n1 + n2} total rows ({n1} by China, {n2} against China)")
    return n1 + n2


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    parser = argparse.ArgumentParser(description="Fetch WITS China tariff data")
    parser.add_argument("--show", action="store_true", help="Print latest snapshot")
    parser.add_argument("--force", action="store_true", help="Refetch all years")
    args = parser.parse_args()

    conn = get_trade_stats_db()

    if args.show:
        try:
            import pandas as pd
            df = pd.read_sql_query(
                "SELECT reporter_iso, partner_iso, product_code, year, indicator, value "
                "FROM china_tariffs ORDER BY year DESC, reporter_iso, product_code LIMIT 60",
                conn)
            print(df.to_string(index=False))
            print(f"\nTotal rows: {conn.execute('SELECT COUNT(*) FROM china_tariffs').fetchone()[0]}")
            print(f"Reporters: {conn.execute('SELECT COUNT(DISTINCT reporter_iso) FROM china_tariffs').fetchone()[0]}")
        except Exception as e:
            print(f"No data yet: {e}")
        conn.close()
        return

    ok = fetch_all(conn, force_full=args.force)
    log.info(f"Done: {ok} rows stored")
    conn.close()


if __name__ == "__main__":
    main()
