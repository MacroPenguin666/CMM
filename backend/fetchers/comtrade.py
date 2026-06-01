"""
UN Comtrade+ trade data fetcher.
Requires COMTRADE_API_KEY in .env (comtradedeveloper.un.org subscription).

Three tables in data/comtrade.db:
  comtrade_hs4       reporter × HS4 code × flow × year  (vs. World, partnerCode=0)
  comtrade_hs2       reporter × HS2 chapter × flow × year  (rolled up from hs4)
  comtrade_bilateral reporter × partner × flow × year  (TOTAL commodity, all partners)

Fetch strategy:
  First run  — last HISTORY_YEARS of annual data for all ~200 reporters
  Subsequent — most-recent completed calendar year only (revisions + new-year rollover)

Reporter batching:
  HS4 calls:       BATCH_HS4  reporters per call  (cmdCode=ALL, partnerCode=0)
  Bilateral calls: BATCH_BIL  reporters per call  (cmdCode=TOTAL)
"""

import logging
import sqlite3
import time
from datetime import datetime, timezone

import requests

from backend.config import COMTRADE_API_KEY
from backend.storage import DB_DIR

log = logging.getLogger("comtrade")

_BASE       = "https://comtradeapi.un.org/data/v1/get/C"
REF_HS      = "https://comtradeapi.un.org/files/v1/app/reference/HS.json"
REF_REPORTERS = "https://comtradeapi.un.org/files/v1/app/reference/Reporters.json"

HISTORY_YEARS = 5     # years to back-fill on first run
MAX_YEAR      = 2024  # last confirmed complete calendar year
BATCH_HS4     = 10    # reporters per HS4 call   (10 × ~1228 HS4 × 2 flows ≈ 24k rows)
BATCH_BIL     = 20    # reporters per bilateral call (20 × ~200 partners × 2 ≈ 8k rows)
DELAY_SEC     = 0.5   # inter-call pause; subscription tier allows ~250 req/min


_SCHEMA = """
CREATE TABLE IF NOT EXISTS comtrade_hs4 (
    year          INTEGER NOT NULL,
    reporter_iso  TEXT    NOT NULL,
    reporter_name TEXT,
    cmd_code      TEXT    NOT NULL,
    cmd_desc      TEXT,
    flow_code     TEXT    NOT NULL,
    value_usd     REAL,
    fetched_at    TEXT    NOT NULL,
    PRIMARY KEY (year, reporter_iso, cmd_code, flow_code)
);
CREATE INDEX IF NOT EXISTS idx_hs4_reporter ON comtrade_hs4(reporter_iso);
CREATE INDEX IF NOT EXISTS idx_hs4_cmd      ON comtrade_hs4(cmd_code);
CREATE INDEX IF NOT EXISTS idx_hs4_year     ON comtrade_hs4(year);

CREATE TABLE IF NOT EXISTS comtrade_hs2 (
    year          INTEGER NOT NULL,
    reporter_iso  TEXT    NOT NULL,
    reporter_name TEXT,
    cmd_code      TEXT    NOT NULL,
    cmd_desc      TEXT,
    flow_code     TEXT    NOT NULL,
    value_usd     REAL,
    fetched_at    TEXT    NOT NULL,
    PRIMARY KEY (year, reporter_iso, cmd_code, flow_code)
);
CREATE INDEX IF NOT EXISTS idx_hs2_reporter ON comtrade_hs2(reporter_iso);
CREATE INDEX IF NOT EXISTS idx_hs2_cmd      ON comtrade_hs2(cmd_code);
CREATE INDEX IF NOT EXISTS idx_hs2_year     ON comtrade_hs2(year);

CREATE TABLE IF NOT EXISTS comtrade_bilateral (
    year          INTEGER NOT NULL,
    reporter_iso  TEXT    NOT NULL,
    reporter_name TEXT,
    partner_iso   TEXT    NOT NULL,
    partner_name  TEXT,
    flow_code     TEXT    NOT NULL,
    value_usd     REAL,
    fetched_at    TEXT    NOT NULL,
    PRIMARY KEY (year, reporter_iso, partner_iso, flow_code)
);
CREATE INDEX IF NOT EXISTS idx_bil_reporter ON comtrade_bilateral(reporter_iso);
CREATE INDEX IF NOT EXISTS idx_bil_partner  ON comtrade_bilateral(partner_iso);
CREATE INDEX IF NOT EXISTS idx_bil_year     ON comtrade_bilateral(year);
"""


def get_comtrade_db() -> sqlite3.Connection:
    from backend.storage import get_conn as _storage_get_conn
    conn = _storage_get_conn()
    conn.executescript(_SCHEMA)
    return conn


def stored_years(conn: sqlite3.Connection, table: str) -> set[int]:
    try:
        cur = conn.execute(f'SELECT DISTINCT year FROM "{table}"')
        return {row[0] for row in cur.fetchall()}
    except sqlite3.OperationalError:
        return set()


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _headers() -> dict:
    return {"Ocp-Apim-Subscription-Key": COMTRADE_API_KEY}


def _get_all(url: str, params: dict, retries: int = 3) -> list[dict]:
    """Single Comtrade query, following nextLink pages."""
    all_rows: list[dict] = []
    current_url: str | None = url
    page = 0
    while current_url:
        page += 1
        for attempt in range(retries):
            try:
                r = requests.get(
                    current_url,
                    headers=_headers(),
                    params=params if page == 1 else None,
                    timeout=90,
                )
                if r.status_code == 429:
                    wait = 60 * (2 ** min(attempt, 3))
                    log.warning(f"  [429 rate-limit → sleep {wait}s]")
                    time.sleep(wait)
                    continue
                if r.status_code in (400, 404):
                    log.debug(f"  [HTTP {r.status_code}] {r.text[:200]}")
                    return all_rows
                r.raise_for_status()
                js = r.json() or {}
                all_rows.extend(js.get("data") or [])
                current_url = js.get("nextLink")
                if current_url:
                    time.sleep(0.5)
                break
            except requests.exceptions.Timeout:
                if attempt == retries - 1:
                    log.warning("  [timeout — skipping batch]")
                    return all_rows
                time.sleep(20 * (attempt + 1))
            except Exception as exc:
                if attempt == retries - 1:
                    log.warning(f"  [error: {exc}]")
                    return all_rows
                time.sleep(10)
        else:
            break
    return all_rows


def _parse_value(row: dict) -> float | None:
    for key in ("primaryValue", "cifValue", "fobValue"):
        v = row.get(key)
        if v is not None:
            try:
                return float(v)
            except (ValueError, TypeError):
                pass
    return None


# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------

def load_reporters() -> list[dict]:
    """Return [{id, iso, name}] for all individual (non-group) reporters."""
    try:
        r = requests.get(REF_REPORTERS, timeout=30)
        r.raise_for_status()
        items = r.json().get("results") or []
        return [
            {
                "id":   str(it["reporterCode"]),
                "iso":  it.get("reporterCodeIsoAlpha3", ""),
                "name": it.get("text", ""),
            }
            for it in items
            if not it.get("isGroup")
            and it.get("reporterCode") is not None
            and it.get("reporterCodeIsoAlpha3")
        ]
    except Exception as exc:
        log.error(f"Reporter list unavailable: {exc}")
        return []


def load_hs_labels() -> dict[str, str]:
    """Return {hs_code → description} for all HS codes from the reference file."""
    try:
        r = requests.get(REF_HS, timeout=30)
        r.raise_for_status()
        return {
            str(it.get("id", "")).strip(): it.get("text", "")
            for it in (r.json().get("results") or [])
            if str(it.get("id", "")).strip().isdigit()
        }
    except Exception as exc:
        log.warning(f"HS label load failed: {exc}")
        return {}


# ---------------------------------------------------------------------------
# Fetch functions
# ---------------------------------------------------------------------------

def fetch_hs4(conn: sqlite3.Connection, reporters: list[dict],
              years: list[int], freq: str = "A") -> int:
    """
    Fetch HS4-level trade vs. World for all reporters.
    Also rolls up HS2 chapter totals and stores them in comtrade_hs2.
    Returns number of HS4 rows inserted.
    """
    now = datetime.now(timezone.utc).isoformat()
    period_str = ",".join(str(y) for y in years)
    url = f"{_BASE}/{freq}/HS"
    total_hs4 = 0
    total_hs2 = 0
    n = len(reporters)

    # accumulators for HS2 rollup: (year, iso, chapter, flow) → value
    hs2_acc: dict[tuple, float] = {}
    hs2_meta: dict[tuple, tuple] = {}  # → (reporter_name, cmd_desc)

    for i in range(0, n, BATCH_HS4):
        chunk = reporters[i : i + BATCH_HS4]
        end = min(i + BATCH_HS4, n)
        log.info(f"  hs4 reporters {i+1}–{end}/{n} …")

        raw = _get_all(url, {
            "reporterCode": ",".join(r["id"] for r in chunk),
            "period":       period_str,
            "partnerCode":  "0",    # World
            "cmdCode":      "ALL",  # all HS levels — we filter HS4 (4-digit) below
            "flowCode":     "X,M",
            "maxRecords":   250000,
            "includeDesc":  "true",
        })

        id_to = {r["id"]: r for r in chunk}
        hs4_rows: list[tuple] = []

        for row in raw:
            cmd = str(row.get("cmdCode", "")).strip()
            if len(cmd) != 4 or not cmd.isdigit():
                continue
            val = _parse_value(row)
            if val is None:
                continue
            rep_code = str(row.get("reporterCode", ""))
            rep = id_to.get(rep_code, {})
            rep_iso  = row.get("reporterISO")  or rep.get("iso", "")
            rep_name = row.get("reporterDesc") or rep.get("name", "")
            flow     = str(row.get("flowCode", ""))
            year     = int(row.get("refYear", years[0]))
            cmd_desc = row.get("cmdDesc", "")

            hs4_rows.append((year, rep_iso, rep_name, cmd, cmd_desc, flow, val, now))

            # accumulate HS2 rollup
            chapter = cmd[:2]
            key = (year, rep_iso, chapter, flow)
            hs2_acc[key] = hs2_acc.get(key, 0.0) + val
            if key not in hs2_meta:
                hs2_meta[key] = (rep_name, "")  # HS2 desc added from labels later

        conn.executemany(
            "INSERT OR REPLACE INTO comtrade_hs4 "
            "(year, reporter_iso, reporter_name, cmd_code, cmd_desc, flow_code, value_usd, fetched_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            hs4_rows,
        )
        conn.commit()
        total_hs4 += len(hs4_rows)
        time.sleep(DELAY_SEC)

    # Write HS2 rollup
    hs2_rows = [
        (year, iso, hs2_meta[(year, iso, ch, fl)][0], ch, "", fl, val, now)
        for (year, iso, ch, fl), val in hs2_acc.items()
    ]
    if hs2_rows:
        conn.executemany(
            "INSERT OR REPLACE INTO comtrade_hs2 "
            "(year, reporter_iso, reporter_name, cmd_code, cmd_desc, flow_code, value_usd, fetched_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            hs2_rows,
        )
        conn.commit()
        total_hs2 = len(hs2_rows)

    log.info(f"  Stored {total_hs4:,} HS4 rows, {total_hs2:,} HS2 chapter rows")
    return total_hs4


def fetch_bilateral(conn: sqlite3.Connection, reporters: list[dict],
                    years: list[int], freq: str = "A") -> int:
    """
    Fetch total (all-commodity) trade between each reporter and every partner.
    Returns number of rows inserted.
    """
    now = datetime.now(timezone.utc).isoformat()
    period_str = ",".join(str(y) for y in years)
    url = f"{_BASE}/{freq}/HS"
    total = 0
    n = len(reporters)

    for i in range(0, n, BATCH_BIL):
        chunk = reporters[i : i + BATCH_BIL]
        end = min(i + BATCH_BIL, n)
        log.info(f"  bilateral reporters {i+1}–{end}/{n} …")

        raw = _get_all(url, {
            "reporterCode": ",".join(r["id"] for r in chunk),
            "period":       period_str,
            "cmdCode":      "TOTAL",
            "flowCode":     "X,M",
            "maxRecords":   250000,
            "includeDesc":  "true",
        })

        id_to = {r["id"]: r for r in chunk}
        rows: list[tuple] = []
        for row in raw:
            val = _parse_value(row)
            if val is None:
                continue
            rep_code = str(row.get("reporterCode", ""))
            rep = id_to.get(rep_code, {})
            rows.append((
                int(row.get("refYear", years[0])),
                row.get("reporterISO")  or rep.get("iso", ""),
                row.get("reporterDesc") or rep.get("name", ""),
                row.get("partnerISO",   ""),
                row.get("partnerDesc",  ""),
                str(row.get("flowCode", "")),
                val,
                now,
            ))

        conn.executemany(
            "INSERT OR REPLACE INTO comtrade_bilateral "
            "(year, reporter_iso, reporter_name, partner_iso, partner_name, flow_code, value_usd, fetched_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            rows,
        )
        conn.commit()
        total += len(rows)
        time.sleep(DELAY_SEC)

    return total
