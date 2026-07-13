"""
Chartbook fetcher — pulls every raw series defined in chartbook_registry into
the central data/cmm.db (tables `chartbook_series` + `chartbook_data`).

Two no-key sources:
  * FRED  — keyless CSV endpoint  https://fred.stlouisfed.org/graph/fredgraph.csv?id=ID
  * World Bank — v2 JSON API      https://api.worldbank.org/v2/country/{iso}/indicator/{ind}

Raw values only — no transforms (those happen at read-time in backend/api.py).
Idempotent: re-running upserts the same (series_id, date) rows.

CLI:
    python -m backend.fetchers.chartbook            # fetch everything
    python -m backend.fetchers.chartbook --show     # print row counts
"""

import argparse
import csv
import io
import logging
import subprocess
import time
import urllib.parse
from datetime import datetime, timezone

import requests

from backend.storage import get_conn
from backend.fetchers.chartbook_registry import FRED_SERIES, WB_SERIES

log = logging.getLogger("fetcher.chartbook")

FRED_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv"
WB_API = "https://api.worldbank.org/v2/country/{iso}/indicator/{ind}"
HEADERS = {"User-Agent": "Mozilla/5.0 (research chartbook)"}
TIMEOUT = 45


def _http_get(url, params=None):
    """GET text via curl, falling back to requests if curl is unavailable.

    curl is used first because some hosts (notably FRED's fredgraph.csv) stall
    under the requests/urllib TLS stack on this platform but respond fine to
    curl. curl ships with macOS and is present in the launchd runtime; requests
    is the fallback for environments without curl.
    """
    if params:
        url = url + "?" + urllib.parse.urlencode(params)
    try:
        # --http1.1 avoids intermittent HTTP/2 stream errors (curl rc 92) some
        # CDNs return; --retry rides out transient throttling.
        out = subprocess.run(
            ["curl", "-s", "--http1.1", "--retry", "2", "--retry-delay", "1",
             "--max-time", str(TIMEOUT), url],
            capture_output=True, text=True,
        )
        if out.returncode == 0 and out.stdout:
            return out.stdout
        log.debug("curl rc=%s for %s; trying requests", out.returncode, url)
    except FileNotFoundError:
        log.debug("curl not found; using requests for %s", url)
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text

SCHEMA = """
CREATE TABLE IF NOT EXISTS chartbook_series (
    series_id  TEXT PRIMARY KEY,
    source     TEXT,
    title      TEXT,
    frequency  TEXT,
    updated_at TEXT
);
CREATE TABLE IF NOT EXISTS chartbook_data (
    series_id  TEXT NOT NULL,
    date       TEXT NOT NULL,
    value      REAL,
    PRIMARY KEY (series_id, date)
);
CREATE INDEX IF NOT EXISTS idx_chartbook_data_sid ON chartbook_data(series_id);
"""


def _ensure_schema(conn):
    conn.executescript(SCHEMA)
    conn.commit()


def _upsert_series(conn, series_id, source, title, freq):
    conn.execute(
        """INSERT INTO chartbook_series (series_id, source, title, frequency, updated_at)
           VALUES (?,?,?,?,?)
           ON CONFLICT(series_id) DO UPDATE SET
               source=excluded.source, title=excluded.title,
               frequency=excluded.frequency, updated_at=excluded.updated_at""",
        (series_id, source, title, freq, datetime.now(timezone.utc).isoformat()),
    )


def _upsert_data(conn, series_id, rows):
    """rows = list of (date, value); value may be None."""
    conn.executemany(
        """INSERT INTO chartbook_data (series_id, date, value) VALUES (?,?,?)
           ON CONFLICT(series_id, date) DO UPDATE SET value=excluded.value""",
        [(series_id, d, v) for (d, v) in rows],
    )


# ---------------------------------------------------------------------------
# FRED — keyless CSV
# ---------------------------------------------------------------------------
def fetch_fred_series(series_id, start="1960-01-01"):
    """Return list of (date, value|None) for a FRED series via the keyless CSV."""
    text = _http_get(FRED_CSV, {"id": series_id, "cosd": start})
    reader = csv.reader(io.StringIO(text))
    rows = []
    header = next(reader, None)
    for line in reader:
        if len(line) < 2:
            continue
        date, raw = line[0].strip(), line[1].strip()
        if not date:
            continue
        try:
            val = float(raw)
        except ValueError:
            val = None          # FRED uses "." for missing
        rows.append((date, val))
    return rows


# ---------------------------------------------------------------------------
# World Bank — v2 JSON
# ---------------------------------------------------------------------------
def fetch_wb_series(indicator, iso3):
    """Return list of (year, value|None) for a World Bank indicator/country."""
    import json
    text = _http_get(WB_API.format(iso=iso3, ind=indicator),
                     {"format": "json", "per_page": "500"})
    payload = json.loads(text)
    if not isinstance(payload, list) or len(payload) < 2 or payload[1] is None:
        return []
    rows = []
    for obs in payload[1]:
        year = obs.get("date")
        val = obs.get("value")
        if year:
            rows.append((year, val))
    rows.sort(key=lambda t: t[0])     # WB returns newest-first
    return rows


def run() -> dict:
    """Fetch all chartbook series into cmm.db. Returns a summary dict."""
    conn = get_conn()
    _ensure_schema(conn)

    ok, fail, total_rows = 0, 0, 0
    errors = []

    # --- FRED ---
    for sid, (freq, title) in FRED_SERIES.items():
        try:
            rows = fetch_fred_series(sid)
            if not rows:
                raise ValueError("no rows returned")
            _upsert_series(conn, sid, "FRED", title, freq)
            _upsert_data(conn, sid, rows)
            conn.commit()
            ok += 1
            total_rows += len(rows)
            log.info("FRED %-22s %d rows (latest %s)", sid, len(rows), rows[-1][0])
        except Exception as e:
            fail += 1
            errors.append(f"FRED:{sid}: {e}")
            log.warning("FRED %s FAILED: %s", sid, e)
        time.sleep(0.15)

    # --- World Bank ---
    for indicator, iso3, title in WB_SERIES:
        sid = f"WB:{indicator}:{iso3}"
        try:
            rows = fetch_wb_series(indicator, iso3)
            if not rows:
                raise ValueError("no rows returned")
            _upsert_series(conn, sid, "World Bank", title, "A")
            _upsert_data(conn, sid, rows)
            conn.commit()
            ok += 1
            total_rows += len(rows)
            log.info("WB   %-28s %d rows", sid, len(rows))
        except Exception as e:
            fail += 1
            errors.append(f"{sid}: {e}")
            log.warning("WB %s FAILED: %s", sid, e)
        time.sleep(0.15)

    # --- fetch_log bookkeeping (matches project convention) ---
    try:
        conn.execute(
            """INSERT INTO fetch_log (source, feed_url, ok, error, item_count, fetched_at)
               VALUES (?,?,?,?,?,?)""",
            ("CHARTBOOK", FRED_CSV, 1 if fail == 0 else 0,
             "; ".join(errors)[:2000] if errors else None,
             total_rows, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
    except Exception:
        pass

    log.info("Chartbook fetch done: %d ok, %d failed, %d rows", ok, fail, total_rows)
    return {"ok": ok, "failed": fail, "rows": total_rows, "errors": errors}


def show():
    conn = get_conn()
    try:
        n_series = conn.execute("SELECT COUNT(*) FROM chartbook_series").fetchone()[0]
        n_rows = conn.execute("SELECT COUNT(*) FROM chartbook_data").fetchone()[0]
        print(f"chartbook_series: {n_series} series")
        print(f"chartbook_data:   {n_rows} rows")
        for r in conn.execute(
            """SELECT s.series_id, s.source, COUNT(d.date) n, MAX(d.date) latest
               FROM chartbook_series s LEFT JOIN chartbook_data d USING(series_id)
               GROUP BY s.series_id ORDER BY s.source, s.series_id"""
        ):
            print(f"  {r[1]:<11} {r[0]:<28} {r[2]:>5} rows  latest {r[3]}")
    except Exception as e:
        print("no chartbook tables yet:", e)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--show", action="store_true", help="print row counts and exit")
    args = ap.parse_args()
    if args.show:
        show()
    else:
        run()
