"""
Fetch data from China's National Bureau of Statistics (NBS) EasyQuery API.

Ported from CMMold (nbs_tree.py, Categories.py, main.py).  Uses proper
browser headers and urllib3 retry so the NBS API responds correctly.

Two-phase workflow:
  1. Discover all series once:   python -m policy_monitor.nbs --discover
     Walks the NBS indicator tree and saves data/nbs_tree_cache.json.
  2. Fetch data:                 python -m policy_monitor.nbs
     Reads the cache and pulls time-series data for every leaf indicator.

Usage:
    python -m policy_monitor.nbs --discover         # walk tree, cache all series
    python -m policy_monitor.nbs                    # fetch data (uses cache)
    python -m policy_monitor.nbs --show             # show latest snapshots
    python -m policy_monitor.nbs --show-series A01010101
"""

import argparse
import json
import logging
import sqlite3
import time
from datetime import datetime, date, timezone
from pathlib import Path
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

import requests

import sys as _sys
_sys.path.insert(0, str(Path(__file__).parent.parent))

from policy_monitor.storage import DB_DIR, DB_PATH

log = logging.getLogger("nbs")

BASE_URL = "http://data.stats.gov.cn/easyquery.htm"

# Headers that make NBS API respond — without these it returns 403
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "http://data.stats.gov.cn/",
    "X-Requested-With": "XMLHttpRequest",
}

TREE_CACHE_PATH = DB_DIR / "nbs_tree_cache.json"

# ---------------------------------------------------------------------------
# DB schema
# ---------------------------------------------------------------------------
NBS_SCHEMA = """
CREATE TABLE IF NOT EXISTS nbs_series (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    indicator   TEXT NOT NULL,
    name        TEXT,
    category    TEXT,
    freq        TEXT,
    date        TEXT,
    value       REAL,
    unit        TEXT,
    fetched_at  TEXT NOT NULL,
    UNIQUE(indicator, date)
);

CREATE TABLE IF NOT EXISTS nbs_snapshots (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    indicator    TEXT NOT NULL,
    name         TEXT,
    category     TEXT,
    freq         TEXT,
    latest_value REAL,
    unit         TEXT,
    data_date    TEXT,
    fetched_at   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_nbs_series_ind  ON nbs_series(indicator);
CREATE INDEX IF NOT EXISTS idx_nbs_series_date ON nbs_series(date);
"""


def get_nbs_db() -> sqlite3.Connection:
    DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.executescript(NBS_SCHEMA)
    return conn


# ---------------------------------------------------------------------------
# Shared HTTP session
# ---------------------------------------------------------------------------

def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update(HEADERS)
    return session


# ---------------------------------------------------------------------------
# Period helpers
# ---------------------------------------------------------------------------

def _monthly_periods(n: int = 60) -> str:
    today = date.today()
    year, month = today.year, today.month
    codes: list[str] = []
    for _ in range(n):
        codes.append(f"{year:04d}{month:02d}")
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    return ",".join(reversed(codes))


def _annual_periods(n: int = 25) -> str:
    year = date.today().year
    return ",".join(str(y) for y in range(year - n + 1, year + 1))


# ---------------------------------------------------------------------------
# Data fetch (ported from CMMold/main.py)
# ---------------------------------------------------------------------------

def _load_nbs(series: str, periods: str, freq: str,
              session: requests.Session) -> list[tuple[str, float]]:
    """
    Query NBS QueryData for one series + period range.
    Returns [(date_str ISO, value), ...] sorted ascending.
    """
    dbcode = "hgyd" if freq == "month" else "hgnd"
    params = {
        "m": "QueryData",
        "dbcode": dbcode,
        "rowcode": "zb",
        "colcode": "sj",
        "wds": "[]",
        "dfwds": json.dumps([
            {"wdcode": "zb", "valuecode": series},
            {"wdcode": "sj", "valuecode": periods},
        ]),
        "k1": int(datetime.now().timestamp() * 1000),
    }
    r = session.get(BASE_URL, params=params, timeout=20)
    r.raise_for_status()

    nodes = r.json().get("returndata", {}).get("datanodes", [])
    results: list[tuple[str, float]] = []
    for d in nodes:
        raw_val = d.get("data", {}).get("data")
        if raw_val is None:
            continue
        try:
            val = float(raw_val)
        except (TypeError, ValueError):
            continue
        wds = d.get("wds", [])
        date_code = next((w["valuecode"] for w in wds if w.get("wdcode") == "sj"), None)
        if not date_code:
            continue
        if freq == "month" and len(date_code) >= 6:
            date_str = f"{date_code[:4]}-{date_code[4:6]}-01"
        else:
            date_str = f"{date_code[:4]}-01-01"
        results.append((date_str, val))

    results.sort(key=lambda x: x[0])
    return results


# ---------------------------------------------------------------------------
# Tree discovery (ported from CMMold/nbs_tree.py + Categories.py)
# ---------------------------------------------------------------------------

def _get_tree_children(code: str, dbcode: str, session: requests.Session) -> list[dict]:
    """
    Fetch one level of the NBS indicator tree.
    Handles both response shapes returned by the NBS API.
    """
    params = {"m": "getTree", "id": code, "dbcode": dbcode, "wdcode": "zb"}
    r = session.get(BASE_URL, params=params, timeout=15)
    r.raise_for_status()
    payload = r.json()

    # Shape 1: {"wdnodes": [{"nodes": [...]}]}
    if isinstance(payload, dict):
        wdnodes = payload.get("wdnodes") or []
        if wdnodes:
            return wdnodes[0].get("nodes", [])

    # Shape 2: plain list of nodes
    if isinstance(payload, list):
        return payload

    return []


def _crawl_tree(root: str, dbcode: str, session: requests.Session,
                delay: float = 0.15) -> list[dict]:
    """
    Recursively walk the NBS tree. Returns a flat list of all nodes
    (both parent and leaf), each with keys: code, name, cname, isParent, unit, parent.
    """
    visited: set[str] = set()
    rows: list[dict] = []

    def _walk(node_id: str, parent: str | None):
        if node_id in visited:
            return
        visited.add(node_id)
        try:
            children = _get_tree_children(node_id, dbcode, session)
            time.sleep(delay)
        except Exception as e:
            log.warning(f"  Tree fetch failed for {node_id}: {e}")
            return
        for ch in children:
            code = ch.get("code") or ch.get("id", "")
            rows.append({
                "parent": parent,
                "code": code,
                "name": ch.get("name") or ch.get("cname", ""),
                "cname": ch.get("cname", ""),
                "isParent": ch.get("isParent", False),
                "unit": ch.get("unit", ""),
            })
            if ch.get("isParent", False):
                _walk(code, parent=code)

    _walk(root, parent=None)
    return rows


def discover_all_series(dbcodes: list[str] | None = None,
                        delay: float = 0.15) -> dict[str, dict]:
    """
    Walk the NBS tree for each dbcode, cache all leaf-level series to
    TREE_CACHE_PATH and return the registry dict.
    """
    if dbcodes is None:
        dbcodes = ["hgyd", "hgnd"]

    freq_map = {"hgyd": "month", "hgnd": "year"}
    session = _make_session()
    registry: dict[str, dict] = {}

    for dbcode in dbcodes:
        freq = freq_map.get(dbcode, "month")
        log.info(f"Discovering NBS tree for dbcode={dbcode} ...")
        nodes = _crawl_tree("zb", dbcode, session, delay=delay)
        leaves = [n for n in nodes if not n["isParent"]]
        for node in leaves:
            code = node["code"]
            if not code:
                continue
            # Use the first non-leaf ancestor as category
            category = "nbs"
            registry[code] = {
                "name": node["name"],
                "cname": node["cname"],
                "unit": node["unit"],
                "category": category,
                "freq": freq,
            }
        log.info(f"  {dbcode}: {len(leaves)} leaf series found")

    DB_DIR.mkdir(parents=True, exist_ok=True)
    TREE_CACHE_PATH.write_text(
        json.dumps(registry, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    log.info(f"Saved {len(registry)} series to {TREE_CACHE_PATH}")
    return registry


def load_indicator_registry() -> dict[str, dict]:
    """Return registry from tree cache, or empty dict if not yet discovered."""
    if TREE_CACHE_PATH.exists():
        try:
            data = json.loads(TREE_CACHE_PATH.read_text(encoding="utf-8"))
            log.info(f"Loaded {len(data)} NBS series from {TREE_CACHE_PATH.name}")
            return data
        except Exception as e:
            log.warning(f"Failed to load tree cache: {e}")
    log.warning("No NBS tree cache found. Run --discover first.")
    return {}


# ---------------------------------------------------------------------------
# Per-series fetch with safe wrapper
# ---------------------------------------------------------------------------

def _safe_fetch(name: str, fn):
    try:
        result = fn()
        log.info(f"  OK  {name}")
        return result
    except Exception as e:
        log.warning(f"  FAIL {name}: {type(e).__name__}: {str(e)[:120]}")
        return None


def fetch_nbs_series(code: str, meta: dict,
                     session: requests.Session) -> tuple[list, dict | None]:
    freq = meta.get("freq", "month")
    periods = _monthly_periods(60) if freq == "month" else _annual_periods(25)
    points = _load_nbs(code, periods, freq, session)
    if not points:
        return [], None

    name = meta.get("name") or meta.get("cname") or code
    category = meta.get("category", "nbs")
    unit = meta.get("unit", "")
    now = datetime.now(timezone.utc).isoformat()

    rows = [
        (code, name, category, freq, date_str, val, unit, now)
        for date_str, val in points
    ]
    latest_date, latest_val = points[-1]
    snapshot = {
        "indicator": code, "name": name, "category": category, "freq": freq,
        "latest_value": latest_val, "unit": unit,
        "data_date": latest_date, "fetched_at": now,
    }
    return rows, snapshot


# ---------------------------------------------------------------------------
# Bulk fetch
# ---------------------------------------------------------------------------

def fetch_all_nbs(inter_request_delay: float = 0.5) -> tuple[list, list, int, int]:
    """
    Fetch all NBS indicators from the registry.
    Returns (all_rows, all_snapshots, ok_count, fail_count).
    """
    registry = load_indicator_registry()
    if not registry:
        return [], [], 0, 0

    session = _make_session()
    all_rows: list = []
    all_snapshots: list = []
    ok = fail = 0

    for code, meta in registry.items():
        def _fetch(c=code, m=meta):
            result = fetch_nbs_series(c, m, session)
            time.sleep(inter_request_delay)
            return result

        result = _safe_fetch(f"{code} {meta.get('name', '')[:40]}", _fetch)
        if result is not None:
            rows, snap = result
            all_rows.extend(rows)
            if snap:
                all_snapshots.append(snap)
            ok += 1
        else:
            fail += 1

    return all_rows, all_snapshots, ok, fail


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

def store_nbs_data(conn: sqlite3.Connection, rows: list, snapshots: list) -> int:
    inserted = 0
    for indicator, name, category, freq, date_str, value, unit, fetched_at in rows:
        try:
            conn.execute(
                "INSERT OR IGNORE INTO nbs_series "
                "(indicator, name, category, freq, date, value, unit, fetched_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (indicator, name, category, freq, date_str, value, unit, fetched_at),
            )
            inserted += conn.total_changes
        except sqlite3.IntegrityError:
            pass

    for snap in snapshots:
        conn.execute(
            "INSERT INTO nbs_snapshots "
            "(indicator, name, category, freq, latest_value, unit, data_date, fetched_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (
                snap["indicator"], snap["name"], snap["category"], snap["freq"],
                snap["latest_value"], snap["unit"], snap["data_date"], snap["fetched_at"],
            ),
        )
    conn.commit()
    return inserted


def get_nbs_snapshots(conn: sqlite3.Connection) -> list[dict]:
    cur = conn.execute("""
        SELECT indicator, name, category, freq, latest_value, unit, data_date, fetched_at
        FROM nbs_snapshots
        WHERE id IN (SELECT MAX(id) FROM nbs_snapshots GROUP BY indicator)
        ORDER BY category, indicator
    """)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_nbs_series(conn: sqlite3.Connection, indicator: str, limit: int = 120) -> list[dict]:
    cur = conn.execute(
        "SELECT date, value FROM nbs_series WHERE indicator = ? ORDER BY date DESC LIMIT ?",
        (indicator, limit),
    )
    return [{"date": row[0], "value": row[1]} for row in cur.fetchall()]


def get_nbs_indicators(conn: sqlite3.Connection) -> list[dict]:
    cur = conn.execute("""
        SELECT indicator, name, category, freq,
               MIN(date) as min_date, MAX(date) as max_date, COUNT(*) as cnt
        FROM nbs_series WHERE value IS NOT NULL
        GROUP BY indicator ORDER BY category, indicator
    """)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Fetch NBS (China National Bureau of Statistics) data")
    parser.add_argument("--discover", action="store_true",
                        help="Walk NBS tree and cache all series codes (run once)")
    parser.add_argument("--show", action="store_true",
                        help="Show latest snapshots from DB")
    parser.add_argument("--show-series", metavar="CODE",
                        help="Show time series for one indicator code")
    args = parser.parse_args()

    if args.discover:
        log.info("Discovering NBS indicator tree (takes a few minutes)...")
        registry = discover_all_series()
        print(f"Discovered {len(registry)} series. Saved to {TREE_CACHE_PATH}")
        return

    conn = get_nbs_db()

    if args.show:
        snaps = get_nbs_snapshots(conn)
        if not snaps:
            print("No NBS data yet. Run: python -m policy_monitor.nbs --discover  then  python -m policy_monitor.nbs")
            conn.close()
            return
        print(f"{'Code':<14} {'Name':<40} {'Value':>12} {'Unit':<12} {'Date':<12}")
        print("-" * 94)
        for s in snaps:
            print(f"{s['indicator']:<14} {(s['name'] or '')[:40]:<40} "
                  f"{s['latest_value']:>12.4f} {s['unit']:<12} {s['data_date']:<12}")
        conn.close()
        return

    if args.show_series:
        data = get_nbs_series(conn, args.show_series)
        if not data:
            print(f"No data for '{args.show_series}'.")
            conn.close()
            return
        for d in reversed(data):
            print(f"  {d['date']}  {d['value']}")
        conn.close()
        return

    registry = load_indicator_registry()
    if not registry:
        print("No series registry found. Run --discover first.")
        conn.close()
        return

    log.info(f"Fetching NBS data ({len(registry)} indicators)...")
    all_rows, all_snapshots, ok, fail = fetch_all_nbs()
    inserted = store_nbs_data(conn, all_rows, all_snapshots)
    total = conn.execute("SELECT COUNT(*) FROM nbs_series").fetchone()[0]
    log.info(f"Done: {ok} OK, {fail} failed, {inserted} new points, {total} total in DB")
    conn.close()


if __name__ == "__main__":
    main()
