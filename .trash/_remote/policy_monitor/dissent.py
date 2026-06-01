"""
Fetch dissent events from the China Dissent Monitor (chinadissent.net).

Data is fetched via their internal JSON API (requires CSRF token).
Events are stored in data/feeds.db in the `dissent_events` table.

Usage:
    python dissent.py              # fetch all events, store to DB
    python dissent.py --show       # show latest stored events
    python dissent.py --summary    # province-level summary
"""

import argparse
import json
import logging
import re
import sqlite3
from datetime import datetime
from pathlib import Path

import requests

from policy_monitor.storage import DB_DIR, DB_PATH

log = logging.getLogger("dissent")

DISSENT_SCHEMA = """
CREATE TABLE IF NOT EXISTS dissent_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id         TEXT UNIQUE NOT NULL,
    date_start      TEXT,
    date_end        TEXT,
    province        TEXT,
    province_id     INTEGER,
    location        TEXT,
    offline_online  TEXT,
    mode            TEXT,
    issue           TEXT,
    target          TEXT,
    group_name      TEXT,
    demands         TEXT,
    description     TEXT,
    participants    TEXT,
    repression      TEXT,
    concession      TEXT,
    verification    TEXT,
    fetched_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dissent_provinces (
    id      INTEGER PRIMARY KEY,
    name    TEXT NOT NULL,
    name_cn TEXT
);
"""


def get_dissent_db() -> sqlite3.Connection:
    DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.executescript(DISSENT_SCHEMA)
    return conn


def _get_session():
    """Create a session with CSRF token for chinadissent.net API."""
    session = requests.Session()
    session.headers["User-Agent"] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    resp = session.get("https://chinadissent.net/en/detail", timeout=30)
    resp.raise_for_status()
    match = re.search(r'csrf-token" content="([^"]+)', resp.text)
    if not match:
        raise RuntimeError("Could not extract CSRF token from chinadissent.net")
    csrf = match.group(1)
    session.headers.update({
        "X-CSRF-TOKEN": csrf,
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json",
        "Referer": "https://chinadissent.net/en/detail",
    })
    return session


def fetch_provinces(session) -> list[dict]:
    """Fetch province list."""
    resp = session.post(
        "https://chinadissent.net/api/dissents/provinces",
        json={"lang": "en"},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def fetch_events(session, max_pages: int = 35) -> list[dict]:
    """Fetch all dissent events page by page."""
    all_events = []
    for page in range(1, max_pages + 1):
        log.info(f"  Fetching page {page}...")
        resp = session.post(
            "https://chinadissent.net/api/dissents/dissents",
            json={"lang": "en", "page": page},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break
        all_events.extend(data)
        log.info(f"  Page {page}: {len(data)} events (total: {len(all_events)})")
        if len(data) < 100:  # likely last page
            # Keep going — their page size might vary
            pass
    return all_events


def _extract_name(obj):
    """Extract name from a relational object or list."""
    if not obj:
        return ""
    if isinstance(obj, dict):
        trans = obj.get("translations", [])
        for t in trans:
            if t.get("locale") == "en":
                return t.get("name", "")
        return obj.get("name", "")
    if isinstance(obj, list):
        names = []
        for item in obj:
            n = _extract_name(item)
            if n:
                names.append(n)
        return "; ".join(names)
    return str(obj)


def _extract_description(event):
    """Extract English description from translations."""
    trans = event.get("translations", [])
    for t in trans:
        if t.get("locale") == "en":
            return t.get("description", "")
    return ""


def store_events(conn: sqlite3.Connection, events: list[dict]) -> int:
    """Store events to DB. Returns count of new events inserted."""
    now = datetime.utcnow().isoformat()
    inserted = 0
    for e in events:
        case_id = e.get("case_id", "")
        if not case_id:
            continue
        try:
            province_obj = e.get("province", {}) or {}
            location_obj = e.get("location", {}) or {}

            conn.execute(
                "INSERT OR IGNORE INTO dissent_events "
                "(case_id, date_start, date_end, province, province_id, location, "
                "offline_online, mode, issue, target, group_name, demands, description, "
                "participants, repression, concession, verification, fetched_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    case_id,
                    e.get("date_format") or e.get("start_date_format") or "",
                    e.get("end_date_format") or "",
                    _extract_name(province_obj),
                    province_obj.get("id"),
                    _extract_name(location_obj),
                    _extract_name(e.get("type")),
                    _extract_name(e.get("has_modes")),
                    _extract_name(e.get("has_issues")),
                    _extract_name(e.get("has_targets")),
                    _extract_name(e.get("group")),
                    e.get("demands", ""),
                    _extract_description(e),
                    str(e.get("participants", "")),
                    _extract_name(e.get("repression_type")),
                    _extract_name(e.get("concession_type")),
                    _extract_name(e.get("verification_tier")),
                    now,
                ),
            )
            inserted += 1
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    return inserted


def store_provinces(conn: sqlite3.Connection, provinces: list[dict]):
    for p in provinces:
        pid = p.get("id")
        trans = p.get("translations", [])
        name_en = ""
        name_cn = ""
        for t in trans:
            if t.get("locale") == "en":
                name_en = t.get("name", "")
            elif t.get("locale") == "zh":
                name_cn = t.get("name", "")
        if pid and name_en:
            conn.execute(
                "INSERT OR REPLACE INTO dissent_provinces (id, name, name_cn) VALUES (?,?,?)",
                (pid, name_en, name_cn),
            )
    conn.commit()


def get_province_summary(conn: sqlite3.Connection) -> list[dict]:
    """Aggregate dissent events by province."""
    cur = conn.execute("""
        SELECT province, province_id, COUNT(*) as count,
               MIN(date_start) as earliest, MAX(date_start) as latest
        FROM dissent_events
        WHERE province != ''
        GROUP BY province
        ORDER BY count DESC
    """)
    return [
        {"province": r[0], "province_id": r[1], "count": r[2], "earliest": r[3], "latest": r[4]}
        for r in cur.fetchall()
    ]


def get_recent_events(conn: sqlite3.Connection, limit: int = 50, province: str = "") -> list[dict]:
    """Return recent dissent events."""
    query = """
        SELECT case_id, date_start, province, location, offline_online,
               mode, issue, target, description, participants
        FROM dissent_events WHERE 1=1
    """
    params = []
    if province:
        query += " AND province = ?"
        params.append(province)
    query += " ORDER BY date_start DESC LIMIT ?"
    params.append(limit)
    cur = conn.execute(query, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    parser = argparse.ArgumentParser(description="Fetch China Dissent Monitor data")
    parser.add_argument("--show", action="store_true", help="Show latest events from DB")
    parser.add_argument("--summary", action="store_true", help="Province summary")
    parser.add_argument("--max-pages", type=int, default=35, help="Max pages to fetch")
    args = parser.parse_args()

    conn = get_dissent_db()

    if args.summary:
        summary = get_province_summary(conn)
        total = sum(s["count"] for s in summary)
        print(f"Total events: {total} across {len(summary)} provinces\n")
        print(f"{'Province':<25} {'Count':>6} {'Earliest':<12} {'Latest':<12}")
        print("-" * 58)
        for s in summary:
            print(f"{s['province']:<25} {s['count']:>6} {s['earliest']:<12} {s['latest']:<12}")
        conn.close()
        return

    if args.show:
        events = get_recent_events(conn, limit=20)
        for e in events:
            print(f"[{e['date_start']}] {e['province']} / {e['location']}")
            print(f"  {e['mode']} — {e['issue']}")
            if e["description"]:
                print(f"  {e['description'][:120]}")
            print()
        conn.close()
        return

    log.info("Connecting to chinadissent.net...")
    session = _get_session()

    log.info("Fetching provinces...")
    provinces = fetch_provinces(session)
    store_provinces(conn, provinces)
    log.info(f"  {len(provinces)} provinces stored")

    log.info("Fetching dissent events...")
    events = fetch_events(session, max_pages=args.max_pages)
    inserted = store_events(conn, events)
    total = conn.execute("SELECT COUNT(*) FROM dissent_events").fetchone()[0]
    log.info(f"Done: {len(events)} fetched, {inserted} new, {total} total in DB")
    conn.close()


if __name__ == "__main__":
    main()
