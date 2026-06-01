"""
USITC Harmonized Tariff Schedule (HTS) scraper — China-specific.

Fetches current US tariff rates from the USITC HTS REST API (no auth required):
  https://hts.usitc.gov/reststop/api/details/sectionJSON

Two passes:
  1. Chapter 99 (9903.xx.xx) — Section 301 additional duties on Chinese goods.
     These are the executive-action tariffs (7.5 %, 25 %, 145 %, etc.) layered on
     top of normal MFN rates.  Filter: rows whose description mentions China/PRC.

  2. Configurable chapters (default: 84, 85, 87, 72, 73, 27, 28, 39, 62, 90) —
     standard Column 1 General (MFN) rates for key trade categories.
     Together with Chapter 99 this gives: base_rate + section_301 = effective rate.

Table: usitc_hts in data/trade_stats.db

Each row = one HTS line item (8–10 digit code), replaced on every run so the
table always reflects the current published schedule.

Usage:
    python usitc_hts.py            # fetch Chapter 99 + default chapters
    python usitc_hts.py --show     # print Section 301 snapshot
    python usitc_hts.py --full     # fetch all 99 chapters (slow, ~15 min)
    python usitc_hts.py --force    # re-fetch even if already fetched today

TODO: EU TARIC — ec.europa.eu/taxation_customs/dds2/taric — current EU MFN
      and applied rates for Chinese imports; update when EU Trade Helpdesk API
      is stable enough for automated use.

TODO: China tariff schedule — tariff.customs.gov.cn — China's current MFN and
      preferential rates; requires Chinese-IP access or VPN (same constraint as
      MOFCOM).  Prioritise once the proxy/VPN solution is in place.
"""

import argparse
import logging
import sqlite3
import time
from datetime import datetime, timezone

import requests

from backend.storage import DB_DIR

log = logging.getLogger("usitc_hts")


_BASE = "https://hts.usitc.gov/reststop/api/details/sectionJSON"
_HEADERS = {"User-Agent": "CMM/1.0", "Accept": "application/json"}

# Chapters to fetch by default (key US-China trade categories)
DEFAULT_CHAPTERS = [27, 28, 39, 62, 72, 73, 84, 85, 87, 90]

_CHINA_KEYWORDS = {"china", "people's republic", "prc", "chinese"}

_SCHEMA = """
CREATE TABLE IF NOT EXISTS usitc_hts (
    hts_code        TEXT PRIMARY KEY,
    description     TEXT,
    general_rate    TEXT,
    special_rates   TEXT,
    other_rate      TEXT,
    units           TEXT,
    chapter         INTEGER,
    is_section301   INTEGER NOT NULL DEFAULT 0,
    fetched_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_uh_chapter      ON usitc_hts(chapter);
CREATE INDEX IF NOT EXISTS idx_uh_section301   ON usitc_hts(is_section301);
"""


def get_trade_stats_db() -> sqlite3.Connection:
    from backend.storage import get_conn as _storage_get_conn
    conn = _storage_get_conn()
    conn.executescript(_SCHEMA)
    return conn


def _fetch_chapter(chapter: int) -> list[dict]:
    """Fetch all HTS line items for one chapter via paginated API calls."""
    rows = []
    offset = 0
    limit = 500

    while True:
        params = {"query": f"chapter:{chapter:02d}", "offset": offset, "limit": limit}
        try:
            r = requests.get(_BASE, headers=_HEADERS, params=params, timeout=30)
            if r.status_code == 404:
                break
            if r.status_code in (429, 503):
                log.warning(f"  Rate limited on chapter {chapter}, sleeping 60s")
                time.sleep(60)
                r = requests.get(_BASE, headers=_HEADERS, params=params, timeout=30)
            r.raise_for_status()
        except Exception as exc:
            log.warning(f"  Chapter {chapter} offset {offset}: {exc}")
            break

        batch = r.json() if isinstance(r.json(), list) else r.json().get("results", [])
        if not batch:
            break

        rows.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
        time.sleep(0.3)

    return rows


def _is_china_section301(description: str) -> bool:
    desc_lower = (description or "").lower()
    return any(kw in desc_lower for kw in _CHINA_KEYWORDS)


def _parse_chapter(hts_code: str) -> int | None:
    try:
        return int(hts_code.replace(".", "")[:2])
    except (ValueError, TypeError):
        return None


def store_chapter(conn: sqlite3.Connection, chapter: int, rows: list[dict],
                  is_section301_chapter: bool = False) -> int:
    now = datetime.now(timezone.utc).isoformat()
    stored = 0
    for item in rows:
        hts_code = (item.get("htsno") or "").strip()
        if not hts_code:
            continue

        description = item.get("description") or ""
        is_301 = 1 if (is_section301_chapter and _is_china_section301(description)) else 0

        conn.execute(
            """INSERT OR REPLACE INTO usitc_hts
               (hts_code, description, general_rate, special_rates, other_rate,
                units, chapter, is_section301, fetched_at)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                hts_code,
                description,
                item.get("general") or item.get("rate") or "",
                item.get("special") or "",
                item.get("other") or "",
                item.get("unit1") or item.get("units") or "",
                chapter,
                is_301,
                now,
            ),
        )
        stored += 1

    conn.commit()
    return stored


def fetch_chapter99(conn: sqlite3.Connection) -> int:
    """Fetch Chapter 99 — Section 301 and other special duties."""
    log.info("[USITC] Fetching Chapter 99 (Section 301 duties)")
    rows = _fetch_chapter(99)
    china_rows = [r for r in rows if _is_china_section301(r.get("description", ""))]
    n = store_chapter(conn, 99, rows, is_section301_chapter=True)
    log.info(f"  Chapter 99: {n} total lines, {len(china_rows)} China-specific Section 301 entries")
    return n


def fetch_chapters(conn: sqlite3.Connection, chapters: list[int]) -> int:
    """Fetch standard MFN chapters."""
    total = 0
    for ch in chapters:
        log.info(f"[USITC] Fetching Chapter {ch:02d}")
        rows = _fetch_chapter(ch)
        n = store_chapter(conn, ch, rows)
        log.info(f"  Chapter {ch:02d}: {n} lines")
        total += n
        time.sleep(0.5)
    return total


def already_fetched_today(conn: sqlite3.Connection) -> bool:
    today = datetime.now(timezone.utc).date().isoformat()
    row = conn.execute(
        "SELECT fetched_at FROM usitc_hts WHERE is_section301=1 LIMIT 1"
    ).fetchone()
    return bool(row and row[0][:10] == today)


def fetch_all(conn: sqlite3.Connection, full: bool = False,
              force: bool = False) -> int:
    if not force and already_fetched_today(conn):
        log.info("USITC HTS already fetched today; use --force to re-fetch")
        return 0

    n1 = fetch_chapter99(conn)
    chapters = list(range(1, 100)) if full else DEFAULT_CHAPTERS
    n2 = fetch_chapters(conn, chapters)
    log.info(f"USITC HTS done: {n1 + n2} total lines stored")
    return n1 + n2


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    parser = argparse.ArgumentParser(description="Fetch USITC HTS tariff data (China focus)")
    parser.add_argument("--show",  action="store_true", help="Print Section 301 China rates")
    parser.add_argument("--full",  action="store_true", help="Fetch all 99 chapters")
    parser.add_argument("--force", action="store_true", help="Re-fetch even if done today")
    args = parser.parse_args()

    conn = get_trade_stats_db()

    if args.show:
        try:
            rows = conn.execute(
                """SELECT hts_code, description, general_rate, fetched_at
                   FROM usitc_hts
                   WHERE is_section301 = 1
                   ORDER BY hts_code
                   LIMIT 80"""
            ).fetchall()
            if not rows:
                print("No Section 301 data yet — run without --show first.")
            else:
                print(f"{'HTS Code':<16} {'Rate':<12} Description")
                print("-" * 80)
                for hts, desc, rate, _ in rows:
                    print(f"{hts:<16} {(rate or ''):<12} {(desc or '')[:60]}")
                total = conn.execute("SELECT COUNT(*) FROM usitc_hts WHERE is_section301=1").fetchone()[0]
                print(f"\n{total} Section 301 China entries | "
                      f"fetched {rows[0][3][:10] if rows else 'never'}")
        except Exception as e:
            print(f"No data yet: {e}")
        conn.close()
        return

    ok = fetch_all(conn, full=args.full, force=args.force)
    log.info(f"Done: {ok} lines stored")
    conn.close()


if __name__ == "__main__":
    main()
