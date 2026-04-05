"""
Local SQLite storage for fetched policy items.

Database: data/feeds.db (auto-created)
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

DB_DIR = Path(__file__).parent.parent / "data"
DB_PATH = DB_DIR / "feeds.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS items (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source      TEXT NOT NULL,
    source_cn   TEXT,
    category    TEXT,
    title       TEXT NOT NULL,
    link        TEXT,
    published   TEXT,
    summary     TEXT,
    fetched_at  TEXT NOT NULL,
    UNIQUE(source, title, link)
);

CREATE TABLE IF NOT EXISTS fetch_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source      TEXT NOT NULL,
    feed_url    TEXT,
    ok          INTEGER NOT NULL,
    error       TEXT,
    item_count  INTEGER DEFAULT 0,
    fetched_at  TEXT NOT NULL
);
"""


def get_db() -> sqlite3.Connection:
    DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.executescript(SCHEMA)
    return conn


def store_feed_result(conn: sqlite3.Connection, result: dict) -> int:
    """Store a feed fetch result. Returns number of new items inserted."""
    now = datetime.utcnow().isoformat()

    conn.execute(
        "INSERT INTO fetch_log (source, feed_url, ok, error, item_count, fetched_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (
            result["source"],
            result.get("feed_url", ""),
            1 if result["ok"] else 0,
            result.get("error"),
            len(result.get("entries", [])),
            now,
        ),
    )

    new_count = 0
    for entry in result.get("entries", []):
        try:
            conn.execute(
                "INSERT OR IGNORE INTO items "
                "(source, source_cn, category, title, link, published, summary, fetched_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    result["source"],
                    result.get("source_cn", ""),
                    result.get("category", ""),
                    entry.get("title", ""),
                    entry.get("link", ""),
                    entry.get("published", ""),
                    entry.get("summary", ""),
                    now,
                ),
            )
            new_count += conn.total_changes  # rough proxy
        except sqlite3.IntegrityError:
            pass

    conn.commit()
    return new_count


def get_recent_items(conn: sqlite3.Connection, limit: int = 50) -> list[dict]:
    """Return the most recently fetched items."""
    cur = conn.execute(
        "SELECT source, source_cn, category, title, link, published, summary, fetched_at "
        "FROM items ORDER BY fetched_at DESC, id DESC LIMIT ?",
        (limit,),
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_item_count(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM items").fetchone()[0]


def get_fetch_stats(conn: sqlite3.Connection) -> dict:
    """Summary stats from fetch_log."""
    row = conn.execute(
        "SELECT COUNT(*), SUM(ok), SUM(item_count) FROM fetch_log"
    ).fetchone()
    return {
        "total_fetches": row[0],
        "successful": row[1] or 0,
        "total_items_seen": row[2] or 0,
    }
