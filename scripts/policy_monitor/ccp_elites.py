"""
CCP Elite Leadership Database.
Source: Sine CPC Elite Leadership Database (cpcleadershipdata.pages.dev)
        7th–20th Party Congress · 1945–2022

Tables: ccp_cc_members, ccp_pb_members, ccp_psc_members
DB: data/ccp_elites.db
"""

import sqlite3
from pathlib import Path

_DB_DIR = Path(__file__).parent.parent.parent / "data"
_DB_PATH = _DB_DIR / "ccp_elites.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS ccp_cc_members (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    congress         TEXT NOT NULL,
    name             TEXT NOT NULL,
    name_cn          TEXT,
    birth_year       INTEGER,
    province         TEXT,
    is_alternate     TEXT,
    is_politburo     TEXT,
    is_psc           TEXT,
    entry_year       INTEGER,
    exit_year        INTEGER,
    congresses_served INTEGER,
    expelled         TEXT,
    expelled_when    TEXT,
    fate             TEXT,
    in_previous_cc   TEXT
);
CREATE INDEX IF NOT EXISTS idx_cc_name     ON ccp_cc_members(name);
CREATE INDEX IF NOT EXISTS idx_cc_congress ON ccp_cc_members(congress);

CREATE TABLE IF NOT EXISTS ccp_pb_members (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    congress         TEXT NOT NULL,
    name             TEXT NOT NULL,
    name_cn          TEXT,
    birth_year       INTEGER,
    is_psc           TEXT,
    fate             TEXT,
    in_previous_pb   TEXT,
    province         TEXT,
    congresses_served INTEGER
);
CREATE INDEX IF NOT EXISTS idx_pb_name     ON ccp_pb_members(name);
CREATE INDEX IF NOT EXISTS idx_pb_congress ON ccp_pb_members(congress);

CREATE TABLE IF NOT EXISTS ccp_psc_members (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    congress         TEXT NOT NULL,
    rank             INTEGER,
    name             TEXT NOT NULL,
    name_cn          TEXT,
    birth_year       INTEGER,
    province         TEXT,
    role             TEXT,
    notes            TEXT,
    congresses_served INTEGER
);
CREATE INDEX IF NOT EXISTS idx_psc_name     ON ccp_psc_members(name);
CREATE INDEX IF NOT EXISTS idx_psc_congress ON ccp_psc_members(congress);

CREATE TABLE IF NOT EXISTS ccp_elites_meta (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    source       TEXT,
    source_url   TEXT,
    imported_at  TEXT,
    cc_rows      INTEGER,
    pb_rows      INTEGER,
    psc_rows     INTEGER
);
"""


def get_db() -> sqlite3.Connection:
    _DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    return conn


def get_psc_by_congress(conn: sqlite3.Connection) -> list[dict]:
    cur = conn.execute(
        "SELECT congress, rank, name, name_cn, birth_year, province, role, notes, congresses_served "
        "FROM ccp_psc_members ORDER BY congress, rank"
    )
    return [dict(r) for r in cur.fetchall()]


def get_pb_by_congress(conn: sqlite3.Connection, congress: str = None) -> list[dict]:
    if congress:
        cur = conn.execute(
            "SELECT * FROM ccp_pb_members WHERE congress = ? ORDER BY name", (congress,)
        )
    else:
        cur = conn.execute("SELECT * FROM ccp_pb_members ORDER BY congress, name")
    return [dict(r) for r in cur.fetchall()]


def get_cc_by_congress(conn: sqlite3.Connection, congress: str = None) -> list[dict]:
    if congress:
        cur = conn.execute(
            "SELECT * FROM ccp_cc_members WHERE congress = ? ORDER BY is_alternate, name",
            (congress,),
        )
    else:
        cur = conn.execute(
            "SELECT * FROM ccp_cc_members ORDER BY congress, is_alternate, name"
        )
    return [dict(r) for r in cur.fetchall()]


def search_person(conn: sqlite3.Connection, q: str) -> dict:
    """All CC/PB/PSC appearances for a name query."""
    like = f"%{q}%"
    cc = conn.execute(
        "SELECT * FROM ccp_cc_members WHERE name LIKE ? OR name_cn LIKE ? ORDER BY congress",
        (like, like),
    ).fetchall()
    pb = conn.execute(
        "SELECT * FROM ccp_pb_members WHERE name LIKE ? OR name_cn LIKE ? ORDER BY congress",
        (like, like),
    ).fetchall()
    psc = conn.execute(
        "SELECT * FROM ccp_psc_members WHERE name LIKE ? OR name_cn LIKE ? ORDER BY congress",
        (like, like),
    ).fetchall()
    return {
        "cc": [dict(r) for r in cc],
        "pb": [dict(r) for r in pb],
        "psc": [dict(r) for r in psc],
    }


def get_congresses(conn: sqlite3.Connection) -> list[str]:
    cur = conn.execute(
        "SELECT DISTINCT congress FROM ccp_cc_members ORDER BY entry_year"
    )
    return [r[0] for r in cur.fetchall()]


def get_meta(conn: sqlite3.Connection) -> dict:
    row = conn.execute(
        "SELECT * FROM ccp_elites_meta ORDER BY id DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row else {}
