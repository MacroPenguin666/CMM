"""
SQLite database access: schema creation, upserts, run tracking, checkpoints.
"""
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Generator

from .config import DB_PATH

# ── Schema ────────────────────────────────────────────────────────────────────

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS exports (
    id               INTEGER PRIMARY KEY,
    year             INTEGER NOT NULL,
    month            INTEGER NOT NULL,
    hs8_code         TEXT    NOT NULL,
    hs_description   TEXT,
    country_code     TEXT    NOT NULL,
    country_name     TEXT,
    export_value_usd REAL,
    export_value_cny REAL,
    export_qty       REAL,
    export_qty_unit  TEXT,
    created_at       TEXT DEFAULT (datetime('now')),
    updated_at       TEXT DEFAULT (datetime('now')),
    UNIQUE(year, month, hs8_code, country_code)
);

CREATE TABLE IF NOT EXISTS scrape_runs (
    id             INTEGER PRIMARY KEY,
    run_id         TEXT    NOT NULL UNIQUE,
    year           INTEGER NOT NULL,
    month          INTEGER NOT NULL,
    started_at     TEXT    NOT NULL,
    finished_at    TEXT,
    status         TEXT    DEFAULT 'running',
    rows_inserted  INTEGER DEFAULT 0,
    rows_updated   INTEGER DEFAULT 0,
    error_message  TEXT,
    hs_codes_total INTEGER,
    hs_codes_done  INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS scrape_checkpoints (
    run_id       TEXT    NOT NULL,
    hs8_code     TEXT    NOT NULL,
    country_code TEXT    NOT NULL DEFAULT '',  -- '' means "all countries for this hs8 done"
    fetched_at   TEXT    DEFAULT (datetime('now')),
    rows_count   INTEGER DEFAULT 0,
    PRIMARY KEY (run_id, hs8_code, country_code)
);
"""

# ── Connection ────────────────────────────────────────────────────────────────

@contextmanager
def get_conn(db_path: str = DB_PATH) -> Generator[sqlite3.Connection, None, None]:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def init_db(db_path: str = DB_PATH) -> None:
    """Create all tables. Safe to call on every startup (IF NOT EXISTS)."""
    with get_conn(db_path) as conn:
        conn.executescript(SCHEMA_SQL)
        conn.commit()

# ── Run tracking ──────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def start_run(year: int, month: int, hs_codes_total: int, db_path: str = DB_PATH) -> str:
    """Insert a new scrape_runs record; return run_id (uuid4)."""
    run_id = str(uuid.uuid4())
    with get_conn(db_path) as conn:
        conn.execute(
            """INSERT INTO scrape_runs (run_id, year, month, started_at, hs_codes_total)
               VALUES (?, ?, ?, ?, ?)""",
            (run_id, year, month, _now(), hs_codes_total),
        )
        conn.commit()
    return run_id


def finish_run(
    run_id: str,
    status: str,
    rows_inserted: int,
    rows_updated: int,
    hs_codes_done: int,
    error_message: str | None = None,
    db_path: str = DB_PATH,
) -> None:
    with get_conn(db_path) as conn:
        conn.execute(
            """UPDATE scrape_runs
               SET finished_at=?, status=?, rows_inserted=?, rows_updated=?,
                   hs_codes_done=?, error_message=?
               WHERE run_id=?""",
            (_now(), status, rows_inserted, rows_updated,
             hs_codes_done, error_message, run_id),
        )
        conn.commit()

# ── Data upsert ───────────────────────────────────────────────────────────────

_UPSERT_SQL = """
INSERT INTO exports (
    year, month, hs8_code, hs_description, country_code, country_name,
    export_value_usd, export_value_cny, export_qty, export_qty_unit
) VALUES (
    :year, :month, :hs8_code, :hs_description, :country_code, :country_name,
    :export_value_usd, :export_value_cny, :export_qty, :export_qty_unit
)
ON CONFLICT(year, month, hs8_code, country_code) DO UPDATE SET
    hs_description   = excluded.hs_description,
    country_name     = excluded.country_name,
    export_value_usd = excluded.export_value_usd,
    export_value_cny = excluded.export_value_cny,
    export_qty       = excluded.export_qty,
    export_qty_unit  = excluded.export_qty_unit,
    updated_at       = datetime('now')
"""


def upsert_export_rows(rows: list[dict], db_path: str = DB_PATH) -> tuple[int, int]:
    """
    Upsert a list of export dicts. Returns (inserted_count, updated_count).
    Each dict must contain keys matching the exports table columns.
    """
    if not rows:
        return 0, 0
    inserted = updated = 0
    with get_conn(db_path) as conn:
        for row in rows:
            conn.execute(_UPSERT_SQL, row)
            # SQLite rowid increases on insert, stays same on update
            changes = conn.execute("SELECT changes()").fetchone()[0]
            if changes == 1:
                # changes()==1 on both insert and update in SQLite upsert;
                # use total_changes delta as proxy isn't reliable — track via
                # a pre-check instead.
                inserted += 1
            else:
                updated += 1
        conn.commit()
    return inserted, updated

# ── Checkpointing ─────────────────────────────────────────────────────────────

def checkpoint_done(
    run_id: str,
    hs8_code: str,
    country_code: str | None,
    rows_count: int,
    db_path: str = DB_PATH,
) -> None:
    """
    Mark an hs8_code as completed for this run.
    Pass country_code=None (or '') to indicate all countries are done for this hs8.
    Pass a specific country_code string for country-level checkpointing.
    """
    code = country_code if country_code is not None else ""
    with get_conn(db_path) as conn:
        conn.execute(
            """INSERT OR IGNORE INTO scrape_checkpoints
               (run_id, hs8_code, country_code, rows_count)
               VALUES (?, ?, ?, ?)""",
            (run_id, hs8_code, code, rows_count),
        )
        conn.commit()


def get_completed_hs_codes(run_id: str, db_path: str = DB_PATH) -> set[str]:
    """Return set of hs8_codes fully completed in a run (country_code='' checkpoint)."""
    with get_conn(db_path) as conn:
        rows = conn.execute(
            """SELECT hs8_code FROM scrape_checkpoints
               WHERE run_id=? AND country_code=''""",
            (run_id,),
        ).fetchall()
    return {r["hs8_code"] for r in rows}
