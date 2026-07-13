"""
Built-in background refresh for cmm-serve.

On server start (`backend.run.main` calls `start()`), a single daemon thread
checks every CHECK_INTERVAL whether any fetch group is stale and runs the due
ones one at a time as subprocesses (most-stale first). The server keeps
serving from data/cmm.db the whole time — fetchers never block a request.

Staleness is tracked in the `auto_refresh_runs` table (data/cmm.db), except
for commodities, where data/commodities.json's mtime is the source of truth.
Subprocess output is appended to data/logs/auto_refresh.log.

launchd plists in backend/scheduler/ remain optional — only needed if you
want fetching while the server is down. Duplicate runs are harmless: all
fetchers dedupe on insert.
"""

import logging
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from backend.storage import LOG_DIR, get_conn

log = logging.getLogger("auto_refresh")

PROJECT_ROOT = Path(__file__).parent.parent

CHECK_INTERVAL = 15 * 60        # staleness re-check cadence (seconds)
SUBPROCESS_TIMEOUT = 2 * 3600   # kill a hung fetcher after 2 h


@dataclass(frozen=True)
class Group:
    name: str
    argv: list                      # command after `python -m`
    interval: float                 # seconds between successful runs
    mtime_path: Optional[Path] = None  # if set, file mtime tracks freshness


GROUPS = [
    Group("news", ["backend.cli", "news"], 4 * 3600),
    Group("policies", ["backend.cli", "policies"], 12 * 3600),
    Group("batch", ["backend.cli", "batch"], 24 * 3600),
    Group("commodities", ["backend.runners.fetch_commodities", "--no-trade"],
          7 * 86400, mtime_path=PROJECT_ROOT / "data" / "commodities.json"),
]


def ensure_table(conn):
    conn.execute(
        """CREATE TABLE IF NOT EXISTS auto_refresh_runs (
               id          INTEGER PRIMARY KEY AUTOINCREMENT,
               group_name  TEXT NOT NULL,
               started_at  REAL NOT NULL,
               finished_at REAL,
               ok          INTEGER
           )"""
    )
    conn.commit()


def record_run(conn, group_name, started_at, finished_at, ok):
    conn.execute(
        "INSERT INTO auto_refresh_runs (group_name, started_at, finished_at, ok) "
        "VALUES (?, ?, ?, ?)",
        (group_name, started_at, finished_at, 1 if ok else 0),
    )
    conn.commit()


def last_success(conn, group_name):
    """Unix time the group last finished successfully, or None."""
    row = conn.execute(
        "SELECT MAX(finished_at) FROM auto_refresh_runs WHERE group_name = ? AND ok = 1",
        (group_name,),
    ).fetchone()
    return row[0] if row and row[0] is not None else None


def _last_fresh(conn, group):
    """Unix time of the group's last known freshness marker, or None."""
    if group.mtime_path is not None:
        try:
            return group.mtime_path.stat().st_mtime
        except OSError:
            return None
    return last_success(conn, group.name)


def due_groups(conn, now=None, groups=GROUPS):
    """Groups whose data is older than their interval, most-stale first."""
    now = time.time() if now is None else now
    due = []
    for g in groups:
        last = _last_fresh(conn, g) or 0
        overdue = now - (last + g.interval)
        if overdue >= 0:
            due.append((overdue, g))
    due.sort(key=lambda t: t[0], reverse=True)
    return [g for _, g in due]


def get_status(conn, now=None, groups=GROUPS):
    """Per-group freshness snapshot for /api/refresh/status."""
    now = time.time() if now is None else now
    status = []
    for g in groups:
        last = _last_fresh(conn, g)
        status.append({
            "group": g.name,
            "interval_s": g.interval,
            "last_success": last,
            "next_due": (last + g.interval) if last else None,
            "due": last is None or now >= last + g.interval,
            "running": _state["current"] == g.name,
        })
    return status


# ---------------------------------------------------------------------------
# Scheduler thread
# ---------------------------------------------------------------------------

_state = {"current": None, "thread": None}


def _run_group(g):
    """Run one fetch group as a subprocess; record the outcome."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    started = time.time()
    ok = False
    try:
        with open(LOG_DIR / "auto_refresh.log", "a") as out:
            out.write(f"\n=== {g.name} started {time.strftime('%Y-%m-%d %H:%M:%S')} ===\n")
            out.flush()
            proc = subprocess.run(
                [sys.executable, "-m"] + g.argv,
                cwd=PROJECT_ROOT, stdout=out, stderr=subprocess.STDOUT,
                timeout=SUBPROCESS_TIMEOUT,
            )
        ok = proc.returncode == 0
    except subprocess.TimeoutExpired:
        log.error(f"auto-refresh {g.name}: timed out after {SUBPROCESS_TIMEOUT}s")
    except Exception as e:
        log.error(f"auto-refresh {g.name}: {e}")
    if g.mtime_path is None:
        conn = get_conn()
        ensure_table(conn)
        record_run(conn, g.name, started, time.time(), ok)
        conn.close()
    log.info(f"auto-refresh {g.name}: {'ok' if ok else 'FAILED'} "
             f"({time.time() - started:.0f}s)")


def _loop():
    while True:
        try:
            conn = get_conn()
            ensure_table(conn)
            due = due_groups(conn)
            conn.close()
            for g in due:
                _state["current"] = g.name
                _run_group(g)
                _state["current"] = None
        except Exception as e:
            log.error(f"auto-refresh loop error: {e}")
            _state["current"] = None
        time.sleep(CHECK_INTERVAL)


def start():
    """Start the background scheduler (idempotent)."""
    if _state["thread"] is not None and _state["thread"].is_alive():
        return
    t = threading.Thread(target=_loop, name="auto-refresh", daemon=True)
    _state["thread"] = t
    t.start()
    log.info("auto-refresh scheduler started "
             f"(groups: {', '.join(g.name for g in GROUPS)})")
