"""Tests for backend.auto_refresh staleness logic (no real subprocesses)."""

import sqlite3
import time

import pytest

from backend.auto_refresh import (
    GROUPS,
    Group,
    due_groups,
    ensure_table,
    get_status,
    last_success,
    record_run,
)

NOW = 1_800_000_000.0  # fixed "now" for deterministic tests


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    ensure_table(c)
    yield c
    c.close()


def _mem_groups():
    """Table-tracked groups only (no mtime files involved)."""
    return [g for g in GROUPS if g.mtime_path is None]


def test_all_table_groups_due_when_never_run(conn):
    due = due_groups(conn, now=NOW, groups=_mem_groups())
    assert [g.name for g in due] == ["news", "policies", "batch"]


def test_recent_success_not_due(conn):
    news = next(g for g in GROUPS if g.name == "news")
    record_run(conn, "news", started_at=NOW - 3700, finished_at=NOW - 3600, ok=True)
    assert news not in due_groups(conn, now=NOW, groups=[news])


def test_stale_success_due(conn):
    news = next(g for g in GROUPS if g.name == "news")
    record_run(conn, "news", started_at=NOW - 5 * 3600, finished_at=NOW - 5 * 3600 + 60, ok=True)
    assert news in due_groups(conn, now=NOW, groups=[news])


def test_failed_run_does_not_reset_staleness(conn):
    news = next(g for g in GROUPS if g.name == "news")
    record_run(conn, "news", started_at=NOW - 60, finished_at=NOW - 30, ok=False)
    assert news in due_groups(conn, now=NOW, groups=[news])
    assert last_success(conn, "news") is None


def test_most_stale_first(conn):
    # batch (24 h) last ran 10 days ago, news (4 h) 5 hours ago:
    # batch is far more overdue and must come first.
    record_run(conn, "news", NOW - 5 * 3600, NOW - 5 * 3600 + 1, ok=True)
    record_run(conn, "policies", NOW - 60, NOW - 30, ok=True)  # fresh, not due
    record_run(conn, "batch", NOW - 10 * 86400, NOW - 10 * 86400 + 1, ok=True)
    due = due_groups(conn, now=NOW, groups=_mem_groups())
    assert [g.name for g in due] == ["batch", "news"]


def test_commodities_due_by_mtime(conn, tmp_path):
    fresh = tmp_path / "fresh.json"
    fresh.write_text("{}")
    stale = tmp_path / "stale.json"
    stale.write_text("{}")
    import os
    os.utime(fresh, (NOW - 3600, NOW - 3600))
    os.utime(stale, (NOW - 8 * 86400, NOW - 8 * 86400))

    g_fresh = Group("commodities", ["x"], 7 * 86400, mtime_path=fresh)
    g_stale = Group("commodities", ["x"], 7 * 86400, mtime_path=stale)
    g_missing = Group("commodities", ["x"], 7 * 86400, mtime_path=tmp_path / "nope.json")

    assert due_groups(conn, now=NOW, groups=[g_fresh]) == []
    assert due_groups(conn, now=NOW, groups=[g_stale]) == [g_stale]
    assert due_groups(conn, now=NOW, groups=[g_missing]) == [g_missing]


def test_status_reports_per_group(conn):
    record_run(conn, "news", NOW - 3600, NOW - 3590, ok=True)
    status = get_status(conn, now=NOW, groups=_mem_groups())
    by_name = {s["group"]: s for s in status}
    assert set(by_name) == {"news", "policies", "batch"}
    assert by_name["news"]["due"] is False
    assert by_name["news"]["last_success"] is not None
    assert by_name["batch"]["due"] is True
    assert by_name["batch"]["last_success"] is None
    # next_due = last success + interval (unix seconds)
    assert by_name["news"]["next_due"] == pytest.approx(NOW - 3590 + 4 * 3600)


def test_batch_covers_all_db_sources():
    # A fresh clone bootstraps solely via the auto-refresh groups; the batch
    # group must therefore reach every non-realtime DB source.
    from backend.runners.fetch_batch import ALL_SOURCES, FETCHERS

    assert set(FETCHERS) == set(ALL_SOURCES)
    for source in ("fyp_tech", "chartbook", "eurostat_trade", "ccp_elites"):
        assert source in ALL_SOURCES
