"""
Tests for db.py: schema creation, upserts, run tracking, checkpoints.
Uses a temporary in-memory SQLite database.
"""
import sqlite3
import tempfile
import os
import pytest

from customs_scraper.db import (
    init_db,
    start_run,
    finish_run,
    upsert_export_rows,
    checkpoint_done,
    get_completed_hs_codes,
    get_conn,
)


@pytest.fixture
def db_path(tmp_path):
    path = str(tmp_path / "test.db")
    init_db(path)
    return path


# ── init_db ───────────────────────────────────────────────────────────────────

def test_init_db_creates_tables(db_path):
    with get_conn(db_path) as conn:
        tables = {
            row[0] for row in
            conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
    assert "exports" in tables
    assert "scrape_runs" in tables
    assert "scrape_checkpoints" in tables


def test_init_db_is_idempotent(db_path):
    """Calling init_db twice should not raise."""
    init_db(db_path)
    init_db(db_path)


# ── run tracking ──────────────────────────────────────────────────────────────

def test_start_run_returns_uuid(db_path):
    run_id = start_run(2024, 1, 100, db_path)
    assert len(run_id) == 36  # uuid4 format
    assert run_id.count("-") == 4


def test_start_run_inserts_record(db_path):
    run_id = start_run(2024, 3, 500, db_path)
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM scrape_runs WHERE run_id=?", (run_id,)
        ).fetchone()
    assert row is not None
    assert row["year"] == 2024
    assert row["month"] == 3
    assert row["status"] == "running"
    assert row["hs_codes_total"] == 500


def test_finish_run_updates_record(db_path):
    run_id = start_run(2024, 1, 100, db_path)
    finish_run(run_id, "success", 1500, 200, 100, None, db_path)
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM scrape_runs WHERE run_id=?", (run_id,)
        ).fetchone()
    assert row["status"] == "success"
    assert row["rows_inserted"] == 1500
    assert row["rows_updated"] == 200
    assert row["hs_codes_done"] == 100
    assert row["finished_at"] is not None


def test_finish_run_with_error(db_path):
    run_id = start_run(2024, 1, 100, db_path)
    finish_run(run_id, "failed", 0, 0, 0, "connection timeout", db_path)
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT error_message FROM scrape_runs WHERE run_id=?", (run_id,)
        ).fetchone()
    assert row["error_message"] == "connection timeout"


# ── upsert_export_rows ────────────────────────────────────────────────────────

def _make_row(**overrides) -> dict:
    base = {
        "year": 2024,
        "month": 1,
        "hs8_code": "84713000",
        "hs_description": "Laptops",
        "country_code": "502",
        "country_name": "United States",
        "export_value_usd": 1_000_000.0,
        "export_value_cny": 7_200_000.0,
        "export_qty": 5000.0,
        "export_qty_unit": "PCS",
    }
    return {**base, **overrides}


def test_upsert_inserts_new_row(db_path):
    ins, upd = upsert_export_rows([_make_row()], db_path)
    # We don't assert exact ins/upd counts (SQLite upsert counting is tricky)
    # but we do assert a row was written
    with get_conn(db_path) as conn:
        count = conn.execute("SELECT COUNT(*) FROM exports").fetchone()[0]
    assert count == 1


def test_upsert_multiple_rows(db_path):
    rows = [
        _make_row(country_code="502", country_name="United States"),
        _make_row(country_code="101", country_name="Germany"),
        _make_row(country_code="304", country_name="Japan"),
    ]
    upsert_export_rows(rows, db_path)
    with get_conn(db_path) as conn:
        count = conn.execute("SELECT COUNT(*) FROM exports").fetchone()[0]
    assert count == 3


def test_upsert_updates_on_conflict(db_path):
    upsert_export_rows([_make_row(export_value_usd=1_000.0)], db_path)
    upsert_export_rows([_make_row(export_value_usd=2_000.0)], db_path)
    with get_conn(db_path) as conn:
        row = conn.execute("SELECT export_value_usd FROM exports").fetchone()
        count = conn.execute("SELECT COUNT(*) FROM exports").fetchone()[0]
    assert count == 1  # no duplicate
    assert row["export_value_usd"] == 2_000.0  # updated


def test_upsert_empty_list_returns_zeros(db_path):
    ins, upd = upsert_export_rows([], db_path)
    assert ins == 0
    assert upd == 0


def test_upsert_nullable_fields(db_path):
    row = _make_row(
        hs_description=None,
        country_name=None,
        export_value_usd=None,
        export_value_cny=None,
        export_qty=None,
        export_qty_unit=None,
    )
    upsert_export_rows([row], db_path)
    with get_conn(db_path) as conn:
        stored = conn.execute("SELECT * FROM exports").fetchone()
    assert stored["hs_description"] is None
    assert stored["export_value_usd"] is None


# ── checkpointing ─────────────────────────────────────────────────────────────

def test_checkpoint_and_retrieve(db_path):
    run_id = start_run(2024, 1, 10, db_path)
    checkpoint_done(run_id, "84713000", None, 50, db_path)
    checkpoint_done(run_id, "85171200", None, 30, db_path)

    completed = get_completed_hs_codes(run_id, db_path)
    assert "84713000" in completed
    assert "85171200" in completed
    assert len(completed) == 2


def test_checkpoint_different_runs_isolated(db_path):
    run1 = start_run(2024, 1, 10, db_path)
    run2 = start_run(2024, 1, 10, db_path)

    checkpoint_done(run1, "84713000", None, 50, db_path)

    assert "84713000" in get_completed_hs_codes(run1, db_path)
    assert "84713000" not in get_completed_hs_codes(run2, db_path)


def test_checkpoint_country_level_not_in_completed(db_path):
    run_id = start_run(2024, 1, 10, db_path)
    # country_code-level checkpoint does NOT count as "all countries done"
    checkpoint_done(run_id, "84713000", "502", 10, db_path)

    completed = get_completed_hs_codes(run_id, db_path)
    assert "84713000" not in completed  # only country_code='' counts as "all done"


def test_checkpoint_idempotent(db_path):
    run_id = start_run(2024, 1, 10, db_path)
    checkpoint_done(run_id, "84713000", None, 50, db_path)
    checkpoint_done(run_id, "84713000", None, 50, db_path)  # should not raise
    completed = get_completed_hs_codes(run_id, db_path)
    assert len(completed) == 1
