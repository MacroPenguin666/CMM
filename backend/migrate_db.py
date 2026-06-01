"""
Phase 2: Consolidate all source SQLite databases into data/cmm.db.

Copies every table from:
    feeds.db, unctad_trade.db, trade_stats.db, ccp_elites.db, regulations.db
into a single data/cmm.db, preserving all rows and recreating all indexes.

Usage:
    python -m backend.migrate_db            # dry-run (shows plan, no writes)
    python -m backend.migrate_db --run      # execute migration
    python -m backend.migrate_db --verify   # verify row counts match after migration
    python -m backend.migrate_db --rename-sources  # rename source DBs to .db.bak
"""

import argparse
import sqlite3
import sys
from pathlib import Path

from backend.storage import DB_DIR

SOURCE_DBS = [
    "feeds.db",
    "unctad_trade.db",
    "trade_stats.db",
    "ccp_elites.db",
    "regulations.db",
]

TARGET_DB = DB_DIR / "cmm.db"


def get_tables(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    return [r[0] for r in rows]


def get_row_count(conn: sqlite3.Connection, table: str) -> int:
    return conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]


def migrate(dry_run: bool = True) -> dict[str, int]:
    plan: list[tuple[str, list[tuple[str, int]]]] = []

    for db_file in SOURCE_DBS:
        path = DB_DIR / db_file
        if not path.exists():
            print(f"  SKIP (not found): {db_file}")
            continue
        conn = sqlite3.connect(path)
        tables = [(t, get_row_count(conn, t)) for t in get_tables(conn)]
        conn.close()
        plan.append((db_file, tables))

    total = sum(len(tables) for _, tables in plan)
    print(f"\n{'DRY RUN — ' if dry_run else ''}Migration plan: {total} tables → {TARGET_DB.name}\n")
    for db_file, tables in plan:
        for table, count in tables:
            print(f"  {db_file:25s}  {table:35s}  {count:>8,} rows")

    if dry_run:
        print("\nRe-run with --run to execute.")
        return {}

    print("\nStarting migration...")
    target = sqlite3.connect(TARGET_DB)
    target.execute("PRAGMA journal_mode=WAL")
    target.execute("PRAGMA synchronous=NORMAL")

    results: dict[str, int] = {}

    for db_file, tables in plan:
        src_path = DB_DIR / db_file
        # Sanitise alias: replace . and - with _
        alias = db_file.replace(".", "_").replace("-", "_")

        target.execute(f"ATTACH DATABASE '{src_path}' AS {alias}")

        for table, _ in tables:
            existing = target.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
            ).fetchone()
            if existing:
                print(f"  SKIP (already exists): {table}")
                continue

            # Get CREATE TABLE DDL from source
            ddl = target.execute(
                f"SELECT sql FROM {alias}.sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
            if not ddl or not ddl[0]:
                print(f"  SKIP (no DDL): {table}")
                continue

            target.execute(ddl[0])
            target.execute(f'INSERT INTO "{table}" SELECT * FROM {alias}."{table}"')
            count = get_row_count(target, table)
            results[table] = count
            print(f"  ✓ {table:45s} {count:>8,} rows")

            # Recreate indexes
            idx_rows = target.execute(
                f"SELECT sql FROM {alias}.sqlite_master "
                f"WHERE type='index' AND tbl_name=? AND sql IS NOT NULL",
                (table,),
            ).fetchall()
            for (idx_sql,) in idx_rows:
                try:
                    target.execute(idx_sql)
                except sqlite3.OperationalError:
                    pass

        target.commit()
        target.execute(f"DETACH DATABASE {alias}")
        print(f"  — {db_file} done\n")

    target.close()
    print(f"Migration complete. {len(results)} tables written to {TARGET_DB}")
    return results


def verify() -> bool:
    if not TARGET_DB.exists():
        print("cmm.db does not exist. Run --run first.")
        return False

    target = sqlite3.connect(TARGET_DB)
    all_ok = True

    for db_file in SOURCE_DBS:
        path = DB_DIR / db_file
        if not path.exists():
            continue
        src = sqlite3.connect(path)
        for table in get_tables(src):
            src_count = get_row_count(src, table)
            try:
                dst_count = get_row_count(target, table)
            except sqlite3.OperationalError:
                dst_count = -1
            ok = src_count == dst_count
            mark = "✓" if ok else "✗"
            print(f"  {mark} {db_file:25s} {table:35s}  src={src_count:>8,}  dst={dst_count:>8,}")
            if not ok:
                all_ok = False
        src.close()

    target.close()
    return all_ok


def rename_sources():
    for db_file in SOURCE_DBS:
        path = DB_DIR / db_file
        if path.exists():
            bak = path.with_suffix(".db.bak")
            path.rename(bak)
            print(f"  {db_file} → {bak.name}")


def main():
    parser = argparse.ArgumentParser(description="Migrate source DBs into cmm.db")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--run", action="store_true", help="Execute migration")
    group.add_argument("--verify", action="store_true", help="Verify row counts match")
    group.add_argument("--rename-sources", action="store_true", help="Rename source DBs to .db.bak")
    args = parser.parse_args()

    if args.verify:
        ok = verify()
        sys.exit(0 if ok else 1)
    elif args.rename_sources:
        rename_sources()
    else:
        migrate(dry_run=not args.run)


if __name__ == "__main__":
    main()
