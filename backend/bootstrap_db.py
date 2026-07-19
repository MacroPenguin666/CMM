"""
Build a fresh data/cmm.db from scratch by applying schema.sql then running all fetchers.

Foreground alternative to the automatic first-run bootstrap that cmm-serve
performs in the background — same public sources, no API keys required
(key-gated sources like WTO/paid-Comtrade soft-skip when keys are unset).

Usage:
    python -m backend.bootstrap_db              # full build (all fetchers)
    python -m backend.bootstrap_db --schema-only  # apply DDL only, no fetch
    python -m backend.bootstrap_db --skip news commodities  # skip specific runners
"""

import argparse
import sqlite3
import sys
import time
from pathlib import Path

from backend.storage import DB_PATH, get_conn

SCHEMA_PATH = Path(__file__).parent / "schema.sql"

# batch covers all non-realtime DB sources (macro, comtrade, trade-stats,
# ccp-elites, eurostat-trade, fyp-tech, chartbook, …) — see fetch_batch.ALL_SOURCES.
# commodities writes data/commodities.json rather than cmm.db.
RUNNERS = [
    ("news",        "backend.runners.fetch_news",        "run"),
    ("policies",    "backend.runners.fetch_policies",    "run"),
    ("batch",       "backend.runners.fetch_batch",       "run"),
    ("commodities", "backend.runners.fetch_commodities", "run"),
]


def apply_schema():
    ddl = SCHEMA_PATH.read_text(encoding="utf-8")
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(ddl)
    conn.close()
    print(f"Schema applied → {DB_PATH}")


def run_fetcher(name: str, module: str, fn: str) -> bool:
    import importlib
    t0 = time.time()
    print(f"\n[{name}] starting…")
    try:
        mod = importlib.import_module(module)
        getattr(mod, fn)()
        print(f"[{name}] done ({time.time() - t0:.1f}s)")
        return True
    except Exception as exc:
        print(f"[{name}] FAILED: {exc}", file=sys.stderr)
        return False


def main():
    parser = argparse.ArgumentParser(description="Bootstrap data/cmm.db from scratch")
    parser.add_argument("--schema-only", action="store_true",
                        help="Apply DDL only, do not run any fetchers")
    parser.add_argument("--skip", nargs="+", default=[],
                        metavar="RUNNER",
                        help="Runner names to skip (e.g. --skip news realtime)")
    args = parser.parse_args()

    if DB_PATH.exists():
        print(f"WARNING: {DB_PATH} already exists — schema will be applied with IF NOT EXISTS "
              "(existing data is preserved).")

    apply_schema()

    if args.schema_only:
        print("Schema-only mode — done.")
        return

    skip = set(args.skip)
    results = {}
    for name, module, fn in RUNNERS:
        if name in skip:
            print(f"[{name}] skipped")
            continue
        results[name] = run_fetcher(name, module, fn)

    print("\n── Bootstrap summary ──")
    for name, ok in results.items():
        mark = "✓" if ok else "✗"
        print(f"  {mark} {name}")

    failed = [n for n, ok in results.items() if not ok]
    if failed:
        print(f"\n{len(failed)} runner(s) failed. Re-run with: python -m backend.bootstrap_db "
              f"--skip {' '.join(set(results) - set(failed))}")
        sys.exit(1)


if __name__ == "__main__":
    main()