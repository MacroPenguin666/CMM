"""
Orchestrator for the raw ingestion pipeline.

    python _run.py <source>            # run one source (e.g. bis, imf, nbs)
    python _run.py --all               # run every batch-cadence source once
    python _run.py --list              # list registered sources
    python _run.py --realtime          # daemon: poll realtime sources forever
    python _run.py --realtime --once   # one poll of each realtime source, then exit

Each fetcher module exposes ``run(run_id=...)`` and writes parquet under
``02_inputs/`` via ``_raw_store``. This orchestrator only sequences them and shares
one ``run_id`` across a batch so all parts from a run are grouped.
"""

from __future__ import annotations

import argparse
import importlib
import logging
import sys
import time
from pathlib import Path

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_HERE.parents[1]))

import _raw_store as store  # noqa: E402
import _registry as registry  # noqa: E402

log = logging.getLogger("raw_run")


def _run_source(source: str, run_id: str) -> bool:
    meta = registry.SOURCES.get(source)
    if not meta:
        log.error("unknown source: %s (see --list)", source)
        return False
    log.info("=== %s (%s) ===", source, meta["note"])
    try:
        mod = importlib.import_module(meta["module"])
        mod.run(run_id=run_id)
        return True
    except Exception as e:
        log.error("%s failed: %s", source, str(e)[:200])
        return False


def run_all(sources: list[str]) -> None:
    run_id = store.new_run_id()
    ok = 0
    for s in sources:
        ok += _run_source(s, run_id)
    log.info("batch done: %d/%d sources ok (run_id=%s)", ok, len(sources), run_id)


def run_realtime(once: bool = False) -> None:
    sources = registry.realtime_sources()
    next_due = {s: 0.0 for s in sources}
    log.info("realtime daemon: %s", ", ".join(sources))
    while True:
        now = time.monotonic()
        for s in sources:
            if now >= next_due[s]:
                _run_source(s, store.new_run_id())
                next_due[s] = now + registry.REALTIME_INTERVALS.get(s, 60)
        if once:
            return
        time.sleep(5)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s",
                        datefmt="%H:%M:%S")
    ap = argparse.ArgumentParser(prog="raw-run", description="Raw ingestion pipeline runner")
    ap.add_argument("source", nargs="?", help="single source id to run")
    ap.add_argument("--all", action="store_true", help="run all batch sources")
    ap.add_argument("--realtime", action="store_true", help="poll realtime sources")
    ap.add_argument("--once", action="store_true", help="with --realtime: one poll then exit")
    ap.add_argument("--list", action="store_true", help="list registered sources")
    args = ap.parse_args()

    if args.list:
        for s, m in registry.SOURCES.items():
            print(f"  {s:12} {m['cadence']:9} {m['note']}")
        return
    if args.realtime:
        run_realtime(once=args.once)
        return
    if args.all:
        run_all(registry.batch_sources())
        return
    if args.source:
        run_all([args.source])
        return
    ap.print_help()


if __name__ == "__main__":
    main()
