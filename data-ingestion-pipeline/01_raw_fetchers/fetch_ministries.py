"""
Raw ministries fetcher — full .gov.cn announcement archives.

Scrapes every configured Chinese ministry/agency target (NDRC, MOFCOM, PBOC, MIIT,
…) with **deep pagination** and stores all article records raw to
``02_inputs/ministries/articles``, append-only.

Counterpart to ``backend/fetchers/ministry_scraper.py`` (shallow refresh into
cmm.db). Reuses that module's ``TARGETS`` and ``scrape_all(paginate=True)``.

NOTE: several .gov.cn hosts are unreachable outside China; failed targets are
logged and the run continues. Run from a China-capable network for full coverage.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_HERE.parents[1]))

import _raw_store as store  # noqa: E402
from backend.fetchers.ministry_scraper import TARGETS, scrape_all  # noqa: E402

log = logging.getLogger("raw_ministries")
SOURCE = "ministries"


def run(run_id: str | None = None, *, max_pages: int = 100) -> None:
    run_id = run_id or store.new_run_id()
    datasets: dict[str, int] = {}
    try:
        results = scrape_all(TARGETS, paginate=True, max_pages=max_pages, page_delay=1.0)
        rows: list[dict] = []
        for res in results:
            name = res.get("name") or res.get("source") or ""
            arts = res.get("articles") or res.get("items") or []
            for a in arts:
                rec = dict(a)
                rec.setdefault("source", name)
                rec["category"] = res.get("category", "")
                rec["doc_type"] = res.get("doc_type", "")
                rec["base_url"] = res.get("url", "")
                rows.append(rec)
            log.info("  %-34s %4d articles", name[:34], len(arts))
        if rows:
            store.append(SOURCE, "articles", pd.DataFrame(rows), run_id=run_id)
            datasets["articles"] = len(rows)
        store.write_manifest(SOURCE, status="ok", datasets=datasets, run_id=run_id)
    except Exception as e:
        store.write_manifest(SOURCE, status="error", datasets=datasets,
                             run_id=run_id, error=str(e))
        raise


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s",
                        datefmt="%H:%M:%S")
    ap = argparse.ArgumentParser(description="Raw ministries fetcher")
    ap.add_argument("--max-pages", type=int, default=100)
    args = ap.parse_args()
    run(max_pages=args.max_pages)


if __name__ == "__main__":
    main()
