"""
Raw academic fetcher — CrossRef, China-relevant scholarship in full.

For each tracked journal, pulls the **entire** works history via CrossRef cursor
pagination (not just the latest 50). China-studies journals (``china_only``) are
kept whole; general-economics journals are filtered to China-relevant articles via
the shared keyword test. Stored raw to ``02_inputs/academic/works``, append-only.

Counterpart to ``backend/fetchers/academic.py`` (latest 50/journal into cmm.db).
Reuses that module's ``JOURNALS``, ``CROSSREF_API`` and ``_is_china_related``.
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
import time
from pathlib import Path

import pandas as pd
import requests

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_HERE.parents[1]))

import _raw_store as store  # noqa: E402
from backend.fetchers.academic import JOURNALS, CROSSREF_API, _is_china_related  # noqa: E402

log = logging.getLogger("raw_academic")
SOURCE = "academic"
UA = {"User-Agent": "ChinaPolicyMonitor/1.0 (mailto:contact@example.com)"}


def _parse_item(item: dict, journal_name: str) -> dict | None:
    title = item.get("title", [""])
    title = re.sub(r"<[^>]+>", "", (title[0] if title else "")).strip()
    if not title:
        return None
    authors = "; ".join(
        f"{a.get('family', '')}, {a.get('given', '')}".strip(", ")
        for a in item.get("author", [])
    )
    published = ""
    for k in ("published-print", "published-online", "created"):
        dp = item.get(k, {}).get("date-parts", [[]])
        if dp and dp[0]:
            published = "-".join(str(p).zfill(2) for p in dp[0])
            break
    abstract = re.sub(r"<[^>]+>", "", item.get("abstract", "") or "").strip()
    doi = item.get("DOI", "")
    return {
        "journal": journal_name, "title": title, "authors": authors,
        "doi": doi, "link": item.get("URL", "") or (f"https://doi.org/{doi}" if doi else ""),
        "published": published, "abstract": abstract[:2000],
        "type": item.get("type", ""), "container": (item.get("container-title") or [""])[0],
    }


def _fetch_journal_full(journal: dict, page: int = 200, timeout: int = 40) -> list[dict]:
    issn, name, china_only = journal["issn"], journal["name"], journal.get("china_only", False)
    url = CROSSREF_API.format(issn=issn)
    cursor = "*"
    out: list[dict] = []
    while True:
        try:
            r = requests.get(url, params={
                "rows": page, "cursor": cursor,
                "select": "title,author,DOI,published-print,published-online,created,abstract,URL,type,container-title",
            }, headers=UA, timeout=timeout)
            r.raise_for_status()
            msg = r.json().get("message", {})
        except Exception as e:
            log.warning("  %s page FAIL: %s", name, str(e)[:90])
            break
        items = msg.get("items", [])
        if not items:
            break
        for it in items:
            rec = _parse_item(it, name)
            if not rec:
                continue
            if not china_only and not _is_china_related(rec["title"], rec["abstract"]):
                continue
            out.append(rec)
        cursor = msg.get("next-cursor")
        if not cursor or len(items) < page:
            break
        time.sleep(0.2)
    log.info("  %-28s %5d kept", name, len(out))
    return out


def run(run_id: str | None = None) -> None:
    run_id = run_id or store.new_run_id()
    datasets: dict[str, int] = {}
    try:
        rows: list[dict] = []
        for j in JOURNALS:
            rows.extend(_fetch_journal_full(j))
        if rows:
            store.append(SOURCE, "works", pd.DataFrame(rows), run_id=run_id,
                         endpoint="https://api.crossref.org/journals/{issn}/works")
            datasets["works"] = len(rows)
        store.write_manifest(SOURCE, status="ok", datasets=datasets, run_id=run_id)
    except Exception as e:
        store.write_manifest(SOURCE, status="error", datasets=datasets,
                             run_id=run_id, error=str(e))
        raise


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s",
                        datefmt="%H:%M:%S")
    argparse.ArgumentParser(description="Raw academic fetcher").parse_args()
    run()


if __name__ == "__main__":
    main()
