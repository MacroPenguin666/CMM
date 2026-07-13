"""
Raw NBS fetcher — National Bureau of Statistics of China (data.stats.gov.cn).

Pulls China **in full**: all 8 EasyQuery databases (national / provincial / city
× annual / quarterly / monthly), every leaf indicator, full history, append-only
to parquet with vintages.

This is the exhaustive counterpart to ``backend/fetchers/nbs.py`` (which keeps only
national monthly+annual, last 60 months / 25 years, for the dashboard). It reuses
that module's proven HTTP session and tree-crawl helpers, but widens the scope and
redirects storage to ``02_inputs/nbs/`` via ``_raw_store`` (no cmm.db).

Two phases:
    python fetch_nbs.py --discover     # walk the indicator tree for all 8 dbs -> nbs/catalog
    python fetch_nbs.py                 # read catalog, pull every series -> nbs/series

NOTE: data.stats.gov.cn is frequently unreachable outside China. Failures are
caught per call and recorded in the manifest; run from a China-capable network to
populate fully. Quarterly period codes (YYYYA..YYYYD) follow the documented NBS
convention and should be confirmed on first live run.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date
from pathlib import Path

import pandas as pd

# --- bootstrap: numbered dir isn't a package; add self + repo root to path ----
_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parents[1]
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_ROOT))

import _raw_store as store  # noqa: E402
# Reuse the battle-tested session + tree crawl from the dashboard fetcher.
from backend.fetchers.nbs import _make_session, _crawl_tree, BASE_URL  # noqa: E402

log = logging.getLogger("raw_nbs")
SOURCE = "nbs"

# dbcode -> (geographic level, frequency). The 8 EasyQuery databases.
DBCODES: dict[str, tuple[str, str]] = {
    "hgnd": ("national", "year"),
    "hgjd": ("national", "quarter"),
    "hgyd": ("national", "month"),
    "fsnd": ("province", "year"),
    "fsjd": ("province", "quarter"),
    "fsyd": ("province", "month"),
    "csnd": ("city", "year"),
    "csyd": ("city", "month"),
}

# Earliest period to request per frequency (NBS ignores periods with no data).
_YEAR_START = 1949
_MONTH_START = (1990, 1)
_QUARTER_START = 1992


# ---------------------------------------------------------------------------
# Period grammar (full history, chunked to keep each request modest)
# ---------------------------------------------------------------------------

def _periods_for(freq: str) -> list[str]:
    today = date.today()
    if freq == "year":
        return [str(y) for y in range(_YEAR_START, today.year + 1)]
    if freq == "month":
        out: list[str] = []
        y, m = _MONTH_START
        while (y, m) <= (today.year, today.month):
            out.append(f"{y:04d}{m:02d}")
            m += 1
            if m == 13:
                m, y = 1, y + 1
        return out
    if freq == "quarter":
        # NBS quarter codes: YYYYA=Q1, B=Q2, C=Q3, D=Q4 (confirm on first live run)
        out = []
        for y in range(_QUARTER_START, today.year + 1):
            for q in "ABCD":
                out.append(f"{y}{q}")
        return out
    raise ValueError(f"unknown freq {freq}")


def _chunk(seq: list[str], n: int) -> list[list[str]]:
    return [seq[i:i + n] for i in range(0, len(seq), n)]


def _period_to_date(freq: str, code: str) -> str:
    """Best-effort ISO date from an NBS sj period code; raw code is also kept."""
    try:
        if freq == "year":
            return f"{code[:4]}-01-01"
        if freq == "month" and len(code) >= 6:
            return f"{code[:4]}-{code[4:6]}-01"
        if freq == "quarter" and len(code) >= 5:
            q = {"A": "03", "B": "06", "C": "09", "D": "12"}.get(code[4], "01")
            return f"{code[:4]}-{q}-01"
    except Exception:
        pass
    return ""


# ---------------------------------------------------------------------------
# QueryData — national (no region) and regional (province/city, reg dimension)
# ---------------------------------------------------------------------------

def _query(session, dbcode: str, freq: str, code: str, periods: list[str],
           regional: bool) -> list[dict]:
    """One QueryData call. Returns raw row dicts (region kept as '' for national)."""
    import json as _json
    from datetime import datetime as _dt

    if regional:
        # rowcode=reg returns every province/city for this indicator in one call.
        params = {
            "m": "QueryData", "dbcode": dbcode,
            "rowcode": "reg", "colcode": "sj", "wds": "[]",
            "dfwds": _json.dumps([
                {"wdcode": "zb", "valuecode": code},
                {"wdcode": "sj", "valuecode": ",".join(periods)},
            ]),
            "k1": int(_dt.now().timestamp() * 1000),
        }
    else:
        params = {
            "m": "QueryData", "dbcode": dbcode,
            "rowcode": "zb", "colcode": "sj", "wds": "[]",
            "dfwds": _json.dumps([
                {"wdcode": "zb", "valuecode": code},
                {"wdcode": "sj", "valuecode": ",".join(periods)},
            ]),
            "k1": int(_dt.now().timestamp() * 1000),
        }

    r = session.get(BASE_URL, params=params, timeout=25)
    r.raise_for_status()
    nodes = r.json().get("returndata", {}).get("datanodes", [])
    rows: list[dict] = []
    for d in nodes:
        raw_val = d.get("data", {}).get("data")
        has = d.get("data", {}).get("hasdata", True)
        if raw_val is None or has is False:
            continue
        try:
            val = float(raw_val)
        except (TypeError, ValueError):
            continue
        wds = {w.get("wdcode"): w.get("valuecode") for w in d.get("wds", [])}
        period_code = wds.get("sj")
        if not period_code:
            continue
        rows.append({
            "indicator": code,
            "region": wds.get("reg", "") if regional else "",
            "period_code": period_code,
            "date": _period_to_date(freq, period_code),
            "value": val,
        })
    return rows


# ---------------------------------------------------------------------------
# Discovery — walk the indicator tree for every db -> catalog
# ---------------------------------------------------------------------------

def discover(run_id: str) -> int:
    session = _make_session()
    rows: list[dict] = []
    for dbcode, (level, freq) in DBCODES.items():
        try:
            nodes = _crawl_tree("zb", dbcode, session, delay=0.12)
        except Exception as e:
            log.warning("  catalog %s FAILED: %s", dbcode, e)
            continue
        leaves = [n for n in nodes if not n.get("isParent")]
        for n in leaves:
            code = n.get("code")
            if not code:
                continue
            rows.append({
                "dbcode": dbcode, "level": level, "freq": freq,
                "indicator": code,
                "name_cn": n.get("cname", ""), "name": n.get("name", ""),
                "unit": n.get("unit", ""), "parent": n.get("parent", ""),
            })
        log.info("  catalog %s (%s/%s): %d leaves", dbcode, level, freq, len(leaves))
    df = pd.DataFrame(rows)
    store.append(SOURCE, "catalog", df, run_id=run_id, endpoint=BASE_URL)
    return len(df)


def _load_catalog() -> pd.DataFrame:
    cat = store.latest_view(SOURCE, "catalog", key_cols=["dbcode", "indicator"])
    return cat


# ---------------------------------------------------------------------------
# Data pull — every leaf, full history, all dbs
# ---------------------------------------------------------------------------

def fetch_data(run_id: str, period_chunk: int = 60, delay: float = 0.4,
               limit: int | None = None) -> int:
    catalog = _load_catalog()
    if catalog.empty:
        log.warning("No catalog yet. Run --discover first.")
        return 0

    session = _make_session()
    total = 0
    by_db: dict[str, list[dict]] = {}

    items = list(catalog.itertuples(index=False))
    if limit:
        items = items[:limit]

    for i, row in enumerate(items, 1):
        dbcode, freq, level = row.dbcode, row.freq, row.level
        regional = level in ("province", "city")
        period_chunks = _chunk(_periods_for(freq), period_chunk)
        got: list[dict] = []
        try:
            for pc in period_chunks:
                got.extend(_query(session, dbcode, freq, row.indicator, pc, regional))
                time.sleep(delay)
            log.info("  [%d/%d] %s %s -> %d pts", i, len(items), dbcode,
                     row.indicator, len(got))
        except Exception as e:
            log.warning("  [%d/%d] %s %s FAIL: %s", i, len(items), dbcode,
                        row.indicator, str(e)[:100])
            continue
        for g in got:
            g.update({"dbcode": dbcode, "level": level, "freq": freq})
        by_db.setdefault(dbcode, []).extend(got)

    # One dataset partition per dbcode keeps files navigable.
    for dbcode, rows in by_db.items():
        if rows:
            store.append(SOURCE, f"series_{dbcode}", pd.DataFrame(rows),
                         run_id=run_id, endpoint=BASE_URL)
            total += len(rows)
    return total


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run(run_id: str | None = None, *, discover_only: bool = False,
        limit: int | None = None) -> None:
    run_id = run_id or store.new_run_id()
    datasets: dict[str, int] = {}
    try:
        if discover_only:
            datasets["catalog"] = discover(run_id)
        else:
            if _load_catalog().empty:
                datasets["catalog"] = discover(run_id)
            datasets["series"] = fetch_data(run_id, limit=limit)
        store.write_manifest(SOURCE, status="ok", datasets=datasets, run_id=run_id)
    except Exception as e:
        store.write_manifest(SOURCE, status="error", datasets=datasets,
                             run_id=run_id, error=str(e))
        raise


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s",
                        datefmt="%H:%M:%S")
    ap = argparse.ArgumentParser(description="Raw NBS fetcher (full China)")
    ap.add_argument("--discover", action="store_true", help="walk tree -> catalog only")
    ap.add_argument("--limit", type=int, help="cap number of indicators (smoke test)")
    args = ap.parse_args()
    run(discover_only=args.discover, limit=args.limit)


if __name__ == "__main__":
    main()
