"""Commodity markets data — prices, production, refining, and trade for all
materials in the materials registry (backend/fetchers/materials.py).

Keyless sources (stdlib only):
  - World mine/refinery/smelter production by country .... USGS Mineral Commodity
    Summaries data releases: MCS 2026 (years 2024 + 2025e, ScienceBase CSV) and
    MCS 2025 world CSV (adds 2023). Copper keeps a static 2020-22 tail below.
  - Daily exchange prices (6 exchange-traded metals) ...... Yahoo Finance chart API
  - Monthly benchmark prices (IMF PCPS series) ............ FRED CSV endpoint
  - Annual trade (USD + kg) by reporter, per HS code ...... UN Comtrade public
    preview API (slow: one call per code/flow/year, retried, resumable — already
    fetched code-years in data/commodities.json are kept and skipped)

Output is a single JSON blob at data/commodities.json (schema 2):
  {generated_utc, schema, categories, materials: {slug: {..., production.stages,
   prices_daily?, prices_monthly?, trade_codes}}, trade: {commodities: {code: ...}}}

Refresh from a runner or the CLI. `refresh(trade=False)` does a fast pass
(production + prices only); the full trade backfill takes hours on first run.
"""

import csv
import io
import json
import re
import sys
import time
import urllib.request
from datetime import datetime, timezone, date

from backend.storage import DATA_DIR
from backend.fetchers.materials import (MATERIALS, CATEGORIES,
                                        TRADE_START, TRADE_START_DEFAULT)

DATA_PATH = DATA_DIR / "commodities.json"

UA = {"User-Agent": "Mozilla/5.0 (CMM commodities fetcher)"}
PRICE_START = "2019-01-01"

# USGS Mineral Commodity Summaries data releases (ScienceBase file URLs).
MCS2026_URL = ("https://www.sciencebase.gov/catalog/file/get/69837e43b66b01367d7ec7c7"
               "?f=__disk__d3%2Fac%2F84%2Fd3ac8466552946c5e8caa2c2c6338d9e1aff655d")
MCS2025_URL = ("https://www.sciencebase.gov/catalog/file/get/6798fd34d34ea8c18376e8ee"
               "?f=__disk__92%2Ff6%2F90%2F92f690853b1b1dc6a8000c1da24a7bbfd9f670d0")
USGS_SOURCE = "USGS Mineral Commodity Summaries 2025-2026"
ESTIMATED_YEAR = 2025

# ------------------------------------------------------------- copper static
# Pre-2023 copper history from earlier USGS MCS editions (2022-2024), kept so
# the copper charts retain their 2020-22 tail (the CSV data releases start 2023).
# Thousand metric tons of contained copper.
COPPER_STATIC = {
    "years": [2020, 2021, 2022],
    "mine": {
        "Chile": [5730, 5620, 5330], "DR Congo": [1600, 1740, 2350],
        "Peru": [2150, 2300, 2450], "China": [1720, 1910, 1940],
        "United States": [1200, 1230, 1230], "Indonesia": [505, 731, 941],
        "Russia": [810, 940, 936], "Australia": [885, 813, 819],
        "Zambia": [853, 842, 797], "Kazakhstan": [552, 510, 593],
        "Mexico": [733, 734, 754], "Canada": [585, 550, 520],
        "Poland": [393, 391, 393],
    },
    "mine_world": [20600, 21200, 21900],
    "refinery": {"China": [10000, 10500, 11100]},
    "refinery_world": [25300, 25300, 25900],
}

COUNTRY_RENAMES = {
    # UN Comtrade reporter names
    "USA": "United States",
    "Dem. Rep. of the Congo": "DR Congo",
    "Congo, Dem. Rep. of the": "DR Congo",
    "Russian Federation": "Russia",
    "Rep. of Korea": "South Korea",
    "Other Asia, nes": "Taiwan (Other Asia, nes)",
    "China, Hong Kong SAR": "Hong Kong SAR",
    "United Rep. of Tanzania": "Tanzania",
    "Lao People's Dem. Rep.": "Laos",
    "Viet Nam": "Vietnam",
    "Türkiye": "Turkiye",
    # USGS country names
    "Congo (Kinshasa)": "DR Congo",
    "Korea, Republic of": "South Korea",
    "Korea, North": "North Korea",
    "Burma": "Myanmar",
    "Cote d'Ivoire": "Ivory Coast",
}


def _get(url, timeout=60, attempts=3):
    req = urllib.request.Request(url, headers=UA)
    for i in range(attempts):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except Exception:
            if i == attempts - 1:
                raise
            time.sleep(5 * (i + 1))


# ------------------------------------------------------------------ USGS MCS

def _norm(s):
    """Normalise commodity/detail strings for matching across CSV editions."""
    s = (s or "").replace("—", "-").replace("–", "-").replace("�", "-")
    return re.sub(r"\s+", " ", s).strip().lower()


def _clean_value(v):
    """USGS value cell → float or None (handles W, NA, dashes, commas, <, e)."""
    v = (v or "").strip().replace(",", "")
    if not v or v in ("W", "NA", "XX") or _norm(v) in ("-", "--"):
        return None
    v = v.lstrip("<>~").rstrip("e")
    try:
        return float(v)
    except ValueError:
        return None


def _rename(c):
    c = re.sub(r"\s+", " ", (c or "")).strip()
    low = c.lower()
    if low.startswith("world total"):     # e.g. "World total (rounded)"
        return "World total"
    if low.startswith("other countries") or low == "other":
        return "Other countries"
    return COUNTRY_RENAMES.get(c, c)


def fetch_mcs2026():
    """MCS 2026 long CSV → {norm_commodity: [(detail, country, unit, year, value, rounded)]}."""
    text = _get(MCS2026_URL, timeout=180).decode("cp1252", errors="replace")
    out = {}
    for r in csv.DictReader(io.StringIO(text)):
        if not r.get("Section", "").startswith("World") or r.get("Statistics") != "Production":
            continue
        detail = r.get("Statistics_detail") or ""
        rounded = ": rounded" in detail.lower() or ", rounded" in detail.lower()
        detail = re.sub(r"[:,] rounded", "", detail, flags=re.I).strip()
        try:
            year = int(r["Year"])
        except (KeyError, ValueError):
            continue
        out.setdefault(_norm(r["Commodity"]), []).append(
            (detail, _rename(r["Country"]), r.get("Unit") or "", year,
             _clean_value(r.get("Value")), rounded))
    return out


def fetch_mcs2025():
    """MCS 2025 wide world CSV → {norm_commodity: [(type, country, unit, year, value)]}."""
    text = _get(MCS2025_URL, timeout=180).decode("cp1252", errors="replace")
    text = text.lstrip("﻿")
    out = {}
    for r in csv.DictReader(io.StringIO(text)):
        typ = r.get("TYPE") or ""
        rows = out.setdefault(_norm(r.get("COMMODITY")), [])
        country = _rename(r.get("COUNTRY"))
        unit = r.get("UNIT_MEAS") or ""
        for col, year in (("PROD_2023", 2023), ("PROD_EST_ 2024", 2024)):
            val = _clean_value(r.get(col))
            if val is not None:
                rows.append((typ, country, unit, year, val))
    return out


def _stage_from_mcs2026(rows, detail_prefix):
    """Pick matching rows; prefer un-rounded values. → {(country, year): (val, unit)}"""
    want = _norm(detail_prefix)
    got = {}  # (country, year) -> (val, unit, rounded)
    for detail, country, unit, year, val, rounded in rows:
        if not _norm(detail).startswith(want):
            continue
        key = (country, year)
        cur = got.get(key)
        better = (cur is None
                  or (cur[0] is None and val is not None)
                  or (cur[2] and not rounded and val is not None))
        if better:
            got[key] = (val, unit, rounded)
    return {k: (v, u) for k, (v, u, _r) in got.items()}


def _stage_from_mcs2025(rows, type_prefix):
    want = _norm(type_prefix)
    got = {}
    for typ, country, unit, year, val in rows:
        if not _norm(typ).startswith(want):
            continue
        got.setdefault((country, year), (val, unit))
    return got


def build_production(spec, m26, m25):
    """Assemble one material's production stages from both MCS editions."""
    stages = {}
    for key, rule in (spec.get("usgs") or {}).items():
        cells = {}   # (country, year) -> value
        unit = ""
        if rule.get("mcs2025"):
            com, prefix = rule["mcs2025"]
            for (country, year), (val, u) in _stage_from_mcs2025(m25.get(_norm(com), []), prefix).items():
                cells[(country, year)] = val
                unit = unit or u
        if rule.get("mcs2026"):
            com, prefix = rule["mcs2026"]
            for (country, year), (val, u) in _stage_from_mcs2026(m26.get(_norm(com), []), prefix).items():
                if val is not None or (country, year) not in cells:
                    cells[(country, year)] = val   # newer edition wins
                unit = u or unit
        if not cells:
            continue
        years = sorted({y for _, y in cells})
        countries, world = {}, None
        names = sorted({c for c, _ in cells})
        for c in names:
            series = [cells.get((c, y)) for y in years]
            if all(v is None for v in series):
                continue
            if c == "World total":
                world = series
            else:
                countries[c] = series
        stages[key] = {"label": rule["label"], "unit": unit, "years": years,
                       "countries": countries, "world": world}
    return stages


def _merge_copper_static(stages):
    """Prepend the 2020-22 static copper tail to the dynamically fetched stages."""
    st = COPPER_STATIC
    for key, cdata, world in (("mine", st["mine"], st["mine_world"]),
                              ("refinery", st["refinery"], st["refinery_world"])):
        stage = stages.get(key)
        if not stage:
            continue
        old_years = [y for y in st["years"] if y not in stage["years"]]
        if not old_years:
            continue
        idx = [st["years"].index(y) for y in old_years]
        years = old_years + stage["years"]
        for c in set(cdata) | set(stage["countries"]):
            head = [cdata[c][i] if c in cdata else None for i in idx]
            tail = stage["countries"].get(c, [None] * len(stage["years"]))
            if any(v is not None for v in head + tail):
                stage["countries"][c] = head + tail
        if stage["world"]:
            stage["world"] = [world[i] for i in idx] + stage["world"]
        stage["years"] = years
    return stages


# -------------------------------------------------------------------- prices

def fetch_daily_prices(ticker, label, unit):
    p1 = int(datetime.strptime(PRICE_START, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
    p2 = int(time.time())
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
           f"?period1={p1}&period2={p2}&interval=1d")
    d = json.loads(_get(url))["chart"]["result"][0]
    ts = d["timestamp"]
    close = d["indicators"]["quote"][0]["close"]
    series = [[datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%d"), round(c, 4)]
              for t, c in zip(ts, close) if c is not None]
    return {"label": label, "unit": unit, "source": f"Yahoo Finance ({ticker})",
            "series": series}


def fetch_monthly_prices(series_id, label, unit):
    rows = _get(f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}").decode().splitlines()
    series = []
    for line in rows[1:]:
        d, v = line.split(",")
        if d >= PRICE_START and v not in ("", "."):
            series.append([d, round(float(v), 2)])
    return {"label": label, "unit": unit, "source": f"IMF via FRED ({series_id})",
            "series": series}


# --------------------------------------------------------------------- trade

def fetch_comtrade_year(cmd, flow, year):
    url = ("https://comtradeapi.un.org/public/v1/preview/C/A/HS"
           f"?cmdCode={cmd}&flowCode={flow}&period={year}&partnerCode=0&includeDesc=true")
    for attempt in range(3):
        try:
            d = json.loads(_get(url, timeout=90))
            rows = d.get("data") or []
            best = {}  # one row per reporter (dedupe classification variants)
            for r in rows:
                name = r.get("reporterDesc") or r.get("reporterISO") or "?"
                name = COUNTRY_RENAMES.get(name, name)
                v = r.get("primaryValue") or 0
                if v and (name not in best or v > best[name]["v"]):
                    best[name] = {"c": name, "v": round(v), "w": round(r.get("netWgt") or 0)}
            out = sorted(best.values(), key=lambda x: -x["v"])
            time.sleep(1.5)
            return out
        except Exception as e:
            print(f"  comtrade {cmd}/{flow}/{year} attempt {attempt+1} failed: {e}", file=sys.stderr)
            time.sleep(10 * (attempt + 1))
    return None


def _mark_partial(by_year):
    """Years whose world total (either flow) is well below the prior year's are
    incompletely reported — mark them partial. A value drop alone can be a price
    move (e.g. cobalt 2023), so when net weights are stored the volume must have
    dropped too."""
    partial = []
    ys = sorted(by_year)
    for i, y in enumerate(ys):
        if not i:
            continue
        for key in ("exports", "imports"):
            cur, prev = by_year[y].get(key), by_year[ys[i - 1]].get(key)
            if not (cur and prev) or cur["total"] >= 0.8 * prev["total"]:
                continue
            cw, pw = cur.get("totalW"), prev.get("totalW")
            if cw and pw and cw >= 0.8 * pw:
                continue          # volume held up — price move, not missing filings
            partial.append(int(y))
            break
    return partial


def trade_code_order():
    """All HS codes across the registry: every material's primary code first
    (so each material becomes useful early in a long backfill), then the rest."""
    primary, secondary = [], []
    for spec in MATERIALS.values():
        for i, (code, _) in enumerate(spec["hs"]):
            bucket = primary if i == 0 else secondary
            if code not in primary and code not in secondary:
                bucket.append(code)
    return [c for c in primary + secondary]


def code_labels():
    labels = {}
    for spec in MATERIALS.values():
        for code, label in spec["hs"]:
            labels.setdefault(code, f"{label} (HS {code})")
    return labels


def fetch_trade(existing_trade, on_progress=None, codes=None):
    """Fetch annual world trade per HS code, resuming from existing data.

    Already-stored (code, year, flow) cells are kept; only the two most recent
    years are refetched (reporters keep filing late). `on_progress(trade)` is
    called after each completed code so partial results can be persisted.
    """
    last_year = date.today().year - 1
    labels = code_labels()
    trade = {"source": "UN Comtrade", "unit": "USD (netWgt in kg)",
             "commodities": dict(existing_trade.get("commodities", {}))}
    for cmd in codes or trade_code_order():
        start = TRADE_START.get(cmd, TRADE_START_DEFAULT)
        entry = trade["commodities"].get(cmd) or {"label": labels[cmd], "by_year": {}}
        entry["label"] = labels[cmd]
        by_year = entry["by_year"]
        missing = []
        for year in range(start, last_year + 1):
            have = by_year.get(str(year), {})
            refetch = year >= last_year - 1
            for flow, key in (("X", "exports"), ("M", "imports")):
                if key not in have or refetch:
                    missing.append((year, flow, key))
        if not missing:
            continue
        print(f"trade HS {cmd}: fetching {len(missing)} year-flows...")
        for year, flow, key in missing:
            rows = fetch_comtrade_year(cmd, flow, year)
            if rows is None:
                print(f"  MISSING comtrade {cmd} {key} {year}", file=sys.stderr)
                continue
            if rows:
                by_year.setdefault(str(year), {})[key] = {
                    "total": sum(r["v"] for r in rows),
                    "totalW": sum(r["w"] for r in rows), "top": rows[:12]}
        entry["partial_years"] = _mark_partial(by_year)
        trade["commodities"][cmd] = entry
        if on_progress:
            on_progress(trade)
    return trade


# --------------------------------------------------------------------- build

def build_materials_block(m26, m25):
    materials = {}
    for slug, spec in MATERIALS.items():
        entry = {"name": spec["name"], "symbol": spec["symbol"],
                 "category": spec["category"], "sourcing": spec["sourcing"],
                 "sourcing_note": spec.get("sourcing_note", ""),
                 "uses": spec["uses"],
                 "trade_codes": [c for c, _ in spec["hs"]],
                 "trade_labels": {c: lbl for c, lbl in spec["hs"]}}
        if spec.get("prod_note"):
            entry["prod_note"] = spec["prod_note"]
        stages = build_production(spec, m26, m25)
        if slug == "copper":
            stages = _merge_copper_static(stages)
        if stages:
            entry["production"] = {"source": USGS_SOURCE,
                                   "estimated_year": ESTIMATED_YEAR, "stages": stages}
        if spec.get("yahoo"):
            ticker, unit, label = spec["yahoo"]
            try:
                entry["prices_daily"] = fetch_daily_prices(ticker, label, unit)
                print(f"  {slug}: {len(entry['prices_daily']['series'])} daily pts ({ticker})")
            except Exception as e:
                print(f"  {slug}: daily prices failed ({ticker}): {e}", file=sys.stderr)
        if spec.get("fred"):
            series_id, label, unit = spec["fred"]
            try:
                entry["prices_monthly"] = fetch_monthly_prices(series_id, label, unit)
                print(f"  {slug}: {len(entry['prices_monthly']['series'])} monthly pts ({series_id})")
            except Exception as e:
                print(f"  {slug}: monthly prices failed ({series_id}): {e}", file=sys.stderr)
        materials[slug] = entry
    return materials


def _write(path, data):
    data["generated_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    path.write_text(json.dumps(data, separators=(",", ":")))


def refresh(path=DATA_PATH, trade=True, base=True, codes=None):
    """Fetch and write data/commodities.json (schema 2). Returns the data dict.

    trade=False skips the (hours-long on first run) Comtrade backfill; existing
    trade data in the file is preserved either way. base=False skips rebuilding
    production + prices and only tops up trade (needs an existing schema-2 file).
    """
    existing = get_commodities_data(path)
    old_trade = migrate_legacy_trade(existing)
    if base or existing.get("schema") != 2:
        print("fetching USGS MCS production CSVs...")
        m26, m25 = fetch_mcs2026(), fetch_mcs2025()
        print(f"  {len(m26)} + {len(m25)} commodities parsed")
        print("building materials (production + prices)...")
        data = {"schema": 2, "categories": CATEGORIES,
                "materials": build_materials_block(m26, m25), "trade": old_trade}
        _write(path, data)
        print(f"wrote {path} ({path.stat().st_size // 1024} KB) — production + prices")
    else:
        data = existing
        data["trade"] = old_trade
    if trade:
        print("fetching trade (UN Comtrade, resumable)...")
        def save(t):
            data["trade"] = t
            _write(path, data)
        data["trade"] = fetch_trade(old_trade, on_progress=save, codes=codes)
        _write(path, data)
        print(f"wrote {path} ({path.stat().st_size // 1024} KB) — with trade")
    return data


def migrate_legacy_trade(existing):
    """Carry trade data forward from either schema (v1 blob or v2)."""
    trade = existing.get("trade") or {}
    return {"source": "UN Comtrade", "unit": "USD (netWgt in kg)",
            "commodities": dict(trade.get("commodities", {}))}


def get_commodities_data(path=DATA_PATH):
    """Read the stored commodities JSON blob, or {} if not fetched yet."""
    if not path.exists():
        return {}
    return json.loads(path.read_text())


if __name__ == "__main__":
    refresh(trade="--no-trade" not in sys.argv,
            base="--trade-only" not in sys.argv)
