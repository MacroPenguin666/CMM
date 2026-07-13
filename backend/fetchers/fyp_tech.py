"""
15th Five-Year Plan — Tech Self-Reliance (科技自立自强) tracker.

The 15th FYP outline (adopted NPC 2026-03-12) makes tech self-reliance its #2
strategic task and #2 of seven 2030 objectives. Its only OFFICIAL numerical
S&T targets (主要指标 table, rows 4-6) are:
  - society-wide R&D spending growth  > 7 %/yr avg (constant prices); 2025: 9.1 %
  - high-value invention patents      > 22 per 10k population;        2025: 16
  - core digital economy              > 12.5 % of GDP;                2024: 10.5 %
There is NO official chip self-sufficiency ratio, R&D-intensity or compute
target — the chip-trade indicators below are CMM proxy metrics.

Three tables in data/cmm.db:
  fyp_chip_trade        China (reporter) annual trade in critical HS4 lines,
                        by partner — UN Comtrade+ public preview, no key.
  fyp_chip_eu_monthly   EU27 <-> China monthly trade in the same HS4 lines —
                        Eurostat COMEXT DS-045409, flows 1 (EU imports) and
                        2 (EU exports). Captures the ASML/equipment lifeline.
  fyp_indicators        Official indicator datapoints (seeded with cited NBS/
                        Customs/plan values) + World Bank R&D-intensity series
                        (auto-refreshed).

Usage:
    python -m backend.fetchers.fyp_tech            # incremental
    python -m backend.fetchers.fyp_tech --full     # backfill from 2015 / 2014-01
"""

import argparse
import csv
import io
import logging
import sqlite3
import time
import zipfile
from datetime import datetime, timezone

import requests

log = logging.getLogger("fyp_tech")

_HEADERS = {"User-Agent": "CMM/1.0 (research dashboard)", "Accept": "application/json"}

# ---------------------------------------------------------------------------
# The five tech domains named in the plan's tech-self-reliance push, each with
# its trackable HS lines (UN Comtrade, HS4 or HS6) and an OpenAlex search term
# for the research-output side. `data_depth` drives the UI status badge.
# ---------------------------------------------------------------------------
TECH_DOMAINS = {
    "semi": {
        "name_en": "Semiconductors", "name_cn": "集成电路",
        "codes": ["8542", "8541", "8486"],
        "search": "semiconductor",
        "data_depth": "rich",
        "goal": "Plan box 3-01: refine mature process nodes, raise advanced-node "
                "manufacturing capability; wide/ultra-wide-bandgap semiconductors. "
                "'Decisive breakthroughs' via extraordinary measures (超常规措施).",
    },
    "ai_robotics": {
        "name_en": "AI & Robotics", "name_cn": "人工智能与机器人",
        "codes": ["847950"],
        "search": "artificial intelligence",
        "data_depth": "partial",
        "goal": "'AI+' initiative across six domains; high-performance AI chips "
                "and software stack; embodied AI among major engineering projects. "
                "No numerical adoption targets.",
    },
    "quantum": {
        "name_en": "Quantum Computing", "name_cn": "量子科技",
        "codes": [],
        "search": "quantum computing",
        "data_depth": "proxy",
        "goal": "Frontier-tech box: fault-tolerant universal quantum computer; "
                "integrated space-ground quantum communications. Qualitative only — "
                "no HS trade line exists for quantum hardware.",
    },
    "sixg": {
        "name_en": "6G & Networks", "name_cn": "6G/未来网络",
        "codes": ["851761", "851762"],
        "search": "6G network",
        "data_depth": "partial",
        "goal": "Next-generation networks listed among future industries; national "
                "integrated compute network (no EFLOPS target). Trade proxy: "
                "base stations (851761) + network equipment (851762).",
    },
    "materials": {
        "name_en": "Advanced Materials", "name_cn": "新材料",
        "codes": ["2846", "8505"],
        "search": "advanced materials",
        "data_depth": "partial",
        "goal": "Advanced materials among the six 'decisive breakthrough' fields. "
                "Reverse-leverage view: China's rare-earth compounds (2846) and "
                "magnet (8505) exports are the chokepoint it holds over others.",
    },
}

# code → label for everything we fetch from Comtrade
TECH_HS = {
    "8542":   "Integrated circuits",
    "8541":   "Semiconductor devices (diodes, transistors, PV)",
    "8486":   "Semiconductor-manufacturing machines",
    "847950": "Industrial robots",
    "851761": "Cellular base stations",
    "851762": "Network/transmission equipment",
    "2846":   "Rare-earth compounds",
    "8505":   "Magnets (incl. NdFeB permanent)",
}

# semiconductor lines only — the EU-monthly lifeline stays chip-focused
CHIP_HS4 = {c: TECH_HS[c] for c in ("8542", "8541", "8486")}

# ---------------------------------------------------------------------------
# Official 15th FYP targets (plan text constants, not fetched data)
# Source: 中华人民共和国国民经济和社会发展第十五个五年规划纲要, NDRC 2026-03
# https://www.ndrc.gov.cn/fggz/fzzlgh/gjfzgh/202603/U020260317369114704096.pdf
# ---------------------------------------------------------------------------
_PLAN_URL = "https://www.ndrc.gov.cn/fggz/fzzlgh/gjfzgh/202603/U020260317369114704096.pdf"

PLAN_TARGETS = [
    {
        "id": "rd_growth",
        "name_en": "Society-wide R&D spending growth",
        "name_cn": "全社会研发经费投入增长",
        "baseline_period": "2025", "baseline": 9.1,
        "target": 7.0, "target_kind": "min_avg",
        "unit": "% per year, constant prices",
        "binding": "indicative (预期性)",
        "indicator": "rd_spending_growth_real",
        "source_url": _PLAN_URL,
    },
    {
        "id": "patents",
        "name_en": "High-value invention patents per 10k population",
        "name_cn": "每万人口高价值发明专利拥有量",
        "baseline_period": "2025", "baseline": 16.0,
        "target": 22.0, "target_kind": "min_level",
        "unit": "patents / 10k population",
        "binding": "indicative (预期性)",
        "indicator": "patents_per_10k",
        "source_url": _PLAN_URL,
    },
    {
        "id": "digital",
        "name_en": "Core digital economy share of GDP",
        "name_cn": "数字经济核心产业增加值占GDP比重",
        "baseline_period": "2024", "baseline": 10.5,
        "target": 12.5, "target_kind": "min_level",
        "unit": "% of GDP",
        "binding": "indicative (预期性)",
        "indicator": "digital_economy_share",
        "source_url": _PLAN_URL,
    },
]

# Qualitative plan goals (no numbers in plan text) — shown as context in the UI.
PLAN_QUALITATIVE = [
    "Decisive breakthroughs (决定性突破) in integrated circuits, machine tools, "
    "high-end instruments, basic software, advanced materials, biomanufacturing",
    "Chips: refine mature process nodes, raise advanced-node manufacturing "
    "capability; wide/ultra-wide-bandgap semiconductors (专栏3-01)",
    "Basic research share of R&D 'notably higher' — no numerical target",
    "'AI+' initiative across six domains; high-performance AI chips + software stack",
    "National integrated compute network — no EFLOPS target",
    "NOT in the plan: chip self-sufficiency %, R&D/GDP intensity target, "
    "five-year GDP growth number",
]

# ---------------------------------------------------------------------------
# Seeded official datapoints (all values verified against the cited source)
# ---------------------------------------------------------------------------
_NBS_2025 = "https://www.stats.gov.cn/sj/zxfbhjd/202602/t20260228_1962662.html"   # 2025 statistical communiqué
_NBS_RD_2024 = "https://www.stats.gov.cn/sj/zxfbhjd/202509/t20250929_1961429.html"  # 2024 S&T funding communiqué

SEED_INDICATORS = [
    # (indicator, period, value, unit, source, source_url)
    ("rd_spending_bn_cny",      "2024", 3632.68, "bn CNY", "NBS 2024 S&T funding communiqué", _NBS_RD_2024),
    ("rd_spending_bn_cny",      "2025", 3926.2,  "bn CNY", "NBS 2025 statistical communiqué", _NBS_2025),
    ("rd_spending_growth_nom",  "2024", 8.9,     "% yoy nominal", "NBS 2024 S&T funding communiqué", _NBS_RD_2024),
    ("rd_spending_growth_nom",  "2025", 8.1,     "% yoy nominal", "NBS 2025 statistical communiqué", _NBS_2025),
    ("rd_spending_growth_real", "2025", 9.1,     "% (plan-table 2025 baseline)", "15th FYP outline 专栏1", _PLAN_URL),
    ("rd_intensity",            "2024", 2.69,    "% of GDP", "NBS 2024 S&T funding communiqué", _NBS_RD_2024),
    ("rd_intensity",            "2025", 2.80,    "% of GDP", "NBS 2025 statistical communiqué", _NBS_2025),
    ("basic_research_share",    "2024", 6.88,    "% of R&D", "NBS 2024 S&T funding communiqué", _NBS_RD_2024),
    ("basic_research_share",    "2025", 7.08,    "% of R&D", "NBS 2025 statistical communiqué", _NBS_2025),
    ("patents_per_10k",         "2025", 16.0,    "per 10k pop", "15th FYP outline 专栏1 baseline", _PLAN_URL),
    ("digital_economy_share",   "2024", 10.5,    "% of GDP", "15th FYP outline 专栏1 baseline", _PLAN_URL),
    ("ic_output_bn_units",      "2024", 451.4,   "bn units", "NBS via 2024 communiqué", _NBS_2025),
    ("ic_output_bn_units",      "2025", 484.28,  "bn units", "NBS 2025 statistical communiqué", _NBS_2025),
    ("ic_imports_bn_usd",       "2025", 424.33,  "bn USD", "China Customs (GACC) 2025", "http://www.customs.gov.cn"),
    ("ic_exports_bn_usd",       "2025", 201.9,   "bn USD", "China Customs (GACC) 2025", "http://www.customs.gov.cn"),
]

# IFR World Robotics 2025 (China press release, 2025-09-25) — robot adoption.
# Installations chart on p.3 gives the full annual series in 1,000 units.
_IFR_PR = ("https://ifr.org/downloads/press_docs/"
           "2025-09-25-IFR_press_release_China_in_English.pdf")
_IFR_DENSITY = ("https://ifr.org/ifr-press-releases/news/"
                "robot-density-surges-in-europe-asia-and-americas")
# Official China AI-adoption series (no full industry-level survey exists).
_CNNIC_GENAI = ("https://english.www.gov.cn/archive/statistics/202602/05/"
                "content_WS698442cac6d00ca5f9a08edc.html")
_CAICT_MFG = ("https://sinolytics.de/global-business-news/blog/technology/"
              "china-ai-manufacturing-adoption-2025/")

_IFR_INSTALLS = {  # year -> '000 units installed in China
    "2014": 57, "2015": 69, "2016": 97, "2017": 156, "2018": 155, "2019": 145,
    "2020": 176, "2021": 275, "2022": 290, "2023": 276, "2024": 295,
}
_IFR_INDUSTRY_2024 = {  # customer industry -> '000 units installed 2024
    "Electrical/electronics": 83.0, "Automotive": 57.2, "Metal & machinery": 54.6,
    "Food & beverage": 8.9, "Textiles & apparel": 5.7, "Wood products": 4.3,
}

SEED_INDICATORS += (
    [("robot_installations_k", y, float(v), "'000 units/yr",
      "IFR World Robotics 2025", _IFR_PR) for y, v in _IFR_INSTALLS.items()]
    + [("robot_industry:" + k, "2024", v, "'000 units",
        "IFR World Robotics 2025", _IFR_PR) for k, v in _IFR_INDUSTRY_2024.items()]
    + [
        ("robot_stock_mn",       "2024", 2.027, "mn units in operation", "IFR World Robotics 2025", _IFR_PR),
        ("robot_domestic_share", "2023", 47.0,  "% of installations by CN suppliers", "IFR World Robotics 2025", _IFR_PR),
        ("robot_domestic_share", "2024", 57.0,  "% of installations by CN suppliers", "IFR World Robotics 2025", _IFR_PR),
        ("robot_density",        "2024", 166.0, "per 10k employees (all industries, 2025 IFR methodology)", "IFR World Robotics 2025", _IFR_DENSITY),
        # CNNIC: 42.8% gen-AI adoption Dec 2025, +25.2pp yoy -> 17.6% Dec 2024
        ("genai_adoption_pct",   "2024", 17.6,  "% of population using gen AI", "CNNIC via gov.cn (derived: 42.8 − 25.2pp yoy)", _CNNIC_GENAI),
        ("genai_adoption_pct",   "2025", 42.8,  "% of population using gen AI", "CNNIC via gov.cn", _CNNIC_GENAI),
        ("ai_mfg_share_pct",     "2024", 19.9,  "% share of AI applications in manufacturing", "CAICT (via Sinolytics)", _CAICT_MFG),
        ("ai_mfg_share_pct",     "2025", 25.9,  "% share of AI applications in manufacturing", "CAICT (via Sinolytics)", _CAICT_MFG),
    ])

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
_SCHEMA = """
CREATE TABLE IF NOT EXISTS fyp_chip_trade (
    year         INTEGER NOT NULL,
    hs4          TEXT    NOT NULL,
    flow         TEXT    NOT NULL,            -- 'M' imports / 'X' exports
    partner_code INTEGER NOT NULL,            -- UN M49; 0 = World
    partner_iso  TEXT,
    partner_name TEXT,
    value_usd    REAL,
    fetched_at   TEXT    NOT NULL,
    PRIMARY KEY (year, hs4, flow, partner_code)
);
CREATE INDEX IF NOT EXISTS idx_fct_hs4  ON fyp_chip_trade(hs4);
CREATE INDEX IF NOT EXISTS idx_fct_year ON fyp_chip_trade(year);

CREATE TABLE IF NOT EXISTS fyp_chip_eu_monthly (
    period     TEXT NOT NULL,                 -- 'YYYY-MM'
    hs4        TEXT NOT NULL,
    flow       TEXT NOT NULL,                 -- '1' EU imports from CN / '2' EU exports to CN
    value_eur  REAL,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (period, hs4, flow)
);

CREATE TABLE IF NOT EXISTS fyp_indicators (
    indicator  TEXT NOT NULL,
    period     TEXT NOT NULL,
    value      REAL,
    unit       TEXT,
    source     TEXT,
    source_url TEXT,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (indicator, period)
);

CREATE TABLE IF NOT EXISTS fyp_publications (
    domain      TEXT NOT NULL,             -- TECH_DOMAINS key
    year        INTEGER NOT NULL,
    cn_count    INTEGER,
    world_count INTEGER,
    fetched_at  TEXT NOT NULL,
    PRIMARY KEY (domain, year)
);

CREATE TABLE IF NOT EXISTS fyp_ai_benchmarks (
    benchmark    TEXT NOT NULL,            -- 'eci' | 'gpqa_diamond'
    model        TEXT NOT NULL,            -- Epoch model-version id
    name         TEXT,                     -- display name
    org          TEXT,
    country      TEXT,                     -- 'CN' | 'US' | 'other'
    release_date TEXT,                     -- 'YYYY-MM-DD'
    score        REAL,
    fetched_at   TEXT NOT NULL,
    PRIMARY KEY (benchmark, model)
);
"""


def get_db() -> sqlite3.Connection:
    from backend.storage import get_conn
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA)
    conn.commit()
    return conn


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# 1. China chip trade by partner — UN Comtrade+ public preview (no key)
# ---------------------------------------------------------------------------
_COMTRADE_BASE = "https://comtradeapi.un.org/public/v1/preview/C/A/HS"
_REF_PARTNERS  = "https://comtradeapi.un.org/files/v1/app/reference/partnerAreas.json"
_CHN_CODE      = 156
_FIRST_YEAR    = 2015
_DELAY_SEC     = 1.0
_MAX_RETRIES   = 4


def _load_partner_map() -> dict[int, tuple[str, str]]:
    try:
        r = requests.get(_REF_PARTNERS, headers=_HEADERS, timeout=30)
        r.raise_for_status()
        out = {}
        for it in r.json().get("results", []):
            code, iso3 = it.get("PartnerCode"), it.get("PartnerCodeIsoAlpha3", "")
            if code is not None:
                out[int(code)] = (iso3 or "", it.get("PartnerDesc", ""))
        return out
    except Exception as exc:
        log.warning(f"Partner map load failed: {exc}")
        return {}


def _fetch_comtrade(year: int, hs4: str, flow: str) -> list[dict] | None:
    """One preview call; returns rows or None on hard failure."""
    params = {
        "reporterCode": _CHN_CODE,
        "period":       str(year),
        "cmdCode":      hs4,
        "flowCode":     flow,
        # pin the aggregate slice — older vintages (2015-17) otherwise return
        # partner2/mode-of-transport breakdowns and truncate at maxRecords
        "partner2Code": "0",
        "motCode":      "0",
        "customsCode":  "C00",
        "maxRecords":   500,
    }
    for attempt in range(_MAX_RETRIES):
        try:
            r = requests.get(_COMTRADE_BASE, params=params, headers=_HEADERS, timeout=60)
            if r.status_code == 429:
                wait = 60 * (2 ** attempt)
                log.warning(f"    429 rate-limit → sleeping {wait}s")
                time.sleep(wait)
                continue
            if r.status_code == 404:
                return []
            r.raise_for_status()
            return r.json().get("data") or []
        except Exception as exc:
            if attempt == _MAX_RETRIES - 1:
                log.warning(f"    {hs4}/{flow}/{year} failed: {exc}")
                return None
            time.sleep(10 * (attempt + 1))
    return None


def fetch_chip_trade(conn: sqlite3.Connection, full: bool = False) -> int:
    """
    Fetch China's tech-line trade by partner for every code in TECH_HS.
    Incremental mode is computed PER CODE: re-fetch that code's two most recent
    stored years (revisions) plus any newer year; a code with no stored data
    backfills from _FIRST_YEAR (so newly added codes self-heal).
    """
    latest_possible = datetime.now(timezone.utc).year - 1
    all_years = list(range(_FIRST_YEAR, latest_possible + 1))
    stored: dict[str, set[int]] = {}
    for code, year in conn.execute("SELECT DISTINCT hs4, year FROM fyp_chip_trade"):
        stored.setdefault(code, set()).add(year)

    partners = _load_partner_map()
    now = _now()
    inserted = 0
    for code in TECH_HS:
        have = stored.get(code, set())
        if full or not have:
            years = all_years
        else:
            years = sorted({y for y in have if y >= max(have) - 1}
                           | set(range(max(have) + 1, latest_possible + 1)))
        for year in years:
            for flow in ("M", "X"):
                raw = _fetch_comtrade(year, code, flow)
                time.sleep(_DELAY_SEC)
                if raw is None:
                    continue
                rows = []
                for row in raw:
                    val = row.get("primaryValue")
                    if val is None:
                        continue
                    pcode = int(row.get("partnerCode") or 0)
                    iso, name = partners.get(pcode, ("", row.get("partnerDesc") or ""))
                    rows.append((int(row.get("refYear", year)), code, flow,
                                 pcode, iso, name, float(val), now))
                conn.executemany(
                    "INSERT OR REPLACE INTO fyp_chip_trade "
                    "(year, hs4, flow, partner_code, partner_iso, partner_name, value_usd, fetched_at) "
                    "VALUES (?,?,?,?,?,?,?,?)", rows)
                conn.commit()
                inserted += len(rows)
        log.info(f"  trade {code}: {len(years)} year(s) ({inserted:,} rows cumulative)")
    return inserted


# ---------------------------------------------------------------------------
# 2. EU27 <-> China monthly chip trade — Eurostat COMEXT DS-045409
# ---------------------------------------------------------------------------
_COMEXT_BASE = ("https://ec.europa.eu/eurostat/api/comext/dissemination/"
                "statistics/1.0/data/DS-045409")
_EU_FIRST = "2014-01"


def _month_range(start: str, end: str) -> list[str]:
    sy, sm = int(start[:4]), int(start[5:7])
    ey, em = int(end[:4]), int(end[5:7])
    out = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return out


def _fetch_comext_chunk(months: list[str]) -> dict[tuple[str, str, str], float]:
    """One COMEXT call covering several months, all chip HS4 lines, both flows.
    Returns {(period, hs4, flow): value_eur}."""
    params = [("freq", "M"), ("reporter", "EU27_2020"), ("partner", "CN"),
              ("indicators", "VALUE_IN_EUROS")]
    params += [("product", p) for p in CHIP_HS4]
    params += [("flow", "1"), ("flow", "2")]
    params += [("time", m) for m in months]
    r = requests.get(_COMEXT_BASE, params=params, headers=_HEADERS, timeout=180)
    r.raise_for_status()
    js = r.json()
    ids, size = js.get("id", []), js.get("size", [])
    dims = js.get("dimension", {})

    def labels(dim):
        idx = dims.get(dim, {}).get("category", {}).get("index", {})
        return sorted(idx, key=idx.get)

    prod_l, flow_l, time_l = labels("product"), labels("flow"), labels("time")
    pos = {d: ids.index(d) for d in ("product", "flow", "time")}
    out = {}
    for flat, val in (js.get("value") or {}).items():
        flat = int(flat)
        coords = []
        for n in reversed(size):
            coords.append(flat % n)
            flat //= n
        coords.reverse()
        out[(time_l[coords[pos["time"]]],
             prod_l[coords[pos["product"]]],
             flow_l[coords[pos["flow"]]])] = float(val)
    return out


def fetch_eu_monthly(conn: sqlite3.Connection, full: bool = False) -> int:
    """Fetch EU27<->CN monthly values for the chip HS4 lines. Incremental mode
    covers a trailing 12-month window (COMEXT revises ~3 months back)."""
    now_dt = datetime.now(timezone.utc)
    end = f"{now_dt.year:04d}-{now_dt.month:02d}"
    stored = conn.execute("SELECT COUNT(*) FROM fyp_chip_eu_monthly").fetchone()[0]
    if full or not stored:
        months = _month_range(_EU_FIRST, end)
    else:
        start_dt_y, start_dt_m = now_dt.year - 1, now_dt.month
        months = _month_range(f"{start_dt_y:04d}-{start_dt_m:02d}", end)

    now = _now()
    inserted = 0
    for i in range(0, len(months), 24):
        chunk = months[i:i + 24]
        try:
            data = _fetch_comext_chunk(chunk)
        except Exception as exc:
            log.warning(f"  COMEXT chunk {chunk[0]}..{chunk[-1]} failed: {exc}")
            continue
        rows = [(p, h, f, v, now) for (p, h, f), v in data.items()]
        conn.executemany(
            "INSERT OR REPLACE INTO fyp_chip_eu_monthly "
            "(period, hs4, flow, value_eur, fetched_at) VALUES (?,?,?,?,?)", rows)
        conn.commit()
        inserted += len(rows)
        time.sleep(1.0)
    log.info(f"  EU monthly: {inserted} rows stored")
    return inserted


# ---------------------------------------------------------------------------
# 3. Indicators — seeds + World Bank R&D-intensity history
# ---------------------------------------------------------------------------
_WB_RD = ("https://api.worldbank.org/v2/country/CHN/indicator/"
          "GB.XPD.RSDV.GD.ZS?format=json&per_page=100")


def seed_indicators(conn: sqlite3.Connection) -> int:
    now = _now()
    cur = conn.executemany(
        "INSERT OR IGNORE INTO fyp_indicators "
        "(indicator, period, value, unit, source, source_url, fetched_at) "
        "VALUES (?,?,?,?,?,?,?)",
        [(*row, now) for row in SEED_INDICATORS])
    conn.commit()
    return cur.rowcount


def fetch_wb_rd_intensity(conn: sqlite3.Connection) -> int:
    """Historical R&D/GDP context series (lags ~2y; NBS seeds cover recent years)."""
    try:
        r = requests.get(_WB_RD, headers=_HEADERS, timeout=60)
        r.raise_for_status()
        data = r.json()[1] or []
    except Exception as exc:
        log.warning(f"  World Bank R&D fetch failed: {exc}")
        return 0
    now = _now()
    rows = [("rd_intensity_wb", d["date"], float(d["value"]), "% of GDP",
             "World Bank WDI GB.XPD.RSDV.GD.ZS", "https://data.worldbank.org", now)
            for d in data if d.get("value") is not None]
    conn.executemany(
        "INSERT OR REPLACE INTO fyp_indicators "
        "(indicator, period, value, unit, source, source_url, fetched_at) "
        "VALUES (?,?,?,?,?,?,?)", rows)
    conn.commit()
    return len(rows)


# ---------------------------------------------------------------------------
# 4. Research output — OpenAlex publication counts (no key)
# ---------------------------------------------------------------------------
_OPENALEX = "https://api.openalex.org/works"


def _openalex_by_year(search: str, cn_only: bool) -> dict[int, int]:
    flt = f'title_and_abstract.search:"{search}"'
    if cn_only:
        flt += ",authorships.countries:CN"
    r = requests.get(_OPENALEX, params={"filter": flt, "group_by": "publication_year",
                                        "per-page": 200},
                     headers=_HEADERS, timeout=60)
    r.raise_for_status()
    out = {}
    for g in r.json().get("group_by", []):
        try:
            out[int(g["key"])] = int(g["count"])
        except (ValueError, TypeError):
            pass
    return out


def fetch_publications(conn: sqlite3.Connection) -> int:
    """China vs world publication counts per domain, 2015→now (current year is
    a partial count; the UI drops it)."""
    now = _now()
    this_year = datetime.now(timezone.utc).year
    total = 0
    for domain, meta in TECH_DOMAINS.items():
        try:
            world = _openalex_by_year(meta["search"], cn_only=False)
            time.sleep(0.5)
            cn = _openalex_by_year(meta["search"], cn_only=True)
            time.sleep(0.5)
        except Exception as exc:
            log.warning(f"  OpenAlex {domain} failed: {exc}")
            continue
        rows = [(domain, y, cn.get(y, 0), world.get(y, 0), now)
                for y in range(_FIRST_YEAR, this_year + 1) if y in world]
        conn.executemany(
            "INSERT OR REPLACE INTO fyp_publications "
            "(domain, year, cn_count, world_count, fetched_at) VALUES (?,?,?,?,?)",
            rows)
        conn.commit()
        total += len(rows)
    log.info(f"  publications: {total} domain-year rows stored")
    return total


def publication_series(conn: sqlite3.Connection) -> dict[str, list[dict]]:
    this_year = datetime.now(timezone.utc).year
    rows = conn.execute(
        "SELECT domain, year, cn_count, world_count FROM fyp_publications "
        "WHERE year < ? ORDER BY domain, year", (this_year,)).fetchall()
    out: dict[str, list[dict]] = {}
    for r in rows:
        share = round(100.0 * r[2] / r[3], 1) if r[3] else None
        out.setdefault(r[0], []).append(
            {"year": r[1], "cn": r[2], "world": r[3], "share": share})
    return out


# ---------------------------------------------------------------------------
# 5. AI benchmarks — Epoch AI Benchmarking Hub (CC-BY, no key)
# ---------------------------------------------------------------------------
_EPOCH_ZIP = "https://epoch.ai/data/benchmark_data.zip"
_EPOCH_FILES = {  # csv name inside the zip -> (benchmark key, score column)
    "epoch_capabilities_index.csv": ("eci", "ECI Score"),
    "gpqa_diamond.csv": ("gpqa_diamond", "Best score (across scorers)"),
}
_COUNTRY_MAP = {"China": "CN", "United States of America": "US"}


def fetch_ai_benchmarks(conn: sqlite3.Connection) -> int:
    """Model scores (with org/country/release date) from Epoch AI's
    benchmarking hub — refreshed on every run; the zip updates near-daily."""
    try:
        r = requests.get(_EPOCH_ZIP, headers={"User-Agent": _HEADERS["User-Agent"]},
                         timeout=120)
        r.raise_for_status()
        zf = zipfile.ZipFile(io.BytesIO(r.content))
    except Exception as exc:
        log.warning(f"  Epoch benchmark zip failed: {exc}")
        return 0
    now, rows = _now(), []
    for fname, (bench, score_col) in _EPOCH_FILES.items():
        try:
            raw = zf.read(fname).decode("utf-8")
        except KeyError:
            log.warning(f"  {fname} missing from Epoch zip")
            continue
        for rec in csv.DictReader(io.StringIO(raw)):
            model = (rec.get("Model version") or "").strip()
            score = (rec.get(score_col) or "").strip()
            date = (rec.get("Release date") or "").strip()
            if not model or not score or not date:
                continue
            try:
                score = float(score)
            except ValueError:
                continue
            country = _COUNTRY_MAP.get((rec.get("Country") or "").strip(), "other")
            name = (rec.get("Display name") or rec.get("Model name")
                    or rec.get("Name") or model).strip()
            rows.append((bench, model, name, (rec.get("Organization") or "").strip(),
                         country, date, score, now))
    conn.executemany(
        "INSERT OR REPLACE INTO fyp_ai_benchmarks "
        "(benchmark, model, name, org, country, release_date, score, fetched_at) "
        "VALUES (?,?,?,?,?,?,?,?)", rows)
    conn.commit()
    log.info(f"  AI benchmarks: {len(rows)} model scores stored")
    return len(rows)


def benchmark_series(conn: sqlite3.Connection) -> dict[str, list[dict]]:
    """CN/US model scores per benchmark, date-sorted (frontier computed in UI)."""
    rows = conn.execute(
        "SELECT benchmark, name, org, country, release_date, score "
        "FROM fyp_ai_benchmarks WHERE country IN ('CN','US') "
        "ORDER BY benchmark, release_date").fetchall()
    out: dict[str, list[dict]] = {}
    for r in rows:
        out.setdefault(r[0], []).append(
            {"name": r[1], "org": r[2], "country": r[3], "date": r[4],
             "score": round(r[5], 2)})
    return out


# ---------------------------------------------------------------------------
# Aggregations for the API
# ---------------------------------------------------------------------------

def world_series(conn: sqlite3.Connection) -> list[dict]:
    """Annual world-total value per HS4 line and flow."""
    rows = conn.execute(
        "SELECT year, hs4, flow, value_usd FROM fyp_chip_trade "
        "WHERE partner_code=0 ORDER BY year").fetchall()
    return [dict(r) for r in rows]


def hhi_series(conn: sqlite3.Connection, flow: str = "M") -> list[dict]:
    """Supplier-concentration HHI (0-10000) per HS4 line and year."""
    rows = conn.execute("""
        WITH p AS (
            SELECT year, hs4, partner_code, value_usd FROM fyp_chip_trade
            WHERE flow=? AND partner_code != 0 AND value_usd > 0
        ), t AS (
            SELECT year, hs4, SUM(value_usd) AS tot FROM p GROUP BY year, hs4
        )
        SELECT p.year, p.hs4,
               SUM((100.0 * p.value_usd / t.tot) * (100.0 * p.value_usd / t.tot)) AS hhi
        FROM p JOIN t ON p.year=t.year AND p.hs4=t.hs4
        GROUP BY p.year, p.hs4 ORDER BY p.year
    """, (flow,)).fetchall()
    return [{"year": r[0], "hs4": r[1], "hhi": round(r[2])} for r in rows]


def top_partners(conn: sqlite3.Connection, hs4: str, flow: str = "M",
                 n: int = 8) -> dict:
    """Top partners and shares for the latest stored year."""
    year = conn.execute(
        "SELECT MAX(year) FROM fyp_chip_trade WHERE hs4=? AND flow=? AND partner_code!=0",
        (hs4, flow)).fetchone()[0]
    if year is None:
        return {"year": None, "partners": []}
    rows = conn.execute("""
        SELECT partner_iso, partner_name, value_usd FROM fyp_chip_trade
        WHERE hs4=? AND flow=? AND year=? AND partner_code!=0 AND value_usd>0
        ORDER BY value_usd DESC
    """, (hs4, flow, year)).fetchall()
    total = sum(r[2] for r in rows) or 1.0
    return {
        "year": year,
        "partners": [{"iso": r[0], "name": r[1], "value_usd": r[2],
                      "share": round(100.0 * r[2] / total, 1)} for r in rows[:n]],
    }


def eu_monthly_series(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT period, hs4, flow, value_eur FROM fyp_chip_eu_monthly ORDER BY period")
    return [dict(r) for r in rows]


def eu_hs85_share(conn: sqlite3.Connection) -> list[dict]:
    """China's share of total EU27 imports of HS-85 electronics, monthly —
    computed from the existing eurostat_imports table (competitiveness feed)."""
    try:
        rows = conn.execute("""
            WITH cn AS (
                SELECT period, value_eur FROM eurostat_imports
                WHERE product='85' AND partner='CN'
            ), tot AS (
                SELECT period, SUM(value_eur) AS total FROM eurostat_imports
                WHERE product='85' AND partner IN ('INT_EU27_2020','EXT_EU27_2020')
                GROUP BY period
            )
            SELECT cn.period, 100.0 * cn.value_eur / tot.total AS share
            FROM cn JOIN tot ON cn.period=tot.period
            WHERE tot.total > 0 ORDER BY cn.period
        """).fetchall()
        return [{"period": r[0], "share": round(r[1], 2)} for r in rows]
    except sqlite3.OperationalError:
        return []


def indicator_series(conn: sqlite3.Connection) -> dict[str, list[dict]]:
    rows = conn.execute(
        "SELECT indicator, period, value, unit, source, source_url "
        "FROM fyp_indicators ORDER BY indicator, period").fetchall()
    out: dict[str, list[dict]] = {}
    for r in rows:
        out.setdefault(r[0], []).append(
            {"period": r[1], "value": r[2], "unit": r[3],
             "source": r[4], "source_url": r[5]})
    return out


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s",
                        datefmt="%H:%M:%S")
    ap = argparse.ArgumentParser(description="FYP tech self-reliance data fetch")
    ap.add_argument("--full", action="store_true", help="backfill full history")
    args = ap.parse_args()

    conn = get_db()
    log.info("Seeding official indicator datapoints …")
    seed_indicators(conn)
    log.info("World Bank R&D intensity …")
    fetch_wb_rd_intensity(conn)
    log.info("OpenAlex publication counts …")
    fetch_publications(conn)
    log.info("Epoch AI benchmark scores …")
    fetch_ai_benchmarks(conn)
    log.info("EU27<->CN monthly chip trade (COMEXT) …")
    fetch_eu_monthly(conn, full=args.full)
    log.info("China tech-line trade by partner (Comtrade preview) …")
    fetch_chip_trade(conn, full=args.full)
    n = conn.execute("SELECT COUNT(*) FROM fyp_chip_trade").fetchone()[0]
    m = conn.execute("SELECT COUNT(*) FROM fyp_chip_eu_monthly").fetchone()[0]
    log.info(f"Done — fyp_chip_trade: {n:,} rows, fyp_chip_eu_monthly: {m:,} rows")
    conn.close()


if __name__ == "__main__":
    main()
