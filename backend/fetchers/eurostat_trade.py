"""
Eurostat COMEXT monthly trade fetcher — EU27 imports by HS2 chapter and partner.

Pulls dataset DS-045409 ("EU trade since 1988 by HS2-4-6") from the Eurostat
COMEXT dissemination API: one API call per month returns all 97 HS2 chapters ×
all ~278 partner codes for EU27 imports (flow=1), value in EUR.

Data lands in the single consolidated data/cmm.db (table `eurostat_imports`) via
storage.get_conn() — NOT a separate DB.

The dashboard "Competitiveness" tab consumes compute_shares() / compute_gain(),
which bucket partners into China / Germany / USA / Vietnam / Australia&NZ /
Other Europe / Other Asia / Africa / South America / Rest of World and express
each group as a share of total EU imports (= INT_EU27_2020 + EXT_EU27_2020).

CLI:
    python -m backend.fetchers.eurostat_trade --backfill 2014-01
    python -m backend.fetchers.eurostat_trade --recent 6
    python -m backend.fetchers.eurostat_trade --show
"""

import argparse
import logging
import sqlite3
import time
from datetime import datetime, timezone

import requests

from backend.storage import get_conn

log = logging.getLogger("fetcher.eurostat_trade")

# ---------------------------------------------------------------------------
# API configuration
# ---------------------------------------------------------------------------
COMEXT_BASE = (
    "https://ec.europa.eu/eurostat/api/comext/dissemination/"
    "statistics/1.0/data/DS-045409"
)
HEADERS = {"User-Agent": "CMM/1.0 (research dashboard)"}
TIMEOUT = 180
REPORTER = "EU27_2020"          # the EU as the import market
FLOW_IMPORT = "1"
INDICATOR = "VALUE_IN_EUROS"
DEFAULT_START = "2014-01"

# 97 HS2 chapters (01..99; 98/99 are special "other" chapters). Sending the
# explicit list confines the product dimension to HS2 — without it the API
# would also return HS4/HS6/CN8 levels and the extraction blows past limits.
HS2_CODES = [f"{i:02d}" for i in range(1, 100)]

# Authoritative total-imports denominator: intra + extra EU27 partitions the
# world exactly (no double-counting). The WORLD partner code returns empty.
TOTAL_PARTNERS = ("INT_EU27_2020", "EXT_EU27_2020")

# ---------------------------------------------------------------------------
# Concise HS2 chapter labels (standard HS nomenclature, stable reference data)
# ---------------------------------------------------------------------------
HS2_LABELS = {
    "01": "Live animals", "02": "Meat", "03": "Fish & seafood",
    "04": "Dairy, eggs, honey", "05": "Other animal products",
    "06": "Live trees & plants", "07": "Vegetables", "08": "Fruit & nuts",
    "09": "Coffee, tea, spices", "10": "Cereals", "11": "Milling products",
    "12": "Oil seeds", "13": "Lacs, gums, resins", "14": "Vegetable plaiting",
    "15": "Animal/vegetable fats & oils", "16": "Prepared meat/fish",
    "17": "Sugars", "18": "Cocoa", "19": "Cereal/flour preparations",
    "20": "Prepared vegetables/fruit", "21": "Misc. edible preparations",
    "22": "Beverages & spirits", "23": "Food residues; animal feed",
    "24": "Tobacco", "25": "Salt, sulphur, stone, cement", "26": "Ores & ash",
    "27": "Mineral fuels & oils", "28": "Inorganic chemicals",
    "29": "Organic chemicals", "30": "Pharmaceuticals", "31": "Fertilisers",
    "32": "Tanning/dyeing extracts", "33": "Essential oils; cosmetics",
    "34": "Soap, waxes", "35": "Albuminoids; glues; enzymes",
    "36": "Explosives; matches", "37": "Photographic goods",
    "38": "Misc. chemical products", "39": "Plastics", "40": "Rubber",
    "41": "Raw hides & leather", "42": "Leather articles", "43": "Furskins",
    "44": "Wood", "45": "Cork", "46": "Straw/basketware",
    "47": "Wood pulp", "48": "Paper & paperboard", "49": "Printed books",
    "50": "Silk", "51": "Wool", "52": "Cotton", "53": "Other vegetable fibres",
    "54": "Man-made filaments", "55": "Man-made staple fibres",
    "56": "Wadding, felt, nonwovens", "57": "Carpets",
    "58": "Special woven fabrics", "59": "Coated textile fabrics",
    "60": "Knitted fabrics", "61": "Knitted apparel",
    "62": "Non-knitted apparel", "63": "Other made-up textiles",
    "64": "Footwear", "65": "Headgear", "66": "Umbrellas",
    "67": "Prepared feathers", "68": "Stone/cement/asbestos articles",
    "69": "Ceramics", "70": "Glass", "71": "Pearls, precious metals",
    "72": "Iron & steel", "73": "Iron/steel articles", "74": "Copper",
    "75": "Nickel", "76": "Aluminium", "78": "Lead", "79": "Zinc",
    "80": "Tin", "81": "Other base metals", "82": "Tools & cutlery",
    "83": "Misc. base metal articles", "84": "Machinery & mechanical appliances",
    "85": "Electrical machinery & electronics", "86": "Railway",
    "87": "Vehicles", "88": "Aircraft", "89": "Ships & boats",
    "90": "Optical, medical, precision instruments", "91": "Clocks & watches",
    "92": "Musical instruments", "93": "Arms & ammunition", "94": "Furniture",
    "95": "Toys, games, sports", "96": "Misc. manufactured articles",
    "97": "Works of art", "98": "Project goods (special)",
    "99": "Other (special)",
}

# ---------------------------------------------------------------------------
# Partner → competitiveness group mapping
# ---------------------------------------------------------------------------
# Singled-out competitors get their own group; the remaining members of each
# continent fall into the "Other <continent>" bucket. Only individual country
# ISO2 codes are listed here — aggregate codes (INT_*, EXT_*, WORLD, EXT_EA…)
# are never summed (the total comes from TOTAL_PARTNERS instead), and any code
# not listed is absorbed by the "Rest of World" residual so group shares always
# sum to 100% of total EU imports. EU member states (except DE) all sit in
# "Other Europe", so intra-EU trade is fully captured.

CHINA = {"CN"}
GERMANY = {"DE"}
USA = {"US"}
VIETNAM = {"VN"}
ANZ = {"AU", "NZ", "XO", "XZ"}  # incl. Australian/NZ Oceania territory codes

EUROPE = {
    # EU27 (minus DE — its own group)
    "AT", "BE", "BG", "CY", "CZ", "DK", "EE", "ES", "FI", "FR", "GR", "HR",
    "HU", "IE", "IT", "LT", "LU", "LV", "MT", "NL", "PL", "PT", "RO", "SE",
    "SI", "SK",
    # Non-EU Europe
    "AD", "AL", "BA", "BY", "CH", "FO", "GB", "GI", "IS", "LI", "MD", "ME",
    "MK", "NO", "RU", "SM", "TR", "UA", "VA", "XK", "XS", "XM",
}

ASIA = {
    # East / SE / South / Central Asia (minus CN, VN)
    "AF", "BD", "BN", "BT", "HK", "ID", "IN", "JP", "KG", "KH", "KP", "KR",
    "KZ", "LA", "LK", "MM", "MN", "MO", "MV", "MY", "NP", "PH", "PK", "SG",
    "TH", "TJ", "TL", "TM", "TW", "UZ",
    # Middle East
    "AE", "BH", "IL", "IQ", "IR", "JO", "KW", "LB", "OM", "PS", "QA", "SA",
    "SY", "YE",
    # Caucasus (geographic Asia)
    "AM", "AZ", "GE",
}

AFRICA = {
    "AO", "BF", "BI", "BJ", "BW", "CD", "CF", "CG", "CI", "CM", "CV", "DJ",
    "DZ", "EG", "EH", "ER", "ET", "GA", "GH", "GM", "GN", "GQ", "GW", "KE",
    "KM", "LR", "LS", "LY", "MA", "MG", "ML", "MR", "MU", "MW", "MZ", "NA",
    "NE", "NG", "RW", "SC", "SD", "SH", "SL", "SN", "SO", "SS", "ST", "SZ",
    "TD", "TG", "TN", "TZ", "UG", "ZA", "ZM", "ZW",
}

# South America (GF/French Guiana excluded — folded into FR by Eurostat).
SOUTH_AMERICA = {
    "AR", "BO", "BR", "CL", "CO", "EC", "FK", "GS", "GY", "PE", "PY", "SR",
    "UY", "VE",
}

# Display order matters for the stacked chart (China first / most-watched).
GROUPS: dict[str, set[str]] = {
    "China": CHINA,
    "Germany": GERMANY,
    "USA": USA,
    "Vietnam": VIETNAM,
    "Other Asia": ASIA,
    "Other Europe": EUROPE,
    "South America": SOUTH_AMERICA,
    "Africa": AFRICA,
    "Australia & NZ": ANZ,
}
GROUP_ORDER = list(GROUPS.keys()) + ["Rest of World"]

# Reverse index: partner code -> group name (for fast bucketing).
_PARTNER_TO_GROUP: dict[str, str] = {}
for _grp, _codes in GROUPS.items():
    for _c in _codes:
        _PARTNER_TO_GROUP[_c] = _grp

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS eurostat_imports (
    reporter   TEXT NOT NULL DEFAULT 'EU27_2020',
    partner    TEXT NOT NULL,
    product    TEXT NOT NULL,
    period     TEXT NOT NULL,
    value_eur  REAL,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (reporter, partner, product, period)
);
CREATE INDEX IF NOT EXISTS idx_eui_period  ON eurostat_imports(period);
CREATE INDEX IF NOT EXISTS idx_eui_product ON eurostat_imports(product);
"""


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)


# ---------------------------------------------------------------------------
# JSON-STAT 2.0 parsing
# ---------------------------------------------------------------------------
def _parse_jsonstat(raw: dict) -> list[dict]:
    """Flatten a Eurostat JSON-STAT 2.0 response into per-cell records.

    Returns a list of dicts with one key per dimension plus "value".
    Mirrors the stride-decoding approach in backend/fetchers/eurostat.py.
    """
    dims: list[str] = raw["id"]
    sizes: list[int] = raw["size"]
    dim_data: dict = raw["dimension"]
    values_raw = raw.get("value", {})

    ordered_codes: dict[str, list[str]] = {}
    for dim in dims:
        idx_map = dim_data[dim]["category"].get("index", {})
        if isinstance(idx_map, dict):
            ordered_codes[dim] = sorted(idx_map.keys(), key=lambda k: idx_map[k])
        else:
            ordered_codes[dim] = list(idx_map)

    if isinstance(values_raw, list):
        values = {i: v for i, v in enumerate(values_raw) if v is not None}
    elif isinstance(values_raw, dict):
        values = {int(k): v for k, v in values_raw.items() if v is not None}
    else:
        values = {}

    strides = [1] * len(dims)
    for i in range(len(dims) - 2, -1, -1):
        strides[i] = strides[i + 1] * sizes[i + 1]

    records: list[dict] = []
    for pos, val in values.items():
        rec: dict = {"value": float(val)}
        remaining = pos
        for i, dim in enumerate(dims):
            idx = remaining // strides[i]
            remaining %= strides[i]
            codes = ordered_codes[dim]
            rec[dim] = codes[idx] if 0 <= idx < len(codes) else ""
        records.append(rec)
    return records


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------
def _fetch_raw(period: str) -> dict:
    """One API call: all HS2 chapters × all partners, EU27 imports for `period`."""
    params = [
        ("format", "JSON"),
        ("lang", "EN"),
        ("freq", "M"),
        ("reporter", REPORTER),
        ("flow", FLOW_IMPORT),
        ("indicators", INDICATOR),
        ("time", period),
    ]
    params += [("product", p) for p in HS2_CODES]
    resp = requests.get(COMEXT_BASE, params=params, headers=HEADERS, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def fetch_month(conn: sqlite3.Connection, period: str) -> int:
    """Fetch + upsert one month. Returns rows stored (0 if no data yet)."""
    raw = _fetch_raw(period)
    if "error" in raw:
        msg = raw["error"][0].get("label", str(raw["error"])) if raw["error"] else "unknown"
        # "no data" for a not-yet-published month comes back as an error/empty.
        log.info("eurostat_trade %s: %s", period, msg)
        return 0

    records = _parse_jsonstat(raw)
    now = datetime.now(timezone.utc).isoformat()
    rows = [
        (REPORTER, r["partner"], r["product"], period, r["value"], now)
        for r in records
        if r.get("partner") and r.get("product")
    ]
    if not rows:
        return 0
    conn.executemany(
        "INSERT INTO eurostat_imports "
        "(reporter, partner, product, period, value_eur, fetched_at) "
        "VALUES (?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(reporter, partner, product, period) "
        "DO UPDATE SET value_eur=excluded.value_eur, fetched_at=excluded.fetched_at",
        rows,
    )
    conn.commit()
    return len(rows)


def _month_range(start: str, end: str) -> list[str]:
    """Inclusive list of 'YYYY-MM' from start to end."""
    sy, sm = (int(x) for x in start.split("-"))
    ey, em = (int(x) for x in end.split("-"))
    out: list[str] = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return out


def _now_period() -> str:
    n = datetime.now(timezone.utc)
    return f"{n.year:04d}-{n.month:02d}"


def backfill(start: str = DEFAULT_START, end: str | None = None,
             pause: float = 0.4) -> dict:
    """Fetch every month from `start` to `end` (default: current month)."""
    end = end or _now_period()
    conn = get_conn()
    _ensure_schema(conn)
    months = _month_range(start, end)
    total, ok = 0, 0
    for period in months:
        try:
            n = fetch_month(conn, period)
            total += n
            if n:
                ok += 1
                log.info("eurostat_trade %s: %d rows", period, n)
        except Exception as e:
            log.warning("eurostat_trade %s failed: %s", period, e)
        time.sleep(pause)
    _log_fetch(conn, ok, total, len(months), None)
    conn.close()
    return {"months": len(months), "months_with_data": ok, "rows": total}


def fetch_recent(n: int = 6) -> dict:
    """Fetch the trailing `n` months — catches new releases + revisions."""
    end = _now_period()
    months = _month_range(_shift_period(end, -(n - 1)), end)
    conn = get_conn()
    _ensure_schema(conn)
    total, got = 0, 0
    err = None
    for period in months:
        try:
            rows = fetch_month(conn, period)
            total += rows
            if rows:
                got += 1
        except Exception as e:
            err = str(e)
            log.warning("eurostat_trade %s failed: %s", period, e)
        time.sleep(0.4)
    _log_fetch(conn, got, total, len(months), err)
    conn.close()
    log.info("eurostat_trade recent: %d/%d months, %d rows", got, len(months), total)
    return {"months": len(months), "months_with_data": got, "rows": total}


def _shift_period(period: str, delta: int) -> str:
    y, m = (int(x) for x in period.split("-"))
    total = y * 12 + (m - 1) + delta
    return f"{total // 12:04d}-{total % 12 + 1:02d}"


def _log_fetch(conn: sqlite3.Connection, ok: int, rows: int,
               months: int, err: str | None) -> None:
    conn.execute(
        "INSERT INTO fetch_log (source, feed_url, ok, error, item_count, fetched_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (
            "EUROSTAT_TRADE",
            COMEXT_BASE,
            1 if err is None else 0,
            err,
            rows,
            datetime.now(timezone.utc).isoformat(),
        ),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Share / gain computation (consumed by the API + dashboard)
# ---------------------------------------------------------------------------
def list_products(conn: sqlite3.Connection) -> list[dict]:
    """HS2 chapters present in the DB, with concise labels."""
    rows = conn.execute(
        "SELECT DISTINCT product FROM eurostat_imports "
        "WHERE product NOT IN ('98','99') ORDER BY product"
    ).fetchall()
    out = [{"hs2": "all", "label": "All chapters (total)"}]
    for r in rows:
        code = r[0]
        out.append({"hs2": code, "label": f"{code} — {HS2_LABELS.get(code, code)}"})
    return out


def _group_series(conn: sqlite3.Connection, product: str,
                  since: str | None) -> dict[str, dict[str, float]]:
    """Return {period: {group: value_eur, '_total': value_eur}} for a product.

    product='all' sums across HS2 chapters. The total denominator uses the
    INT+EXT EU27 aggregates; group values sum the mapped individual partners.
    """
    where_product = "" if product == "all" else "AND product = ?"
    where_since = "AND period >= ?" if since else ""
    args: list = []
    if product != "all":
        args.append(product)
    if since:
        args.append(since)

    rows = conn.execute(
        f"SELECT period, partner, SUM(value_eur) AS v FROM eurostat_imports "
        f"WHERE reporter = '{REPORTER}' {where_product} {where_since} "
        f"GROUP BY period, partner",
        args,
    ).fetchall()

    out: dict[str, dict[str, float]] = {}
    for period, partner, v in rows:
        if v is None:
            continue
        bucket = out.setdefault(period, {})
        if partner in TOTAL_PARTNERS:
            bucket["_total"] = bucket.get("_total", 0.0) + v
        grp = _PARTNER_TO_GROUP.get(partner)
        if grp:
            bucket[grp] = bucket.get(grp, 0.0) + v
    return out


def compute_shares(conn: sqlite3.Connection, product: str = "all",
                   since: str | None = None) -> dict:
    """Per-group monthly value + share of total EU imports for a product."""
    series = _group_series(conn, product, since)
    periods = sorted(series.keys())
    result: dict[str, list] = {"periods": periods, "groups": {}, "total": []}

    for grp in GROUP_ORDER:
        result["groups"][grp] = {"value": [], "share": []}

    for period in periods:
        bucket = series[period]
        total = bucket.get("_total", 0.0)
        result["total"].append(total)
        named_sum = 0.0
        for grp in GROUPS:
            v = bucket.get(grp, 0.0)
            named_sum += v
            result["groups"][grp]["value"].append(v)
            result["groups"][grp]["share"].append(
                round(100.0 * v / total, 3) if total else None
            )
        residual = max(total - named_sum, 0.0)
        result["groups"]["Rest of World"]["value"].append(residual)
        result["groups"]["Rest of World"]["share"].append(
            round(100.0 * residual / total, 3) if total else None
        )
    return result


def compute_gain(conn: sqlite3.Connection, product: str = "all",
                 window: int = 12) -> dict:
    """Per-group share now vs `window` months earlier (percentage-point change).

    Uses the latest period with data and the period `window` months before it.
    """
    shares = compute_shares(conn, product, since=None)
    periods = shares["periods"]
    if len(periods) < 2:
        return {"product": product, "groups": [], "latest": None, "base": None}

    latest = periods[-1]
    target = _shift_period(latest, -window)
    base = min(periods, key=lambda p: abs(_period_index(p) - _period_index(target)))
    li = periods.index(latest)
    bi = periods.index(base)

    groups = []
    for grp in GROUP_ORDER:
        s_now = shares["groups"][grp]["share"][li]
        s_base = shares["groups"][grp]["share"][bi]
        if s_now is None or s_base is None:
            continue
        groups.append({
            "group": grp,
            "share_now": s_now,
            "share_base": s_base,
            "gain_pp": round(s_now - s_base, 3),
        })
    return {"product": product, "latest": latest, "base": base, "groups": groups}


def _period_index(period: str) -> int:
    y, m = (int(x) for x in period.split("-"))
    return y * 12 + (m - 1)


def compute_gain_matrix(conn: sqlite3.Connection, group: str = "China",
                        window: int = 12) -> dict:
    """One group's share gain (pp) across ALL HS2 chapters, ranked."""
    products = [p["hs2"] for p in list_products(conn) if p["hs2"] != "all"]
    out = []
    for hs2 in products:
        g = compute_gain(conn, hs2, window)
        match = next((x for x in g["groups"] if x["group"] == group), None)
        if match and g["latest"]:
            out.append({
                "hs2": hs2,
                "label": HS2_LABELS.get(hs2, hs2),
                "share_now": match["share_now"],
                "share_base": match["share_base"],
                "gain_pp": match["gain_pp"],
            })
    out.sort(key=lambda x: x["gain_pp"], reverse=True)
    latest = compute_gain(conn, "all", window)
    return {
        "group": group, "window": window,
        "latest": latest.get("latest"), "base": latest.get("base"),
        "products": out,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _show() -> None:
    conn = get_conn()
    _ensure_schema(conn)
    row = conn.execute(
        "SELECT COUNT(*), MIN(period), MAX(period), "
        "COUNT(DISTINCT partner), COUNT(DISTINCT product) FROM eurostat_imports"
    ).fetchone()
    print(f"rows={row[0]:,}  periods={row[1]}..{row[2]}  "
          f"partners={row[3]}  products={row[4]}")
    conn.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser(description="Eurostat COMEXT monthly HS-trade fetcher")
    ap.add_argument("--backfill", metavar="YYYY-MM", nargs="?", const=DEFAULT_START,
                    help=f"backfill from this month (default {DEFAULT_START})")
    ap.add_argument("--recent", type=int, metavar="N",
                    help="fetch trailing N months")
    ap.add_argument("--show", action="store_true", help="print DB summary")
    args = ap.parse_args()

    if args.show:
        _show()
    elif args.recent:
        print(fetch_recent(args.recent))
    elif args.backfill:
        print(backfill(args.backfill))
    else:
        ap.print_help()


if __name__ == "__main__":
    main()
