"""
BIS statistics fetcher — credit, exchange rates, and policy rates.

Fetches from the BIS SDMX REST API (no auth required):
  WS_CREDIT_GAP : Credit-to-GDP gap, Germany, quarterly
  WS_TC         : Credit-to-GDP ratio, all countries, quarterly
  WS_GLI        : USD credit to non-US borrowers, quarterly
  WS_EER        : Real effective exchange rates, monthly
  WS_CBPOL      : Central bank policy rates, daily

Data stored in data/feeds.db.

Usage:
    python bis.py          # fetch all datasets
    python bis.py --show   # print latest snapshots
    python bis.py --json   # output as JSON
"""

import argparse
import io
import json
import logging
import sqlite3
from datetime import datetime, timezone

import pandas as pd
import requests

from policy_monitor.storage import DB_DIR, DB_PATH

log = logging.getLogger("bis")

BIS_BASE = "https://stats.bis.org/api/v1/data"

_ISO2_TO_ISO3 = {
    "AR": "ARG", "AU": "AUS", "AT": "AUT", "BE": "BEL", "BR": "BRA",
    "CA": "CAN", "CL": "CHL", "CN": "CHN", "CO": "COL", "CZ": "CZE",
    "DK": "DNK", "FI": "FIN", "FR": "FRA", "DE": "DEU", "GR": "GRC",
    "HK": "HKG", "HU": "HUN", "IN": "IND", "ID": "IDN", "IE": "IRL",
    "IL": "ISR", "IT": "ITA", "JP": "JPN", "KR": "KOR", "LU": "LUX",
    "MY": "MYS", "MX": "MEX", "NL": "NLD", "NZ": "NZL", "NO": "NOR",
    "PL": "POL", "PT": "PRT", "RU": "RUS", "SA": "SAU", "SG": "SGP",
    "ZA": "ZAF", "ES": "ESP", "SE": "SWE", "CH": "CHE", "TH": "THA",
    "TR": "TUR", "GB": "GBR", "US": "USA", "CY": "CYP", "EE": "EST",
    "LV": "LVA", "LT": "LTU", "SI": "SVN", "SK": "SVK", "RO": "ROU",
    "BG": "BGR", "HR": "HRV", "MT": "MLT", "IS": "ISL", "MK": "MKD",
    "RS": "SRB", "UA": "UKR", "PH": "PHL", "VN": "VNM", "PE": "PER",
}

_GLI_REGION_LABELS = {
    "2A": "Emerging market economies (Asia-Pacific)",
    "2R": "Emerging market economies (Europe)",
    "3C": "All countries outside the US",
    "3P": "All countries outside the US (ex. offshore)",
    "4T": "All reporting countries",
    "4U": "Advanced economies",
    "4W": "International organisations",
    "4Y": "Offshore centres",
    "5C": "Emerging market economies (Africa & Middle East)",
    "5J": "Emerging market economies",
    "6E": "Emerging market economies (Latin America)",
}

_GLI_INSTRUMENT_MAP = {"G": "loans", "D": "debt_sec", "B": "total"}

_REER_SERIES = {
    "XM": "reer_eur",
    "US": "reer_usd",
    "CN": "reer_cny",
    "JP": "reer_jpy",
    "GB": "reer_gbp",
}

_CBPOL_SERIES = {
    "US": "fed_funds",
    "XM": "ecb_rate",
    "GB": "boe_rate",
    "JP": "boj_rate",
    "CN": "pboc_rate",
    "CH": "snb_rate",
    "CA": "boc_rate",
    "AU": "rba_rate",
    "SE": "riksbank_rate",
    "NO": "norges_rate",
    "DK": "dnb_rate",
}

# ---------------------------------------------------------------------------
# DB schema
# ---------------------------------------------------------------------------

BIS_SCHEMA = """
CREATE TABLE IF NOT EXISTS bis_credit_gap (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    period                TEXT NOT NULL,
    credit_to_gdp_gap_pct REAL,
    fetched_at            TEXT NOT NULL,
    UNIQUE(period)
);

CREATE TABLE IF NOT EXISTS bis_credit_to_gdp (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    period            TEXT NOT NULL,
    country_code      TEXT NOT NULL,
    country_name      TEXT,
    credit_to_gdp_pct REAL,
    fetched_at        TEXT NOT NULL,
    UNIQUE(period, country_code)
);

CREATE TABLE IF NOT EXISTS bis_usd_credit (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    period           TEXT NOT NULL,
    borrower_country TEXT NOT NULL,
    borrower_iso3    TEXT,
    borrower_label   TEXT,
    instrument       TEXT NOT NULL,
    usd_credit_bn    REAL,
    fetched_at       TEXT NOT NULL,
    UNIQUE(period, borrower_country, instrument)
);

CREATE TABLE IF NOT EXISTS bis_reer (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    period     TEXT NOT NULL,
    currency   TEXT NOT NULL,
    reer_idx   REAL,
    yoy_pct    REAL,
    mom_pct    REAL,
    fetched_at TEXT NOT NULL,
    UNIQUE(period, currency)
);

CREATE TABLE IF NOT EXISTS bis_policy_rates (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    date         TEXT NOT NULL,
    country_code TEXT NOT NULL,
    series_name  TEXT,
    policy_rate  REAL,
    fetched_at   TEXT NOT NULL,
    UNIQUE(date, country_code)
);
"""


def get_bis_db() -> sqlite3.Connection:
    DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.executescript(BIS_SCHEMA)
    return conn


# ---------------------------------------------------------------------------
# Core HTTP fetch
# ---------------------------------------------------------------------------

def _fetch_bis(dataset: str, key: str, start: str = "2000-Q1") -> pd.DataFrame:
    url = f"{BIS_BASE}/{dataset}/{key}"
    resp = requests.get(
        url,
        params={"format": "csv", "startPeriod": start},
        headers={"Accept": "text/csv"},
        timeout=60,
    )
    if not resp.ok:
        raise RuntimeError(f"BIS API error {resp.status_code}: {resp.text[:300]}")

    lines = resp.text.strip().splitlines()
    header_idx = next(
        (i for i, ln in enumerate(lines)
         if ln.upper().startswith(("FREQ", "TIME_PERIOD", "KEY", "BORROWER"))),
        0,
    )
    return pd.read_csv(io.StringIO("\n".join(lines[header_idx:])))


def _quarter_to_date(s) -> "pd.Timestamp":
    s = str(s).replace("-Q", "Q")
    if "Q" not in s:
        return pd.NaT
    year, q = s.split("Q")
    month = (int(q) - 1) * 3 + 1
    return pd.Timestamp(f"{year}-{month:02d}-01")


# ---------------------------------------------------------------------------
# Dataset fetchers — each fetches + stores one BIS dataset
# ---------------------------------------------------------------------------

def _fetch_credit_gap(conn: sqlite3.Connection) -> int:
    dataset, key = "WS_CREDIT_GAP", "Q.DE.P.A.B"
    log.info(f"Fetching BIS {dataset}/{key}...")
    df = _fetch_bis(dataset, key)
    df.columns = [c.strip().upper() for c in df.columns]

    tp_col = next((c for c in df.columns if "TIME_PERIOD" in c or c.endswith("PERIOD")), None)
    ov_col = next((c for c in df.columns if c.startswith("OBS_V")), None)
    if not tp_col or not ov_col:
        raise RuntimeError(f"TIME_PERIOD/OBS_VALUE not found. Columns: {list(df.columns)}")

    df["period"] = df[tp_col].apply(_quarter_to_date)
    df["value"]  = pd.to_numeric(df[ov_col], errors="coerce")
    df = df.dropna(subset=["period"]).sort_values("period")
    df["period"] = df["period"].astype(str)

    now = datetime.now(timezone.utc).isoformat()
    inserted = 0
    for _, row in df.iterrows():
        conn.execute(
            "INSERT OR REPLACE INTO bis_credit_gap (period, credit_to_gdp_gap_pct, fetched_at) "
            "VALUES (?, ?, ?)",
            (row["period"], row["value"], now),
        )
        inserted += 1
    conn.commit()
    log.info(f"  bis_credit_gap: {inserted} rows upserted")
    return inserted


def _fetch_credit_to_gdp(conn: sqlite3.Connection) -> int:
    dataset, key = "WS_TC", "Q..P.A.M.770.A"
    log.info(f"Fetching BIS {dataset}/{key}...")
    df = _fetch_bis(dataset, key, start="1990-Q1")
    df.columns = [c.strip().upper() for c in df.columns]

    tp_col  = next((c for c in df.columns if "TIME_PERIOD" in c or c.endswith("PERIOD")), None)
    ov_col  = next((c for c in df.columns if c.startswith("OBS_V")), None)
    cty_col = next((c for c in df.columns if c in ("BORROWERS_CTY", "REF_AREA", "CTY", "COUNTRY")), None)
    if not tp_col or not ov_col:
        raise RuntimeError(f"TIME_PERIOD/OBS_VALUE not found. Columns: {list(df.columns)}")
    if not cty_col:
        raise RuntimeError(f"Country column not found. Columns: {list(df.columns)}")

    df = df[[cty_col, tp_col, ov_col]].rename(
        columns={cty_col: "iso2", tp_col: "TIME_PERIOD", ov_col: "value"}
    )
    df["country_code"]      = df["iso2"].map(_ISO2_TO_ISO3)
    df["period"]            = df["TIME_PERIOD"].apply(_quarter_to_date)
    df["credit_to_gdp_pct"] = pd.to_numeric(df["value"], errors="coerce")
    df = df[
        df["country_code"].notna() & df["period"].notna() & df["credit_to_gdp_pct"].notna()
    ].copy()
    df["period"] = df["period"].astype(str)

    now = datetime.now(timezone.utc).isoformat()
    inserted = 0
    for _, row in df.iterrows():
        conn.execute(
            "INSERT OR REPLACE INTO bis_credit_to_gdp "
            "(period, country_code, country_name, credit_to_gdp_pct, fetched_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (row["period"], row["country_code"], row["country_code"], row["credit_to_gdp_pct"], now),
        )
        inserted += 1
    conn.commit()
    log.info(f"  bis_credit_to_gdp: {inserted} rows across {df['country_code'].nunique()} countries")
    return inserted


def _fetch_usd_credit(conn: sqlite3.Connection) -> int:
    dataset, key = "WS_GLI", "Q.USD"
    log.info(f"Fetching BIS {dataset} (USD credit)...")
    df = _fetch_bis(dataset, key, start="2000-Q1")
    df.columns = [c.strip().upper() for c in df.columns]

    for col in ("UNIT_MEASURE", "L_INSTR", "BORROWERS_CTY"):
        if col not in df.columns:
            raise RuntimeError(f"{col} column missing. Columns: {list(df.columns)}")

    tp_col = next((c for c in df.columns if "TIME_PERIOD" in c or c.endswith("PERIOD")), None)
    ov_col = next((c for c in df.columns if c.startswith("OBS_V")), None)
    if not tp_col or not ov_col:
        raise RuntimeError(f"TIME_PERIOD/OBS_VALUE not found. Columns: {list(df.columns)}")

    df = df[df["UNIT_MEASURE"].str.strip() == "USD"].copy()
    if "BORROWERS_SECTOR" in df.columns:
        df = df[df["BORROWERS_SECTOR"].str.strip() == "N"].copy()
    df = df[df["L_INSTR"].str.strip().isin(_GLI_INSTRUMENT_MAP)].copy()
    df = df[df["BORROWERS_CTY"].str.strip() != "US"].copy()

    df["period"]           = df[tp_col].apply(_quarter_to_date)
    df["borrower_country"] = df["BORROWERS_CTY"].str.strip()
    df["instrument"]       = df["L_INSTR"].str.strip().map(_GLI_INSTRUMENT_MAP)

    unit_mult = pd.to_numeric(
        df.get("UNIT_MULT", pd.Series(6, index=df.index)), errors="coerce"
    ).fillna(6)
    df["usd_credit_bn"] = pd.to_numeric(df[ov_col], errors="coerce") * (10 ** unit_mult) / 1e9
    df = df.dropna(subset=["period", "usd_credit_bn"]).copy()

    df["borrower_iso3"]  = df["borrower_country"].map(lambda c: _ISO2_TO_ISO3.get(c, c))
    df["borrower_label"] = df["borrower_country"].apply(
        lambda c: _GLI_REGION_LABELS.get(c, _ISO2_TO_ISO3.get(c, c))
    )
    df["period"] = df["period"].astype(str)

    now = datetime.now(timezone.utc).isoformat()
    inserted = 0
    for _, row in df.iterrows():
        conn.execute(
            "INSERT OR REPLACE INTO bis_usd_credit "
            "(period, borrower_country, borrower_iso3, borrower_label, instrument, usd_credit_bn, fetched_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (row["period"], row["borrower_country"], row["borrower_iso3"],
             row["borrower_label"], row["instrument"], row["usd_credit_bn"], now),
        )
        inserted += 1
    conn.commit()
    log.info(f"  bis_usd_credit: {inserted} rows, {df['borrower_country'].nunique()} borrowers")
    return inserted


def _fetch_reer(conn: sqlite3.Connection) -> int:
    reer_frames = {}
    for area_code, col in _REER_SERIES.items():
        key = f"M.R.B.{area_code}"
        log.info(f"Fetching BIS WS_EER/{key}...")
        try:
            df = _fetch_bis("WS_EER", key, start="1994-01")
            df.columns = [c.strip().upper() for c in df.columns]

            tp_col = next((c for c in df.columns if "TIME_PERIOD" in c or c.endswith("PERIOD")), None)
            ov_col = next((c for c in df.columns if c.startswith("OBS_V")), None)
            if not tp_col or not ov_col:
                raise RuntimeError(f"TIME_PERIOD/OBS_VALUE not found. Columns: {list(df.columns)}")

            df["period"] = pd.to_datetime(df[tp_col].astype(str) + "-01", errors="coerce")
            df[col]      = pd.to_numeric(df[ov_col], errors="coerce")
            reer_frames[col] = (
                df[["period", col]].dropna(subset=["period", col]).set_index("period")[col]
            )
            log.info(f"  {area_code}: {len(reer_frames[col])} monthly rows")
        except Exception as exc:
            log.warning(f"  WARN WS_EER {area_code}: {exc}")

    if not reer_frames:
        log.warning("No BIS REER data fetched")
        return 0

    df_reer = pd.concat(reer_frames.values(), axis=1).reset_index()
    if "index" in df_reer.columns:
        df_reer.rename(columns={"index": "period"}, inplace=True)
    df_reer.sort_values("period", inplace=True)
    df_reer.reset_index(drop=True, inplace=True)

    for col in [c for c in df_reer.columns if c != "period"]:
        df_reer[f"{col}_yoy_pct"] = df_reer[col].pct_change(12) * 100
        df_reer[f"{col}_mom_pct"] = df_reer[col].pct_change(1) * 100

    now = datetime.now(timezone.utc).isoformat()
    inserted = 0
    for _, col in _REER_SERIES.items():
        if col not in df_reer.columns:
            continue
        yoy = f"{col}_yoy_pct"
        mom = f"{col}_mom_pct"
        currency = col.replace("reer_", "").upper()
        for _, row in df_reer.iterrows():
            val = row.get(col)
            if pd.isna(val):
                continue
            conn.execute(
                "INSERT OR REPLACE INTO bis_reer "
                "(period, currency, reer_idx, yoy_pct, mom_pct, fetched_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    str(row["period"])[:10],
                    currency,
                    float(val),
                    float(row[yoy]) if yoy in df_reer.columns and not pd.isna(row.get(yoy)) else None,
                    float(row[mom]) if mom in df_reer.columns and not pd.isna(row.get(mom)) else None,
                    now,
                ),
            )
            inserted += 1
    conn.commit()
    log.info(f"  bis_reer: {inserted} rows, {len(reer_frames)} currencies")
    return inserted


def _fetch_policy_rates(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT MAX(date) FROM bis_policy_rates").fetchone()
    latest = row[0] if row and row[0] else "1990-01-01"
    start = (pd.Timestamp(latest) - pd.DateOffset(days=30)).strftime("%Y-%m-%d")

    now = datetime.now(timezone.utc).isoformat()
    inserted = 0
    for area_code, series_name in _CBPOL_SERIES.items():
        key = f"D.{area_code}"
        log.info(f"Fetching BIS WS_CBPOL/{key}...")
        try:
            df = _fetch_bis("WS_CBPOL", key, start=start)
            df.columns = [c.strip().upper() for c in df.columns]

            tp_col = next((c for c in df.columns if "TIME_PERIOD" in c or c.endswith("PERIOD")), None)
            ov_col = next((c for c in df.columns if c.startswith("OBS_V")), None)
            if not tp_col or not ov_col:
                raise RuntimeError(f"TIME_PERIOD/OBS_VALUE not found. Columns: {list(df.columns)}")

            df["date"]        = pd.to_datetime(df[tp_col], errors="coerce")
            df["policy_rate"] = pd.to_numeric(df[ov_col], errors="coerce")
            df = df.dropna(subset=["date", "policy_rate"])
            df["date"] = df["date"].dt.strftime("%Y-%m-%d")

            for _, r in df.iterrows():
                conn.execute(
                    "INSERT OR REPLACE INTO bis_policy_rates "
                    "(date, country_code, series_name, policy_rate, fetched_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (r["date"], area_code, series_name, r["policy_rate"], now),
                )
                inserted += 1
            conn.commit()
            log.info(f"  {area_code}: {len(df)} daily rows")
        except Exception as exc:
            log.warning(f"  WARN WS_CBPOL {area_code}: {exc}")

    log.info(f"  bis_policy_rates: {inserted} rows total")
    return inserted


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_all_bis(conn: sqlite3.Connection | None = None) -> tuple[int, int]:
    """Fetch all BIS datasets and store to DB. Returns (ok_count, fail_count)."""
    own_conn = conn is None
    if own_conn:
        conn = get_bis_db()

    ok, fail = 0, 0
    for fn in (_fetch_credit_gap, _fetch_credit_to_gdp, _fetch_usd_credit, _fetch_reer, _fetch_policy_rates):
        try:
            fn(conn)
            ok += 1
        except Exception as exc:
            log.warning(f"  {fn.__name__} failed: {exc}")
            fail += 1

    if own_conn:
        conn.close()

    return ok, fail


def get_bis_snapshots(conn: sqlite3.Connection) -> list[dict]:
    """Return latest values from each BIS table for dashboard display."""
    snapshots = []

    row = conn.execute(
        "SELECT period, credit_to_gdp_gap_pct FROM bis_credit_gap ORDER BY period DESC LIMIT 1"
    ).fetchone()
    if row:
        snapshots.append({
            "indicator": "BIS_credit_gap_DE",
            "category": "credit",
            "latest_value": row[1],
            "data_date": row[0],
            "source": "bis",
        })

    cur = conn.execute("""
        SELECT country_code, period, credit_to_gdp_pct FROM bis_credit_to_gdp
        WHERE (country_code, period) IN (
            SELECT country_code, MAX(period) FROM bis_credit_to_gdp GROUP BY country_code
        )
        ORDER BY country_code
    """)
    for r in cur.fetchall():
        snapshots.append({
            "indicator": f"BIS_credit_gdp_{r[0]}",
            "category": "credit",
            "latest_value": r[2],
            "data_date": r[1],
            "source": "bis",
        })

    cur = conn.execute("""
        SELECT currency, period, reer_idx, yoy_pct FROM bis_reer
        WHERE (currency, period) IN (
            SELECT currency, MAX(period) FROM bis_reer GROUP BY currency
        )
        ORDER BY currency
    """)
    for r in cur.fetchall():
        snapshots.append({
            "indicator": f"BIS_reer_{r[0]}",
            "category": "exchange_rate",
            "latest_value": r[2],
            "change": r[3],
            "data_date": r[1],
            "source": "bis",
        })

    cur = conn.execute("""
        SELECT series_name, date, policy_rate FROM bis_policy_rates
        WHERE (country_code, date) IN (
            SELECT country_code, MAX(date) FROM bis_policy_rates GROUP BY country_code
        )
        ORDER BY series_name
    """)
    for r in cur.fetchall():
        snapshots.append({
            "indicator": f"BIS_rate_{r[0]}",
            "category": "policy_rate",
            "latest_value": r[2],
            "data_date": r[1],
            "source": "bis",
        })

    return snapshots


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Fetch BIS statistics")
    parser.add_argument("--show", action="store_true", help="Show latest stored snapshots")
    parser.add_argument("--json", action="store_true", dest="as_json", help="Output as JSON")
    args = parser.parse_args()

    conn = get_bis_db()

    if args.show:
        snapshots = get_bis_snapshots(conn)
        if args.as_json:
            print(json.dumps(snapshots, ensure_ascii=False, indent=2))
        else:
            print(f"{'Indicator':<45} {'Value':>10} {'Date':<12} {'Category':<15}")
            print("-" * 85)
            for s in snapshots:
                print(f"{s['indicator']:<45} {s['latest_value']:>10.4f} {s['data_date']:<12} {s['category']:<15}")
            print(f"\nTotal: {len(snapshots)} snapshots")
        conn.close()
        return

    log.info("Fetching BIS statistics...")
    ok, fail = fetch_all_bis(conn)
    log.info(f"Done: {ok} datasets OK, {fail} failed")
    conn.close()


if __name__ == "__main__":
    main()
