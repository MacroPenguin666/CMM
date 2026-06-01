"""
ECB data fetcher — bank lending, balance sheets, yield curve, interest rates, systemic stress.

Fetches from the ECB SDMX REST API (no auth required):
  BLS  : Bank Lending Survey (quarterly, Germany)
  BSI  : MFI Balance Sheet Items — loans & deposits (monthly, Germany)
  YC   : Euro area yield curve, par yields (daily → monthly)
  MIR  : MFI Interest Rates — NFC lending rates (monthly)
  EST  : €STR overnight rate (daily)
  CISS : Composite Indicator of Systemic Stress (daily)

Data stored in data/feeds.db.

Usage:
    python ecb.py          # fetch all
    python ecb.py --show   # print latest snapshots
    python ecb.py --json   # output as JSON
"""

import argparse
import io
import json
import logging
import sqlite3
from datetime import datetime, timezone

import pandas as pd
import requests

from policy_monitor.config import ECB_BASE_URL as ECB_BASE
from policy_monitor.storage import DB_DIR, DB_PATH, upsert_df, get_latest_date, get_latest_quarter

log = logging.getLogger("ecb")

ECB_SCHEMA = """
CREATE TABLE IF NOT EXISTS ecb_bls (
    period                    TEXT PRIMARY KEY,
    credit_standards_net_pct  REAL,
    loan_demand_net_pct       REAL,
    fetched_at                TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ecb_bsi (
    period                    TEXT PRIMARY KEY,
    loans_nfc_eur_mn          REAL,
    loans_hh_eur_mn           REAL,
    deposits_nfc_eur_mn       REAL,
    deposits_hh_eur_mn        REAL,
    loans_total_eur_mn        REAL,
    deposits_total_eur_mn     REAL,
    loan_growth_yoy_pct       REAL,
    deposit_growth_yoy_pct    REAL,
    fetched_at                TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ecb_yield_curve (
    period      TEXT PRIMARY KEY,
    ea_2y       REAL,
    ea_5y       REAL,
    ea_10y      REAL,
    ea_20y      REAL,
    ea_30y      REAL,
    slope_2s10s REAL,
    fetched_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ecb_mir (
    period                    TEXT PRIMARY KEY,
    nfc_loan_rate_new_pct     REAL,
    nfc_loan_rate_new_pct_de  REAL,
    fetched_at                TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ecb_estr (
    date       TEXT PRIMARY KEY,
    estr_pct   REAL,
    fetched_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ecb_ciss (
    date       TEXT PRIMARY KEY,
    ciss       REAL,
    fetched_at TEXT NOT NULL
);
"""

BLS_SERIES = {
    "BLS.Q.DE.ALL.O.E.Z.B3.ST.S.FNET": "credit_standards_net_pct",
    "BLS.Q.DE.ALL.O.E.Z.F3.ZZ.D.FNET": "loan_demand_net_pct",
}

BSI_SERIES = {
    "BSI.M.DE.N.A.A20.A.1.U2.2240.Z01.E": "loans_nfc_eur_mn",
    "BSI.M.DE.N.A.A20.A.1.U2.2250.Z01.E": "loans_hh_eur_mn",
    "BSI.M.DE.N.A.L20.A.1.U2.2240.Z01.E": "deposits_nfc_eur_mn",
    "BSI.M.DE.N.A.L20.A.1.U2.2250.Z01.E": "deposits_hh_eur_mn",
}

_YC_TENORS = {
    "SR_2Y":  "ea_2y",
    "SR_5Y":  "ea_5y",
    "SR_10Y": "ea_10y",
    "SR_20Y": "ea_20y",
    "SR_30Y": "ea_30y",
}

_MIR_SERIES = {
    "MIR.M.U2.B.A2I.AM.R.A.2240.EUR.N": "nfc_loan_rate_new_pct",
    "MIR.M.DE.B.A2I.AM.R.A.2240.EUR.N": "nfc_loan_rate_new_pct_de",
}


def get_ecb_db() -> sqlite3.Connection:
    DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.executescript(ECB_SCHEMA)
    return conn


# ---------------------------------------------------------------------------
# Core fetch
# ---------------------------------------------------------------------------

def _fetch_ecb(series_key: str, start: str, params: dict | None = None) -> pd.DataFrame:
    dataflow = series_key.split(".")[0]
    key = series_key[len(dataflow) + 1:]
    url = f"{ECB_BASE}/{dataflow}/{key}"
    p = {"format": "csvdata", "startPeriod": start}
    if params:
        p.update(params)
    resp = requests.get(url, params=p, headers={"Accept": "text/csv"}, timeout=60)
    if not resp.ok:
        raise RuntimeError(f"ECB API error {resp.status_code}: {resp.text[:300]}")
    df = pd.read_csv(io.StringIO(resp.text))
    df.columns = [c.strip().upper() for c in df.columns]
    if "TIME_PERIOD" not in df.columns or "OBS_VALUE" not in df.columns:
        raise RuntimeError(f"Unexpected ECB columns: {list(df.columns[:6])}")
    return df[["TIME_PERIOD", "OBS_VALUE"]].rename(columns={"OBS_VALUE": "value"})


def _quarter_to_date(s: str) -> pd.Timestamp:
    year, q = str(s).split("-Q")
    month = (int(q) - 1) * 3 + 1
    return pd.Timestamp(f"{year}-{month:02d}-01")


def _month_to_date(s: str) -> pd.Timestamp:
    return pd.to_datetime(str(s) + "-01")


# ---------------------------------------------------------------------------
# Dataset fetchers
# ---------------------------------------------------------------------------

def _fetch_bls(conn: sqlite3.Connection) -> int:
    now = datetime.now(timezone.utc).isoformat()
    start = get_latest_quarter(conn, "ecb_bls", default="2003-Q1")
    log.info(f"Fetching ECB BLS from {start}...")
    dfs = {}
    for series_key, col in BLS_SERIES.items():
        try:
            df = _fetch_ecb(series_key, start=start)
            df["period"] = df["TIME_PERIOD"].apply(_quarter_to_date)
            df["value"]  = pd.to_numeric(df["value"], errors="coerce")
            dfs[col] = df[["period", "value"]].rename(columns={"value": col})
        except Exception as exc:
            log.warning(f"  BLS {series_key}: {exc}")

    if len(dfs) < len(BLS_SERIES):
        log.warning("  Not all BLS series fetched — ecb_bls not updated")
        return 0

    df_bls = (dfs["credit_standards_net_pct"]
              .merge(dfs["loan_demand_net_pct"], on="period", how="outer")
              .dropna(subset=["credit_standards_net_pct", "loan_demand_net_pct"], how="all")
              .sort_values("period").reset_index(drop=True))
    df_bls["period"]     = df_bls["period"].astype(str)
    df_bls["fetched_at"] = now
    n = upsert_df(conn, df_bls, "ecb_bls", ["period"])
    log.info(f"  ecb_bls: {n} rows upserted")
    return n


def _fetch_bsi(conn: sqlite3.Connection) -> int:
    now = datetime.now(timezone.utc).isoformat()
    log.info("Fetching ECB BSI...")
    frames = {}
    for series_key, col in BSI_SERIES.items():
        try:
            df = _fetch_ecb(series_key, start="2010-01")
            df["period"] = df["TIME_PERIOD"].apply(_month_to_date)
            df["value"]  = pd.to_numeric(df["value"], errors="coerce")
            frames[col]  = df[["period", "value"]].rename(columns={"value": col}).set_index("period")
            log.info(f"  {col}: {len(frames[col])} rows")
        except Exception as exc:
            log.warning(f"  BSI {series_key}: {exc}")

    if not frames:
        log.warning("  No ECB BSI data — skipping")
        return 0

    bsi = list(frames.values())[0].join(list(frames.values())[1:], how="outer")

    loan_cols = [c for c in ["loans_nfc_eur_mn", "loans_hh_eur_mn"] if c in bsi.columns]
    dep_cols  = [c for c in ["deposits_nfc_eur_mn", "deposits_hh_eur_mn"] if c in bsi.columns]
    if loan_cols:
        bsi["loans_total_eur_mn"] = bsi[loan_cols].sum(axis=1, min_count=1)
    if dep_cols:
        bsi["deposits_total_eur_mn"] = bsi[dep_cols].sum(axis=1, min_count=1)
    if "loans_total_eur_mn" in bsi.columns:
        bsi["loan_growth_yoy_pct"] = bsi["loans_total_eur_mn"].pct_change(12) * 100
    if "deposits_total_eur_mn" in bsi.columns:
        bsi["deposit_growth_yoy_pct"] = bsi["deposits_total_eur_mn"].pct_change(12) * 100

    bsi = bsi.reset_index().sort_values("period")
    bsi["period"]     = bsi["period"].astype(str)
    bsi["fetched_at"] = now
    n = upsert_df(conn, bsi, "ecb_bsi", ["period"])
    log.info(f"  ecb_bsi: {n} rows upserted")
    return n


def _fetch_yield_curve(conn: sqlite3.Connection) -> int:
    now = datetime.now(timezone.utc).isoformat()
    log.info("Fetching ECB yield curve (WS_EER)...")
    frames = {}
    for tenor_code, col in _YC_TENORS.items():
        series_key = f"YC.B.U2.EUR.4F.G_N_A.SV_C_YM.{tenor_code}"
        try:
            df = _fetch_ecb(series_key, start="2004-01-01")
            df["date"]  = pd.to_datetime(df["TIME_PERIOD"])
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            monthly = df.set_index("date")["value"].resample("ME").last().dropna().rename(col)
            frames[col] = monthly
            log.info(f"  {tenor_code}: {len(monthly)} monthly rows")
        except Exception as exc:
            log.warning(f"  YC {tenor_code}: {exc}")

    if not frames:
        log.warning("  No ECB yield curve data — skipping")
        return 0

    df_yc = pd.concat(frames.values(), axis=1).reset_index()
    df_yc.rename(columns={"date": "period"}, inplace=True)
    if "ea_2y" in df_yc.columns and "ea_10y" in df_yc.columns:
        df_yc["slope_2s10s"] = df_yc["ea_10y"] - df_yc["ea_2y"]
    df_yc["period"]     = df_yc["period"].astype(str)
    df_yc["fetched_at"] = now
    n = upsert_df(conn, df_yc, "ecb_yield_curve", ["period"])
    log.info(f"  ecb_yield_curve: {n} rows upserted")
    return n


def _fetch_mir(conn: sqlite3.Connection) -> int:
    now = datetime.now(timezone.utc).isoformat()
    log.info("Fetching ECB MIR...")
    frames = {}
    for series_key, col in _MIR_SERIES.items():
        try:
            df = _fetch_ecb(series_key, start="2003-01")
            df["period"] = df["TIME_PERIOD"].apply(_month_to_date)
            df["value"]  = pd.to_numeric(df["value"], errors="coerce")
            frames[col]  = df[["period", "value"]].rename(columns={"value": col}).set_index("period")
            log.info(f"  {col}: {len(frames[col])} rows")
        except Exception as exc:
            log.warning(f"  MIR {series_key}: {exc}")

    if not frames:
        log.warning("  No ECB MIR data — skipping")
        return 0

    df_mir = list(frames.values())[0].join(list(frames.values())[1:], how="outer")
    df_mir = df_mir.reset_index().sort_values("period")
    df_mir["period"]     = df_mir["period"].astype(str)
    df_mir["fetched_at"] = now
    n = upsert_df(conn, df_mir, "ecb_mir", ["period"])
    log.info(f"  ecb_mir: {n} rows upserted")
    return n


def _fetch_estr(conn: sqlite3.Connection) -> int:
    now = datetime.now(timezone.utc).isoformat()
    origin = "2019-10-02"
    latest = get_latest_date(conn, "ecb_estr", date_col="date", default=origin)
    start  = (pd.Timestamp(latest) - pd.offsets.BDay(5)).strftime("%Y-%m-%d")
    log.info(f"Fetching ECB €STR from {start}...")
    df = _fetch_ecb("EST.B.EU000A2X2A25.WT", start=start)
    df["date"]     = pd.to_datetime(df["TIME_PERIOD"])
    df["estr_pct"] = pd.to_numeric(df["value"], errors="coerce")
    df_out = df[["date", "estr_pct"]].dropna(subset=["estr_pct"]).sort_values("date").reset_index(drop=True)
    df_out["date"]       = df_out["date"].astype(str)
    df_out["fetched_at"] = now
    n = upsert_df(conn, df_out, "ecb_estr", ["date"])
    log.info(f"  ecb_estr: {n} rows upserted")
    return n


def _fetch_ciss(conn: sqlite3.Connection) -> int:
    now = datetime.now(timezone.utc).isoformat()
    origin = "1999-01-04"
    latest = get_latest_date(conn, "ecb_ciss", date_col="date", default=origin)
    start  = (pd.Timestamp(latest) - pd.offsets.Day(10)).strftime("%Y-%m-%d")
    log.info(f"Fetching ECB CISS from {start}...")
    df = _fetch_ecb("CISS.D.U2.Z0Z.4F.EC.SS_CI.IDX", start=start)
    df["date"] = pd.to_datetime(df["TIME_PERIOD"])
    df["ciss"] = pd.to_numeric(df["value"], errors="coerce")
    df_out = df[["date", "ciss"]].dropna(subset=["ciss"]).sort_values("date").reset_index(drop=True)
    df_out["date"]       = df_out["date"].astype(str)
    df_out["fetched_at"] = now
    n = upsert_df(conn, df_out, "ecb_ciss", ["date"])
    log.info(f"  ecb_ciss: {n} rows upserted")
    return n


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_all_ecb(conn: sqlite3.Connection | None = None) -> tuple[int, int]:
    """Fetch all ECB datasets. Returns (ok_count, fail_count)."""
    own_conn = conn is None
    if own_conn:
        conn = get_ecb_db()

    ok, fail = 0, 0
    for fn in (_fetch_bls, _fetch_bsi, _fetch_yield_curve, _fetch_mir, _fetch_estr, _fetch_ciss):
        try:
            fn(conn)
            ok += 1
        except Exception as exc:
            log.warning(f"  {fn.__name__} failed: {exc}")
            fail += 1

    if own_conn:
        conn.close()
    return ok, fail


def get_ecb_snapshots(conn: sqlite3.Connection) -> list[dict]:
    snapshots = []

    for table, col, cat in [
        ("ecb_estr",        "estr_pct",                  "policy_rate"),
        ("ecb_bls",         "credit_standards_net_pct",  "credit"),
    ]:
        try:
            row = conn.execute(
                f"SELECT period, {col} FROM {table} ORDER BY period DESC LIMIT 1"
            ).fetchone()
            if row:
                snapshots.append({"indicator": f"ECB_{col}", "category": cat,
                                   "latest_value": row[1], "data_date": row[0], "source": "ecb"})
        except Exception:
            pass

    try:
        cur = conn.execute(
            "SELECT period, ea_2y, ea_10y, slope_2s10s FROM ecb_yield_curve ORDER BY period DESC LIMIT 1"
        )
        row = cur.fetchone()
        if row:
            for col, val in [("ea_2y", row[1]), ("ea_10y", row[2]), ("slope_2s10s", row[3])]:
                if val is not None:
                    snapshots.append({"indicator": f"ECB_{col}", "category": "yield_curve",
                                      "latest_value": val, "data_date": row[0], "source": "ecb"})
    except Exception:
        pass

    return snapshots


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    parser = argparse.ArgumentParser(description="Fetch ECB data")
    parser.add_argument("--show", action="store_true")
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args()

    conn = get_ecb_db()
    if args.show:
        snaps = get_ecb_snapshots(conn)
        if args.as_json:
            print(json.dumps(snaps, ensure_ascii=False, indent=2))
        else:
            for s in snaps:
                print(f"{s['indicator']:<40} {s['latest_value']:>10.4f}  {s['data_date']}")
        conn.close()
        return

    log.info("Fetching ECB data...")
    ok, fail = fetch_all_ecb(conn)
    log.info(f"Done: {ok} datasets OK, {fail} failed")
    conn.close()


if __name__ == "__main__":
    main()
