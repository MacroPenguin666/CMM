"""
Global macro fetcher — IMF WEO + World Bank indicators for all countries.

Fetches from IMF DataMapper and World Bank APIs (no auth required).
Data stored in data/feeds.db in the `global_macro` table.

Usage:
    python global_macro.py          # fetch all
    python global_macro.py --show   # print latest snapshot
"""

import argparse
import logging
import sqlite3
import time
from datetime import datetime, timezone

import pandas as pd
import requests

from policy_monitor.config import IMF_BASE_URL, WB_BASE_URL
from policy_monitor.storage import DB_DIR, DB_PATH, upsert_df

log = logging.getLogger("global_macro")

START_YEAR = 2000

IMF_INDICATORS = {
    "NGDP_RPCH":   "gdp_growth_pct",
    "PCPIPCH":     "inflation_pct",
    "LUR":         "unemployment_rate",
    "BCA_NGDPD":   "current_account_pct_gdp",
    "GGXCNL_NGDP": "fiscal_balance_pct_gdp",
    "GGXWDG_NGDP": "govt_debt_pct_gdp",
    "NGDPDPC":     "gdp_per_capita_usd",
    "NGDPD":       "gdp_usd_bn",
}

WB_INDICATORS = {
    "FI.RES.TOTL.CD":    "fx_reserves_incl_gold_usd",
    "FI.RES.TOTL.MO":    "fx_reserves_months_imports",
    "FR.INR.LEND":       "lending_rate_pct",
    "NE.EXP.GNFS.CD":    "exports_usd",
    "NE.IMP.GNFS.CD":    "imports_usd",
    "NE.EXP.GNFS.ZS":    "exports_pct_gdp",
    "NE.IMP.GNFS.ZS":    "imports_pct_gdp",
    "BX.GSR.MRCH.CD":    "goods_exports_usd",
    "BX.GSR.NFSV.CD":    "services_exports_usd",
    "BM.GSR.MRCH.CD":    "goods_imports_usd",
    "BM.GSR.NFSV.CD":    "services_imports_usd",
    "DT.DOD.DECT.CD":    "external_debt_usd",
    "FB.AST.NPER.ZS":    "npl_ratio_pct",
    "FB.BNK.CAPA.ZS":    "bank_capital_to_assets_pct",
    "FS.AST.DOMO.GD.ZS": "credit_to_gdp_pct",
    "GC.XPN.INTP.RV.ZS": "govt_debt_service_pct_revenue",
}


def get_global_macro_db() -> sqlite3.Connection:
    DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    return conn


def _fetch_imf_countries() -> dict[str, str]:
    resp = requests.get(f"{IMF_BASE_URL}/countries", timeout=30)
    resp.raise_for_status()
    return {
        code: info.get("label", code)
        for code, info in resp.json().get("countries", {}).items()
    }


def _fetch_imf_indicator(indicator_id: str, col_name: str) -> pd.DataFrame:
    resp = requests.get(f"{IMF_BASE_URL}/{indicator_id}", timeout=30)
    resp.raise_for_status()
    data = resp.json().get("values", {}).get(indicator_id, {})
    rows = []
    for country_code, year_values in data.items():
        for year_str, raw_value in year_values.items():
            try:
                year  = int(year_str)
                value = float(raw_value)
            except (ValueError, TypeError):
                continue
            if year >= START_YEAR:
                rows.append({"country_code": country_code, "year": year, col_name: value})
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["country_code", "year", col_name])


def _fetch_wb_indicator(indicator_id: str, col_name: str, retries: int = 3) -> pd.DataFrame:
    rows = []
    page = 1
    while True:
        resp = None
        for attempt in range(retries):
            try:
                resp = requests.get(
                    f"{WB_BASE_URL}/{indicator_id}",
                    params={"format": "json", "per_page": 1000,
                            "date": f"{START_YEAR}:2030", "page": page},
                    timeout=120,
                )
                if resp.status_code in (400, 404):
                    return pd.DataFrame(rows) if rows else pd.DataFrame(
                        columns=["country_code", "year", col_name])
                resp.raise_for_status()
                break
            except requests.exceptions.Timeout:
                if attempt == retries - 1:
                    raise
                wait = 10 * (attempt + 1)
                log.warning(f"    [timeout, retrying in {wait}s ({attempt+2}/{retries})...]")
                time.sleep(wait)

        if resp is None:
            break
        meta, data = resp.json()
        for item in (data or []):
            iso3  = item.get("countryiso3code", "")
            value = item.get("value")
            if not iso3 or value is None:
                continue
            try:
                rows.append({"country_code": iso3, "year": int(item["date"]), col_name: float(value)})
            except (ValueError, TypeError):
                continue
        if page >= meta.get("pages", 1):
            break
        page += 1
        time.sleep(0.5)

    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["country_code", "year", col_name])


def fetch_all_global_macro(conn: sqlite3.Connection | None = None) -> tuple[int, int]:
    """Fetch IMF + World Bank macro data for all countries. Returns (ok, fail)."""
    own_conn = conn is None
    if own_conn:
        conn = get_global_macro_db()

    log.info("Fetching IMF country names...")
    try:
        country_names = _fetch_imf_countries()
    except Exception as exc:
        log.warning(f"  Could not fetch IMF country names: {exc}")
        country_names = {}

    frames = []

    log.info("[IMF WEO]")
    ok_imf = fail_imf = 0
    for imf_code, col_name in IMF_INDICATORS.items():
        log.info(f"  {imf_code:20s} -> {col_name}")
        try:
            df = _fetch_imf_indicator(imf_code, col_name)
            if df.empty:
                log.info(f"    [no data]")
                fail_imf += 1
            else:
                frames.append(df.set_index(["country_code", "year"]))
                ok_imf += 1
        except Exception as exc:
            log.warning(f"    [ERROR: {exc}]")
            fail_imf += 1
        time.sleep(0.3)

    log.info("[World Bank]")
    ok_wb = fail_wb = 0
    for wb_code, col_name in WB_INDICATORS.items():
        log.info(f"  {wb_code:20s} -> {col_name}")
        try:
            df = _fetch_wb_indicator(wb_code, col_name)
            if df.empty:
                log.info(f"    [no data]")
                fail_wb += 1
            else:
                frames.append(df.set_index(["country_code", "year"]))
                ok_wb += 1
        except Exception as exc:
            log.warning(f"    [ERROR: {exc}]")
            fail_wb += 1
        time.sleep(0.3)

    if not frames:
        log.warning("No global macro data fetched.")
        if own_conn:
            conn.close()
        return 0, len(IMF_INDICATORS) + len(WB_INDICATORS)

    df_all = frames[0].join(frames[1:], how="outer").reset_index()
    df_all.insert(1, "country_name", df_all["country_code"].map(country_names).fillna(""))

    for col in list(IMF_INDICATORS.values()) + list(WB_INDICATORS.values()):
        if col not in df_all.columns:
            df_all[col] = None

    if "external_debt_usd" in df_all.columns and "gdp_usd_bn" in df_all.columns:
        df_all["external_debt_pct_gdp"] = (
            df_all["external_debt_usd"] / (df_all["gdp_usd_bn"] * 1_000_000_000) * 100
        ).round(2)

    if "goods_exports_usd" in df_all.columns and "services_exports_usd" in df_all.columns:
        exp_total = df_all["goods_exports_usd"] + df_all["services_exports_usd"]
        df_all["goods_exports_pct"]    = (df_all["goods_exports_usd"]    / exp_total * 100).round(1)
        df_all["services_exports_pct"] = (df_all["services_exports_usd"] / exp_total * 100).round(1)

    if "goods_imports_usd" in df_all.columns and "services_imports_usd" in df_all.columns:
        imp_total = df_all["goods_imports_usd"] + df_all["services_imports_usd"]
        df_all["goods_imports_pct"]    = (df_all["goods_imports_usd"]    / imp_total * 100).round(1)
        df_all["services_imports_pct"] = (df_all["services_imports_usd"] / imp_total * 100).round(1)

    df_all["fetched_at"] = datetime.now(timezone.utc).isoformat()
    df_all = df_all.sort_values(["country_code", "year"]).reset_index(drop=True)

    n = upsert_df(conn, df_all, "global_macro", ["country_code", "year"])
    log.info(f"  {len(df_all)} rows saved ({df_all['country_code'].nunique()} countries)")

    if own_conn:
        conn.close()

    total_ok = ok_imf + ok_wb
    total_fail = fail_imf + fail_wb
    return total_ok, total_fail


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    parser = argparse.ArgumentParser(description="Fetch global macro data (IMF + World Bank)")
    parser.add_argument("--show", action="store_true")
    args = parser.parse_args()

    conn = get_global_macro_db()
    if args.show:
        try:
            df = pd.read_sql_query(
                "SELECT country_code, year, gdp_growth_pct, inflation_pct "
                "FROM global_macro ORDER BY country_code, year DESC LIMIT 50", conn)
            print(df.to_string(index=False))
        except Exception as e:
            print(f"No data yet: {e}")
        conn.close()
        return

    log.info("Fetching global macro data...")
    ok, fail = fetch_all_global_macro(conn)
    log.info(f"Done: {ok} indicators OK, {fail} failed")
    conn.close()


if __name__ == "__main__":
    main()
