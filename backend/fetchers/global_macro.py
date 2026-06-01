"""
Global macro fetcher — IMF WEO indicators for all countries.

Fetches from IMF DataMapper API (no auth required).
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

from backend.config import IMF_BASE_URL
from backend.storage import DB_DIR, DB_PATH, upsert_df

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


def fetch_all_global_macro(conn: sqlite3.Connection | None = None) -> tuple[int, int]:
    """Fetch IMF macro data for all countries. Returns (ok, fail)."""
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
    ok = fail = 0
    for imf_code, col_name in IMF_INDICATORS.items():
        log.info(f"  {imf_code:20s} -> {col_name}")
        try:
            df = _fetch_imf_indicator(imf_code, col_name)
            if df.empty:
                log.info("    [no data]")
                fail += 1
            else:
                frames.append(df.set_index(["country_code", "year"]))
                ok += 1
        except Exception as exc:
            log.warning(f"    [ERROR: {exc}]")
            fail += 1
        time.sleep(0.3)

    if not frames:
        log.warning("No global macro data fetched.")
        if own_conn:
            conn.close()
        return 0, len(IMF_INDICATORS)

    df_all = frames[0].join(frames[1:], how="outer").reset_index()
    df_all.insert(1, "country_name", df_all["country_code"].map(country_names).fillna(""))

    for col in IMF_INDICATORS.values():
        if col not in df_all.columns:
            df_all[col] = None

    df_all["fetched_at"] = datetime.now(timezone.utc).isoformat()
    df_all = df_all.sort_values(["country_code", "year"]).reset_index(drop=True)

    upsert_df(conn, df_all, "global_macro", ["country_code", "year"])
    log.info(f"  {len(df_all)} rows saved ({df_all['country_code'].nunique()} countries)")

    if own_conn:
        conn.close()

    return ok, fail


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    parser = argparse.ArgumentParser(description="Fetch global macro data (IMF WEO)")
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
