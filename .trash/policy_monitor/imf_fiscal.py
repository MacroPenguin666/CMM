"""
IMF Fiscal Monitor fetcher — government fiscal indicators for all countries.

Fetches from IMF DataMapper API (no auth required).
Data stored in data/feeds.db in the `imf_fiscal` table.

Usage:
    python imf_fiscal.py          # fetch all
    python imf_fiscal.py --show   # print latest snapshot
"""

import argparse
import logging
import sqlite3
import time
from datetime import datetime, timezone

import pandas as pd
import requests

from policy_monitor.config import IMF_BASE_URL
from policy_monitor.storage import DB_DIR, DB_PATH, upsert_df

log = logging.getLogger("imf_fiscal")

START_YEAR = 2015

IMF_FISCAL_INDICATORS = {
    "rev":                 "revenue_pct_gdp",
    "exp":                 "expenditure_pct_gdp",
    "pb":                  "primary_balance_pct_gdp",
    "GGXWDG_NGDP":         "gross_debt_pct_gdp",
    "GGCBP_G01_PGDP_PT":   "fm_capb_pct_pot_gdp",
    "GGCB_G01_PGDP_PT":    "fm_cab_pct_pot_gdp",
    "GGXCNL_G01_GDP_PT":   "fm_net_lending_borrowing_pct_gdp",
    "GGXONLB_G01_GDP_PT":  "fm_primary_balance_pct_gdp",
    "ie":                  "fm_interest_pct_gdp",
    "G_XWDG_G01_GDP_PT":   "fm_gross_debt_pct_gdp",
    "GGR_G01_GDP_PT":      "fm_revenue_pct_gdp",
    "GGXWDN_G01_GDP_PT":   "fm_net_debt_pct_gdp",
}


def get_imf_fiscal_db() -> sqlite3.Connection:
    DB_DIR.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(str(DB_PATH))


def _fetch_indicator(indicator_id: str, col_name: str, retries: int = 3) -> pd.DataFrame:
    for attempt in range(retries):
        try:
            resp = requests.get(f"{IMF_BASE_URL}/{indicator_id}", timeout=60)
            if resp.status_code in (429, 503):
                wait = 30 * (2 ** attempt)
                log.warning(f"    [{resp.status_code}, retry {attempt + 1}/{retries} in {wait}s]")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            break
        except requests.exceptions.Timeout:
            if attempt == retries - 1:
                log.warning(f"    [timeout after {retries} attempts — skipping {indicator_id}]")
                return pd.DataFrame(columns=["country_code", "year", col_name])
            time.sleep(20 * (attempt + 1))
    else:
        return pd.DataFrame(columns=["country_code", "year", col_name])

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


def fetch_all_imf_fiscal(conn: sqlite3.Connection | None = None) -> tuple[int, int]:
    """Fetch all IMF Fiscal Monitor indicators. Returns (ok, fail)."""
    own_conn = conn is None
    if own_conn:
        conn = get_imf_fiscal_db()

    log.info("Fetching IMF Fiscal Monitor indicators...")
    frames = []
    ok = fail = 0
    for imf_code, col_name in IMF_FISCAL_INDICATORS.items():
        log.info(f"  {imf_code} -> {col_name}")
        try:
            df = _fetch_indicator(imf_code, col_name)
            if df.empty:
                log.info(f"    [no data]")
                fail += 1
            else:
                frames.append(df)
                ok += 1
        except Exception as exc:
            log.warning(f"    [ERROR: {exc}]")
            fail += 1

    if not frames:
        log.warning("  No data fetched.")
        if own_conn:
            conn.close()
        return 0, len(IMF_FISCAL_INDICATORS)

    merged = frames[0]
    for df in frames[1:]:
        merged = merged.merge(df, on=["country_code", "year"], how="outer")

    merged = merged.sort_values(["country_code", "year"]).reset_index(drop=True)
    merged["fetched_at"] = datetime.now(timezone.utc).isoformat()
    upsert_df(conn, merged, "imf_fiscal", ["country_code", "year"])
    log.info(f"  {len(merged)} rows saved ({merged['country_code'].nunique()} countries)")

    if own_conn:
        conn.close()
    return ok, fail


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    parser = argparse.ArgumentParser(description="Fetch IMF Fiscal Monitor indicators")
    parser.add_argument("--show", action="store_true")
    args = parser.parse_args()

    conn = get_imf_fiscal_db()
    if args.show:
        try:
            df = pd.read_sql_query(
                "SELECT country_code, year, revenue_pct_gdp, gross_debt_pct_gdp "
                "FROM imf_fiscal ORDER BY country_code, year DESC LIMIT 50", conn)
            print(df.to_string(index=False))
        except Exception as e:
            print(f"No data yet: {e}")
        conn.close()
        return

    ok, fail = fetch_all_imf_fiscal(conn)
    log.info(f"Done: {ok} indicators OK, {fail} failed")
    conn.close()


if __name__ == "__main__":
    main()
