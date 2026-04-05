"""
Fetch China macroeconomic data from the Global Macro Database.

The GMD (Müller, Xu, Lehbib & Chen 2025) provides 75 annual macro variables
for 243 countries, with historical data back to 1640 and IMF/WB projections
through 2030.  This module fetches China-specific data, stores it in SQLite,
and exposes metadata for the dashboard.

Usage:
    python macro.py                # fetch (skip if version unchanged)
    python macro.py --show         # print latest stored data
    python macro.py --force        # re-download even if version unchanged
"""

import argparse
import logging
import math
import sqlite3
from datetime import datetime
from pathlib import Path

from policy_monitor.storage import DB_DIR, DB_PATH

log = logging.getLogger("macro")

# ---------------------------------------------------------------------------
# DB schema
# ---------------------------------------------------------------------------
MACRO_SCHEMA = """
CREATE TABLE IF NOT EXISTS macro_series (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    variable    TEXT NOT NULL,
    year        INTEGER NOT NULL,
    value       REAL,
    version     TEXT,
    fetched_at  TEXT NOT NULL,
    UNIQUE(variable, year)
);

CREATE TABLE IF NOT EXISTS macro_history (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    variable    TEXT NOT NULL,
    year        INTEGER NOT NULL,
    value       REAL,
    version     TEXT,
    fetched_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_mh_var_yr ON macro_history(variable, year);

CREATE TABLE IF NOT EXISTS macro_versions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    version     TEXT NOT NULL,
    fetched_at  TEXT NOT NULL
);
"""


def get_macro_db() -> sqlite3.Connection:
    DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.executescript(MACRO_SCHEMA)
    return conn


# ---------------------------------------------------------------------------
# Variable metadata — used by dashboard + CLI
# ---------------------------------------------------------------------------
VARIABLE_META = {
    # GDP & Growth
    "rGDP":         {"name": "Real GDP",                  "unit": "LCU millions", "category": "gdp",      "desc": "Real GDP (constant prices, local currency)"},
    "rGDP_pc":      {"name": "Real GDP per capita",       "unit": "LCU",          "category": "gdp",      "desc": "Real GDP per capita"},
    "rGDP_USD":     {"name": "Real GDP (USD)",            "unit": "USD millions",  "category": "gdp",      "desc": "Real GDP in constant USD"},
    "rGDP_pc_USD":  {"name": "Real GDP/cap (USD)",        "unit": "USD",           "category": "gdp",      "desc": "Real GDP per capita in constant USD"},
    "nGDP":         {"name": "Nominal GDP",               "unit": "LCU millions", "category": "gdp",      "desc": "Nominal GDP (current prices, local currency)"},
    "nGDP_USD":     {"name": "Nominal GDP (USD)",         "unit": "USD millions",  "category": "gdp",      "desc": "Nominal GDP in current USD"},
    "deflator":     {"name": "GDP Deflator",              "unit": "index",         "category": "gdp",      "desc": "GDP deflator (price level index)"},
    # Prices & Inflation
    "CPI":          {"name": "CPI",                       "unit": "index (2015=100)", "category": "prices", "desc": "Consumer Price Index (2015=100)"},
    "infl":         {"name": "Inflation Rate",            "unit": "%",             "category": "prices",   "desc": "Annual inflation rate"},
    # Labor & Population
    "pop":          {"name": "Population",                "unit": "millions",      "category": "labor",    "desc": "Total population"},
    "unemp":        {"name": "Unemployment Rate",         "unit": "%",             "category": "labor",    "desc": "Unemployment rate"},
    # Trade & FX
    "exports":      {"name": "Exports",                   "unit": "LCU millions", "category": "trade",    "desc": "Total exports (local currency)"},
    "exports_GDP":  {"name": "Exports (% GDP)",           "unit": "% GDP",         "category": "trade",    "desc": "Exports as share of GDP"},
    "exports_USD":  {"name": "Exports (USD)",             "unit": "USD millions",  "category": "trade",    "desc": "Total exports in USD"},
    "imports":      {"name": "Imports",                   "unit": "LCU millions", "category": "trade",    "desc": "Total imports (local currency)"},
    "imports_GDP":  {"name": "Imports (% GDP)",           "unit": "% GDP",         "category": "trade",    "desc": "Imports as share of GDP"},
    "imports_USD":  {"name": "Imports (USD)",             "unit": "USD millions",  "category": "trade",    "desc": "Total imports in USD"},
    "CA":           {"name": "Current Account",           "unit": "LCU millions", "category": "trade",    "desc": "Current account balance"},
    "CA_GDP":       {"name": "Current Account (% GDP)",   "unit": "% GDP",         "category": "trade",    "desc": "Current account as share of GDP"},
    "CA_USD":       {"name": "Current Account (USD)",     "unit": "USD millions",  "category": "trade",    "desc": "Current account balance in USD"},
    "USDfx":        {"name": "USD Exchange Rate",         "unit": "LCU per USD",   "category": "trade",    "desc": "Local currency units per USD"},
    "REER":         {"name": "Real Effective Exch Rate",  "unit": "index",         "category": "trade",    "desc": "Real effective exchange rate index"},
    # Fiscal — general government
    "govexp":       {"name": "Gov Expenditure",           "unit": "LCU millions", "category": "fiscal",   "desc": "Government expenditure (local currency)"},
    "govexp_GDP":   {"name": "Gov Expenditure (% GDP)",   "unit": "% GDP",         "category": "fiscal",   "desc": "Government expenditure as share of GDP"},
    "govrev":       {"name": "Gov Revenue",               "unit": "LCU millions", "category": "fiscal",   "desc": "Government revenue (local currency)"},
    "govrev_GDP":   {"name": "Gov Revenue (% GDP)",       "unit": "% GDP",         "category": "fiscal",   "desc": "Government revenue as share of GDP"},
    "govtax":       {"name": "Gov Tax Revenue",           "unit": "LCU millions", "category": "fiscal",   "desc": "Government tax revenue (local currency)"},
    "govtax_GDP":   {"name": "Gov Tax Revenue (% GDP)",   "unit": "% GDP",         "category": "fiscal",   "desc": "Government tax revenue as share of GDP"},
    "govdef":       {"name": "Gov Deficit",               "unit": "LCU millions", "category": "fiscal",   "desc": "Government deficit (local currency)"},
    "govdef_GDP":   {"name": "Gov Deficit (% GDP)",       "unit": "% GDP",         "category": "fiscal",   "desc": "Government deficit as share of GDP"},
    "govdebt":      {"name": "Gov Debt",                  "unit": "LCU millions", "category": "fiscal",   "desc": "Government debt (local currency)"},
    "govdebt_GDP":  {"name": "Gov Debt (% GDP)",          "unit": "% GDP",         "category": "fiscal",   "desc": "Government debt as share of GDP"},
    # Fiscal — general government (consolidated)
    "gen_govexp":     {"name": "Gen Gov Expenditure",       "unit": "LCU millions", "category": "fiscal",   "desc": "General government expenditure"},
    "gen_govexp_GDP": {"name": "Gen Gov Expenditure (% GDP)","unit": "% GDP",        "category": "fiscal",   "desc": "General government expenditure as share of GDP"},
    "gen_govrev":     {"name": "Gen Gov Revenue",           "unit": "LCU millions", "category": "fiscal",   "desc": "General government revenue"},
    "gen_govrev_GDP": {"name": "Gen Gov Revenue (% GDP)",   "unit": "% GDP",         "category": "fiscal",   "desc": "General government revenue as share of GDP"},
    "gen_govtax":     {"name": "Gen Gov Tax Revenue",       "unit": "LCU millions", "category": "fiscal",   "desc": "General government tax revenue"},
    "gen_govtax_GDP": {"name": "Gen Gov Tax (% GDP)",       "unit": "% GDP",         "category": "fiscal",   "desc": "General government tax as share of GDP"},
    "gen_govdef":     {"name": "Gen Gov Deficit",           "unit": "LCU millions", "category": "fiscal",   "desc": "General government deficit"},
    "gen_govdef_GDP": {"name": "Gen Gov Deficit (% GDP)",   "unit": "% GDP",         "category": "fiscal",   "desc": "General government deficit as share of GDP"},
    "gen_govdebt":    {"name": "Gen Gov Debt",              "unit": "LCU millions", "category": "fiscal",   "desc": "General government debt"},
    "gen_govdebt_GDP":{"name": "Gen Gov Debt (% GDP)",      "unit": "% GDP",         "category": "fiscal",   "desc": "General government debt as share of GDP"},
    # Fiscal — central government
    "cgovrev":      {"name": "Central Gov Revenue",       "unit": "LCU millions", "category": "fiscal",   "desc": "Central government revenue"},
    "cgovrev_GDP":  {"name": "Central Gov Revenue (% GDP)","unit": "% GDP",        "category": "fiscal",   "desc": "Central government revenue as share of GDP"},
    "cgovtax":      {"name": "Central Gov Tax Revenue",   "unit": "LCU millions", "category": "fiscal",   "desc": "Central government tax revenue"},
    "cgovtax_GDP":  {"name": "Central Gov Tax (% GDP)",   "unit": "% GDP",         "category": "fiscal",   "desc": "Central government tax as share of GDP"},
    "cgovdebt":     {"name": "Central Gov Debt",          "unit": "LCU millions", "category": "fiscal",   "desc": "Central government debt"},
    "cgovdebt_GDP": {"name": "Central Gov Debt (% GDP)",  "unit": "% GDP",         "category": "fiscal",   "desc": "Central government debt as share of GDP"},
    # Monetary
    "cbrate":       {"name": "Central Bank Rate",         "unit": "%",             "category": "monetary", "desc": "Central bank policy rate"},
    "strate":       {"name": "Short-term Rate",           "unit": "%",             "category": "monetary", "desc": "Short-term interest rate"},
    "ltrate":       {"name": "Long-term Rate",            "unit": "%",             "category": "monetary", "desc": "Long-term interest rate"},
    "M0":           {"name": "M0 (Monetary Base)",        "unit": "LCU millions", "category": "monetary", "desc": "Monetary base"},
    "M1":           {"name": "M1 (Narrow Money)",         "unit": "LCU millions", "category": "monetary", "desc": "Narrow money supply"},
    "M2":           {"name": "M2 (Broad Money)",          "unit": "LCU millions", "category": "monetary", "desc": "Broad money supply"},
    "M3":           {"name": "M3 (Broadest Money)",       "unit": "LCU millions", "category": "monetary", "desc": "Broadest money supply"},
    # Housing
    "HPI":          {"name": "House Price Index",         "unit": "index",         "category": "housing",  "desc": "House Price Index"},
    # Consumption & Investment
    "cons":         {"name": "Consumption",               "unit": "LCU millions", "category": "demand",   "desc": "Total consumption (local currency)"},
    "cons_GDP":     {"name": "Consumption (% GDP)",       "unit": "% GDP",         "category": "demand",   "desc": "Total consumption as share of GDP"},
    "cons_USD":     {"name": "Consumption (USD)",         "unit": "USD millions",  "category": "demand",   "desc": "Total consumption in USD"},
    "hcons":        {"name": "Household Consumption",     "unit": "LCU millions", "category": "demand",   "desc": "Household consumption (local currency)"},
    "hcons_GDP":    {"name": "Household Cons (% GDP)",    "unit": "% GDP",         "category": "demand",   "desc": "Household consumption as share of GDP"},
    "hcons_USD":    {"name": "Household Cons (USD)",      "unit": "USD millions",  "category": "demand",   "desc": "Household consumption in USD"},
    "gcons":        {"name": "Gov Consumption",           "unit": "LCU millions", "category": "demand",   "desc": "Government consumption (local currency)"},
    "gcons_GDP":    {"name": "Gov Consumption (% GDP)",   "unit": "% GDP",         "category": "demand",   "desc": "Government consumption as share of GDP"},
    "gcons_USD":    {"name": "Gov Consumption (USD)",     "unit": "USD millions",  "category": "demand",   "desc": "Government consumption in USD"},
    "inv":          {"name": "Investment",                "unit": "LCU millions", "category": "demand",   "desc": "Total investment (local currency)"},
    "inv_GDP":      {"name": "Investment (% GDP)",        "unit": "% GDP",         "category": "demand",   "desc": "Total investment as share of GDP"},
    "inv_USD":      {"name": "Investment (USD)",          "unit": "USD millions",  "category": "demand",   "desc": "Total investment in USD"},
    "finv":         {"name": "Fixed Investment",          "unit": "LCU millions", "category": "demand",   "desc": "Fixed investment (local currency)"},
    "finv_GDP":     {"name": "Fixed Investment (% GDP)",  "unit": "% GDP",         "category": "demand",   "desc": "Fixed investment as share of GDP"},
    "finv_USD":     {"name": "Fixed Investment (USD)",    "unit": "USD millions",  "category": "demand",   "desc": "Fixed investment in USD"},
    # Crisis indicators
    "SovDebtCrisis":   {"name": "Sovereign Debt Crisis",  "unit": "binary",  "category": "crisis",   "desc": "Sovereign debt crisis indicator (0/1)"},
    "CurrencyCrisis":  {"name": "Currency Crisis",        "unit": "binary",  "category": "crisis",   "desc": "Currency crisis indicator (0/1)"},
    "BankingCrisis":   {"name": "Banking Crisis",         "unit": "binary",  "category": "crisis",   "desc": "Banking crisis indicator (0/1)"},
}

CATEGORIES = {
    "gdp":      "GDP & Growth",
    "prices":   "Prices & Inflation",
    "labor":    "Labor & Population",
    "trade":    "Trade & FX",
    "fiscal":   "Fiscal",
    "monetary": "Monetary",
    "housing":  "Housing",
    "demand":   "Consumption & Investment",
    "crisis":   "Crisis Events",
}


# ---------------------------------------------------------------------------
# Storage helpers
# ---------------------------------------------------------------------------

def get_stored_version(conn: sqlite3.Connection) -> str | None:
    cur = conn.execute("SELECT version FROM macro_versions ORDER BY id DESC LIMIT 1")
    row = cur.fetchone()
    return row[0] if row else None


def store_macro_series(conn: sqlite3.Connection, rows: list[tuple], version: str):
    """Bulk upsert macro data. rows = [(variable, year, value), ...]"""
    now = datetime.now().astimezone().isoformat()
    for variable, year, value in rows:
        # Live table — upsert for quick lookups
        conn.execute(
            "INSERT OR REPLACE INTO macro_series (variable, year, value, version, fetched_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (variable, year, value, version, now),
        )
        # History table — always append (preserves revisions)
        conn.execute(
            "INSERT INTO macro_history (variable, year, value, version, fetched_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (variable, year, value, version, now),
        )
    conn.execute("INSERT INTO macro_versions (version, fetched_at) VALUES (?, ?)", (version, now))
    conn.commit()


def get_macro_series(conn: sqlite3.Connection, variable: str,
                     start_year: int | None = None, end_year: int | None = None) -> list[dict]:
    query = "SELECT year, value FROM macro_series WHERE variable = ?"
    params: list = [variable]
    if start_year:
        query += " AND year >= ?"
        params.append(start_year)
    if end_year:
        query += " AND year <= ?"
        params.append(end_year)
    query += " ORDER BY year"
    return [{"year": r[0], "value": r[1]} for r in conn.execute(query, params).fetchall()]


def get_macro_variables(conn: sqlite3.Connection) -> list[dict]:
    """List stored variables with year range and latest value."""
    cur = conn.execute(
        "SELECT variable, MIN(year) as min_yr, MAX(year) as max_yr, COUNT(*) as cnt "
        "FROM macro_series WHERE value IS NOT NULL GROUP BY variable ORDER BY variable"
    )
    result = []
    for var, min_yr, max_yr, cnt in cur.fetchall():
        meta = VARIABLE_META.get(var, {"name": var, "unit": "", "category": "other", "desc": ""})
        result.append({
            "variable": var,
            "name": meta["name"],
            "unit": meta["unit"],
            "category": meta["category"],
            "description": meta["desc"],
            "min_year": min_yr,
            "max_year": max_yr,
            "count": cnt,
        })
    return result


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def fetch_china_macro(force: bool = False) -> dict:
    """Fetch China macro data from GMD. Returns summary dict."""
    from global_macro_data import gmd, get_current_version

    current_version = get_current_version()
    conn = get_macro_db()
    stored_version = get_stored_version(conn)

    if stored_version == current_version and not force:
        log.info(f"GMD version unchanged ({current_version}), skipping download")
        conn.close()
        return {"status": "skipped", "version": current_version, "reason": "version unchanged"}

    log.info(f"Downloading GMD v{current_version} for China (stored: {stored_version or 'none'})...")
    df = gmd(country="CHN", fast="yes")

    # Extract all numeric columns
    id_cols = {"countryname", "ISO3", "id", "year", "income_group"}
    var_cols = [c for c in df.columns if c not in id_cols]

    rows = []
    for _, r in df.iterrows():
        year = int(r["year"])
        for col in var_cols:
            val = r[col]
            if val is not None and not (isinstance(val, float) and math.isnan(val)):
                rows.append((col, year, float(val)))

    store_macro_series(conn, rows, current_version)
    total = conn.execute("SELECT COUNT(*) FROM macro_series").fetchone()[0]
    conn.close()

    log.info(f"Stored {len(rows)} data points ({len(var_cols)} variables), {total} total in DB")
    return {"status": "updated", "version": current_version, "new_points": len(rows), "total": total}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Fetch China macro data from Global Macro Database")
    parser.add_argument("--show", action="store_true", help="Show stored macro variables")
    parser.add_argument("--force", action="store_true", help="Re-download even if version unchanged")
    parser.add_argument("--series", type=str, help="Show time series for a variable (e.g. rGDP)")
    args = parser.parse_args()

    conn = get_macro_db()

    if args.show:
        version = get_stored_version(conn)
        print(f"GMD version: {version or 'not yet fetched'}")
        print()
        variables = get_macro_variables(conn)
        if not variables:
            print("No data stored yet. Run: python macro.py")
            conn.close()
            return
        print(f"{'Variable':<20} {'Name':<28} {'Category':<12} {'Years':<14} {'Points':>6}")
        print("-" * 86)
        for v in variables:
            print(f"{v['variable']:<20} {v['name']:<28} {v['category']:<12} "
                  f"{v['min_year']}-{v['max_year']:<8} {v['count']:>6}")
        conn.close()
        return

    if args.series:
        data = get_macro_series(conn, args.series)
        if not data:
            print(f"No data for '{args.series}'. Run --show to see available variables.")
            conn.close()
            return
        meta = VARIABLE_META.get(args.series, {})
        print(f"{meta.get('name', args.series)} ({meta.get('unit', '')})")
        print("-" * 40)
        for d in data:
            print(f"  {d['year']}  {d['value']:>14.2f}")
        conn.close()
        return

    result = fetch_china_macro(force=args.force)
    print(f"Result: {result['status']} (version {result['version']})")
    conn.close()


if __name__ == "__main__":
    main()
