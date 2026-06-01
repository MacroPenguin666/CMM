"""
Destatis GENESIS table fetcher — all datasets in one module.

Tables covered:
  13211-0002  labor_market
  42153-0002  production_index
  42151-0002  manufacturing_orders_idx
  42152-0002  manufacturing_turnover_idx
  42155-0004  factory_orders
  48311-0001  production_index_total
  51000-0002  trade_simple (unadjusted)
  51000-0020  trade_simple (seasonally adjusted, merged)
  51000-0003  trade_by_country
  51000-0006  trade_by_commodity
  52311-0011  startups_by_industry
  52311-0012  startups_by_type_legal
  52411-0010  insolvency
  61111-0002  cpi_data
  81000-0001  germany_gdp_quarterly (unadjusted)
  81000-0002  germany_gdp_quarterly (seasonally adjusted)
  61411-0002  import_price_index
  61421-0002  export_price_index

Usage:
    python destatis.py          # fetch all
    python destatis.py --show   # list tables and row counts
"""

import argparse
import logging
import re
import sqlite3
from datetime import datetime, timezone

import pandas as pd

from policy_monitor.destatis_utils import (
    DesatisNoDataError,
    _find_time_var_col,
    build_monthly_period,
    build_quarterly_period,
    fetch_raw_datencsv,
    fetch_table,
    fetch_table_meta,
    get_start_year,
)
from policy_monitor.storage import DB_DIR, DB_PATH, upsert_df

log = logging.getLogger("destatis")

_ADJ_CODE = "X13JDKSB"

_ALL_TABLES = [
    "13211-0002",
    "42153-0002",
    "42151-0002",
    "42152-0002",
    "42155-0004",
    "48311-0001",
    "51000-0002",
    "51000-0003",
    "51000-0006",
    "51000-0020",
    "52311-0011",
    "52311-0012",
    "52411-0010",
    "61111-0002",
    "81000-0001",
    "81000-0002",
    "61411-0002",
    "61421-0002",
]


# ── Trade-by-country helpers ──────────────────────────────────────────────────

_DESTATIS_TO_WB_NAME = {
    "china, people's republic of":            "china",
    "republic of korea":                      "korea, rep.",
    "korea, republic of":                     "korea, rep.",
    "united states of america":               "united states",
    "viet nam":                               "vietnam",
    "türkiye":                                "turkiye",
    "czechia":                                "czechia",
    "russian federation":                     "russian federation",
    "taiwan, province of china":              "taiwan, china",
    "iran, islamic republic of":              "iran, islamic rep.",
    "venezuela, bolivarian republic of":      "venezuela, rb",
    "bolivarian republic of venezuela":       "venezuela, rb",
    "bolivia, plurinational state of":        "bolivia",
    "tanzania, united republic of":           "tanzania",
    "congo, democratic republic of the":      "congo, dem. rep.",
    "congo, democratic republic of":          "congo, dem. rep.",
    "congo, republic of the":                 "congo, rep.",
    "congo":                                  "congo, rep.",
    "côte d'ivoire":                          "cote d'ivoire",
    "lao people's democratic republic":       "lao pdr",
    "moldova, republic of":                   "moldova",
    "north macedonia":                        "north macedonia",
    "eswatini":                               "eswatini",
    "myanmar":                                "myanmar",
    "palestinians territories":               "west bank and gaza",
    "palestinian territories":                "west bank and gaza",
    "state of palestine":                     "west bank and gaza",
    "hong kong":                              "hong kong sar, china",
    "macao":                                  "macao sar, china",
    "macau":                                  "macao sar, china",
    "korea, democratic people's republic of": "korea, dem. people's rep.",
    "bahamas":                                "bahamas, the",
    "gambia":                                 "gambia, the",
    "egypt":                                  "egypt, arab rep.",
    "syria":                                  "syrian arab republic",
    "syrian arab republic":                   "syrian arab republic",
    "yemen":                                  "yemen, rep.",
    "kyrgyzstan":                             "kyrgyz republic",
    "kyrgyz republic":                        "kyrgyz republic",
    "slovakia":                               "slovak republic",
    "micronesia":                             "micronesia, fed. sts.",
    "micronesia, federated states of":        "micronesia, fed. sts.",
    "saint kitts and nevis":                  "st. kitts and nevis",
    "saint lucia":                            "st. lucia",
    "saint vincent and the grenadines":       "st. vincent and the grenadines",
    "são tomé and príncipe":                  "sao tome and principe",
    "sao tome and principe":                  "sao tome and principe",
    "timor-leste":                            "timor-leste",
    "east timor":                             "timor-leste",
    "cabo verde":                             "cabo verde",
    "cape verde":                             "cabo verde",
    "comoros":                                "comoros",
    "comoro islands":                         "comoros",
    "brunei darussalam":                      "brunei darussalam",
    "brunei":                                 "brunei darussalam",
}


def _normalize_destatis_country(label: str) -> str:
    name = re.sub(r"\s*\(.*?\)", "", label).strip().lower()
    return _DESTATIS_TO_WB_NAME.get(name, name)


def _parse_51000_0003(content: str) -> pd.DataFrame:
    NULL_VALS = {"...", ".", "-", "/", "x", ""}

    def _num(s):
        s = s.replace(",", ".")
        return float(s) if s not in NULL_VALS else None

    rows = []
    for line in content.split("\n"):
        fields = [f.strip() for f in line.split(";")]
        if len(fields) < 11:
            continue
        try:
            year = int(fields[0])
        except ValueError:
            continue
        country_label = fields[1]
        if not country_label:
            continue
        try:
            exports = _num(fields[4])
            imports = _num(fields[10])
        except (ValueError, IndexError):
            continue
        if exports is None and imports is None:
            continue
        rows.append({
            "year":             year,
            "country_label":    country_label,
            "exports_eur_1000": exports,
            "imports_eur_1000": imports,
        })
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ── Trade-by-commodity helpers ────────────────────────────────────────────────

_NULL_VALS = {"...", ".", "-", "/", "x", ""}


def _wa_num(code: str) -> int:
    try:
        return int(code.replace("WA", "").replace("wa", ""))
    except ValueError:
        return -1


def _commodity_group_l1(code: str) -> str:
    n = _wa_num(code)
    if   1  <= n <= 24: return "Food & Agriculture"
    elif 25 <= n <= 27: return "Energy & Raw Materials"
    elif 28 <= n <= 40: return "Chemicals, Pharma & Plastics"
    elif 41 <= n <= 49: return "Basic Materials (Wood, Paper, Construction)"
    elif 50 <= n <= 67: return "Consumer Goods & Other Manufacturing"
    elif 68 <= n <= 70: return "Basic Materials (Wood, Paper, Construction)"
    elif 71 <= n <= 83: return "Metals & Metal Products"
    elif n == 84:       return "Machinery & Transport Equipment"
    elif n == 85:       return "Electronics & Precision Technology"
    elif 86 <= n <= 89: return "Machinery & Transport Equipment"
    elif 90 <= n <= 92: return "Electronics & Precision Technology"
    elif 93 <= n <= 99: return "Consumer Goods & Other Manufacturing"
    else:               return "Other"


def _commodity_group_l2(code: str) -> str:
    n = _wa_num(code)
    if   1  <= n <= 24: return "Food & Agriculture"
    elif 25 <= n <= 26: return "Mining & Raw Materials"
    elif n == 27:       return "Energy"
    elif 28 <= n <= 29: return "Chemicals (core)"
    elif n == 30:       return "Pharmaceuticals"
    elif 31 <= n <= 38: return "Chemicals (core)"
    elif 39 <= n <= 40: return "Plastics & Rubber"
    elif 41 <= n <= 49: return "Forestry, Wood & Paper"
    elif 50 <= n <= 67: return "Textiles & Consumer Goods (low-tech)"
    elif 68 <= n <= 70: return "Construction Materials"
    elif n in (71, 72) or 74 <= n <= 81: return "Primary Metals"
    elif n == 73 or 82 <= n <= 83:       return "Fabricated Metal Products"
    elif n == 84:       return "Machinery & Industrial Equipment"
    elif n == 85:       return "Electronics & Electrical Equipment"
    elif n == 87:       return "Automotive"
    elif n == 86 or 88 <= n <= 89: return "Other Transport Equipment"
    elif 90 <= n <= 92: return "Precision Instruments & Medical Tech"
    elif 93 <= n <= 96: return "Consumer & Miscellaneous Manufacturing"
    elif 97 <= n <= 99: return "Special / Residual"
    else:               return "Other"


def _parse_trade_by_commodity_wide(raw: str) -> pd.DataFrame:
    rows = []
    for line in raw.split("\n"):
        fields = [f.strip() for f in line.split(";")]
        if len(fields) < 5:
            continue
        try:
            yr = int(fields[0])
        except ValueError:
            continue
        if not (1990 <= yr <= 2060):
            continue
        commodity_code  = fields[1].strip()
        commodity_label = fields[2].strip()
        if not commodity_code:
            continue
        values = fields[3:]
        for m in range(12):
            exp_idx = m * 6 + 1
            imp_idx = m * 6 + 4
            if imp_idx >= len(values):
                break
            exp_raw = values[exp_idx]
            imp_raw = values[imp_idx]
            if exp_raw in _NULL_VALS and imp_raw in _NULL_VALS:
                continue
            try:
                exp_bn = float(exp_raw.replace(",", ".")) / 1_000_000 if exp_raw not in _NULL_VALS else None
                imp_bn = float(imp_raw.replace(",", ".")) / 1_000_000 if imp_raw not in _NULL_VALS else None
            except ValueError:
                continue
            period = f"{yr}-{str(m + 1).zfill(2)}-01"
            rows.append({
                "period":                 period,
                "commodity_code":         commodity_code,
                "commodity_label":        commodity_label,
                "commodity_group":        _commodity_group_l1(commodity_code),
                "commodity_group_detail": _commodity_group_l2(commodity_code),
                "exports_eur_bn":         exp_bn,
                "imports_eur_bn":         imp_bn,
            })
    return pd.DataFrame(rows)


# ── DB connection ─────────────────────────────────────────────────────────────

def get_destatis_db() -> sqlite3.Connection:
    DB_DIR.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(str(DB_PATH))


# ── Individual fetch functions ────────────────────────────────────────────────

def _fetch_table_metadata(conn: sqlite3.Connection) -> None:
    log.info("[Table Metadata] Fetching official headlines from Destatis...")
    meta_rows = []
    for tid in _ALL_TABLES:
        m = fetch_table_meta(tid)
        meta_rows.append(m)
        label = m["title"] or "(no title returned)"
        log.info(f"  {tid}: {label}")
    df_meta = pd.DataFrame(meta_rows)[["table_id", "title", "period_from", "period_to"]]
    upsert_df(conn, df_meta, "destatis_table_meta", ["table_id"])
    log.info(f"  Saved {len(df_meta)} table headlines.")


_LM_CODES = {
    "ERW006": "registered_unemployed",
    "ERW116": "unemployment_pct_civilian",
    "ERW007": "registered_vacancies",
}


def _fetch_labor_market(conn: sqlite3.Connection) -> None:
    start_year = get_start_year(conn, "labor_market", lookback=0)
    log.info(f"[Labor Market] Fetching 13211-0002 from {start_year}...")
    try:
        df = fetch_table("13211-0002", startyear=str(start_year))
        if df.empty:
            raise DesatisNoDataError("empty response")
    except (DesatisNoDataError, RuntimeError) as e:
        log.warning(f"  WARNING: Destatis unavailable — existing rows unchanged. ({e})")
        return

    df["period"]    = build_monthly_period(df)
    df["value_num"] = pd.to_numeric(df["value"], errors="coerce")
    df["code"]      = df["value_variable_code"].str.strip()

    log.info(f"  Available variable codes: {df['code'].unique().tolist()}")

    region_col = "2_variable_attribute_code"
    if region_col in df.columns:
        region_vals = df[region_col].str.strip().str.upper().unique().tolist()
        log.info(f"  Region column values: {region_vals}")
        is_total = df[region_col].str.strip().str.upper().isin({"DINSG", "", "D", "TOTAL"})
        if not is_total.any():
            log.info("  NOTE: no Germany-total region found — using all rows")
            is_total = pd.Series(True, index=df.index)
    else:
        log.info("  Region column absent — using all rows")
        is_total = pd.Series(True, index=df.index)

    df_filtered = df[is_total & df["code"].isin(_LM_CODES)]
    if df_filtered.empty:
        log.warning(f"  WARNING: none of {list(_LM_CODES.keys())} matched — labor_market not updated")
        return

    df_lm = (
        df_filtered
        .pivot_table(index="period", columns="code", values="value_num", aggfunc="first")
        .reset_index()
        .rename(columns=_LM_CODES)
        .sort_values("period")
        .reset_index(drop=True)
    )
    for col in _LM_CODES.values():
        if col not in df_lm.columns:
            df_lm[col] = None
    out_cols = ["period", "registered_unemployed", "unemployment_pct_civilian", "registered_vacancies"]
    df_lm["period"] = df_lm["period"].astype(str)
    upsert_df(conn, df_lm[out_cols], "labor_market", ["period"])
    log.info(f"  {len(df_lm)} rows upserted into labor_market.")


def _fetch_production_index(conn: sqlite3.Connection) -> None:
    start_year = get_start_year(conn, "production_index", lookback=5)
    log.info(f"[Production Index] Fetching 42153-0002 from {start_year}...")
    try:
        df = fetch_table("42153-0002", startyear=str(start_year))
        if df.empty:
            raise DesatisNoDataError("empty response")
    except (DesatisNoDataError, RuntimeError) as e:
        log.warning(f"  WARNING: Destatis unavailable — existing rows unchanged. ({e})")
        return

    df["period"]    = build_monthly_period(df)
    df["value_num"] = pd.to_numeric(df["value"], errors="coerce")
    df["code"]      = df["value_variable_code"].str.strip()

    _attr_cols = [c for c in df.columns if c.endswith("_variable_attribute_code")]
    _adj_col = _dir_col = _ind_col = None
    for c in _attr_cols:
        vals = df[c].str.strip().str.upper().unique()
        if _ADJ_CODE in vals:
            _adj_col = c
        elif any(v in vals for v in ("INLAND", "AUSLAND", "INSGESAMT")):
            _dir_col = c
        elif any(v.startswith("WZ08") for v in vals):
            _ind_col = c

    mask = pd.Series(True, index=df.index)
    if _adj_col:
        mask &= df[_adj_col].str.strip().str.upper() == _ADJ_CODE
    if _ind_col:
        mask &= df[_ind_col].str.strip().str.upper().str.startswith("WZ08")

    df_adj = df[mask].copy()
    if df_adj.empty:
        log.warning("  WARNING: no rows matched adjustment filter — using all rows.")
        df_adj = df.copy()

    if _dir_col and df_adj[_dir_col].str.strip().str.upper().isin(
            {"INLAND", "AUSLAND", "INSGESAMT"}).any():
        df_adj["direction"] = df_adj[_dir_col].str.strip().str.upper()
        df_adj = df_adj[df_adj["direction"].isin({"INLAND", "AUSLAND", "INSGESAMT"})]
        pivot = df_adj.pivot_table(index="period", columns="direction",
                                   values="value_num", aggfunc="first")
        pivot.columns.name = None
        df_ct = pivot.rename(columns={
            "INSGESAMT": "production_idx_total",
            "INLAND":    "production_idx_domestic",
            "AUSLAND":   "production_idx_non_domestic",
        }).reset_index()
    elif _ind_col and df_adj[_ind_col].str.strip().str.upper().str.startswith("WZ08").any():
        pivot = df_adj.pivot_table(index="period", columns=_ind_col,
                                   values="value_num", aggfunc="first")
        pivot.columns.name = None
        pivot.columns = [
            c.strip().replace("WZ08-", "WZ_").replace("WZ08_", "WZ_").replace("-", "_")
            for c in pivot.columns
        ]
        df_ct = pivot.reset_index()
    else:
        pivot = df_adj.pivot_table(index="period", columns="code",
                                   values="value_num", aggfunc="first")
        pivot.columns.name = None
        df_ct = pivot.reset_index()

    df_ct.sort_values("period", inplace=True)
    df_ct.reset_index(drop=True, inplace=True)
    for col in [c for c in df_ct.columns if c != "period"]:
        df_ct[f"{col}_yoy_pct"] = df_ct[col].pct_change(12) * 100
        df_ct[f"{col}_mom_pct"] = df_ct[col].pct_change(1)  * 100
    df_ct["period"] = df_ct["period"].astype(str)
    upsert_df(conn, df_ct, "production_index", ["period"])
    log.info(f"  {len(df_ct)} rows upserted into production_index.")


_DIR_RENAME_ORDERS = {
    "INSGESAMT":          "orders_idx_total",
    "TOTAL":              "orders_idx_total",
    "INLAND":             "orders_idx_domestic",
    "DOMESTIC_TERRITORY": "orders_idx_domestic",
    "AUSLAND":            "orders_idx_non_domestic",
    "FOREIGN_COUNTRIES":  "orders_idx_non_domestic",
}


def _fetch_manufacturing_orders(conn: sqlite3.Connection) -> None:
    start_year = get_start_year(conn, "manufacturing_orders_idx", lookback=1)
    log.info(f"[Manufacturing Orders Index] Fetching 42151-0002 from {start_year}...")
    try:
        df = fetch_table("42151-0002", startyear=str(start_year))
        if df.empty:
            raise DesatisNoDataError("empty response")
    except (DesatisNoDataError, RuntimeError) as e:
        log.warning(f"  WARNING: Destatis unavailable — existing rows unchanged. ({e})")
        return

    df["period"]    = build_monthly_period(df)
    df["value_num"] = pd.to_numeric(df["value"], errors="coerce")

    _attr_cols = [c for c in df.columns if c.endswith("_variable_attribute_code")]
    _adj_col = _dir_col = _ind_col = None
    for c in _attr_cols:
        vals = set(df[c].str.strip().str.upper().unique())
        if _ADJ_CODE in vals or "X13_JDEMETRA___CALENDAR_AND_SEASONALLY_ADJUSTED" in vals:
            _adj_col = c
        elif vals & set(_DIR_RENAME_ORDERS):
            _dir_col = c
        elif any(v.startswith("WZ08") for v in vals):
            _ind_col = c

    mask = pd.Series(True, index=df.index)
    if _adj_col:
        _adj_vals = {_ADJ_CODE, "X13_JDEMETRA___CALENDAR_AND_SEASONALLY_ADJUSTED"}
        mask &= df[_adj_col].str.strip().str.upper().isin(_adj_vals)
    if _ind_col:
        if (df[_ind_col].str.strip().str.upper() == "WZ08-C").any():
            mask &= df[_ind_col].str.strip().str.upper() == "WZ08-C"
        else:
            log.info("  NOTE: WZ08-C not in industry column — industry filter skipped")

    df_adj = df[mask].copy()
    if df_adj.empty:
        log.warning("  WARNING: no rows matched adjustment filter — skipping.")
        return

    if _dir_col:
        df_adj["direction"] = df_adj[_dir_col].str.strip().str.upper()
        df_adj = df_adj[df_adj["direction"].isin(_DIR_RENAME_ORDERS)]
        pivot = df_adj.pivot_table(index="period", columns="direction",
                                   values="value_num", aggfunc="first")
        pivot.columns.name = None
        pivot.rename(columns=_DIR_RENAME_ORDERS, inplace=True)
        pivot = pivot.T.groupby(level=0).first().T
        df_mt = pivot.reset_index()
    else:
        df_mt = (df_adj.groupby("period", as_index=False)["value_num"]
                 .first().rename(columns={"value_num": "orders_idx_total"}))

    df_mt.sort_values("period", inplace=True)
    df_mt.reset_index(drop=True, inplace=True)
    for col in [c for c in df_mt.columns if c != "period"]:
        df_mt[f"{col}_yoy_pct"] = df_mt[col].pct_change(12) * 100
        df_mt[f"{col}_mom_pct"] = df_mt[col].pct_change(1)  * 100
    df_mt["period"] = df_mt["period"].astype(str)
    upsert_df(conn, df_mt, "manufacturing_orders_idx", ["period"])
    log.info(f"  {len(df_mt)} rows upserted into manufacturing_orders_idx.")


def _fetch_manufacturing_turnover(conn: sqlite3.Connection) -> None:
    start_year = get_start_year(conn, "manufacturing_turnover_idx", lookback=1)
    log.info(f"[Manufacturing Turnover Index] Fetching 42152-0002 from {start_year}...")
    try:
        df = fetch_table("42152-0002", startyear=str(start_year))
        if df.empty:
            raise DesatisNoDataError("empty response")
    except (DesatisNoDataError, RuntimeError) as e:
        log.warning(f"  WARNING: Destatis unavailable — existing rows unchanged. ({e})")
        return

    df["period"]    = build_monthly_period(df)
    df["value_num"] = pd.to_numeric(df["value"], errors="coerce")
    df["code"]      = df["value_variable_code"].str.strip()

    _attr_cols = [c for c in df.columns if c.endswith("_variable_attribute_code")]
    _adj_col = _ind_col = None
    _adj_vals = {_ADJ_CODE, "X13_JDEMETRA___CALENDAR_AND_SEASONALLY_ADJUSTED"}
    for c in _attr_cols:
        vals = set(df[c].str.strip().str.upper().unique())
        if vals & _adj_vals:
            _adj_col = c
        elif any(v.startswith("WZ08") for v in vals):
            _ind_col = c

    mask = pd.Series(True, index=df.index)
    if _adj_col:
        mask &= df[_adj_col].str.strip().str.upper().isin(_adj_vals)
    if _ind_col:
        if (df[_ind_col].str.strip().str.upper() == "WZ08-C").any():
            mask &= df[_ind_col].str.strip().str.upper() == "WZ08-C"
        else:
            log.info("  NOTE: WZ08-C not in industry column — industry filter skipped")

    df_adj = df[mask].copy()
    if df_adj.empty:
        log.warning("  WARNING: no rows matched adjustment filter — skipping.")
        return

    pivot = df_adj.pivot_table(index="period", columns="code",
                               values="value_num", aggfunc="first")
    pivot.columns.name = None
    _CODE_NAMES = {
        "AUB001": "turnover_idx_total",
        "AUB002": "turnover_idx_domestic",
        "AUB003": "turnover_idx_non_domestic",
    }
    pivot.rename(columns={k: v for k, v in _CODE_NAMES.items() if k in pivot.columns}, inplace=True)

    df_me = pivot.reset_index().sort_values("period").reset_index(drop=True)
    for col in [c for c in df_me.columns if c != "period"]:
        df_me[f"{col}_yoy_pct"] = df_me[col].pct_change(12) * 100
        df_me[f"{col}_mom_pct"] = df_me[col].pct_change(1)  * 100
    df_me["period"] = df_me["period"].astype(str)
    upsert_df(conn, df_me, "manufacturing_turnover_idx", ["period"])
    log.info(f"  {len(df_me)} rows upserted into manufacturing_turnover_idx.")


def _fetch_factory_orders(conn: sqlite3.Connection) -> None:
    start_year = get_start_year(conn, "factory_orders", lookback=1)
    log.info(f"[Factory Orders] Fetching 42155-0004 from {start_year}...")
    try:
        df = fetch_table("42155-0004", startyear=str(start_year), contents="AUB102")
        if df.empty:
            raise DesatisNoDataError("empty response")
    except (DesatisNoDataError, RuntimeError) as e:
        log.warning(f"  WARNING: Destatis unavailable — existing rows unchanged. ({e})")
        return

    df["period"]    = build_monthly_period(df)
    df["value_num"] = pd.to_numeric(df["value"], errors="coerce")
    df["industry"]  = df["2_variable_attribute_code"].str.strip()
    df["direction"] = df["3_variable_attribute_code"].str.strip()

    df_mfg = df[df["industry"] == "Manufacturing"].copy()
    df_mfg["adj_rank"] = df_mfg.groupby(["period", "industry", "direction"]).cumcount()
    df_seas = df_mfg[
        (df_mfg["adj_rank"] == 2) &
        (df_mfg["direction"].isin(["TOTAL", "DOMESTIC_TERRITORY", "FOREIGN_COUNTRIES"]))
    ].copy()

    if df_seas.empty:
        log.warning("  WARNING: no seasonally adjusted rows found — skipping factory_orders.")
        return

    pivot = df_seas.pivot_table(index="period", columns="direction",
                                values="value_num", aggfunc="first")
    pivot.columns.name = None
    df_fo = pivot.rename(columns={
        "TOTAL":              "total",
        "DOMESTIC_TERRITORY": "domestic",
        "FOREIGN_COUNTRIES":  "non_domestic",
    }).reset_index()
    df_fo["period"] = df_fo["period"].astype(str)
    upsert_df(conn, df_fo, "factory_orders", ["period"])
    log.info(f"  {len(df_fo)} rows upserted into factory_orders.")


def _fetch_production_index_total(conn: sqlite3.Connection) -> None:
    start_year = get_start_year(conn, "production_index_total", lookback=1)
    log.info(f"[Production Index Total] Fetching 48311-0001 from {start_year}...")
    try:
        df = fetch_table("48311-0001", startyear=str(start_year))
        if df.empty:
            raise DesatisNoDataError("empty response")
    except (DesatisNoDataError, RuntimeError) as e:
        log.warning(f"  WARNING: Destatis unavailable — existing rows unchanged. ({e})")
        return

    df["period"]    = build_monthly_period(df)
    df["value_num"] = pd.to_numeric(df["value"], errors="coerce")

    _attr_cols = [c for c in df.columns if c.endswith("_variable_attribute_code")]
    _adj_vals = {_ADJ_CODE, "X13_JDEMETRA___CALENDAR_AND_SEASONALLY_ADJUSTED"}
    _adj_col  = next(
        (c for c in _attr_cols
         if set(df[c].str.strip().str.upper().unique()) & _adj_vals),
        None
    )
    if _adj_col:
        df_adj = df[df[_adj_col].str.strip().str.upper().isin(_adj_vals)].copy()
    else:
        log.info("  NOTE: no adjustment column found — using all rows")
        df_adj = df.copy()

    if df_adj.empty:
        log.warning("  WARNING: no seasonally adjusted rows — skipping.")
        return

    df_pit = (df_adj.groupby("period", as_index=False)["value_num"]
              .first().rename(columns={"value_num": "production_idx_total"}))
    df_pit = df_pit.sort_values("period").reset_index(drop=True)
    df_pit["production_idx_total_yoy_pct"] = df_pit["production_idx_total"].pct_change(12) * 100
    df_pit["production_idx_total_mom_pct"] = df_pit["production_idx_total"].pct_change(1)  * 100
    df_pit["period"] = df_pit["period"].astype(str)
    upsert_df(conn, df_pit, "production_index_total", ["period"])
    log.info(f"  {len(df_pit)} rows upserted into production_index_total.")


def _fetch_trade_simple(conn: sqlite3.Connection) -> None:
    start_year = get_start_year(conn, "trade_simple", lookback=1)

    _df_raw = pd.DataFrame()
    log.info(f"[Trade] Fetching unadjusted 51000-0002 from {start_year}...")
    try:
        df = fetch_table("51000-0002", startyear=str(start_year))
        if df.empty:
            raise DesatisNoDataError("empty response")
        df["period"]    = build_monthly_period(df)
        df["value_num"] = pd.to_numeric(df["value"], errors="coerce")
        df["label_lc"]  = df["value_variable_label"].str.strip().str.lower()
        log.info(f"  Available labels (51000-0002): {df['value_variable_label'].str.strip().unique().tolist()}")
        _val_only = ~df["label_lc"].str.contains("change|vorjahr|veränd", na=False)
        exports = (df[df["label_lc"].str.contains("export", na=False) & _val_only]
                   .groupby("period", as_index=False)["value_num"].first()
                   .rename(columns={"value_num": "exports_eur_bn"}))
        imports = (df[df["label_lc"].str.contains("import", na=False) & _val_only]
                   .groupby("period", as_index=False)["value_num"].first()
                   .rename(columns={"value_num": "imports_eur_bn"}))
        _df_raw = (exports.merge(imports, on="period", how="outer")
                          .dropna(subset=["exports_eur_bn", "imports_eur_bn"])
                          .sort_values("period").reset_index(drop=True))
        _df_raw["exports_eur_bn"] /= 1_000_000
        _df_raw["imports_eur_bn"] /= 1_000_000
        log.info(f"  {len(_df_raw)} unadjusted rows")
    except DesatisNoDataError as e:
        log.warning(f"  WARNING: 51000-0002 unavailable ({e})")

    _df_adj = pd.DataFrame()
    log.info(f"[Trade] Fetching seasonally adjusted 51000-0020 from {start_year}...")
    try:
        df = fetch_table("51000-0020", startyear=str(start_year), contents="WERTA,WERTE")
        if df.empty:
            raise DesatisNoDataError("empty response")
        df["period"]    = build_monthly_period(df)
        df["value_num"] = pd.to_numeric(df["value"], errors="coerce")
        df["label_lc"]  = df["value_variable_label"].str.strip().str.lower()
        _val_only = ~df["label_lc"].str.contains("change|vorjahr|veränd", na=False)
        exports_adj = (df[df["label_lc"].str.contains("export", na=False) & _val_only]
                       .groupby("period", as_index=False)["value_num"].first()
                       .rename(columns={"value_num": "exports_adj_eur_bn"}))
        imports_adj = (df[df["label_lc"].str.contains("import", na=False) & _val_only]
                       .groupby("period", as_index=False)["value_num"].first()
                       .rename(columns={"value_num": "imports_adj_eur_bn"}))
        _df_adj = (exports_adj.merge(imports_adj, on="period", how="outer")
                              .dropna(subset=["exports_adj_eur_bn", "imports_adj_eur_bn"])
                              .sort_values("period").reset_index(drop=True))
        _df_adj["exports_adj_eur_bn"] /= 1_000_000
        _df_adj["imports_adj_eur_bn"] /= 1_000_000
        log.info(f"  {len(_df_adj)} seasonally adjusted rows")
    except DesatisNoDataError as e:
        log.warning(f"  WARNING: 51000-0020 unavailable ({e})")

    if _df_raw.empty and _df_adj.empty:
        return

    if _df_raw.empty:
        df_ts = _df_adj
    elif _df_adj.empty:
        df_ts = _df_raw
    else:
        df_ts = _df_raw.merge(_df_adj, on="period", how="outer")

    df_ts = df_ts.sort_values("period").reset_index(drop=True)

    if "exports_eur_bn" in df_ts.columns:
        df_ts["exports_yoy_pct"] = df_ts["exports_eur_bn"].pct_change(12) * 100
        df_ts["imports_yoy_pct"] = df_ts["imports_eur_bn"].pct_change(12) * 100
        df_ts["exports_mom_pct"] = df_ts["exports_eur_bn"].pct_change(1)  * 100
        df_ts["imports_mom_pct"] = df_ts["imports_eur_bn"].pct_change(1)  * 100

    if "exports_adj_eur_bn" in df_ts.columns:
        df_ts["exports_adj_yoy_pct"] = df_ts["exports_adj_eur_bn"].pct_change(12) * 100
        df_ts["imports_adj_yoy_pct"] = df_ts["imports_adj_eur_bn"].pct_change(12) * 100
        df_ts["exports_adj_mom_pct"] = df_ts["exports_adj_eur_bn"].pct_change(1)  * 100
        df_ts["imports_adj_mom_pct"] = df_ts["imports_adj_eur_bn"].pct_change(1)  * 100

    _raw_cols = ["exports_eur_bn", "imports_eur_bn",
                 "exports_yoy_pct", "imports_yoy_pct",
                 "exports_mom_pct", "imports_mom_pct"]
    _adj_cols = ["exports_adj_eur_bn", "imports_adj_eur_bn",
                 "exports_adj_yoy_pct", "imports_adj_yoy_pct",
                 "exports_adj_mom_pct", "imports_adj_mom_pct"]
    _out_cols = ["period"] + [c for c in _raw_cols + _adj_cols if c in df_ts.columns]
    df_ts["period"] = df_ts["period"].astype(str)
    upsert_df(conn, df_ts[_out_cols], "trade_simple", ["period"])
    log.info(f"  {len(df_ts)} rows upserted into trade_simple.")


def _fetch_trade_by_country(conn: sqlite3.Connection) -> None:
    start_year = get_start_year(conn, "trade_by_country", date_col="year", lookback=1, default=2019)
    log.info(f"[Trade by Country] Fetching 51000-0003 from {start_year}...")
    try:
        raw_tbc = fetch_raw_datencsv("51000-0003", startyear=str(start_year), fmt="ffcsv")
        if not raw_tbc.strip():
            raise DesatisNoDataError("empty content")
        df_tbc_raw = _parse_51000_0003(raw_tbc)
        if df_tbc_raw.empty:
            raise DesatisNoDataError("no rows parsed")
        log.info(f"  Parsed {len(df_tbc_raw)} rows for {df_tbc_raw['year'].nunique()} years, "
                 f"{df_tbc_raw['country_label'].nunique()} country labels")
    except (DesatisNoDataError, RuntimeError) as e:
        log.warning(f"  WARNING: Destatis unavailable — existing rows unchanged. ({e})")
        return

    df_tbc_raw["country_normalized"] = df_tbc_raw["country_label"].apply(_normalize_destatis_country)
    df_tbc_raw["exports_eur_bn"] = df_tbc_raw["exports_eur_1000"] / 1_000_000
    df_tbc_raw["imports_eur_bn"] = df_tbc_raw["imports_eur_1000"] / 1_000_000

    df_out = (df_tbc_raw[["year", "country_label", "country_normalized", "exports_eur_bn", "imports_eur_bn"]]
              .sort_values(["year", "country_label"]).reset_index(drop=True))
    upsert_df(conn, df_out, "trade_by_country", ["year", "country_label"])
    log.info(f"  {len(df_out)} rows upserted into trade_by_country.")


def _fetch_trade_by_commodity(conn: sqlite3.Connection) -> None:
    current_year = datetime.now().year
    start_year   = get_start_year(conn, "trade_by_commodity", lookback=1)
    log.info(f"[Trade by Commodity] Fetching 51000-0006 from {start_year}...")
    frames = []
    for year in range(start_year, current_year + 1):
        log.info(f"  {year}...")
        try:
            raw = fetch_raw_datencsv("51000-0006", startyear=str(year), endyear=str(year))
            _f = _parse_trade_by_commodity_wide(raw)
            if _f.empty:
                raise DesatisNoDataError("parsed 0 rows")
            frames.append(_f)
            log.info(f"    ok ({len(_f)} rows)")
        except DesatisNoDataError as e:
            log.info(f"    skipped ({e})")
        except Exception as e:
            log.warning(f"    skipped ({e})")

    if not frames:
        log.warning("  WARNING: no data fetched — existing rows unchanged.")
        return

    df_tbc = pd.concat(frames, ignore_index=True)
    df_tbc["period"] = df_tbc["period"].astype(str)
    upsert_df(conn, df_tbc, "trade_by_commodity", ["period", "commodity_code"])
    log.info(f"  {len(df_tbc)} rows upserted into trade_by_commodity.")


def _fetch_startups_by_industry(conn: sqlite3.Connection) -> None:
    start_year = get_start_year(conn, "startups_by_industry", lookback=1)
    log.info(f"[Startups] Fetching 52311-0011 from {start_year}...")
    try:
        df = fetch_table("52311-0011", startyear=str(start_year))
        if df.empty:
            raise DesatisNoDataError("empty response")
    except (DesatisNoDataError, RuntimeError) as e:
        log.warning(f"  WARNING: Destatis unavailable — existing rows unchanged. ({e})")
        return

    df["period"]       = build_monthly_period(df)
    df["industry_code"] = df["2_variable_attribute_code"].fillna("TOTAL").str.strip()
    _lbl_col = "2_variable_attribute_label" if "2_variable_attribute_label" in df.columns else "2_variable_attribute_code"
    df["industry_label"] = df[_lbl_col].fillna("Total").str.strip()
    df["count"] = pd.to_numeric(df["value"], errors="coerce")
    df_si = (df[["period", "industry_code", "industry_label", "count"]]
             .dropna(subset=["count"]).sort_values(["period", "industry_code"]).reset_index(drop=True))
    df_si["period"] = df_si["period"].astype(str)
    upsert_df(conn, df_si, "startups_by_industry", ["period", "industry_code"])
    log.info(f"  {len(df_si)} rows upserted into startups_by_industry.")


def _fetch_startups_by_type_legal(conn: sqlite3.Connection) -> None:
    start_year = get_start_year(conn, "startups_by_type_legal", lookback=1)
    log.info(f"[Startups] Fetching 52311-0012 from {start_year}...")
    try:
        df = fetch_table("52311-0012", startyear=str(start_year))
        if df.empty:
            raise DesatisNoDataError("empty response")
    except (DesatisNoDataError, RuntimeError) as e:
        log.warning(f"  WARNING: Destatis unavailable — existing rows unchanged. ({e})")
        return

    df["period"]             = build_monthly_period(df)
    df["startup_type_code"]  = df["3_variable_attribute_code"].str.strip()
    df["startup_type_label"] = df["3_variable_attribute_label"].str.strip()
    df["legal_form_label"]   = df["2_variable_attribute_code"].fillna("Total").str.strip()
    df["legal_form_code"]    = df["legal_form_label"].str.upper().str.replace(r"[^A-Z0-9]", "_", regex=True).str.strip("_")
    df["count"]              = pd.to_numeric(df["value"], errors="coerce")
    df3 = (df[["period", "startup_type_code", "startup_type_label", "legal_form_code", "legal_form_label", "count"]]
           .dropna(subset=["count"])
           .sort_values(["period", "startup_type_code", "legal_form_code"]).reset_index(drop=True))
    df3["period"] = df3["period"].astype(str)
    upsert_df(conn, df3, "startups_by_type_legal", ["period", "startup_type_code", "legal_form_code"])
    log.info(f"  {len(df3)} rows upserted into startups_by_type_legal.")


def _fetch_insolvency(conn: sqlite3.Connection) -> None:
    start_year = get_start_year(conn, "insolvency", lookback=1)
    log.info(f"[Insolvency] Fetching 52411-0010 from {start_year}...")
    try:
        df = fetch_table("52411-0010", startyear=str(start_year))
        if df.empty:
            raise DesatisNoDataError("empty response")
    except (DesatisNoDataError, RuntimeError) as e:
        log.warning(f"  WARNING: Destatis unavailable — existing rows unchanged. ({e})")
        return

    df["period"]    = build_monthly_period(df)
    df["value_num"] = pd.to_numeric(df["value"], errors="coerce")
    df["label"]     = df["value_variable_label"].str.strip()
    df["code"]      = df["value_variable_code"].str.strip()

    def _pick(code, label):
        return df[(df["code"] == code) & (df["label"] == label)][["period", "value_num"]]

    df_ins = (
        _pick("ISV006", "Insolvency proceedings (enterprises)")
        .rename(columns={"value_num": "insolvency_count"})
        .merge(_pick("ISV006", "insolvency proceedings (enterprises) (ch0004)")
               .rename(columns={"value_num": "insolvency_yoy_pct"}), on="period", how="left")
        .merge(_pick("FOR002", "Expected claims (against enterprises)")
               .rename(columns={"value_num": "expected_claims_eur_1000"}), on="period", how="left")
        .merge(_pick("ERW020", "Employees affected by insolvency proceedings")
               .rename(columns={"value_num": "employees_affected"}), on="period", how="left")
        .dropna(subset=["insolvency_count"])
        .sort_values("period").reset_index(drop=True)
    )
    df_ins["period"] = df_ins["period"].astype(str)
    upsert_df(conn, df_ins, "insolvency", ["period"])
    log.info(f"  {len(df_ins)} rows upserted into insolvency.")


_NOM_KW  = r"current price|laufende|jeweilige|nominale|jeweiligen"
_REAL_KW = r"chained|preise.*(2015|2020|2010)|verkettete|constant price|price.*year|real|kettenindex"


def _fetch_germany_gdp(conn: sqlite3.Connection) -> None:
    start_year = get_start_year(conn, "germany_gdp_quarterly", lookback=2)

    log.info(f"[Germany GDP] Fetching 81000-0002 from {start_year}...")
    try:
        df = fetch_table("81000-0002", startyear=str(start_year))
        if df.empty:
            raise DesatisNoDataError("empty response")
    except (DesatisNoDataError, RuntimeError) as e:
        log.warning(f"  WARNING: Destatis unavailable — existing rows unchanged. ({e})")
        df = pd.DataFrame()

    if not df.empty:
        try:
            _tc = _find_time_var_col(df)
            if df[_tc].str.contains("QUARTAL", na=False).any():
                df["period"] = build_quarterly_period(df)
            else:
                df["period"] = build_monthly_period(df)
        except Exception:
            df["period"] = build_monthly_period(df)

        df["value_num"] = pd.to_numeric(df["value"], errors="coerce")
        df["label_lc"]  = df["value_variable_label"].str.strip().str.lower()
        df["code"]      = df["value_variable_code"].str.strip()

        _attr_cols = [c for c in df.columns if c.endswith("_variable_attribute_code")]
        _adj_vals  = {_ADJ_CODE, "X13_JDEMETRA___CALENDAR_AND_SEASONALLY_ADJUSTED",
                      "SAISON_KAL_BEREI", "SAISONBEREINIGT", "CALENDAR_AND_SEASONALLY_ADJUSTED"}
        _adj_col   = next(
            (c for c in _attr_cols
             if set(df[c].str.strip().str.upper().unique()) & _adj_vals),
            None
        )
        df_adj = df[df[_adj_col].str.strip().str.upper().isin(_adj_vals)].copy() \
                 if _adj_col else df.copy()
        if df_adj.empty:
            df_adj = df.copy()

        df_nom  = (df_adj[df_adj["label_lc"].str.contains(_NOM_KW,  na=False, regex=True)]
                   .groupby("period", as_index=False)["value_num"].first()
                   .rename(columns={"value_num": "nominal_gdp_eur_mn"}))
        df_real = (df_adj[df_adj["label_lc"].str.contains(_REAL_KW, na=False, regex=True)]
                   .groupby("period", as_index=False)["value_num"].first()
                   .rename(columns={"value_num": "real_gdp_eur_mn"}))

        if df_nom.empty and df_real.empty:
            _codes = df_adj["code"].unique()
            if len(_codes) >= 1:
                df_nom = (df_adj[df_adj["code"] == _codes[0]]
                          .groupby("period", as_index=False)["value_num"].first()
                          .rename(columns={"value_num": "nominal_gdp_eur_mn"}))
            if len(_codes) >= 2:
                df_real = (df_adj[df_adj["code"] == _codes[1]]
                           .groupby("period", as_index=False)["value_num"].first()
                           .rename(columns={"value_num": "real_gdp_eur_mn"}))

        dfs_gdp = [x for x in [df_nom, df_real] if not x.empty]
        if dfs_gdp:
            merged_gdp = dfs_gdp[0]
            for _d in dfs_gdp[1:]:
                merged_gdp = merged_gdp.merge(_d, on="period", how="outer")
            merged_gdp = merged_gdp.sort_values("period").reset_index(drop=True)
            if "nominal_gdp_eur_mn" in merged_gdp.columns:
                merged_gdp["nominal_gdp_qoq_pct"] = merged_gdp["nominal_gdp_eur_mn"].pct_change(1) * 100
                merged_gdp["nominal_gdp_yoy_pct"] = merged_gdp["nominal_gdp_eur_mn"].pct_change(4) * 100
            if "real_gdp_eur_mn" in merged_gdp.columns:
                merged_gdp["real_gdp_qoq_pct"] = merged_gdp["real_gdp_eur_mn"].pct_change(1) * 100
                merged_gdp["real_gdp_yoy_pct"] = merged_gdp["real_gdp_eur_mn"].pct_change(4) * 100
            merged_gdp["period"] = merged_gdp["period"].astype(str)
            upsert_df(conn, merged_gdp, "germany_gdp_quarterly", ["period"])
            log.info(f"  {len(merged_gdp)} rows upserted into germany_gdp_quarterly.")

    log.info(f"[Germany GDP raw] Fetching 81000-0001 from {start_year}...")
    try:
        df_raw_gdp = fetch_table("81000-0001", startyear=str(start_year))
        if df_raw_gdp.empty:
            raise DesatisNoDataError("empty response")
    except (DesatisNoDataError, RuntimeError) as e:
        log.warning(f"  WARNING: Destatis unavailable — existing rows unchanged. ({e})")
        return

    try:
        _tc = _find_time_var_col(df_raw_gdp)
        if df_raw_gdp[_tc].str.contains("QUARTAL", na=False).any():
            df_raw_gdp["period"] = build_quarterly_period(df_raw_gdp)
        else:
            df_raw_gdp["period"] = build_monthly_period(df_raw_gdp)
    except Exception:
        df_raw_gdp["period"] = build_monthly_period(df_raw_gdp)

    df_raw_gdp["value_num"] = pd.to_numeric(df_raw_gdp["value"], errors="coerce")
    df_raw_gdp["label_lc"]  = df_raw_gdp["value_variable_label"].str.strip().str.lower()

    df_nom_raw  = (df_raw_gdp[df_raw_gdp["label_lc"].str.contains(_NOM_KW,  na=False, regex=True)]
                   .groupby("period", as_index=False)["value_num"].first()
                   .rename(columns={"value_num": "nominal_gdp_raw_eur_mn"}))
    df_real_raw = (df_raw_gdp[df_raw_gdp["label_lc"].str.contains(_REAL_KW, na=False, regex=True)]
                   .groupby("period", as_index=False)["value_num"].first()
                   .rename(columns={"value_num": "real_gdp_raw_eur_mn"}))

    dfs_raw = [x for x in [df_nom_raw, df_real_raw] if not x.empty]
    if not dfs_raw:
        log.warning("  WARNING: no unadjusted GDP rows matched — raw columns not added")
        return

    merged_raw = dfs_raw[0]
    for _d in dfs_raw[1:]:
        merged_raw = merged_raw.merge(_d, on="period", how="outer")
    merged_raw = merged_raw.sort_values("period").reset_index(drop=True)
    if "nominal_gdp_raw_eur_mn" in merged_raw.columns:
        merged_raw["nominal_gdp_raw_qoq_pct"] = merged_raw["nominal_gdp_raw_eur_mn"].pct_change(1) * 100
    if "real_gdp_raw_eur_mn" in merged_raw.columns:
        merged_raw["real_gdp_raw_qoq_pct"] = merged_raw["real_gdp_raw_eur_mn"].pct_change(1) * 100
    merged_raw["period"] = merged_raw["period"].astype(str)
    upsert_df(conn, merged_raw, "germany_gdp_quarterly", ["period"])
    log.info(f"  {len(merged_raw)} raw rows upserted into germany_gdp_quarterly.")


def _fetch_cpi(conn: sqlite3.Connection) -> None:
    start_year = get_start_year(conn, "cpi_data", lookback=0)
    log.info(f"[CPI] Fetching 61111-0002 from {start_year}...")
    try:
        df = fetch_table("61111-0002", startyear=str(start_year))
        if df.empty:
            raise DesatisNoDataError("empty response")
    except (DesatisNoDataError, RuntimeError) as e:
        log.warning(f"  WARNING: Destatis unavailable — existing rows unchanged. ({e})")
        return

    df["period"]    = build_monthly_period(df)
    df["value_num"] = pd.to_numeric(df["value"], errors="coerce")
    df["row_rank"]  = df.groupby("period").cumcount()
    pivot = df.pivot_table(index="period", columns="row_rank", values="value_num", aggfunc="first")
    pivot.columns = ["cpi", "yoy_pct", "mom_pct"]
    df_cpi = pivot.dropna(subset=["cpi"]).reset_index()
    df_cpi["period"] = df_cpi["period"].astype(str)
    upsert_df(conn, df_cpi, "cpi_data", ["period"])
    log.info(f"  {len(df_cpi)} rows upserted into cpi_data.")


def _fetch_price_index(conn: sqlite3.Connection, table_id: str,
                       col_prefix: str, storage_name: str) -> None:
    start_year = get_start_year(conn, storage_name, lookback=1)
    log.info(f"[{storage_name}] Fetching {table_id} from {start_year}...")
    try:
        df = fetch_table(table_id, startyear=str(start_year))
        if df.empty:
            raise DesatisNoDataError("empty response")
    except DesatisNoDataError as e:
        log.warning(f"  WARNING: Destatis unavailable — existing rows unchanged. ({e})")
        return

    df["period"]    = build_monthly_period(df)
    df["value_num"] = pd.to_numeric(df["value"], errors="coerce")
    df["row_rank"]  = df.groupby("period").cumcount()
    pivot = df.pivot_table(index="period", columns="row_rank",
                           values="value_num", aggfunc="first")
    col_map = {0: f"{col_prefix}_idx", 1: f"{col_prefix}_yoy_pct", 2: f"{col_prefix}_mom_pct"}
    pivot.rename(columns={k: v for k, v in col_map.items() if k in pivot.columns}, inplace=True)
    pivot.columns.name = None

    df_out = pivot.dropna(subset=[f"{col_prefix}_idx"]).reset_index()
    df_out["period"] = df_out["period"].astype(str)
    upsert_df(conn, df_out, storage_name, ["period"])
    log.info(f"  {len(df_out)} rows upserted into {storage_name}.")


# ── Public API ────────────────────────────────────────────────────────────────

def fetch_all_destatis(conn: sqlite3.Connection | None = None) -> tuple[int, int]:
    """Fetch all Destatis datasets. Returns (ok, fail)."""
    own_conn = conn is None
    if own_conn:
        conn = get_destatis_db()

    steps = [
        ("table_metadata",           _fetch_table_metadata),
        ("labor_market",             _fetch_labor_market),
        ("production_index",         _fetch_production_index),
        ("manufacturing_orders_idx", _fetch_manufacturing_orders),
        ("manufacturing_turnover",   _fetch_manufacturing_turnover),
        ("factory_orders",           _fetch_factory_orders),
        ("production_index_total",   _fetch_production_index_total),
        ("trade_simple",             _fetch_trade_simple),
        ("trade_by_country",         _fetch_trade_by_country),
        ("trade_by_commodity",       _fetch_trade_by_commodity),
        ("startups_by_industry",     _fetch_startups_by_industry),
        ("startups_by_type_legal",   _fetch_startups_by_type_legal),
        ("insolvency",               _fetch_insolvency),
        ("germany_gdp",              _fetch_germany_gdp),
        ("cpi_data",                 _fetch_cpi),
    ]

    ok = fail = 0
    for name, fn in steps:
        try:
            fn(conn)
            ok += 1
        except Exception as exc:
            log.warning(f"  [{name}] ERROR: {exc}")
            fail += 1

    try:
        _fetch_price_index(conn, "61411-0002", "import_price", "import_price_index")
        ok += 1
    except Exception as exc:
        log.warning(f"  [import_price_index] ERROR: {exc}")
        fail += 1

    try:
        _fetch_price_index(conn, "61421-0002", "export_price", "export_price_index")
        ok += 1
    except Exception as exc:
        log.warning(f"  [export_price_index] ERROR: {exc}")
        fail += 1

    if own_conn:
        conn.close()
    return ok, fail


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    parser = argparse.ArgumentParser(description="Fetch all Destatis GENESIS datasets")
    parser.add_argument("--show", action="store_true", help="List tables and row counts")
    args = parser.parse_args()

    conn = get_destatis_db()
    if args.show:
        tables = [
            "destatis_table_meta", "labor_market", "production_index",
            "manufacturing_orders_idx", "manufacturing_turnover_idx",
            "factory_orders", "production_index_total", "trade_simple",
            "trade_by_country", "trade_by_commodity", "startups_by_industry",
            "startups_by_type_legal", "insolvency", "germany_gdp_quarterly",
            "cpi_data", "import_price_index", "export_price_index",
        ]
        for t in tables:
            try:
                n = conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
                print(f"  {t}: {n} rows")
            except Exception:
                print(f"  {t}: (no data)")
        conn.close()
        return

    log.info("Fetching all Destatis datasets...")
    ok, fail = fetch_all_destatis(conn)
    log.info(f"Done: {ok} datasets OK, {fail} failed")
    conn.close()


if __name__ == "__main__":
    main()
