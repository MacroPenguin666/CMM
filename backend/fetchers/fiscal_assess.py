"""
Fiscal-space assessment — pure computation over the fiscal_* tables.

Methodology anchors:
  - GS "China H2 Fiscal Outlook" (2025-08-06) Exhibit 16: effective deficit
    (expenditure − revenue, both budget accounts), net land financing, trust
    loans as shadow proxy, 12-month rolling window. Channels with no public
    feed (policy-bank support, LGFV bond net issuance) enter only as curated
    adjustments from fiscal_reference and are labelled as such.
  - ADB "Fiscal Rules in Monetary Union" (EAWP 251113): interest burden and
    refinancing share as repayment-pressure gauges; provincial dispersion.

Everything here is read-only; no network. All thresholds are fixed and
documented below so verdicts are reproducible, not editorialised.
"""

import json
import statistics
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# fixed assessment thresholds
# ---------------------------------------------------------------------------

# share of gross LGB issuance that merely rolls old debt (YTD)
REFI_SHARE_BANDS = {"higher_worse": True, "green": 0.40, "red": 0.60}
# yoy % of land-sale revenue (cumulative, from MOF release)
LAND_YOY_BANDS = {"higher_worse": False, "green": 0.0, "red": -15.0}
# z-score of credit spreads vs 3y history
SPREAD_Z_BANDS = {"higher_worse": True, "green": 0.5, "red": 1.5}
# effective deficit (both accounts, T12M) as % of GDP — wider = more stimulus
# but judged here as *fiscal space consumption*
DEFICIT_GDP_BANDS = {"higher_worse": True, "green": 6.0, "red": 10.0}


def light(value, bands) -> str:
    if value is None:
        return "grey"
    if bands["higher_worse"]:
        if value >= bands["red"]:
            return "red"
        if value <= bands["green"]:
            return "green"
    else:
        if value <= bands["red"]:
            return "red"
        if value >= bands["green"]:
            return "green"
    return "amber"


def zscore(history, value) -> float:
    if not history:
        return 0.0
    mean = statistics.fmean(history)
    sd = statistics.pstdev(history)
    return 0.0 if sd == 0 else (value - mean) / sd


# ---------------------------------------------------------------------------
# series helpers (MOF releases are cumulative-YTD)
# ---------------------------------------------------------------------------

def monthlyize(cum: dict) -> dict:
    """Cumulative {month: value} → single-month deltas. The first reported
    month (usually 2 — Jan-Feb combined) keeps its full cumulative value."""
    out, prev = {}, 0.0
    for m in sorted(cum):
        out[m] = cum[m] - prev
        prev = cum[m]
    return out


def _cum(conn, metric, year, month):
    row = conn.execute(
        "SELECT value_100m FROM fiscal_national_monthly WHERE metric=? AND year=? AND month=?",
        (metric, year, month)).fetchone()
    return row[0] if row else None


def latest_period(conn, table="fiscal_national_monthly", metric=None) -> tuple:
    q = f"SELECT year, month FROM {table}"
    args = ()
    if metric:
        q += " WHERE metric=?"
        args = (metric,)
    q += " ORDER BY year DESC, month DESC LIMIT 1"
    row = conn.execute(q, args).fetchone()
    return (row[0], row[1]) if row else (None, None)


def trailing_12m(conn, metric, period) -> float | None:
    """T12M sum ending at (year, month): cum(y,m) + cum(y-1,12) − cum(y-1,m).
    Exact even with the Jan-Feb combined release."""
    y, m = period
    if m == 12:
        return _cum(conn, metric, y, 12)
    now, prev_full, prev_part = (_cum(conn, metric, y, m),
                                 _cum(conn, metric, y - 1, 12),
                                 _cum(conn, metric, y - 1, m))
    if None in (now, prev_full, prev_part):
        return None
    return now + prev_full - prev_part


def latest_yoy(conn, metric):
    row = conn.execute(
        "SELECT year, month, value_100m, yoy_pct FROM fiscal_national_monthly"
        " WHERE metric=? ORDER BY year DESC, month DESC LIMIT 1", (metric,)).fetchone()
    return dict(row) if row else None


def _reference(conn, key):
    row = conn.execute(
        "SELECT value_json FROM fiscal_reference WHERE key=? AND province=''",
        (key,)).fetchone()
    return json.loads(row[0]) if row else None


def _national_gdp(conn):
    """Sum of provincial GDP (100M yuan), latest year — the denominator for
    %-of-GDP readings (labelled 'provincial-sum GDP' in the UI)."""
    try:
        row = conn.execute(
            "SELECT year, SUM(value) FROM bruegel_provincial WHERE indicator='GDP'"
            " GROUP BY year ORDER BY year DESC LIMIT 1").fetchone()
        return (row[0], row[1]) if row else (None, None)
    except Exception:
        return (None, None)


# ---------------------------------------------------------------------------
# gauge A — flow space
# ---------------------------------------------------------------------------

def gauge_flow(conn) -> dict:
    y, m = latest_period(conn, metric="gpb_rev")
    if y is None:
        return {}
    period = (y, m)
    t = {k: trailing_12m(conn, k, period)
         for k in ("gpb_rev", "gpb_exp", "fund_rev", "fund_exp", "land_sale_rev",
                   "gpb_rev_central", "gpb_rev_local", "gpb_exp_central",
                   "gpb_exp_local", "debt_interest_exp")}
    out = {"period": {"year": y, "month": m}, "t12m_100m": t}

    net_land = _reference(conn, "net_land_share") or {"share": 0.33}
    if all(t.get(k) is not None for k in ("gpb_rev", "gpb_exp", "fund_rev", "fund_exp")):
        eff = (t["gpb_exp"] - t["gpb_rev"]) + (t["fund_exp"] - t["fund_rev"])
        out["effective_deficit_t12m_100m"] = eff
        if t.get("land_sale_rev") is not None:
            # AFD-lite: replace gross land take with its net-financing share
            afd = eff + t["land_sale_rev"] * (1 - net_land["share"])
            # trust loans (shadow proxy) if monetary table is populated
            trust = conn.execute(
                "SELECT SUM(value_100m) FROM (SELECT value_100m FROM fiscal_monetary_monthly"
                " WHERE metric='tsf_trust_flow' ORDER BY year DESC, month DESC LIMIT 12)"
            ).fetchone()[0]
            if trust is not None:
                afd -= trust  # trust net flow adds to (negative reduces) quasi-fiscal support
            out["afd_lite_t12m_100m"] = afd
            gdp_year, gdp = _national_gdp(conn)
            if gdp:
                out["afd_lite_pct_gdp"] = round(afd / gdp * 100, 2)
                out["effective_deficit_pct_gdp"] = round(eff / gdp * 100, 2)
                out["gdp_denominator"] = {"year": gdp_year, "value_100m": gdp,
                                          "note": "provincial-sum GDP"}
            out["afd_excluded_channels"] = _reference(conn, "afd_excluded_channels")
            out["net_land_share"] = net_land

    for name, metric in (("revenue", "gpb_rev"), ("expenditure", "gpb_exp"),
                         ("land", "land_sale_rev"), ("fund_revenue", "fund_rev")):
        row = latest_yoy(conn, metric)
        if row:
            out.setdefault("latest_yoy", {})[name] = row["yoy_pct"]

    dep = conn.execute(
        "SELECT year, month, value_100m FROM fiscal_monetary_monthly"
        " WHERE metric='fiscal_deposits' ORDER BY year DESC, month DESC LIMIT 1").fetchone()
    if dep:
        prev = conn.execute(
            "SELECT value_100m FROM fiscal_monetary_monthly"
            " WHERE metric='fiscal_deposits' AND year=? AND month=?",
            (dep["year"] - 1, dep["month"])).fetchone()
        out["fiscal_deposits"] = {
            "year": dep["year"], "month": dep["month"], "value_100m": dep["value_100m"],
            "yoy_pct": (round((dep["value_100m"] / prev[0] - 1) * 100, 1)
                        if prev and prev[0] else None)}
    return out


# ---------------------------------------------------------------------------
# gauge B — repayment pressure
# ---------------------------------------------------------------------------

def refi_share(conn) -> dict | None:
    row = conn.execute(
        """SELECT a.year, a.month, a.value AS refi, b.value AS total
           FROM fiscal_lgb_monthly a JOIN fiscal_lgb_monthly b
             ON a.year = b.year AND a.month = b.month
           WHERE a.metric = 'issue_refi_ytd' AND b.metric = 'issue_total_ytd'
             AND b.value > 0
           ORDER BY a.year DESC, a.month DESC LIMIT 1""").fetchone()
    if not row:
        return None
    out = {"year": row["year"], "month": row["month"],
           "share": row["refi"] / row["total"]}
    prev = conn.execute(
        """SELECT a.value AS refi, b.value AS total
           FROM fiscal_lgb_monthly a JOIN fiscal_lgb_monthly b
             ON a.year = b.year AND a.month = b.month
           WHERE a.metric = 'issue_refi_ytd' AND b.metric = 'issue_total_ytd'
             AND a.year = ? AND a.month = 12""", (row["year"] - 1,)).fetchone()
    if prev and prev["total"]:
        out["prev_year_share"] = prev["refi"] / prev["total"]
    return out


def maturity_wall(conn, from_year=None, horizon=3) -> dict:
    if from_year is None:
        from_year = datetime.now().year
    return {r[0]: r[1] for r in conn.execute(
        """SELECT province, SUM(principal_100m) FROM fiscal_maturity
           WHERE maturity_year BETWEEN ? AND ? GROUP BY province""",
        (from_year, from_year + horizon - 1))}


def gauge_repayment(conn) -> dict:
    out = {}
    rs = refi_share(conn)
    if rs:
        out["refi_share"] = rs
    wall = maturity_wall(conn)
    if wall:
        out["maturity_wall_3y_100m"] = wall
        # registry completeness: future principal captured vs bonds outstanding —
        # the wall is misleading until the bond backfill is substantially done
        future = conn.execute(
            "SELECT SUM(principal_100m) FROM fiscal_maturity WHERE province='全国'"
            " AND maturity_year >= ?", (datetime.now().year,)).fetchone()[0] or 0
        outstanding = conn.execute(
            "SELECT value FROM fiscal_lgb_monthly WHERE metric='bonds_outstanding'"
            " ORDER BY year DESC, month DESC LIMIT 1").fetchone()
        if outstanding and outstanding[0]:
            out["registry_coverage_pct"] = round(future / outstanding[0] * 100, 1)
    row = conn.execute(
        "SELECT year, month, value FROM fiscal_lgb_monthly WHERE metric='interest_paid_ytd'"
        " ORDER BY year DESC, month DESC LIMIT 1").fetchone()
    if row:
        out["lgb_interest_paid_ytd_100m"] = dict(row)
    lim = conn.execute(
        """SELECT a.year, a.value AS lim, b.value AS bal
           FROM fiscal_lgb_monthly a JOIN fiscal_lgb_monthly b
             ON a.year = b.year AND a.month = b.month
           WHERE a.metric='debt_limit' AND b.metric='debt_outstanding'
           ORDER BY a.year DESC, a.month DESC LIMIT 1""").fetchone()
    if lim and lim["lim"]:
        out["quota_headroom"] = {"year": lim["year"], "limit_100m": lim["lim"],
                                 "outstanding_100m": lim["bal"],
                                 "used_pct": round(lim["bal"] / lim["lim"] * 100, 1)}
    # per-province interest burden (annualised annex YTD / GDP)
    try:
        y, m = latest_period(conn, "fiscal_lgb_province_monthly", "interest_paid_ytd")
        if y and m:
            gdp = {r[0]: r[1] for r in conn.execute(
                """SELECT province, value FROM bruegel_provincial
                   WHERE indicator='GDP' AND year=
                     (SELECT MAX(year) FROM bruegel_provincial WHERE indicator='GDP')""")}
            from backend.fetchers.fiscal_china import PROVINCE_EN_CN
            gdp_cn = {PROVINCE_EN_CN.get(k, k): v for k, v in gdp.items()}
            rows = conn.execute(
                "SELECT province, value_100m FROM fiscal_lgb_province_monthly"
                " WHERE metric='interest_paid_ytd' AND year=? AND month=?", (y, m))
            burden = {}
            for prov, v in rows:
                if prov in gdp_cn and gdp_cn[prov] and v is not None:
                    burden[prov] = round(v / m * 12 / gdp_cn[prov] * 100, 3)
            if burden:
                out["interest_pct_gdp_by_province"] = {
                    "year": y, "month": m, "annualised": True, "values": burden}
    except Exception:
        pass
    return out


# ---------------------------------------------------------------------------
# gauge C — funding costs
# ---------------------------------------------------------------------------

_SPREAD_DEFS = [
    ("lgb_aaa_10y", "lgb_aaa", 10.0),
    ("cpmtn_aa_5y", "cpmtn_aa", 5.0),
]


def funding_costs(conn) -> dict:
    out = {}
    cgb = {r[0]: r[1] for r in conn.execute(
        "SELECT date, yield_pct FROM fiscal_curves_daily WHERE curve='cgb' AND tenor_y=10.0")}
    if cgb:
        last_date = max(cgb)
        out["cgb_10y"] = {"date": last_date, "yield_pct": cgb[last_date]}
    for name, curve, tenor in _SPREAD_DEFS:
        ref_tenor = 10.0 if tenor == 10.0 else tenor
        base = {r[0]: r[1] for r in conn.execute(
            "SELECT date, yield_pct FROM fiscal_curves_daily WHERE curve='cgb' AND tenor_y=?",
            (ref_tenor,))}
        own = {r[0]: r[1] for r in conn.execute(
            "SELECT date, yield_pct FROM fiscal_curves_daily WHERE curve=? AND tenor_y=?",
            (curve, tenor))}
        spreads = {d: (own[d] - base[d]) * 100 for d in own if d in base}
        if not spreads:
            continue
        last_date = max(spreads)
        hist = [v for d, v in spreads.items() if d != last_date]
        out[name] = {"date": last_date, "yield_pct": own[last_date],
                     "spread_bp": round(spreads[last_date], 1),
                     "spread_z": round(zscore(hist, spreads[last_date]), 2),
                     "history_days": len(hist)}
    rate = conn.execute(
        "SELECT year, month, value FROM fiscal_lgb_monthly WHERE metric='avg_rate_ytd_pct'"
        " ORDER BY year DESC, month DESC LIMIT 1").fetchone()
    if rate:
        prev = conn.execute(
            "SELECT value FROM fiscal_lgb_monthly WHERE metric='avg_rate_ytd_pct'"
            " AND year=? AND month=12", (rate["year"] - 1,)).fetchone()
        out["lgb_avg_issue_rate"] = {"year": rate["year"], "month": rate["month"],
                                     "pct": rate["value"],
                                     "prev_year_pct": prev[0] if prev else None}
    return out


# ---------------------------------------------------------------------------
# verdicts
# ---------------------------------------------------------------------------

def _fmt_bn(v_100m):
    return f"{v_100m / 10000:.1f}tn" if v_100m is not None else "n/a"


def verdicts(flow, repay, fund) -> list:
    out = []

    land_yoy = (flow.get("latest_yoy") or {}).get("land")
    deficit_pct = flow.get("effective_deficit_pct_gdp")
    flow_light = light(deficit_pct, DEFICIT_GDP_BANDS)
    if land_yoy is not None and light(land_yoy, LAND_YOY_BANDS) == "red":
        flow_light = "red" if flow_light == "red" else "amber" if flow_light == "green" else flow_light
    parts = []
    if deficit_pct is not None:
        parts.append(f"effective deficit {deficit_pct}% of GDP (T12M, both budget accounts)")
    if land_yoy is not None:
        parts.append(f"land-sale revenue {land_yoy:+.1f}% yoy")
    dep = flow.get("fiscal_deposits")
    if dep and dep.get("yoy_pct") is not None:
        parts.append(f"fiscal deposits RMB {_fmt_bn(dep['value_100m'])} ({dep['yoy_pct']:+.1f}% yoy)")
    if parts:
        out.append({"level": "flow", "label": "Flow space", "light": flow_light,
                    "text": "; ".join(parts) + "."})

    rs = repay.get("refi_share")
    if rs:
        rl = light(rs["share"], REFI_SHARE_BANDS)
        txt = (f"{rs['share'] * 100:.0f}% of {rs['year']} LGB issuance is refinancing"
               f" (rolls old debt)")
        if rs.get("prev_year_share") is not None:
            txt += f", vs {rs['prev_year_share'] * 100:.0f}% in {rs['year'] - 1}"
        wall = repay.get("maturity_wall_3y_100m", {}).get("全国")
        if wall and repay.get("registry_coverage_pct", 0) >= 50:
            txt += f"; RMB {_fmt_bn(wall)} of LGB principal falls due within 3 years"
        hh = repay.get("quota_headroom")
        if hh:
            txt += f"; {hh['used_pct']}% of the NPC debt limit is used"
        out.append({"level": "repayment", "label": "Repayment pressure", "light": rl,
                    "text": txt + "."})

    spread = fund.get("cpmtn_aa_5y") or fund.get("lgb_aaa_10y")
    if spread:
        fl = light(abs(spread["spread_z"]), SPREAD_Z_BANDS) \
            if spread["spread_z"] > 0 else "green"
        which = "AA credit (chengtou proxy)" if "cpmtn_aa_5y" in fund else "LGB AAA"
        hist_days = spread.get("history_days", 0)
        hist_label = "3y" if hist_days > 700 else f"{hist_days}d of history (still backfilling)"
        txt = (f"{which} spread {spread['spread_bp']:.0f}bp over CGB"
               f" (z={spread['spread_z']:+.1f} vs {hist_label})")
        rate = fund.get("lgb_avg_issue_rate")
        if rate and rate.get("prev_year_pct"):
            txt += (f"; average LGB issuance rate {rate['pct']:.2f}%"
                    f" vs {rate['prev_year_pct']:.2f}% last year")
        out.append({"level": "funding", "label": "Funding costs", "light": fl,
                    "text": txt + "."})
    return out


# ---------------------------------------------------------------------------
# API payload
# ---------------------------------------------------------------------------

_NATIONAL_SERIES = (
    "gpb_rev", "gpb_exp", "gpb_rev_central", "gpb_rev_local", "gpb_exp_central",
    "gpb_exp_local", "fund_rev", "fund_exp", "land_sale_rev", "debt_interest_exp",
    "tax_rev", "nontax_rev",
)
_LGB_SERIES = (
    "issue_new", "issue_refi", "issue_total",
    "issue_new_ytd", "issue_refi_ytd", "issue_total_ytd", "avg_rate_ytd_pct",
    "avg_tenor_ytd_y", "debt_outstanding", "debt_outstanding_general",
    "debt_outstanding_special", "debt_limit", "interest_paid_ytd",
    "principal_repaid_ytd", "principal_repaid_by_refi_ytd", "stock_avg_rate_pct",
)
_PROV_ANNUAL_METRICS = (
    "transfers_general", "transfers_special", "transfers_fund",
    "debt_general_limit", "debt_general_balance",
    "debt_special_limit", "debt_special_balance",
)
_PROV_MONTHLY_METRICS = (
    "issue_new_ytd", "issue_refi_ytd", "issue_total_ytd",
    "principal_repaid_ytd", "interest_paid_ytd",
)


def _province_gdp_cn(conn) -> dict:
    from backend.fetchers.fiscal_china import PROVINCE_EN_CN
    try:
        rows = conn.execute(
            """SELECT province, indicator, value FROM bruegel_provincial
               WHERE indicator IN ('GDP', 'Population') AND year =
                 (SELECT MAX(year) FROM bruegel_provincial WHERE indicator='GDP')""")
    except Exception:
        return {}
    out = {}
    for prov_en, indicator, value in rows:
        cn = PROVINCE_EN_CN.get(prov_en, prov_en)
        out.setdefault(cn, {})["gdp_100m" if indicator == "GDP" else "pop_10k"] = value
    return out


def build_province_panel(conn) -> dict:
    from backend.fetchers.fiscal_china import PROVINCES_CN
    restricted = set(_reference(conn, "restricted_provinces") or [])
    gdp = _province_gdp_cn(conn)
    rows = {p: {"province": p, "restricted": p in restricted, **gdp.get(p, {})}
            for p in PROVINCES_CN}

    fa_year = conn.execute(
        "SELECT MAX(year) FROM fiscal_province_annual").fetchone()[0]
    if fa_year:
        for prov, metric, value in conn.execute(
                "SELECT province, metric, value FROM fiscal_province_annual WHERE year=?",
                (fa_year,)):
            if prov in rows and metric in _PROV_ANNUAL_METRICS:
                rows[prov][metric] = value

    y, m = latest_period(conn, "fiscal_lgb_province_monthly", "interest_paid_ytd")
    if y:
        for prov, metric, value in conn.execute(
                "SELECT province, metric, value_100m FROM fiscal_lgb_province_monthly"
                " WHERE year=? AND month=?", (y, m)):
            if prov in rows and metric in _PROV_MONTHLY_METRICS:
                rows[prov][metric] = value

    for prov, due in maturity_wall(conn).items():
        if prov in rows:
            rows[prov]["maturity_3y_100m"] = due

    for r in rows.values():
        g = r.get("gdp_100m")
        debt = (r.get("debt_general_balance") or 0) + (r.get("debt_special_balance") or 0)
        if debt:
            r["debt_balance_100m"] = debt
        if g:
            if debt:
                r["debt_pct_gdp"] = round(debt / g * 100, 1)
            if r.get("transfers_general") is not None:
                transfers = (r.get("transfers_general") or 0) + (r.get("transfers_special") or 0)
                r["transfers_pct_gdp"] = round(transfers / g * 100, 1)
            if r.get("interest_paid_ytd") is not None and m:
                r["interest_pct_gdp"] = round(r["interest_paid_ytd"] / m * 12 / g * 100, 2)
            if r.get("maturity_3y_100m") is not None:
                r["maturity_3y_pct_gdp"] = round(r["maturity_3y_100m"] / g * 100, 1)
        limit = (r.get("debt_general_limit") or 0) + (r.get("debt_special_limit") or 0)
        if limit and debt:
            r["limit_used_pct"] = round(debt / limit * 100, 1)
        if r.get("issue_total_ytd") and r.get("issue_refi_ytd") is not None:
            r["refi_share"] = round(r["issue_refi_ytd"] / r["issue_total_ytd"], 3)

    return {"annual_year": fa_year,
            "monthly_period": {"year": y, "month": m} if y else None,
            "rows": sorted(rows.values(),
                           key=lambda r: -(r.get("debt_pct_gdp") or 0))}


def build_payload(conn) -> dict:
    national = [dict(r) for r in conn.execute(
        f"""SELECT year, month, metric, value_100m, yoy_pct FROM fiscal_national_monthly
            WHERE metric IN ({','.join('?' * len(_NATIONAL_SERIES))})
            ORDER BY year, month""", _NATIONAL_SERIES)]
    lgb = [dict(r) for r in conn.execute(
        f"""SELECT year, month, metric, value FROM fiscal_lgb_monthly
            WHERE metric IN ({','.join('?' * len(_LGB_SERIES))})
            ORDER BY year, month""", _LGB_SERIES)]

    curves = {}
    for name, curve, tenor in (("cgb_10y", "cgb", 10.0), ("lgb_aaa_10y", "lgb_aaa", 10.0),
                               ("cpmtn_aa_5y", "cpmtn_aa", 5.0)):
        curves[name] = [dict(r) for r in conn.execute(
            "SELECT date, yield_pct FROM fiscal_curves_daily WHERE curve=? AND tenor_y=?"
            " ORDER BY date", (curve, tenor))]

    monetary = [dict(r) for r in conn.execute(
        "SELECT year, month, metric, value_100m FROM fiscal_monetary_monthly"
        " WHERE metric IN ('fiscal_deposits','tsf_trust_flow') ORDER BY year, month")]

    maturity_national = [dict(r) for r in conn.execute(
        "SELECT maturity_year, principal_100m, n_bonds FROM fiscal_maturity"
        " WHERE province='全国' ORDER BY maturity_year")]

    reference = [dict(r) for r in conn.execute(
        "SELECT key, province, value_json, as_of, citation FROM fiscal_reference")]

    meta = {}
    for table in ("fiscal_national_monthly", "fiscal_lgb_monthly", "fiscal_curves_daily",
                  "fiscal_monetary_monthly", "fiscal_province_annual",
                  "fiscal_lgb_province_monthly", "fiscal_lgb_bonds"):
        try:
            n, last = conn.execute(
                f"SELECT COUNT(*), MAX(fetched_at) FROM {table}").fetchone()
        except Exception:
            n, last = 0, None
        meta[table] = {"rows": n, "fetched_at": last}

    return {
        "assessment": build_assessment(conn),
        "national": national,
        "lgb": lgb,
        "curves": curves,
        "monetary": monetary,
        "provinces": build_province_panel(conn),
        "maturity": {"national": maturity_national,
                     "by_province_3y": maturity_wall(conn)},
        "reference": reference,
        "meta": meta,
    }


def build_assessment(conn) -> dict:
    flow = gauge_flow(conn)
    repay = gauge_repayment(conn)
    fund = funding_costs(conn)
    return {
        "flow": flow,
        "repayment": repay,
        "funding": fund,
        "verdicts": verdicts(flow, repay, fund),
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
