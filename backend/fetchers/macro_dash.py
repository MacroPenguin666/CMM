"""
Overview macro-strip payload builder.

Aggregates the key China macro series scattered across cmm.db (financial_series,
bruegel_series, macro_series, fiscal_national_monthly, fiscal_maturity) into a
uniform widget list for GET /api/overview/macro. Full history is returned; the
frontend slices time ranges client-side.

Widget shape:
    {key, title, unit, freq, series: [{name, points: [[date, value], ...]}],
     latest: {date, value} | None, secondary?: {label, value, date},
     forward?: bool}
"""

from datetime import datetime

_SHIBOR_TENORS = ["ON", "1W", "1M", "3M", "6M", "1Y"]


def _financial(conn, indicator):
    cur = conn.execute(
        "SELECT date, value FROM financial_series WHERE indicator=? AND value IS NOT NULL ORDER BY date",
        (indicator,))
    return [[d, v] for d, v in cur.fetchall()]


def _bruegel(conn, indicator):
    cur = conn.execute(
        "SELECT date, value FROM bruegel_series WHERE indicator=? AND value IS NOT NULL ORDER BY date",
        (indicator,))
    return [[d, v] for d, v in cur.fetchall()]


def _financial_or_bruegel(conn, indicator, bruegel_indicator):
    return _financial(conn, indicator) or _bruegel(conn, bruegel_indicator)


def _annual(conn, variable, max_year):
    cur = conn.execute(
        "SELECT year, value FROM macro_series WHERE variable=? AND value IS NOT NULL "
        "AND year<=? ORDER BY year", (variable, max_year))
    return {y: v for y, v in cur.fetchall()}


def _fiscal_monthly(conn, metric):
    cur = conn.execute(
        "SELECT year, month, value_100m FROM fiscal_national_monthly "
        "WHERE metric=? AND value_100m IS NOT NULL ORDER BY year, month", (metric,))
    return {(y, m): v for y, m, v in cur.fetchall()}


def _month_end_str(year, month):
    days = [31, 29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28,
            31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1]
    return f"{year:04d}-{month:02d}-{days:02d}"


def _latest(points):
    return {"date": points[-1][0], "value": points[-1][1]} if points else None


def _widget(key, title, unit, freq, series, latest=None, secondary=None, forward=None):
    w = {"key": key, "title": title, "unit": unit, "freq": freq,
         "series": series, "latest": latest}
    if secondary:
        w["secondary"] = secondary
    if forward is not None:
        w["forward"] = forward
    return w


def build_payload(conn, now_year=None):
    """Return the list of macro-strip widgets from an open cmm.db connection."""
    now_year = now_year or datetime.now().year
    actual_year = now_year - 1  # GMD/WEO rows beyond this are projections
    widgets = []

    # --- Real GDP growth (quarterly, cumulative yoy)
    gdp = _financial(conn, "GDP_YoY")
    widgets.append(_widget(
        "gdp_yoy", "Real GDP growth", "% yoy (ytd)", "Q",
        [{"name": "GDP", "points": gdp}], _latest(gdp)))

    # --- CPI / PPI (monthly yoy; Bruegel fallback)
    cpi = _financial_or_bruegel(conn, "CPI_YoY", "BRU_Inflation_Headline")
    ppi = _financial_or_bruegel(conn, "PPI_YoY", "BRU_Inflation_CEPIHS2_China_PPI_YoY")
    widgets.append(_widget(
        "cpi_ppi", "CPI / PPI", "% yoy", "M",
        [{"name": "CPI", "points": cpi}, {"name": "PPI", "points": ppi}],
        _latest(cpi)))

    # --- Exports / Imports (monthly yoy; Bruegel fallback)
    exports = _financial_or_bruegel(conn, "Exports_YoY", "BRU_Export_YoY_Export")
    imports = _financial_or_bruegel(conn, "Imports_YoY", "BRU_Export_YoY_Import")
    widgets.append(_widget(
        "trade", "Exports / Imports", "% yoy", "M",
        [{"name": "Exports", "points": exports}, {"name": "Imports", "points": imports}],
        _latest(exports)))

    # --- Government budget balance (MOF general public budget, cumulative YTD)
    rev = _fiscal_monthly(conn, "gpb_rev")
    exp = _fiscal_monthly(conn, "gpb_exp")
    balance = [[_month_end_str(y, m), round((rev[(y, m)] - exp[(y, m)]) / 10000, 4)]
               for (y, m) in sorted(rev) if (y, m) in exp]
    defc = _annual(conn, "gen_govdef_GDP", actual_year)
    secondary = None
    if defc:
        year = max(defc)
        secondary = {"label": f"Gen. gov. balance {year} (% GDP)",
                     "value": defc[year], "date": str(year)}
    widgets.append(_widget(
        "fiscal_balance", "Budget balance (YTD)", "tn CNY", "M",
        [{"name": "Balance", "points": balance}], _latest(balance), secondary))

    # --- Government debt (% GDP, actuals) + interest paid YTD
    debt = [[f"{y}-12-31", v] for y, v in sorted(_annual(conn, "gen_govdebt_GDP", actual_year).items())]
    interest = _fiscal_monthly(conn, "debt_interest_exp")
    secondary = None
    if interest:
        y, m = max(interest)
        secondary = {"label": "Debt interest paid YTD (tn CNY)",
                     "value": round(interest[(y, m)] / 10000, 4),
                     "date": _month_end_str(y, m)}
    widgets.append(_widget(
        "debt", "Gen. gov. debt", "% GDP", "A",
        [{"name": "Debt", "points": debt}], _latest(debt), secondary))

    # --- LGB principal repayment schedule (forward)
    cur = conn.execute(
        "SELECT maturity_year, principal_100m FROM fiscal_maturity "
        "WHERE province='全国' ORDER BY maturity_year")
    due = [[str(y), v] for y, v in cur.fetchall()]
    latest = None
    if due:
        current = [p for p in due if int(p[0]) >= now_year]
        latest = {"date": (current or due)[0][0], "value": (current or due)[0][1]}
    widgets.append(_widget(
        "repayments", "LGB repayments due", "亿 CNY", "A",
        [{"name": "Principal due", "points": due}], latest, forward=True))

    # --- GDP composition (annual, % GDP)
    cons = _annual(conn, "cons_GDP", actual_year)
    inv = _annual(conn, "inv_GDP", actual_year)
    ex = _annual(conn, "exports_GDP", actual_year)
    im = _annual(conn, "imports_GDP", actual_year)
    netexp = {y: ex[y] - im[y] for y in ex if y in im}
    comp = [{"name": name, "points": [[f"{y}-12-31", round(v, 4)] for y, v in sorted(d.items())]}
            for name, d in [("Consumption", cons), ("Investment", inv), ("Net exports", netexp)]]
    widgets.append(_widget(
        "gdp_comp", "GDP composition", "% GDP", "A",
        comp, _latest(comp[0]["points"])))

    # --- SHIBOR (daily, all tenors; latest highlights 3M)
    shibor = [{"name": t, "points": _financial(conn, f"SHIBOR_{t}")}
              for t in _SHIBOR_TENORS]
    three_m = next(s["points"] for s in shibor if s["name"] == "3M")
    widgets.append(_widget(
        "shibor", "SHIBOR", "%", "D", shibor, _latest(three_m)))

    return widgets
