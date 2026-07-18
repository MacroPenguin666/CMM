"""Tests for the Overview macro-strip data layer.

Part 1: akshare table parsers in backend/fetchers/financial.py (fixture
DataFrames mirror the real column layouts probed live 2026-07-15).
Part 2: widget payload builder in backend/fetchers/macro_dash.py.
"""

import sqlite3

import pandas as pd
import pytest

from backend.fetchers.financial import (
    parse_gdp,
    parse_cpi,
    parse_ppi,
    parse_customs_trade,
)


# ---------------------------------------------------------------- parsers


class TestParseGdp:
    def _df(self):
        return pd.DataFrame({
            "季度": ["2026年第1-2季度", "2026年第1季度", "2025年第1-4季度", "2025年第1-3季度"],
            "国内生产总值-绝对值": [695704.0, 334192.9, 1401879.2, 1046445.5],
            "国内生产总值-同比增长": [4.7, 5.0, 5.0, 5.1],
        })

    def test_rows_use_quarter_end_dates_ascending(self):
        rows, _ = parse_gdp(self._df())
        assert [r[2] for r in rows] == ["2025-09-30", "2025-12-31", "2026-03-31", "2026-06-30"]

    def test_row_shape(self):
        rows, _ = parse_gdp(self._df())
        assert rows[-1] == ("GDP_YoY", "macro", "2026-06-30", 4.7, "%")

    def test_snapshot_is_latest_quarter(self):
        _, snaps = parse_gdp(self._df())
        assert len(snaps) == 1
        assert snaps[0]["indicator"] == "GDP_YoY"
        assert snaps[0]["latest_value"] == 4.7
        assert snaps[0]["data_date"] == "2026-06-30"


class TestParseCpi:
    def _df(self):
        return pd.DataFrame({
            "月份": ["2026年06月份", "2026年05月份", "2025年12月份"],
            "全国-当月": [101.0, 101.2, 100.4],
            "全国-同比增长": [1.0, 1.2, 0.4],
        })

    def test_rows_month_end_ascending(self):
        rows, _ = parse_cpi(self._df())
        assert [r[2] for r in rows] == ["2025-12-31", "2026-05-31", "2026-06-30"]
        assert rows[-1] == ("CPI_YoY", "macro", "2026-06-30", 1.0, "%")

    def test_snapshot(self):
        _, snaps = parse_cpi(self._df())
        assert snaps[0]["latest_value"] == 1.0
        assert snaps[0]["data_date"] == "2026-06-30"


class TestParsePpi:
    def test_rows_and_snapshot(self):
        df = pd.DataFrame({
            "月份": ["2026年06月份", "2026年05月份"],
            "当月": [104.1, 103.9],
            "当月同比增长": [4.1, 3.9],
        })
        rows, snaps = parse_ppi(df)
        assert rows == [
            ("PPI_YoY", "macro", "2026-05-31", 3.9, "%"),
            ("PPI_YoY", "macro", "2026-06-30", 4.1, "%"),
        ]
        assert snaps[0]["latest_value"] == 4.1


class TestParseCustomsTrade:
    def _df(self):
        return pd.DataFrame({
            "月份": ["2026年06月份", "2026年05月份"],
            "当月出口额-金额": [4.12e8, 3.77e8],
            "当月出口额-同比增长": [27.0, 19.4],
            "当月进口额-金额": [2.87e8, 2.71e8],
            "当月进口额-同比增长": [36.0, 27.4],
        })

    def test_exports_and_imports_rows(self):
        rows, _ = parse_customs_trade(self._df())
        exp = [r for r in rows if r[0] == "Exports_YoY"]
        imp = [r for r in rows if r[0] == "Imports_YoY"]
        assert exp == [
            ("Exports_YoY", "trade", "2026-05-31", 19.4, "%"),
            ("Exports_YoY", "trade", "2026-06-30", 27.0, "%"),
        ]
        assert imp[-1] == ("Imports_YoY", "trade", "2026-06-30", 36.0, "%")

    def test_snapshots_for_both_indicators(self):
        _, snaps = parse_customs_trade(self._df())
        by_ind = {s["indicator"]: s for s in snaps}
        assert by_ind["Exports_YoY"]["latest_value"] == 27.0
        assert by_ind["Imports_YoY"]["latest_value"] == 36.0

    def test_nan_growth_rows_skipped(self):
        df = self._df()
        df.loc[0, "当月出口额-同比增长"] = float("nan")
        rows, _ = parse_customs_trade(df)
        exp = [r for r in rows if r[0] == "Exports_YoY"]
        assert [r[2] for r in exp] == ["2026-05-31"]


# ---------------------------------------------------------------- payload

from backend.fetchers.macro_dash import build_payload  # noqa: E402


@pytest.fixture
def seeded_db():
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE financial_series (
            indicator TEXT, category TEXT, date TEXT, value REAL, unit TEXT,
            fetched_at TEXT DEFAULT '');
        CREATE TABLE bruegel_series (
            indicator TEXT, category TEXT, source_file TEXT DEFAULT '',
            date TEXT, value REAL, unit TEXT, fetched_at TEXT DEFAULT '');
        CREATE TABLE macro_series (
            variable TEXT, year INTEGER, value REAL, version TEXT,
            fetched_at TEXT DEFAULT '');
        CREATE TABLE fiscal_national_monthly (
            year INTEGER, month INTEGER, metric TEXT, value_100m REAL,
            yoy_pct REAL, source_url TEXT, fetched_at TEXT DEFAULT '');
        CREATE TABLE fiscal_maturity (
            province TEXT, maturity_year INTEGER, principal_100m REAL,
            n_bonds INTEGER, built_at TEXT DEFAULT '');
    """)
    fin = [
        ("GDP_YoY", "macro", "2026-03-31", 5.0, "%"),
        ("GDP_YoY", "macro", "2026-06-30", 4.7, "%"),
        ("CPI_YoY", "macro", "2026-05-31", 1.2, "%"),
        ("CPI_YoY", "macro", "2026-06-30", 1.0, "%"),
        ("PPI_YoY", "macro", "2026-06-30", 4.1, "%"),
        ("Exports_YoY", "trade", "2026-06-30", 27.0, "%"),
        ("Imports_YoY", "trade", "2026-06-30", 36.0, "%"),
        ("SHIBOR_ON", "bond", "2026-07-13", 1.42, "%"),
        ("SHIBOR_ON", "bond", "2026-07-14", 1.40, "%"),
        ("SHIBOR_3M", "bond", "2026-07-14", 1.55, "%"),
    ]
    conn.executemany(
        "INSERT INTO financial_series (indicator, category, date, value, unit) VALUES (?,?,?,?,?)",
        fin)
    conn.executemany(
        "INSERT INTO macro_series (variable, year, value) VALUES (?,?,?)", [
            ("gen_govdef_GDP", 2024, -7.57), ("gen_govdef_GDP", 2025, -7.41),
            ("gen_govdef_GDP", 2030, -6.60),
            ("gen_govdebt_GDP", 2024, 88.3), ("gen_govdebt_GDP", 2025, 96.3),
            ("gen_govdebt_GDP", 2030, 116.1),
            ("cons_GDP", 2023, 55.6), ("cons_GDP", 2024, 56.1),
            ("inv_GDP", 2023, 41.6), ("inv_GDP", 2024, 40.5),
            ("exports_GDP", 2023, 19.7), ("exports_GDP", 2024, 19.9),
            ("imports_GDP", 2023, 17.6), ("imports_GDP", 2024, 17.3),
        ])
    conn.executemany(
        "INSERT INTO fiscal_national_monthly (year, month, metric, value_100m, yoy_pct) VALUES (?,?,?,?,?)", [
            (2026, 4, "gpb_rev", 80000, 4.0), (2026, 4, "gpb_exp", 90000, 1.0),
            (2026, 5, "gpb_rev", 100465, 4.0), (2026, 5, "gpb_exp", 113877, 0.8),
            (2026, 5, "debt_interest_exp", 5601, 12.0),
        ])
    conn.executemany(
        "INSERT INTO fiscal_maturity (province, maturity_year, principal_100m, n_bonds) VALUES (?,?,?,?)", [
            ("全国", 2027, 1.46, 1), ("全国", 2031, 585.1, 3),
            ("上海", 2029, 5.2, 1),
        ])
    yield conn
    conn.close()


class TestBuildPayload:
    def test_widget_keys(self, seeded_db):
        keys = [w["key"] for w in build_payload(seeded_db, now_year=2026)]
        assert keys == ["gdp_yoy", "cpi_ppi", "trade", "fiscal_balance",
                        "debt", "repayments", "gdp_comp", "shibor"]

    def _widget(self, conn, key):
        return next(w for w in build_payload(conn, now_year=2026) if w["key"] == key)

    def test_gdp_widget(self, seeded_db):
        w = self._widget(seeded_db, "gdp_yoy")
        assert w["freq"] == "Q"
        assert w["latest"] == {"date": "2026-06-30", "value": 4.7}
        assert w["series"][0]["points"] == [["2026-03-31", 5.0], ["2026-06-30", 4.7]]

    def test_cpi_ppi_two_series(self, seeded_db):
        w = self._widget(seeded_db, "cpi_ppi")
        names = [s["name"] for s in w["series"]]
        assert names == ["CPI", "PPI"]
        assert w["latest"] == {"date": "2026-06-30", "value": 1.0}

    def test_cpi_falls_back_to_bruegel_when_missing(self, seeded_db):
        seeded_db.execute("DELETE FROM financial_series WHERE indicator IN ('CPI_YoY','PPI_YoY')")
        seeded_db.executemany(
            "INSERT INTO bruegel_series (indicator, category, date, value, unit) VALUES (?,?,?,?,?)", [
                ("BRU_Inflation_Headline", "x", "2026-05-31", 1.1, "%"),
                ("BRU_Inflation_CEPIHS2_China_PPI_YoY", "x", "2026-05-31", 3.9, "%"),
            ])
        w = self._widget(seeded_db, "cpi_ppi")
        assert w["series"][0]["points"] == [["2026-05-31", 1.1]]
        assert w["series"][1]["points"] == [["2026-05-31", 3.9]]

    def test_trade_widget(self, seeded_db):
        w = self._widget(seeded_db, "trade")
        assert [s["name"] for s in w["series"]] == ["Exports", "Imports"]
        assert w["latest"]["value"] == 27.0

    def test_fiscal_balance_ytd(self, seeded_db):
        w = self._widget(seeded_db, "fiscal_balance")
        # cumulative rev - exp, converted 亿元 -> tn CNY
        assert w["series"][0]["points"] == [["2026-04-30", -1.0], ["2026-05-31", -1.3412]]
        assert w["latest"]["value"] == -1.3412
        assert w["secondary"]["value"] == -7.41  # latest non-forecast gen_govdef_GDP
        assert "2025" in w["secondary"]["label"] or w["secondary"]["date"] == "2025"

    def test_debt_excludes_forecast_years(self, seeded_db):
        w = self._widget(seeded_db, "debt")
        assert w["series"][0]["points"] == [["2024-12-31", 88.3], ["2025-12-31", 96.3]]
        assert w["latest"] == {"date": "2025-12-31", "value": 96.3}
        assert w["secondary"]["value"] == 5601 / 10000  # debt interest YTD, tn

    def test_repayments_national_forward_schedule(self, seeded_db):
        w = self._widget(seeded_db, "repayments")
        assert w["forward"] is True
        assert w["series"][0]["points"] == [["2027", 1.46], ["2031", 585.1]]

    def test_gdp_comp_derives_net_exports(self, seeded_db):
        w = self._widget(seeded_db, "gdp_comp")
        byname = {s["name"]: s["points"] for s in w["series"]}
        assert byname["Consumption"][-1] == ["2024-12-31", 56.1]
        assert byname["Investment"][-1] == ["2024-12-31", 40.5]
        assert byname["Net exports"] == [["2023-12-31", pytest.approx(2.1)],
                                         ["2024-12-31", pytest.approx(2.6)]]

    def test_shibor_tenor_series(self, seeded_db):
        w = self._widget(seeded_db, "shibor")
        assert w["freq"] == "D"
        names = [s["name"] for s in w["series"]]
        assert names == ["ON", "1W", "1M", "3M", "6M", "1Y"]
        assert w["latest"] == {"date": "2026-07-14", "value": 1.55}  # 3M default

    def test_api_endpoint_returns_all_widgets(self, seeded_db, monkeypatch):
        import backend.api as api
        monkeypatch.setattr(api, "get_financial_db", lambda: seeded_db)
        client = api.app.test_client()
        resp = client.get("/api/overview/macro")
        assert resp.status_code == 200
        data = resp.get_json()
        assert [w["key"] for w in data["widgets"]] == [
            "gdp_yoy", "cpi_ppi", "trade", "fiscal_balance",
            "debt", "repayments", "gdp_comp", "shibor"]

    def test_empty_db_yields_no_crash(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript("""
            CREATE TABLE financial_series (indicator TEXT, category TEXT, date TEXT,
                value REAL, unit TEXT, fetched_at TEXT DEFAULT '');
            CREATE TABLE bruegel_series (indicator TEXT, category TEXT,
                source_file TEXT DEFAULT '', date TEXT, value REAL, unit TEXT,
                fetched_at TEXT DEFAULT '');
            CREATE TABLE macro_series (variable TEXT, year INTEGER, value REAL,
                version TEXT, fetched_at TEXT DEFAULT '');
            CREATE TABLE fiscal_national_monthly (year INTEGER, month INTEGER,
                metric TEXT, value_100m REAL, yoy_pct REAL, source_url TEXT,
                fetched_at TEXT DEFAULT '');
            CREATE TABLE fiscal_maturity (province TEXT, maturity_year INTEGER,
                principal_100m REAL, n_bonds INTEGER, built_at TEXT DEFAULT '');
        """)
        widgets = build_payload(conn, now_year=2026)
        for w in widgets:
            assert w["latest"] is None or w["latest"]["value"] is not None
