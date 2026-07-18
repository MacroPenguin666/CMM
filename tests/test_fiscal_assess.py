"""Tests for backend/fetchers/fiscal_assess.py — pure computation over a temp DB."""

import sqlite3
from pathlib import Path

import pytest

from backend.fetchers import fiscal_assess as fa

SCHEMA = (Path(__file__).parent.parent / "backend" / "schema.sql").read_text()


@pytest.fixture()
def conn():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.executescript(SCHEMA[SCHEMA.index("-- CHINA FISCAL CAPACITY"):])
    c.executescript("""
        CREATE TABLE bruegel_provincial (
            province TEXT, indicator TEXT, year INTEGER, value REAL, unit TEXT);
    """)
    return c


def seed_national(conn, metric, series):
    """series: {(year, month): cumulative_value}"""
    conn.executemany(
        "INSERT INTO fiscal_national_monthly (year, month, metric, value_100m, yoy_pct)"
        " VALUES (?,?,?,?,0)",
        [(y, m, metric, v) for (y, m), v in series.items()])


class TestMonthlyize:
    def test_cumulative_to_single_months_with_janfeb_combined(self):
        cum = {2: 200.0, 3: 350.0, 4: 500.0}
        out = fa.monthlyize(cum)
        assert out[2] == 200.0   # Jan-Feb combined lands on month 2
        assert out[3] == 150.0
        assert out[4] == 150.0

    def test_full_year_series(self):
        cum = {m: m * 100.0 for m in range(2, 13)}
        out = fa.monthlyize(cum)
        assert out[12] == 100.0
        assert sum(out.values()) == 1200.0


class TestTrailing12m:
    def test_sums_across_year_boundary(self, conn):
        # 2025: cumulative 100/month → year total 1200; 2026 through May: 150/month
        seed_national(conn, "gpb_rev", {(2025, m): m * 100.0 for m in range(2, 13)})
        seed_national(conn, "gpb_rev", {(2026, m): m * 150.0 for m in range(2, 6)})
        # T12M at 2026-05: Jun-Dec 2025 (7×100) + Jan-May 2026 (5×150) = 1450
        assert fa.trailing_12m(conn, "gpb_rev", (2026, 5)) == 1450.0

    def test_none_when_history_too_short(self, conn):
        seed_national(conn, "gpb_rev", {(2026, m): m * 150.0 for m in range(2, 6)})
        assert fa.trailing_12m(conn, "gpb_rev", (2026, 5)) is None


class TestRepaymentGauge:
    def test_refi_share_from_latest_ytd(self, conn):
        conn.executemany(
            "INSERT INTO fiscal_lgb_monthly (year, month, metric, value) VALUES (?,?,?,?)",
            [(2026, 5, "issue_refi_ytd", 28872), (2026, 5, "issue_total_ytd", 47219),
             (2025, 12, "issue_refi_ytd", 20000), (2025, 12, "issue_total_ytd", 60000)])
        share = fa.refi_share(conn)
        assert share["year"] == 2026 and share["month"] == 5
        assert round(share["share"], 3) == round(28872 / 47219, 3)
        assert round(share["prev_year_share"], 3) == round(20000 / 60000, 3)

    def test_maturity_wall_next_years(self, conn):
        conn.executemany(
            "INSERT INTO fiscal_maturity (province, maturity_year, principal_100m)"
            " VALUES (?,?,?)",
            [("全国", 2026, 30000), ("全国", 2027, 32000), ("全国", 2028, 28000),
             ("全国", 2040, 90000), ("贵州", 2027, 1500)])
        wall = fa.maturity_wall(conn, from_year=2026, horizon=3)
        assert wall["全国"] == 90000  # 2026+2027+2028
        assert wall["贵州"] == 1500


class TestFundingGauge:
    def test_spread_and_zscore(self, conn):
        rows = []
        # 100 days of history: CGB 10y at 2.0, LGB AAA at 2.2 → spread 20bp;
        # final day CGB 2.0 / LGB 2.6 → spread 60bp, way above history
        for i in range(100):
            d = f"2026-03-{i:02d}"  # date string ordering is all that matters
            wiggle = 0.01 if i % 2 else -0.01
            rows += [("cgb", d, 10.0, 2.0), ("lgb_aaa", d, 10.0, 2.2 + wiggle)]
        rows += [("cgb", "2026-07-01", 10.0, 2.0), ("lgb_aaa", "2026-07-01", 10.0, 2.6)]
        conn.executemany(
            "INSERT INTO fiscal_curves_daily (curve, date, tenor_y, yield_pct)"
            " VALUES (?,?,?,?)", rows)
        out = fa.funding_costs(conn)
        lgb = out["lgb_aaa_10y"]
        assert round(lgb["spread_bp"]) == 60
        assert lgb["spread_z"] > 2  # massively wide vs history

    def test_zscore_helper(self):
        assert fa.zscore([1, 1, 1, 1], 1) == 0.0
        assert fa.zscore([0, 2, 0, 2], 4) > 1


class TestVerdicts:
    def test_traffic_light_thresholds(self):
        assert fa.light(0.65, fa.REFI_SHARE_BANDS) == "red"
        assert fa.light(0.50, fa.REFI_SHARE_BANDS) == "amber"
        assert fa.light(0.20, fa.REFI_SHARE_BANDS) == "green"
        # falling land revenue: more negative = worse
        assert fa.light(-28.7, fa.LAND_YOY_BANDS) == "red"
        assert fa.light(5.0, fa.LAND_YOY_BANDS) == "green"

    def test_build_assessment_smoke(self, conn):
        # minimal seed: enough for the builder to emit structure without crashing
        seed_national(conn, "gpb_rev", {(2025, m): m * 100.0 for m in range(2, 13)})
        seed_national(conn, "gpb_rev", {(2026, m): m * 150.0 for m in range(2, 6)})
        out = fa.build_assessment(conn)
        assert set(out) >= {"flow", "repayment", "funding", "verdicts", "generated_at"}
        assert isinstance(out["verdicts"], list)


class TestPayload:
    def test_build_payload_structure(self, conn):
        seed_national(conn, "gpb_rev", {(2025, m): m * 100.0 for m in range(2, 13)})
        seed_national(conn, "gpb_rev", {(2026, m): m * 150.0 for m in range(2, 6)})
        conn.execute("INSERT INTO fiscal_reference (key, province, value_json, citation)"
                     " VALUES ('restricted_provinces','','[\"贵州\"]','test')")
        conn.execute("INSERT INTO fiscal_province_annual (province, year, metric, value, source)"
                     " VALUES ('贵州', 2024, 'debt_special_balance', 15000, 'mof_final_accounts')")
        conn.execute("INSERT INTO bruegel_provincial (province, indicator, year, value, unit)"
                     " VALUES ('Guizhou', 'GDP', 2024, 20000, '100M_yuan')")
        out = fa.build_payload(conn)
        assert set(out) >= {"assessment", "national", "lgb", "curves", "provinces",
                            "maturity", "reference", "meta"}
        gz = next(p for p in out["provinces"]["rows"] if p["province"] == "贵州")
        assert gz["restricted"] is True
        assert gz["gdp_100m"] == 20000
        assert gz["debt_special_balance"] == 15000
        assert any(s["metric"] == "gpb_rev" for s in out["national"])
