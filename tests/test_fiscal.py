"""Parser tests for backend/fetchers/fiscal_china.py — run against saved MOF fixtures."""

from pathlib import Path

import pytest

from backend.fetchers.fiscal_china import (
    parse_lgb_report,
    parse_listing,
    parse_shouzhi,
)

FIXTURES = Path(__file__).parent / "fixtures" / "fiscal"


def _read(name: str) -> str:
    return (FIXTURES / name).read_text()


# ---------------------------------------------------------------- 收支 monthly


class TestParseShouzhi:
    def test_period_from_cumulative_month_title(self):
        out = parse_shouzhi(_read("shouzhi_202605.html"))
        assert (out["year"], out["month"]) == (2026, 5)

    def test_general_budget_aggregates(self):
        m = parse_shouzhi(_read("shouzhi_202605.html"))["metrics"]
        assert m["gpb_rev"] == (100465, 4.0)
        assert m["tax_rev"] == (82617, 4.4)
        assert m["nontax_rev"] == (17848, 2.2)
        assert m["gpb_rev_central"] == (43850, 5.7)
        assert m["gpb_rev_local"] == (56615, 2.7)

    def test_expenditure_side_with_negative_growth(self):
        m = parse_shouzhi(_read("shouzhi_202605.html"))["metrics"]
        assert m["gpb_exp"] == (113877, 0.8)
        assert m["gpb_exp_central"] == (16815, 6.5)
        assert m["gpb_exp_local"] == (97062, -0.1)  # 同比下降0.1%
        assert m["debt_interest_exp"] == (5808, 5.1)

    def test_government_fund_budget_and_land(self):
        m = parse_shouzhi(_read("shouzhi_202605.html"))["metrics"]
        assert m["fund_rev"] == (12518, -19.2)
        assert m["fund_rev_central"] == (2042, 10.5)
        assert m["fund_rev_local"] == (10476, -23.2)
        assert m["land_sale_rev"] == (8048, -28.7)
        assert m["fund_exp"] == (30734, -4.3)
        assert m["fund_exp_central"] == (1666, 67.7)
        assert m["fund_exp_local"] == (29068, -6.6)
        assert m["land_related_exp"] == (13867, -13.9)

    def test_major_tax_lines(self):
        m = parse_shouzhi(_read("shouzhi_202605.html"))["metrics"]
        assert m["tax_vat"] == (32765, 6.2)
        assert m["tax_consumption"] == (7488, -3.1)
        assert m["tax_cit"] == (21880, 0.2)
        assert m["tax_pit"] == (7375, 12.2)
        assert m["tax_stamp"] == (2426, 35.8)
        assert m["tax_deed"] == (1661, -14.8)
        assert m["tax_land_vat"] == (1742, -14.2)

    def test_quarter_title_variant(self):
        out = parse_shouzhi(_read("mof_shouzhi_2026q1.html"))
        assert (out["year"], out["month"]) == (2026, 3)
        m = out["metrics"]
        assert m["gpb_rev"] == (61613, 2.4)
        assert m["tax_vat"] == (21473, 4.9)

    def test_full_year_variant_uses_bishangnian(self):
        out = parse_shouzhi(_read("mof_shouzhi_2025full.html"))
        assert (out["year"], out["month"]) == (2025, 12)
        m = out["metrics"]
        assert m["gpb_rev"] == (216045, -1.7)
        assert m["tax_rev"] == (176363, 0.8)


# ---------------------------------------------------------------- LGB reports


class TestParseLgbReport:
    def test_period_and_month_issuance(self):
        out = parse_lgb_report(_read("lgb_202605.html"))
        assert (out["year"], out["month"]) == (2026, 5)
        m = out["metrics"]
        assert m["issue_new"] == 1862
        assert m["issue_new_general"] == 254
        assert m["issue_new_special"] == 1608
        assert m["issue_refi"] == 6185
        assert m["issue_total"] == 8047
        assert m["avg_tenor_y"] == 13.5
        assert m["avg_rate_pct"] == 1.96

    def test_ytd_issuance_and_refi_split(self):
        m = parse_lgb_report(_read("lgb_202605.html"))["metrics"]
        assert m["issue_new_ytd"] == 18347
        assert m["issue_new_general_ytd"] == 3396
        assert m["issue_new_special_ytd"] == 14951
        assert m["issue_refi_ytd"] == 28872
        assert m["issue_total_ytd"] == 47219
        assert m["avg_rate_ytd_pct"] == 2.10

    def test_debt_service_lines(self):
        m = parse_lgb_report(_read("lgb_202605.html"))["metrics"]
        assert m["principal_repaid_ytd"] == 13996
        assert m["principal_repaid_by_refi_ytd"] == 12184
        assert m["principal_repaid_by_fiscal_ytd"] == 1812
        assert m["principal_repaid_month"] == 2733
        assert m["interest_paid_ytd"] == 6239
        assert m["interest_paid_month"] == 1451

    def test_outstanding_stock_block(self):
        m = parse_lgb_report(_read("lgb_202605.html"))["metrics"]
        assert m["debt_outstanding"] == 581453
        assert m["debt_outstanding_general"] == 178881
        assert m["debt_outstanding_special"] == 402572
        assert m["bonds_outstanding"] == 579835
        assert m["nonbond_outstanding"] == 1618
        assert m["stock_avg_tenor_y"] == 10.7
        assert m["stock_avg_rate_pct"] == 2.76

    def test_limit_absent_in_2026_may_report(self):
        m = parse_lgb_report(_read("lgb_202605.html"))["metrics"]
        assert "debt_limit" not in m

    def test_2025_sep_variant_with_limits(self):
        out = parse_lgb_report(_read("lgb_202509.html"))
        assert (out["year"], out["month"]) == (2025, 9)
        m = out["metrics"]
        # 当月: 新增债券 (no 地方政府 infix), 合计 phrased "合计，全国发行地方政府债券"
        assert m["issue_new"] == 4741
        assert m["issue_refi"] == 3878
        assert m["issue_total"] == 8619
        assert m["avg_tenor_y"] == 16.8
        # limits only appear in reports after NPC approval
        assert m["debt_limit"] == 579874.3
        assert m["debt_limit_general"] == 180689.22
        assert m["debt_limit_special"] == 399185.08
        assert m["debt_outstanding"] == 536995
        assert m["interest_paid_ytd"] == 11191


# ---------------------------------------------------------------- listings


class TestParseListing:
    def test_mof_index_absolute_links(self):
        items = parse_listing(_read("mof_index.html"),
                              "https://www.mof.gov.cn/zhengwuxinxi/caizhengshuju/")
        titles = {t for t, _ in items}
        assert "2026年1-5月财政收支情况" in titles
        urls = dict(items)
        assert urls["2026年1-5月财政收支情况"] == \
            "http://gks.mof.gov.cn/tongjishuju/202606/t20260622_3992033.htm"

    def test_zwgls_listing_relative_links_resolved(self):
        items = parse_listing(_read("zwgls_tjsj.html"), "https://zwgls.mof.gov.cn/tjsj/")
        urls = dict(items)
        assert urls.get("2026年5月地方政府债券发行和债务余额情况") == \
            "https://zwgls.mof.gov.cn/tjsj/202606/t20260624_3992154.htm"

    def test_yss_listing(self):
        items = parse_listing(_read("yss_dfz.html"),
                              "https://yss.mof.gov.cn/zhuantilanmu/dfzgl/sjtj/")
        urls = dict(items)
        assert urls.get("2025年9月地方政府债券发行和债务余额情况") == \
            "https://yss.mof.gov.cn/zhuantilanmu/dfzgl/sjtj/202510/t20251024_3974884.htm"


# ------------------------------------------------------- AKShare transforms


class TestMarketTransforms:
    def test_month_chunks_cover_range_within_limit(self):
        from backend.fetchers.fiscal_china import _month_chunks
        chunks = _month_chunks("20260501", "20260714")
        assert chunks[0][0] == "20260501"
        assert chunks[-1][1] == "20260714"
        from datetime import datetime
        prev_end = None
        for s, e in chunks:
            ds, de = datetime.strptime(s, "%Y%m%d"), datetime.strptime(e, "%Y%m%d")
            assert (de - ds).days <= 25
            if prev_end is not None:
                assert (ds - prev_end).days == 1  # contiguous, no overlap
            prev_end = de

    def test_pboc_period_disambiguates_october(self):
        from backend.fetchers.fiscal_china import _pboc_period
        assert _pboc_period("2026.5") == (2026, 5)
        assert _pboc_period("2026.10") == (2026, 10)
        assert _pboc_period("1993.6") == (1993, 6)

    def test_transform_curves(self):
        import pandas as pd
        from backend.fetchers.fiscal_china import transform_curves
        df = pd.DataFrame({"日期": ["2026-06-30", "2026-06-30"],
                           "期限": [5.0, 10.0],
                           "到期收益率": [1.5313, 1.8355],
                           "即期收益率": [1.5361, 1.8550],
                           "远期收益率": [None, None]})
        rows = transform_curves(df, "lgb_aaa")
        assert ("lgb_aaa", "2026-06-30", 10.0, 1.8355) in rows
        assert len(rows) == 2

    def test_transform_tsf(self):
        import pandas as pd
        from backend.fetchers.fiscal_china import transform_tsf
        df = pd.DataFrame({"月份": ["202604"], "社会融资规模增量": [6245.0],
                           "其中-信托贷款": [-129.0]})
        rows = transform_tsf(df)
        assert (2026, 4, "tsf_flow", 6245.0) in rows
        assert (2026, 4, "tsf_trust_flow", -129.0) in rows

    def test_transform_pboc_gov_deposits(self):
        import pandas as pd
        from backend.fetchers.fiscal_china import transform_pboc
        df = pd.DataFrame({"统计时间": ["2026.5"], "政府存款": [60155.26]})
        rows = transform_pboc(df)
        assert (2026, 5, "fiscal_deposits", 60155.26) in rows


# ------------------------------------------------------- ChinaMoney registry


class TestRegistryHelpers:
    def test_province_from_issuer(self):
        from backend.fetchers.fiscal_china import province_from_issuer
        assert province_from_issuer("四川省政府") == "四川"
        assert province_from_issuer("北京市政府") == "北京"
        assert province_from_issuer("新疆维吾尔自治区政府") == "新疆"
        assert province_from_issuer("广西壮族自治区政府") == "广西"
        assert province_from_issuer("内蒙古自治区政府") == "内蒙古"
        # 计划单列市 roll up into their parent province
        assert province_from_issuer("宁波市政府") == "浙江"
        assert province_from_issuer("深圳市政府") == "广东"
        assert province_from_issuer("新疆生产建设兵团") == "新疆兵团"

    def test_bond_flags_from_full_name(self):
        from backend.fetchers.fiscal_china import bond_flags
        assert bond_flags("2026年四川省政府专项债券(三十三期)") == (1, 0)
        assert bond_flags("2026年河北省政府再融资一般债券(五期)") == (0, 1)
        assert bond_flags("2025年山东省政府再融资专项债券(一期)") == (1, 1)
        assert bond_flags("2024年北京市政府一般债券(二期)") == (0, 0)


# ------------------------------------------------------- LGB PDF annex


@pytest.fixture(scope="module")
def annex():
    from backend.fetchers.fiscal_china import parse_lgb_annex
    return parse_lgb_annex(str(FIXTURES / "lgb_202605_annex.pdf"))


class TestParseLgbAnnex:

    def test_period_detected(self, annex):
        assert (annex["year"], annex["month"]) == (2026, 5)

    def test_debt_service_by_province(self, annex):
        rows = annex["provinces"]
        assert rows["河北"]["principal_repaid_month"] == 88
        assert rows["河北"]["principal_repaid_ytd"] == 952
        assert rows["河北"]["interest_paid_month"] == 69
        assert rows["河北"]["interest_paid_ytd"] == 295
        # Beijing repaid nothing in May — blank cell must land as 0, not shift columns
        assert rows["北京"]["principal_repaid_month"] == 0
        assert rows["北京"]["principal_repaid_ytd"] == 430
        assert rows["北京"]["interest_paid_month"] == 6
        assert rows["北京"]["interest_paid_ytd"] == 117

    def test_month_issuance_by_province(self, annex):
        rows = annex["provinces"]
        assert rows["江苏"]["issue_new"] == 0        # blank row section in 附表1
        assert rows["江苏"]["issue_refi"] == 711
        assert rows["江苏"]["issue_total"] == 711
        assert rows["河北"]["issue_new"] == 264
        assert rows["河北"]["issue_new_general"] == 70
        assert rows["河北"]["issue_new_special"] == 194

    def test_ytd_issuance_by_province(self, annex):
        rows = annex["provinces"]
        assert rows["江苏"]["issue_new_ytd"] == 1528
        assert rows["江苏"]["issue_refi_ytd"] == 2596
        assert rows["江苏"]["issue_total_ytd"] == 4124

    def test_subregion_rows_are_skipped(self, annex):
        # 其中：辽宁地区 / 大连市 sub-rows must not appear; 辽宁 row is the combined total
        assert "辽宁地区" not in annex["provinces"]
        assert "大连" not in annex["provinces"]
        assert annex["provinces"]["辽宁"]["issue_total"] == 156

    def test_national_total_row(self, annex):
        assert annex["provinces"]["全国"]["issue_total"] == 8047
        assert annex["provinces"]["全国"]["interest_paid_ytd"] == 6239


# ------------------------------------------------------- final accounts (annual)


class TestParseRegionTable:
    def test_transfers_by_region(self):
        from backend.fetchers.fiscal_china import parse_region_table
        header, rows = parse_region_table(_read("transfers_2024.html"))
        assert "决算数" in "".join(header)
        assert rows["北京"] == [1086.65, 1126.00]
        assert rows["新疆"][1] > 0
        assert "合计" not in rows and "未落实到地区数" not in rows

    def test_debt_balance_by_region(self):
        from backend.fetchers.fiscal_china import parse_region_table
        header, rows = parse_region_table(_read("special_debt_region_2024.html"))
        assert rows["北京"] == [10663.10, 10805.10, 9890.10]
        assert rows["天津"][2] == 9889.17
        assert len(rows) == 31

    def test_provincial_sums_cross_check_national(self, annex):
        rows = annex["provinces"]
        for metric in ("interest_paid_ytd", "principal_repaid_ytd", "issue_total_ytd"):
            prov_sum = sum(r.get(metric, 0) for k, r in rows.items() if k != "全国")
            national = rows["全国"][metric]
            assert abs(prov_sum - national) / national < 0.01  # rounding only
