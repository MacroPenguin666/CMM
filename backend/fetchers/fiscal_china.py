"""
China fiscal-capacity fetcher — MOF monthly releases + LGB debt reports.

Sources (all verified reachable from outside China, 2026-07-14):
  - 收支 monthly:  www.mof.gov.cn/zhengwuxinxi/caizhengshuju/ (aggregator, absolute
                   links) and gks.mof.gov.cn/tongjishuju/ (archive; paginated)
  - LGB reports:   zwgls.mof.gov.cn/tjsj/ (债务管理司, 2025-12 onward) and
                   yss.mof.gov.cn/zhuantilanmu/dfzgl/sjtj/ (history to 2024).
                   Both hosts intermittently 502 — every request retries.

Writes fiscal_national_monthly / fiscal_lgb_monthly in data/cmm.db.
All amounts are 亿元 (100m CNY) cumulative-YTD as published; growth is the
release's own same-period yoy %.
"""

import logging
import re
import time
from html import unescape
from pathlib import Path
from urllib.parse import urljoin

import requests

from backend.storage import get_conn

log = logging.getLogger("fiscal_china")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept-Language": "zh-CN,zh;q=0.9",
}

SHOUZHI_LISTINGS = [
    "https://www.mof.gov.cn/zhengwuxinxi/caizhengshuju/",
    "http://gks.mof.gov.cn/tongjishuju/",
    "http://gks.mof.gov.cn/tongjishuju/index_1.htm",
]
LGB_LISTINGS = [
    "https://zwgls.mof.gov.cn/tjsj/",
    "https://yss.mof.gov.cn/zhuantilanmu/dfzgl/sjtj/",
    "https://yss.mof.gov.cn/zhuantilanmu/dfzgl/sjtj/index_1.htm",
]

_NUM = r"(\d+(?:\.\d+)?)"
# growth phrase: 同比增长4% / 同比下降0.1% / 比上年增长2.4% / 持平
_GROWTH = r"(?:同比|比上年)(?:(增长|下降)" + _NUM + r"%|持平)"


def _get_fiscal_db():
    """Open cmm.db and ensure the fiscal tables exist (DDL lives in schema.sql).

    busy_timeout: fiscal fetchers legitimately run in parallel processes
    (registry backfill + MOF pass) — wait out each other's commits instead
    of raising 'database is locked'.
    """
    conn = get_conn()
    conn.execute("PRAGMA busy_timeout = 60000")
    schema = (Path(__file__).parent.parent / "schema.sql").read_text()
    conn.executescript(schema[schema.index("-- CHINA FISCAL CAPACITY"):])
    return conn


def get_fiscal_db():
    return _get_fiscal_db()


# ---------------------------------------------------------------------------
# text normalisation — MOF pages split digits across tags ("1</span>0.4"),
# so strip tags first, then remove ALL whitespace (safe: Chinese has no spaces)
# ---------------------------------------------------------------------------

def _strip(html: str) -> str:
    text = re.sub(r"<script.*?</script>", "", html, flags=re.S | re.I)
    text = re.sub(r"<style.*?</style>", "", text, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", "", text)
    text = unescape(text)
    return re.sub(r"[\s　\xa0]+", "", text)


def _growth_to_pct(direction, num) -> float:
    if num is None:
        return 0.0  # 持平
    val = float(num)
    return -val if direction == "下降" else val


# ---------------------------------------------------------------------------
# 收支 monthly release parser
# ---------------------------------------------------------------------------

# label-regex → metric name; matched against whitespace-stripped text
_SHOUZHI_METRICS = [
    (r"全国一般公共预算收入", "gpb_rev"),
    (r"全国税收收入", "tax_rev"),
    (r"非税收入", "nontax_rev"),
    (r"中央一般公共预算收入", "gpb_rev_central"),
    (r"地方一般公共预算本级收入", "gpb_rev_local"),
    (r"全国一般公共预算支出", "gpb_exp"),
    (r"中央一般公共预算本级支出", "gpb_exp_central"),
    (r"地方一般公共预算支出", "gpb_exp_local"),
    (r"债务付息支出", "debt_interest_exp"),
    (r"全国政府性基金预算收入", "fund_rev"),
    (r"中央政府性基金预算收入", "fund_rev_central"),
    (r"地方政府性基金预算本级收入", "fund_rev_local"),
    (r"国有土地使用权出让收入(?!相关)", "land_sale_rev"),
    (r"全国政府性基金预算支出", "fund_exp"),
    (r"中央政府性基金预算本级支出", "fund_exp_central"),
    (r"地方政府性基金预算支出", "fund_exp_local"),
    (r"国有土地使用权出让收入相关支出", "land_related_exp"),
    (r"国内增值税", "tax_vat"),
    (r"国内消费税", "tax_consumption"),
    (r"企业所得税", "tax_cit"),
    (r"个人所得税", "tax_pit"),
    (r"进口货物增值税、消费税", "tax_import_vat"),
    (r"。关税", "tax_tariff"),
    (r"出口(?:货物退增值税、消费税|退税)", "tax_export_rebate"),
    (r"城市维护建设税", "tax_urban_maint"),
    (r"车辆购置税", "tax_vehicle_purchase"),
    (r"(?<!交易)印花税", "tax_stamp"),
    (r"证券交易印花税", "tax_stamp_securities"),
    (r"资源税", "tax_resource"),
    (r"契税", "tax_deed"),
    (r"房产税", "tax_property"),
    (r"城镇土地使用税", "tax_urban_land_use"),
    (r"土地增值税", "tax_land_vat"),
    (r"耕地占用税", "tax_farmland_occup"),
    (r"环境保护税", "tax_env"),
]


def _parse_period(text: str, suffix: str):
    """Period from a release title like 2026年1-5月财政收支情况 / 2026年一季度… /
    2025年前三季度… / 2025年…（full year）."""
    m = re.search(r"(\d{4})年1-(\d{1,2})月" + suffix, text)
    if m:
        return int(m.group(1)), int(m.group(2))
    m = re.search(r"(\d{4})年(\d{1,2})月" + suffix, text)
    if m:
        return int(m.group(1)), int(m.group(2))
    for marker, month in (("一季度", 3), ("上半年", 6), ("前三季度", 9)):
        m = re.search(r"(\d{4})年" + marker + suffix, text)
        if m:
            return int(m.group(1)), month
    m = re.search(r"(\d{4})年" + suffix, text)
    if m:
        return int(m.group(1)), 12
    raise ValueError(f"no period found for {suffix}")


def parse_shouzhi(html: str) -> dict:
    """Parse a MOF 财政收支情况 release → {year, month, metrics: {name: (亿元, yoy%)}}."""
    text = _strip(html)
    year, month = _parse_period(text, "财政收支情况")
    metrics = {}
    for label_rx, name in _SHOUZHI_METRICS:
        m = re.search(label_rx + _NUM + r"亿元[，,]" + _GROWTH, text)
        if m:
            metrics[name] = (float(m.group(1)), _growth_to_pct(m.group(2), m.group(3)))
    return {"year": year, "month": month, "metrics": metrics}


# ---------------------------------------------------------------------------
# LGB 发行和债务余额 report parser
# ---------------------------------------------------------------------------

def _triple(text, pattern, unit="亿元"):
    """Match total + 一般/专项 split: <pattern>N<unit>，其中一般债券N<unit>[、，]专项债券N<unit>."""
    m = re.search(pattern + _NUM + unit + r"，其中一般债[券务]" + _NUM + unit
                  + r"[、，]专项债[券务]" + _NUM + unit, text)
    if not m:
        return None
    return float(m.group(1)), float(m.group(2)), float(m.group(3))


def _issuance_block(section: str, out: dict, suffix: str = ""):
    new = _triple(section, r"新增(?:地方政府)?债券")
    if new:
        out[f"issue_new{suffix}"], out[f"issue_new_general{suffix}"], \
            out[f"issue_new_special{suffix}"] = new
    refi = _triple(section, r"再融资债券")
    if refi:
        out[f"issue_refi{suffix}"], out[f"issue_refi_general{suffix}"], \
            out[f"issue_refi_special{suffix}"] = refi
    total = _triple(section, r"(?:合计，全国发行地方政府债券|全国发行地方政府债券合计)")
    if total:
        out[f"issue_total{suffix}"], out[f"issue_total_general{suffix}"], \
            out[f"issue_total_special{suffix}"] = total
    tenor = _triple(section, r"平均发行期限", unit="年")
    if tenor:
        key = "avg_tenor_ytd" if suffix else "avg_tenor"
        out[f"{key}_y"], out[f"{key}_general_y"], out[f"{key}_special_y"] = tenor
    rate = _triple(section, r"平均发行利率", unit="%")
    if rate:
        key = "avg_rate_ytd" if suffix else "avg_rate"
        out[f"{key}_pct"], out[f"{key}_general_pct"], out[f"{key}_special_pct"] = rate


def parse_lgb_report(html: str) -> dict:
    """Parse a 地方政府债券发行和债务余额情况 report → {year, month, metrics}."""
    text = _strip(html)
    year, month = _parse_period(text, r"地方政府债券发行和债务余额情况")
    out = {}

    def section(start, end):
        i = text.find(start)
        if i < 0:
            return ""
        j = text.find(end, i) if end else -1
        return text[i:j] if j > 0 else text[i:]

    _issuance_block(section("（一）", "（二）"), out)
    _issuance_block(section("（二）", "（三）"), out, suffix="_ytd")

    service = section("（三）", "二、")
    m = re.search(r"到期偿还本金" + _NUM + r"亿元，其中发行再融资债券偿还本金" + _NUM
                  + r"亿元[、，]安排财政资金等偿还本金" + _NUM + r"亿元", service)
    if m:
        out["principal_repaid_ytd"] = float(m.group(1))
        out["principal_repaid_by_refi_ytd"] = float(m.group(2))
        out["principal_repaid_by_fiscal_ytd"] = float(m.group(3))
    m = re.search(r"月当月到期偿还本金" + _NUM + r"亿元", service)
    if m:
        out["principal_repaid_month"] = float(m.group(1))
    m = re.search(r"支付利息" + _NUM + r"亿元。其中，\d{1,2}月当月地方政府债券支付利息"
                  + _NUM + r"亿元", service)
    if m:
        out["interest_paid_ytd"] = float(m.group(1))
        out["interest_paid_month"] = float(m.group(2))

    stock = section("二、全国地方政府债务余额情况", "注")
    m = re.search(r"地方政府债务限额为" + _NUM + r"亿元，其中一般债务限额" + _NUM
                  + r"亿元[、，]专项债务限额" + _NUM + r"亿元", stock)
    if m:
        out["debt_limit"] = float(m.group(1))
        out["debt_limit_general"] = float(m.group(2))
        out["debt_limit_special"] = float(m.group(3))
    m = re.search(r"全国地方政府债务余额" + _NUM + r"亿元。其中，一般债务" + _NUM
                  + r"亿元，专项债务" + _NUM + r"亿元；政府债券" + _NUM
                  + r"亿元，非政府债券形式存量政府债务" + _NUM + r"亿元", stock)
    if m:
        out["debt_outstanding"] = float(m.group(1))
        out["debt_outstanding_general"] = float(m.group(2))
        out["debt_outstanding_special"] = float(m.group(3))
        out["bonds_outstanding"] = float(m.group(4))
        out["nonbond_outstanding"] = float(m.group(5))
    m = re.search(r"剩余平均年限" + _NUM + r"年，其中一般债券" + _NUM + r"年[、，]专项债券"
                  + _NUM + r"年；平均利率" + _NUM + r"%，其中一般债券" + _NUM
                  + r"%[、，]专项债券" + _NUM + r"%", stock)
    if m:
        out["stock_avg_tenor_y"] = float(m.group(1))
        out["stock_avg_tenor_general_y"] = float(m.group(2))
        out["stock_avg_tenor_special_y"] = float(m.group(3))
        out["stock_avg_rate_pct"] = float(m.group(4))
        out["stock_avg_rate_general_pct"] = float(m.group(5))
        out["stock_avg_rate_special_pct"] = float(m.group(6))

    return {"year": year, "month": month, "metrics": out}


# ---------------------------------------------------------------------------
# listing pages
# ---------------------------------------------------------------------------

_RELEVANT_TITLE = re.compile(r"\d{4}年.*(财政收支情况|地方政府债券发行和债务余额情况)$")


def parse_listing(html: str, base_url: str) -> list:
    """Extract (title, absolute_url) pairs for fiscal releases from a list page."""
    items = []
    for href, title in re.findall(r'<a[^>]+href="([^"]+)"[^>]*>([^<]+)</a>', html):
        title = unescape(title).strip()
        if _RELEVANT_TITLE.search(title):
            items.append((title, urljoin(base_url, unescape(href))))
    return items


# ---------------------------------------------------------------------------
# MOF final accounts (annual, by region): transfers + debt limits/balances
# ---------------------------------------------------------------------------

PROVINCES_CN = {
    "北京", "天津", "河北", "山西", "内蒙古", "辽宁", "吉林", "黑龙江", "上海",
    "江苏", "浙江", "安徽", "福建", "江西", "山东", "河南", "湖北", "湖南",
    "广东", "广西", "海南", "重庆", "四川", "贵州", "云南", "西藏", "陕西",
    "甘肃", "青海", "宁夏", "新疆",
}

# English names used by bruegel_provincial → CN (for GDP/pop joins at read time)
PROVINCE_EN_CN = {
    "Beijing": "北京", "Tianjin": "天津", "Hebei": "河北", "Shanxi": "山西",
    "Inner Mongolia": "内蒙古", "Liaoning": "辽宁", "Jilin": "吉林",
    "Heilongjiang": "黑龙江", "Shanghai": "上海", "Jiangsu": "江苏",
    "Zhejiang": "浙江", "Anhui": "安徽", "Fujian": "福建", "Jiangxi": "江西",
    "Shandong": "山东", "Henan": "河南", "Hubei": "湖北", "Hunan": "湖南",
    "Guangdong": "广东", "Guangxi": "广西", "Hainan": "海南", "Chongqing": "重庆",
    "Sichuan": "四川", "Guizhou": "贵州", "Yunnan": "云南", "Tibet": "西藏",
    "Shaanxi": "陕西", "Gansu": "甘肃", "Qinghai": "青海", "Ningxia": "宁夏",
    "Xinjiang": "新疆",
}

# hub-link title pattern → (metric names per numeric column, or special handling)
_FA_TABLES = [
    (r"中央对地方一般性转移支付分地区决算表", "transfers_general"),
    (r"中央对地方专项转移支付分地区决算表", "transfers_special"),
    (r"中央对地方政府性基金转移支付分地区决算表", "transfers_fund"),
    (r"一般债务分地区余额表", "debt_general"),
    (r"专项债务分地区余额表", "debt_special"),
]


def parse_region_table(html: str):
    """Parse a MOF by-region table → (header_cells, {province_cn: [floats]}).

    Non-province rows (合计, 未落实到地区数, blank) are dropped; province names
    are normalised (新疆维吾尔自治区（含新疆生产建设兵团） → 新疆).
    """
    i = html.find("<table")
    tbl = html[i:html.find("</table>") + 8] if i >= 0 else ""
    header, rows = [], {}
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", tbl, re.S):
        cells = [re.sub(r"<[^>]+>|&nbsp;|[\s　]", "", c)
                 for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, re.S)]
        if not any(cells):
            continue
        if not header:
            if len(cells) >= 2 and ("地区" in cells[0] or "项目" in cells[0]):
                header = cells
            continue
        prov = _annex_province(cells[0])
        if prov not in PROVINCES_CN:
            continue
        vals = []
        for c in cells[1:]:
            try:
                vals.append(float(c.replace(",", "")))
            except ValueError:
                vals.append(None)
        rows[prov] = vals
    return header, rows


def fetch_final_accounts(conn, years=range(2020, 2031)) -> int:
    """Ingest by-region transfer + debt tables from yss.mof.gov.cn/{Y}zyjs hubs."""
    stored = 0
    for year in years:
        hub_url = f"https://yss.mof.gov.cn/{year}zyjs/"
        hub = _get(hub_url, retries=3)
        if not hub:
            continue
        links = re.findall(r'<a[^>]+href="([^"]+)"[^>]*>([^<]+)</a>', hub)
        for pattern, kind in _FA_TABLES:
            match = [(u, t) for u, t in links if re.search(pattern, t)]
            if not match:
                continue
            url = urljoin(hub_url, unescape(match[0][0]))
            page = _get(url, retries=3)
            if not page:
                continue
            header, rows = parse_region_table(page)
            if not rows:
                continue
            out = []
            for prov, vals in rows.items():
                if kind.startswith("transfers_"):
                    # columns: 预算数, 决算数 — prefer 决算 (final), fall back budget
                    value = next((v for v in reversed(vals) if v is not None), None)
                    if value is not None:
                        out.append((prov, year, kind, value))
                else:
                    # debt tables: 限额 [,限额调整后], 余额 — last col is balance,
                    # first non-null is the limit (adjusted preferred when present)
                    non_null = [v for v in vals if v is not None]
                    if len(non_null) >= 2:
                        out.append((prov, year, f"{kind}_limit", non_null[-2]))
                        out.append((prov, year, f"{kind}_balance", non_null[-1]))
            conn.executemany(
                "INSERT OR REPLACE INTO fiscal_province_annual"
                " (province, year, metric, value, source) VALUES (?,?,?,?, 'mof_final_accounts')",
                out)
            conn.commit()
            stored += len(out)
            log.info(f"  final accounts {year} {kind}: {len(out)} rows")
            time.sleep(1.5)
    return stored


# ---------------------------------------------------------------------------
# Curated reference facts (no public machine-readable feed exists — each entry
# carries its citation; UI must label these as curated)
# ---------------------------------------------------------------------------

FISCAL_REFERENCE_SEED = [
    # 12 provinces under borrowing restrictions (高风险债务省份)
    ("restricted_provinces", "",
     '["天津","内蒙古","辽宁","吉林","黑龙江","广西","重庆","贵州","云南","甘肃","青海","宁夏"]',
     "2023-12",
     "国办发〔2023〕35/47号 framework as reported; GS 'China H2 Fiscal Outlook' (2025-08-06) Ex.2 "
     "'12 provinces with high debt pressure'"),
    # VAT central/local sharing history
    ("vat_split_history", "",
     '[{"from": 1994, "central": 0.75, "local": 0.25}, {"from": 2016, "central": 0.5, "local": 0.5}]',
     "2019-10",
     "1994 分税制; 国发〔2016〕26号 transition plan; made permanent by State Council 2019 "
     "(cf. ADB 'Fiscal Rules in Monetary Union' EAWP 251113 §Central-Local Revenue Sharing)"),
    # 2024-28 hidden-debt resolution package
    ("swap_program", "",
     '{"swap_quota_100m": 60000, "special_bond_100m": 40000, "shantytown_100m": 20000,'
     ' "period": "2024-2028", "announced": "2024-11-08"}',
     "2024-11-08",
     "NPC Standing Committee 2024-11-08 (Xinhua); 6tn swap quota + 4tn from special-bond quotas "
     "+ 2tn shantytown bonds due 2029+"),
    # Net share of gross land-sale revenue that is usable financing
    ("net_land_share", "",
     '{"share": 0.33, "note": "net of land acquisition & redevelopment costs"}',
     "2025-08",
     "GS 'China H2 Fiscal Outlook' (2025-08-06) Exhibit 16: net land financing ~1.0-1.1% of GDP "
     "vs ~3% gross → ~1/3 net share (documented assumption)"),
    # AFD channels we cannot compute from public feeds (% of GDP, GS estimates)
    ("afd_excluded_channels", "",
     '{"policy_bank_support": {"2024": 0.2, "2025": 0.6, "2026": 1.2},'
     ' "lgfv_bond_net": {"2024": 0.0, "2025": -0.2, "2026": -0.2},'
     ' "railway_bond_net": {"2024": -0.1, "2025": 0.1, "2026": 0.0}}',
     "2025-08",
     "GS 'China H2 Fiscal Outlook' (2025-08-06) Exhibit 16 rows 8-12 — WIND-sourced, not "
     "publicly replicable; carried as curated adjustment"),
    # National LGFV interest-bearing debt stock estimate
    ("lgfv_debt_est", "",
     '{"total_100m": 570000, "note": "augmented-concept LGFV debt; no public per-province feed"}',
     "2023-12",
     "IMF Article IV People's Republic of China 2023 Selected Issues (augmented debt framework); "
     "order of magnitude also in Shih & Elkobi (2023, UCSD)"),
]


def seed_fiscal_reference(conn) -> int:
    conn.executemany(
        "INSERT OR REPLACE INTO fiscal_reference (key, province, value_json, as_of, citation)"
        " VALUES (?,?,?,?,?)", FISCAL_REFERENCE_SEED)
    conn.commit()
    return len(FISCAL_REFERENCE_SEED)


# ---------------------------------------------------------------------------
# fetch + store (network glue; parsers above are the tested core)
# ---------------------------------------------------------------------------

def _get(url: str, retries: int = 4, timeout: int = 25):
    """GET with retry — MOF hosts intermittently 502 behind their CDN."""
    last = None
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            r.encoding = r.apparent_encoding
            if r.status_code == 200 and "Bad Gateway" not in r.text[:400]:
                return r.text
            last = f"HTTP {r.status_code}"
        except requests.RequestException as e:
            last = type(e).__name__
        time.sleep(3 * (attempt + 1))
    log.warning(f"  giving up on {url} ({last})")
    return None


def _store_metrics(conn, table, year, month, metrics, url):
    for name, val in metrics.items():
        if table == "fiscal_national_monthly":
            value, yoy = val
            conn.execute(
                f"INSERT OR REPLACE INTO {table} (year, month, metric, value_100m, yoy_pct, source_url)"
                " VALUES (?,?,?,?,?,?)", (year, month, name, value, yoy, url))
        else:
            conn.execute(
                f"INSERT OR REPLACE INTO {table} (year, month, metric, value, source_url)"
                " VALUES (?,?,?,?,?)", (year, month, name, val, url))
    conn.commit()


def _known_urls(conn, table) -> set:
    return {r[0] for r in conn.execute(
        f"SELECT DISTINCT source_url FROM {table} WHERE source_url IS NOT NULL")}


def fetch_shouzhi(conn, full: bool = False) -> int:
    """Discover + parse 收支 releases. Returns number of releases ingested."""
    known = set() if full else _known_urls(conn, "fiscal_national_monthly")
    seen, count = set(), 0
    for listing in SHOUZHI_LISTINGS:
        html = _get(listing)
        if not html:
            continue
        for title, url in parse_listing(html, listing):
            if "财政收支情况" not in title or url in known or url in seen:
                continue
            seen.add(url)
            page = _get(url)
            if not page:
                continue
            try:
                parsed = parse_shouzhi(page)
            except ValueError as e:
                log.warning(f"  skip {title}: {e}")
                continue
            _store_metrics(conn, "fiscal_national_monthly",
                           parsed["year"], parsed["month"], parsed["metrics"], url)
            log.info(f"  收支 {parsed['year']}-{parsed['month']:02d}: "
                     f"{len(parsed['metrics'])} metrics ({title})")
            count += 1
            time.sleep(1.5)
    return count


def _fetch_annex(conn, page_html: str, page_url: str):
    """Download + ingest the report's 附表 PDF (by-province flows), if present."""
    m = re.search(r'href="([^"]+\.pdf)"', page_html, re.I)
    if not m:
        return
    pdf_url = urljoin(page_url, unescape(m.group(1)))
    try:
        r = requests.get(pdf_url, headers=HEADERS, timeout=40)
        r.raise_for_status()
    except requests.RequestException as e:
        log.warning(f"  annex fetch failed {pdf_url}: {type(e).__name__}")
        return
    import io
    try:
        annex = parse_lgb_annex(io.BytesIO(r.content))
    except Exception as e:
        log.warning(f"  annex parse failed {pdf_url}: {type(e).__name__}: {e}")
        return
    if not annex["year"] or not annex["provinces"]:
        return
    rows = [(prov, annex["year"], annex["month"], metric, value, pdf_url)
            for prov, metrics in annex["provinces"].items()
            for metric, value in metrics.items()]
    conn.executemany(
        "INSERT OR REPLACE INTO fiscal_lgb_province_monthly"
        " (province, year, month, metric, value_100m, source_url) VALUES (?,?,?,?,?,?)",
        rows)
    conn.commit()
    log.info(f"  annex {annex['year']}-{annex['month']:02d}: "
             f"{len(annex['provinces'])} provinces, {len(rows)} rows")


def fetch_lgb_reports(conn, full: bool = False) -> int:
    """Discover + parse LGB debt reports (+ their by-province PDF annexes).

    A report only counts as known once BOTH its metrics and its annex month
    are stored, so flaky-host runs converge over repeated invocations.
    """
    if full:
        known = set()
    else:
        known = {r[0] for r in conn.execute(
            """SELECT DISTINCT l.source_url FROM fiscal_lgb_monthly l
               JOIN fiscal_lgb_province_monthly p
                 ON l.year = p.year AND l.month = p.month
               WHERE l.source_url IS NOT NULL""")}
    seen, count = set(), 0
    for listing in LGB_LISTINGS:
        html = _get(listing)
        if not html:
            continue
        for title, url in parse_listing(html, listing):
            if "债务余额情况" not in title or url in known or url in seen:
                continue
            seen.add(url)
            page = _get(url)
            if not page:
                continue
            try:
                parsed = parse_lgb_report(page)
            except ValueError as e:
                log.warning(f"  skip {title}: {e}")
                continue
            _store_metrics(conn, "fiscal_lgb_monthly",
                           parsed["year"], parsed["month"], parsed["metrics"], url)
            _fetch_annex(conn, page, url)
            log.info(f"  LGB {parsed['year']}-{parsed['month']:02d}: "
                     f"{len(parsed['metrics'])} metrics ({title})")
            count += 1
            time.sleep(1.5)
    return count


# ---------------------------------------------------------------------------
# LGB report PDF annex (附表) — by-province monthly issuance & debt service.
# The PDF has no ruling lines, so tables are rebuilt from word x-positions:
# the 合计 row (always fully populated) anchors the column grid.
# ---------------------------------------------------------------------------

_ANNEX_COLS = {
    1: ["issue_new", "issue_new_general", "issue_new_special",
        "issue_refi", "issue_refi_general", "issue_refi_special",
        "issue_total", "issue_total_general", "issue_total_special"],
    2: ["issue_new_ytd", "issue_new_general_ytd", "issue_new_special_ytd",
        "issue_refi_ytd", "issue_refi_general_ytd", "issue_refi_special_ytd",
        "issue_total_ytd", "issue_total_general_ytd", "issue_total_special_ytd"],
    3: ["principal_repaid_month", "principal_repaid_ytd",
        "interest_paid_month", "interest_paid_ytd"],
}
_SKIP_ROW_NAMES = {"大连市", "宁波市", "厦门市", "青岛市", "深圳市", "兵团",
                   "新疆生产建设兵团"}
_NUM_TOKEN = re.compile(r"^-?[\d,]+(?:\.\d+)?$")


def _annex_province(raw: str):
    """Row label → normalised province name, or None if the row is skipped."""
    name = re.sub(r"[\s　]", "", raw)
    if not name or "其中" in name or name.endswith("地区") or name in _SKIP_ROW_NAMES:
        return None
    if name.startswith("合计"):
        return "全国"
    name = re.sub(r"（[^）]*）", "", name)
    name = re.sub(r"(维吾尔|壮族|回族)?自治区$|省$|市$", "", name)
    return name or None


def _annex_lines(page):
    """Cluster page words into lines: [(joined_cjk_text, [(x_center, float)])].

    Proximity clustering on `top` (new line when the gap exceeds 3pt) — fixed
    bucketing splits rows whose label and numbers differ by ~1pt across a
    bucket boundary (e.g. 海南省 at 614.4 vs its numbers at 615.9).
    """
    words_sorted = sorted(page.extract_words(), key=lambda w: w["top"])
    lines, current, last_top = [], [], None
    for w in words_sorted:
        if last_top is not None and w["top"] - last_top > 3:
            lines.append(current)
            current = []
        current.append(w)
        last_top = w["top"]
    if current:
        lines.append(current)
    out = []
    for line in lines:
        words = sorted(line, key=lambda w: w["x0"])
        text_parts, nums = [], []
        for w in words:
            token = w["text"].strip()
            if _NUM_TOKEN.match(token):
                nums.append(((w["x0"] + w["x1"]) / 2, float(token.replace(",", ""))))
            else:
                text_parts.append(token)
        out.append(("".join(text_parts), nums))
    return out


def parse_lgb_annex(pdf_path) -> dict:
    """Parse a 附表 PDF → {year, month, provinces: {name: {metric: value}}}."""
    import pdfplumber

    provinces = {}
    year = month = None
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            lines = _annex_lines(page)
            table_no = None
            for text, _ in lines:
                m = re.search(r"附表(\d)", text)
                if m:
                    table_no = int(m.group(1))
                if year is None:
                    m = re.search(r"(\d{4})年(\d{1,2})月地方政府债券发行情况", text)
                    if m:
                        year, month = int(m.group(1)), int(m.group(2))
            cols = _ANNEX_COLS.get(table_no)
            if not cols:
                continue
            anchors = None
            for text, nums in lines:
                clean = re.sub(r"[\s　]", "", text)
                if anchors is None:
                    if clean.startswith("合计") and len(nums) == len(cols):
                        anchors = [x for x, _ in nums]
                        provinces.setdefault("全国", {}).update(
                            {c: v for c, (_, v) in zip(cols, nums)})
                    continue
                prov = _annex_province(text)
                if prov is None or prov == "全国":
                    continue
                row = provinces.setdefault(prov, {})
                for c in cols:
                    row.setdefault(c, 0.0)
                for x, v in nums:
                    idx = min(range(len(anchors)), key=lambda i: abs(anchors[i] - x))
                    row[cols[idx]] = v
    return {"year": year, "month": month, "provinces": provinces}


# ---------------------------------------------------------------------------
# ChinaMoney LGB registry → bond-level table + maturity wall
# ---------------------------------------------------------------------------

CM_LIST_URL = "https://www.chinamoney.com.cn/ags/ms/cm-u-bond-md/BondMarketInfoList2"
CM_DETAIL_URL = "https://www.chinamoney.com.cn/ags/ms/cm-u-bond-md/BondDetailInfo"
CM_HEADERS = {**HEADERS, "Referer": "https://www.chinamoney.com.cn/chinese/zqjc/",
              "Origin": "https://www.chinamoney.com.cn"}
LGB_BOND_TYPE = "100011"

# 计划单列市 issue their own bonds; roll up into parent provinces for the
# 31-province cross-section (新疆生产建设兵团 kept separate — no parent budget)
_CITY_TO_PROVINCE = {"大连": "辽宁", "宁波": "浙江", "厦门": "福建",
                     "青岛": "山东", "深圳": "广东"}


def province_from_issuer(issuer: str) -> str:
    name = re.sub(r"(维吾尔|壮族|回族)?自治区政府$|省政府$|市政府$|政府$", "", issuer)
    if issuer == "新疆生产建设兵团":
        return "新疆兵团"
    return _CITY_TO_PROVINCE.get(name, name)


def bond_flags(full_name: str) -> tuple:
    """(is_special, is_refi) from the bond's full name."""
    return int("专项" in full_name), int("再融资" in full_name)


def _cm_post(url, payload, retries=6):
    for attempt in range(retries):
        try:
            r = requests.post(url, data=payload, headers=CM_HEADERS, timeout=20)
            if r.status_code == 200:
                return r.json()
        except (requests.RequestException, ValueError):
            pass
        time.sleep(10 * (attempt + 1))  # 403 = rate limit; back off hard
    return None


def fetch_lgb_registry(conn, max_new: int = 100000, incremental: bool = True) -> int:
    """Page the ChinaMoney LGB list newest-first; fetch detail for unknown bonds.

    Resumable: known bond_codes are skipped (list pages are cheap; details are
    only fetched for unknown bonds). `incremental=True` stops after 3 fully-known
    pages — correct once the registry is complete, but a resumed BACKFILL must
    pass incremental=False to walk past the already-ingested newest pages.
    Returns number of bonds added.
    """
    known = {r[0] for r in conn.execute("SELECT bond_code FROM fiscal_lgb_bonds")}
    added, page, known_streak = 0, 1, 0
    stop_after_known_pages = 3 if incremental else 10 ** 9
    while added < max_new:
        d = _cm_post(CM_LIST_URL, {
            "pageNo": str(page), "pageSize": "15", "bondName": "", "bondCode": "",
            "issueEnty": "", "bondType": LGB_BOND_TYPE, "bondSpclPrjctVrty": "",
            "couponType": "", "issueYear": "", "entyDefinedCode": "", "rtngShrt": ""})
        if not d:
            log.warning(f"  registry list page {page} failed — stopping this run")
            break
        data = d["data"]
        records = data.get("resultList") or []
        if not records:
            break
        new_here = 0
        for rec in records:
            code = rec.get("bondDefinedCode")
            if not code or code in known:
                continue
            det = _cm_post(CM_DETAIL_URL, {"bondDefinedCode": code})
            if not det:
                continue
            info = det["data"]["bondBaseInfo"]
            full = info.get("bondFullName") or ""
            issuer = info.get("entyFullName") or ""
            try:
                amount = float(info.get("issueAmnt"))
            except (TypeError, ValueError):
                amount = None
            special, refi = bond_flags(full)
            conn.execute(
                "INSERT OR REPLACE INTO fiscal_lgb_bonds (bond_code, bond_name,"
                " bond_full_name, issuer, province, issue_date, maturity_date,"
                " amount_100m, tenor, is_special, is_refi)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (code, info.get("bondName"), full, issuer, province_from_issuer(issuer),
                 info.get("issueDate"), info.get("mrtyDate"), amount,
                 info.get("bondPeriod"), special, refi))
            # commit per insert: a transaction must never stay open across the
            # next network call, or it write-locks cmm.db for minutes
            conn.commit()
            known.add(code)
            added += 1
            new_here += 1
            if added % 200 == 0:
                log.info(f"  registry: {added} bonds added (page {page})")
            time.sleep(1.2)
        conn.commit()
        known_streak = known_streak + 1 if new_here == 0 else 0
        if known_streak >= stop_after_known_pages:
            break
        if page >= int(data.get("pageTotal") or 1):
            break
        page += 1
        time.sleep(2.0)
    return added


def rebuild_maturity(conn) -> int:
    """Aggregate fiscal_lgb_bonds → fiscal_maturity (province × maturity year)."""
    conn.execute("DELETE FROM fiscal_maturity")
    conn.execute("""
        INSERT INTO fiscal_maturity (province, maturity_year, principal_100m, n_bonds)
        SELECT province, CAST(substr(maturity_date, 1, 4) AS INTEGER),
               SUM(amount_100m), COUNT(*)
        FROM fiscal_lgb_bonds
        WHERE maturity_date IS NOT NULL AND amount_100m IS NOT NULL
        GROUP BY 1, 2""")
    conn.execute("""
        INSERT INTO fiscal_maturity (province, maturity_year, principal_100m, n_bonds)
        SELECT '全国', CAST(substr(maturity_date, 1, 4) AS INTEGER),
               SUM(amount_100m), COUNT(*)
        FROM fiscal_lgb_bonds
        WHERE maturity_date IS NOT NULL AND amount_100m IS NOT NULL
        GROUP BY 2""")
    conn.commit()
    return conn.execute("SELECT COUNT(*) FROM fiscal_maturity").fetchone()[0]


# ---------------------------------------------------------------------------
# AKShare market/monetary sub-fetchers
#   curves:  ChinaMoney close yield curves (per-call window ≤ 1 month)
#   tsf:     社会融资规模增量 components (trust loans for the AFD shadow proxy)
#   pboc:    central-bank balance sheet → 政府存款 (fiscal deposits)
# ---------------------------------------------------------------------------

CURVES = {
    "cgb": "国债",
    "lgb_aaa": "地方政府债(AAA)",
    "cpmtn_aa": "中短期票据(AA)",
}
CURVE_BACKFILL_DAYS = 3 * 365 + 30  # 3y history for z-scores


def _month_chunks(start: str, end: str) -> list:
    """Split [start, end] (YYYYMMDD) into contiguous windows of ≤ 25 days."""
    from datetime import datetime, timedelta
    ds = datetime.strptime(start, "%Y%m%d")
    de = datetime.strptime(end, "%Y%m%d")
    chunks = []
    while ds <= de:
        ce = min(ds + timedelta(days=25), de)
        chunks.append((ds.strftime("%Y%m%d"), ce.strftime("%Y%m%d")))
        ds = ce + timedelta(days=1)
    return chunks


def _pboc_period(s: str) -> tuple:
    """'2026.5' → (2026, 5); '2026.10' → (2026, 10). Source column is str —
    a float here would collide .1/.10, so coerce upstream with astype(str)."""
    year, month = str(s).split(".")
    return int(year), int(month)


def transform_curves(df, curve: str) -> list:
    return [(curve, str(r["日期"]), float(r["期限"]), float(r["到期收益率"]))
            for _, r in df.iterrows() if r["到期收益率"] == r["到期收益率"]]


def transform_tsf(df) -> list:
    rows = []
    for _, r in df.iterrows():
        ym = str(r["月份"])
        year, month = int(ym[:4]), int(ym[4:6])
        rows.append((year, month, "tsf_flow", float(r["社会融资规模增量"])))
        if r.get("其中-信托贷款") == r.get("其中-信托贷款"):
            rows.append((year, month, "tsf_trust_flow", float(r["其中-信托贷款"])))
    return rows


def transform_pboc(df) -> list:
    rows = []
    for _, r in df.iterrows():
        if r["政府存款"] != r["政府存款"]:
            continue
        year, month = _pboc_period(r["统计时间"])
        rows.append((year, month, "fiscal_deposits", float(r["政府存款"])))
    return rows


# chinamoney close-curve codes (stable ids from bond_china_close_return_map)
CM_CURVE_CODES = {"lgb_aaa": "CYCC84A", "cpmtn_aa": "CYCC82E"}
CM_CURVE_URL = "https://www.chinamoney.com.cn/ags/ms/cm-u-bk-currency/ClsYldCurvHis"
CURVE_WINDOWS_PER_RUN = 6  # backward-accretion budget per invocation (rate limit)


def _cm_curve_window(code, start_iso, end_iso):
    """All (date, tenor, ytm) rows for a ≤1-month window, following pagination
    (the API caps pageSize at 50 = 2 days; AKShare only reads page 1)."""
    rows, page = [], 1
    while True:
        try:
            r = requests.get(CM_CURVE_URL, params={
                "lang": "CN", "reference": "1", "bondType": code,
                "startDate": start_iso, "endDate": end_iso,
                "termId": "1", "pageNum": str(page), "pageSize": "50",
            }, headers=CM_HEADERS, timeout=20)
            d = r.json()
            recs = d.get("records") or []
            page_total = int((d.get("data") or {}).get("pageTotal") or 1)
        except (requests.RequestException, ValueError):
            time.sleep(8)
            return rows, False
        for rec in recs:
            try:
                rows.append((rec["newDateValueCN"], float(rec["yearTermStr"]),
                             float(rec["maturityYieldStr"])))
            except (KeyError, TypeError, ValueError):
                continue
        if page >= page_total:
            return rows, True
        page += 1
        time.sleep(0.8)


def fetch_curves(conn) -> int:
    """Daily close curves. CGB comes in bulk from EastMoney (one call, 3y+);
    the LGB(AAA)/AA credit curves come from ChinaMoney — incremental forward,
    plus a capped backward-accretion so 3y of history builds up over runs."""
    import akshare as ak
    from datetime import datetime, timedelta
    total = 0
    today = datetime.now()

    # --- CGB via EastMoney: 2/5/10/30y daily ---
    try:
        start = (today - timedelta(days=CURVE_BACKFILL_DAYS)).strftime("%Y%m%d")
        df = ak.bond_zh_us_rate(start_date=start)
        rows = []
        for _, r in df.iterrows():
            for tenor, col in ((2.0, "中国国债收益率2年"), (5.0, "中国国债收益率5年"),
                               (10.0, "中国国债收益率10年"), (30.0, "中国国债收益率30年")):
                v = r.get(col)
                if v == v and v is not None:
                    rows.append(("cgb", str(r["日期"]), tenor, float(v)))
        conn.executemany(
            "INSERT OR REPLACE INTO fiscal_curves_daily (curve, date, tenor_y, yield_pct)"
            " VALUES (?,?,?,?)", rows)
        conn.commit()
        total += len(rows)
        log.info(f"  curve cgb: {len(rows)} rows (EastMoney bulk)")
    except Exception as e:
        log.warning(f"  curve cgb failed: {type(e).__name__}")

    # --- credit curves via ChinaMoney, forward + capped backward ---
    floor = (today - timedelta(days=CURVE_BACKFILL_DAYS)).strftime("%Y-%m-%d")
    budget = CURVE_WINDOWS_PER_RUN
    for curve, code in CM_CURVE_CODES.items():
        lo, hi = conn.execute(
            "SELECT MIN(date), MAX(date) FROM fiscal_curves_daily WHERE curve=?",
            (curve,)).fetchone()
        windows = []
        if hi:
            fwd_start = datetime.strptime(hi, "%Y-%m-%d") + timedelta(days=1)
            if fwd_start.date() <= today.date():
                windows += [(s, e) for s, e in _month_chunks(
                    fwd_start.strftime("%Y%m%d"), today.strftime("%Y%m%d"))]
        else:
            windows.append(((today - timedelta(days=25)).strftime("%Y%m%d"),
                            today.strftime("%Y%m%d")))
        if lo and lo > floor:
            back_end = datetime.strptime(lo, "%Y-%m-%d") - timedelta(days=1)
            back_start = max(back_end - timedelta(days=25 * budget),
                             datetime.strptime(floor, "%Y-%m-%d"))
            windows += list(reversed(_month_chunks(
                back_start.strftime("%Y%m%d"), back_end.strftime("%Y%m%d"))))
        for s, e in windows[:budget + 2]:
            iso = lambda d: f"{d[:4]}-{d[4:6]}-{d[6:]}"
            rows, ok = _cm_curve_window(code, iso(s), iso(e))
            if rows:
                conn.executemany(
                    "INSERT OR REPLACE INTO fiscal_curves_daily (curve, date, tenor_y, yield_pct)"
                    " VALUES (?,?,?,?)",
                    [(curve, d, t, y) for d, t, y in rows])
                conn.commit()
                total += len(rows)
            if not ok:
                log.warning(f"  curve {curve}: window {s}-{e} failed — stopping this run")
                break
            time.sleep(1.5)
        log.info(f"  curve {curve}: coverage {lo or '—'} → {hi or '—'} before this run")
    return total


def fetch_monetary(conn) -> int:
    """TSF components + fiscal deposits → fiscal_monetary_monthly."""
    import akshare as ak
    rows = []
    try:
        rows += transform_tsf(ak.macro_china_shrzgm())
    except Exception as e:
        log.warning(f"  shrzgm failed: {type(e).__name__}")
    try:
        df = ak.macro_china_central_bank_balance()
        df["统计时间"] = df["统计时间"].astype(str)
        rows += transform_pboc(df)
    except Exception as e:
        log.warning(f"  central_bank_balance failed: {type(e).__name__}")
    conn.executemany(
        "INSERT OR REPLACE INTO fiscal_monetary_monthly (year, month, metric, value_100m)"
        " VALUES (?,?,?,?)", rows)
    conn.commit()
    return len(rows)


def fetch_all_mof(full: bool = False):
    """Fetch both MOF release families. Returns (ok_count, fail_count)."""
    conn = get_fiscal_db()
    ok = fail = 0
    for fn in (fetch_shouzhi, fetch_lgb_reports):
        try:
            n = fn(conn, full=full)
            log.info(f"{fn.__name__}: {n} releases ingested")
            ok += 1
        except Exception as e:
            log.error(f"{fn.__name__} failed: {e}")
            fail += 1
    conn.close()
    return ok, fail
