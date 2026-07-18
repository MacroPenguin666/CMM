"""
Direct HTML scraper for Chinese ministry and government body announcement pages.

Most .gov.cn sites are publicly accessible from outside China without a proxy.
Supplements the RSS-based monitor with scraped content where no RSS is available.

Results are stored to the same `items` table via store_feed_result(), so they
appear in the dashboard and are available for the Policy Advisor.
"""

import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import urljoin

import requests
from lxml import html as lhtml

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ---------------------------------------------------------------------------
# Scraping targets
# Verified accessible from outside China as of 2026-05.
# Each entry: name, name_cn, category, url (list page), base (for relative hrefs)
# ---------------------------------------------------------------------------
TARGETS = [
    # =========================================================================
    # NDRC — news releases + full policy archive (xxgk/zcfb, ~66 pages back to 1999)
    # =========================================================================
    {
        "name": "NDRC — News Releases",
        "name_cn": "国家发展改革委-新闻发布",
        "category": "central_government",
        "doc_type": "新闻发布",
        "url": "https://www.ndrc.gov.cn/xwdt/xwfb/",
        "base": "https://www.ndrc.gov.cn",
        "description": "NDRC economic planning news and press releases",
    },
    {
        "name": "NDRC — Orders",
        "name_cn": "国家发展改革委-发展改革委令",
        "category": "central_government",
        "doc_type": "发展改革委令",
        "url": "https://www.ndrc.gov.cn/xxgk/zcfb/fzggwl/",
        "base": "https://www.ndrc.gov.cn",
        "description": "NDRC formal regulatory orders and decrees (9 pages, back to 1999)",
    },
    {
        "name": "NDRC — Normative Documents",
        "name_cn": "国家发展改革委-规范性文件",
        "category": "central_government",
        "doc_type": "规范性文件",
        "url": "https://www.ndrc.gov.cn/xxgk/zcfb/ghxwj/",
        "base": "https://www.ndrc.gov.cn",
        "description": "NDRC normative and regulatory documents (8 pages)",
    },
    {
        "name": "NDRC — Planning Documents",
        "name_cn": "国家发展改革委-规划文本",
        "category": "central_government",
        "doc_type": "规划文本",
        "url": "https://www.ndrc.gov.cn/xxgk/zcfb/ghwb/",
        "base": "https://www.ndrc.gov.cn",
        "description": "NDRC national planning documents and five-year plans (9 pages)",
    },
    {
        "name": "NDRC — Announcements",
        "name_cn": "国家发展改革委-公告",
        "category": "central_government",
        "doc_type": "公告",
        "url": "https://www.ndrc.gov.cn/xxgk/zcfb/gg/",
        "base": "https://www.ndrc.gov.cn",
        "description": "NDRC official announcements (20 pages)",
    },
    {
        "name": "NDRC — Notices",
        "name_cn": "国家发展改革委-通知",
        "category": "central_government",
        "doc_type": "通知",
        "url": "https://www.ndrc.gov.cn/xxgk/zcfb/tz/",
        "base": "https://www.ndrc.gov.cn",
        "description": "NDRC policy notices and circulars (20 pages)",
    },

    # =========================================================================
    # Other ministries
    # =========================================================================
    {
        "name": "MOFCOM — Orders & Announcements",
        "name_cn": "商务部-部令公告",
        "category": "ministry",
        "doc_type": "部令公告",
        "url": "http://www.mofcom.gov.cn/zcfb/blgg/",
        "base": "http://www.mofcom.gov.cn",
        "description": "MOFCOM ministerial orders and official announcements",
    },
    {
        "name": "MOFCOM — Regulations",
        "name_cn": "商务部-规章",
        "category": "ministry",
        "doc_type": "规章",
        "url": "http://www.mofcom.gov.cn/zcfb/zc/",
        "base": "http://www.mofcom.gov.cn",
        "description": "MOFCOM trade regulations and legal rules",
    },
    {
        "name": "MOFCOM — Comprehensive Policy",
        "name_cn": "商务部-综合政策",
        "category": "ministry",
        "doc_type": "综合政策",
        "url": "http://www.mofcom.gov.cn/zcfb/zhzc/",
        "base": "http://www.mofcom.gov.cn",
        "description": "MOFCOM comprehensive and cross-sector trade policy documents",
    },
    {
        "name": "MOFCOM — Domestic Trade Policy",
        "name_cn": "商务部-国内贸易管理",
        "category": "ministry",
        "doc_type": "国内贸易管理",
        "url": "http://www.mofcom.gov.cn/zcfb/gnmygl/",
        "base": "http://www.mofcom.gov.cn",
        "description": "MOFCOM domestic commerce and retail trade policy",
    },
    {
        "name": "MOFCOM — Foreign Trade Policy",
        "name_cn": "商务部-对外贸易管理",
        "category": "ministry",
        "doc_type": "对外贸易管理",
        "url": "http://www.mofcom.gov.cn/zcfb/dwmygl/",
        "base": "http://www.mofcom.gov.cn",
        "description": "MOFCOM foreign trade management policy",
    },
    {
        "name": "MOFCOM — Service Trade",
        "name_cn": "商务部-服务贸易",
        "category": "ministry",
        "doc_type": "服务贸易",
        "url": "http://www.mofcom.gov.cn/zcfb/fwmy/",
        "base": "http://www.mofcom.gov.cn",
        "description": "MOFCOM service trade policy",
    },
    {
        "name": "MOFCOM — Fair Trade",
        "name_cn": "商务部-公平贸易",
        "category": "ministry",
        "doc_type": "公平贸易",
        "url": "http://www.mofcom.gov.cn/zcfb/gpmy/",
        "base": "http://www.mofcom.gov.cn",
        "description": "MOFCOM fair trade, anti-dumping, and countervailing policy",
    },
    {
        "name": "SASAC — SOE News",
        "name_cn": "国务院国有资产监督管理委员会-新闻",
        "category": "regulator",
        "doc_type": "新闻",
        "url": "http://www.sasac.gov.cn/n2588025/n2588124/",
        "base": "http://www.sasac.gov.cn",
        "description": "SASAC state-owned enterprise supervision news",
    },
    {
        "name": "MFA — Press Briefings",
        "name_cn": "外交部-发言人答问",
        "category": "ministry",
        "doc_type": "发言人答问",
        "url": "https://www.mfa.gov.cn/wjdt_674879/fyrbt_674889/",
        "base": "https://www.mfa.gov.cn",
        "description": "Ministry of Foreign Affairs spokesperson press briefings",
    },
    {
        "name": "Ministry of Finance — News",
        "name_cn": "财政部-财政新闻",
        "category": "ministry",
        "doc_type": "新闻发布",
        "url": "http://www.mof.gov.cn/zhengwuxinxi/",
        "base": "http://www.mof.gov.cn",
        "description": "Ministry of Finance fiscal policy and financial news",
    },
    {
        "name": "Ministry of Finance — Policy Releases",
        "name_cn": "财政部-政策发布",
        "category": "ministry",
        "doc_type": "政策文件",
        "url": "http://www.mof.gov.cn/zhengwuxinxi/zhengcefabu/",
        "base": "http://www.mof.gov.cn",
        "description": "Ministry of Finance official policy releases (20 pages of history)",
    },
    {
        "name": "MIIT — Industrial Policy",
        "name_cn": "工业和信息化部-工作动态",
        "category": "ministry",
        "doc_type": "工作动态",
        "url": "https://www.miit.gov.cn/xwdt/",
        "base": "https://www.miit.gov.cn",
        "description": "MIIT industrial and information technology policy updates",
    },
    {
        "name": "State Council — Latest Policies",
        "name_cn": "国务院-最新政策",
        "category": "central_government",
        "doc_type": "最新政策",
        "url": "https://www.gov.cn/zhengce/",
        "base": "https://www.gov.cn",
        "description": "State Council top-level policy releases and regulations",
    },
    {
        "name": "SAMR — Market Regulation News",
        "name_cn": "市场监督管理总局-新闻",
        "category": "regulator",
        "doc_type": "新闻",
        "url": "https://www.samr.gov.cn/xw/",
        "base": "https://www.samr.gov.cn",
        "description": "State Administration for Market Regulation news and enforcement",
    },

    # -------------------------------------------------------------------------
    # Financial Regulators
    # -------------------------------------------------------------------------
    {
        "name": "PBOC — Policy Communications",
        "name_cn": "中国人民银行-货币政策",
        "category": "regulator",
        "doc_type": "政策沟通",
        "url": "http://www.pbc.gov.cn/rmyh/index.html",
        "base": "http://www.pbc.gov.cn",
        "description": "People's Bank of China monetary policy, financial stats, and press releases",
    },
    # NFRA (nfra.gov.cn) blocks all external traffic — excluded until accessible.
    {
        "name": "CSRC — Securities Regulation",
        "name_cn": "中国证监会-最新动态",
        "category": "regulator",
        "doc_type": "最新动态",
        "url": "http://www.csrc.gov.cn/csrc/c101954/",
        "base": "http://www.csrc.gov.cn",
        "description": "China Securities Regulatory Commission capital markets policy and enforcement",
    },
    {
        "name": "SAFE — FX Policy",
        "name_cn": "国家外汇管理局-通知公告",
        "category": "regulator",
        "doc_type": "通知公告",
        "url": "http://www.safe.gov.cn/safe/",
        "base": "http://www.safe.gov.cn",
        "description": "State Administration of Foreign Exchange — full listing page (168 items)",
    },
    # GAC (customs.gov.cn) and MPS (mps.gov.cn) block all external traffic.

    # -------------------------------------------------------------------------
    # Industry & Technology
    # -------------------------------------------------------------------------
    {
        "name": "MOST — Science & Technology Policy",
        "name_cn": "科学技术部-科技报告",
        "category": "ministry",
        "doc_type": "科技报告",
        "url": "https://www.most.gov.cn/kjbgz/",
        "base": "https://www.most.gov.cn",
        "description": "Ministry of Science and Technology R&D policy, tech regulations, and innovation funding",
    },
    {
        "name": "CAC — Internet & AI Regulation",
        "name_cn": "国家互联网信息办公室",
        "category": "regulator",
        "doc_type": "新闻发布",
        "url": "https://www.cac.gov.cn/",
        "base": "https://www.cac.gov.cn",
        "description": "Cyberspace Administration of China internet, data governance, and AI regulation",
    },
    {
        "name": "NEA — Energy Policy",
        "name_cn": "国家能源局-新闻",
        "category": "regulator",
        "doc_type": "新闻动态",
        "url": "http://www.nea.gov.cn/news/",
        "base": "http://www.nea.gov.cn",
        "description": "National Energy Administration energy policy, renewables, and electricity regulation",
    },

    # -------------------------------------------------------------------------
    # Social & Environment
    # -------------------------------------------------------------------------
    {
        "name": "MEE — Environmental Standards",
        "name_cn": "生态环境部-重要动态",
        "category": "ministry",
        "doc_type": "重要动态",
        "url": "https://www.mee.gov.cn/ywdt/spxw/",
        "base": "https://www.mee.gov.cn",
        "description": "Ministry of Ecology and Environment carbon policy and environmental standards",
    },
    {
        "name": "MEE — Laws & Standards",
        "name_cn": "生态环境部-法律法规",
        "category": "ministry",
        "doc_type": "法律法规",
        "url": "https://www.mee.gov.cn/ywgz/fgbz/fl/",
        "base": "https://www.mee.gov.cn",
        "description": "MEE environmental laws, regulations, and standards (3 pages)",
    },
    {
        "name": "MOFCOM — News Releases",
        "name_cn": "商务部-新闻发布",
        "category": "ministry",
        "doc_type": "新闻发布",
        "url": "http://www.mofcom.gov.cn/xwfb/",
        "base": "http://www.mofcom.gov.cn",
        "description": "Ministry of Commerce press releases and news",
    },
    {
        "name": "MEM — Notices & Announcements",
        "name_cn": "应急管理部-通知公告",
        "category": "ministry",
        "doc_type": "通知公告",
        "url": "https://www.mem.gov.cn/gk/tzgg/",
        "base": "https://www.mem.gov.cn",
        "description": "Ministry of Emergency Management notices and announcements",
    },
    {
        "name": "MEM — Government Information",
        "name_cn": "应急管理部-政府信息公开",
        "category": "ministry",
        "doc_type": "政府信息公开",
        "url": "https://www.mem.gov.cn/gk/zfxxgkpt/fdzdgknr/",
        "base": "https://www.mem.gov.cn",
        "description": "Ministry of Emergency Management mandatory disclosure documents",
    },
    {
        "name": "MCT — Government Information",
        "name_cn": "文化和旅游部-政府信息公开",
        "category": "ministry",
        "doc_type": "政府信息公开",
        "url": "https://zwgk.mct.gov.cn/zfxxgkml/index.html",
        "base": "https://zwgk.mct.gov.cn",
        "description": "Ministry of Culture and Tourism public information disclosure",
    },
    {
        "name": "MCT — Policies & Regulations",
        "name_cn": "文化和旅游部-政策法规",
        "category": "ministry",
        "doc_type": "政策法规",
        "url": "https://zwgk.mct.gov.cn/zfxxgkml/zcfg/",
        "base": "https://zwgk.mct.gov.cn",
        "description": "Ministry of Culture and Tourism policies and regulations",
    },
    # MOHRSS, MOE, MOT, MOHURD, MNR, MWR, NHC, MCA, MOJ, NHSA, STA (chinatax),
    # and MOD all block or time out for external traffic (verified 2026-07-07),
    # like NFRA/GAC/MPS above.
    {
        "name": "MARA — Agriculture Regulations",
        "name_cn": "农业农村部-政策法规",
        "category": "ministry",
        "doc_type": "政策法规",
        "url": "http://www.moa.gov.cn/zwllm/zcfg/",
        "base": "http://www.moa.gov.cn",
        "description": "Ministry of Agriculture and Rural Affairs agricultural policy regulations (83 items)",
    },
]

# ---------------------------------------------------------------------------
# Landmark documents that never appear on the scraped list pages above
# (published once via Xinhua/gov.cn 要闻). Shaped like stage-1 scrape results so
# insert_policy_metadata() consumes them unchanged; the content swarm then
# fetches the full text like any pending row.
# ---------------------------------------------------------------------------
SEED_DOCS = [
    {
        "source": "State Council — Landmark Documents",
        "source_cn": "国务院-纲领性文件",
        "category": "central_government",
        "doc_type": "规划纲要",
        "feed_url": "https://www.gov.cn/yaowen/",
        "ok": True,
        "entries": [
            {
                "link": "https://www.gov.cn/yaowen/liebiao/202603/content_7062633.htm",
                "title": "中华人民共和国国民经济和社会发展第十五个五年规划纲要",
                "published": "2026-03-13",
            },
        ],
    },
]


# ---------------------------------------------------------------------------
# Date parsing helpers
# ---------------------------------------------------------------------------
_DATE_PATTERNS = [
    re.compile(r'(\d{4})[/\-](\d{1,2})[/\-](\d{1,2})'),   # 2026/05/09 or 2026-05-09
    re.compile(r'(\d{4})年(\d{1,2})月(\d{1,2})日'),          # 2026年5月9日
    re.compile(r'(\d{4})年(\d{1,2})月'),                      # 2026年5月 (no day)
]

# Pattern in hrefs like t20260509_1234.html → 2026-05-09
_HREF_DATE = re.compile(r't(\d{4})(\d{2})(\d{2})_')
# CAC / many .gov.cn: /2026-05/15/c_... → 2026-05-15
_HREF_DATE2 = re.compile(r'/(\d{4})-(\d{2})/(\d{2})/')
# MIIT / similar: /art/2024/art_... → year 2024
_HREF_YEAR = re.compile(r'/art/(\d{4})/')
# Loose MM-DD in text (MIIT style: "09-19")
_MMDD = re.compile(r'\b(\d{1,2})-(\d{1,2})\b')


def _parse_date(text: str) -> str:
    text = text.strip()
    for pat in _DATE_PATTERNS:
        m = pat.search(text)
        if m:
            groups = m.groups()
            y, mo = groups[0], groups[1].zfill(2)
            day = groups[2].zfill(2) if len(groups) > 2 else "01"
            return f"{y}-{mo}-{day}"
    return ""


def _date_from_href(href: str) -> str:
    # t20260509_... pattern
    m = _HREF_DATE.search(href)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    # /2026-05/15/ pattern (CAC, Xinhua-style)
    m2 = _HREF_DATE2.search(href)
    if m2:
        return f"{m2.group(1)}-{m2.group(2)}-{m2.group(3)}"
    # PBOC: /20260514XXXXXXXXXX/ — 8-digit YYYYMMDD as prefix of long number
    m3 = re.search(r'/(20[0-3]\d[01]\d[0-3]\d)\d{5,}/', href)
    if m3:
        s = m3.group(1)
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    # /YYYYMM/ path segment — only accept plausible years (2000-2035)
    m4 = re.search(r'/(20[0-3]\d[01]\d)/', href)
    if m4:
        s = m4.group(1)
        return f"{s[:4]}-{s[4:6]}-01"
    # /art/YYYY/ — year only (SAMR, MIIT article archives)
    m5 = _HREF_YEAR.search(href)
    if m5:
        return f"{m5.group(1)}-01-01"
    return ""


def _date_from_text_and_href(text: str, href: str) -> str:
    """Extract date when text has MM-DD and year is embedded in href."""
    m = _MMDD.search(text)
    if m:
        year_m = _HREF_YEAR.search(href)
        if year_m:
            y = year_m.group(1)
            mo = m.group(1).zfill(2)
            day = m.group(2).zfill(2)
            # Sanity check: valid month/day
            if 1 <= int(mo) <= 12 and 1 <= int(day) <= 31:
                return f"{y}-{mo}-{day}"
    return ""


# ---------------------------------------------------------------------------
# HTML article extractor
# ---------------------------------------------------------------------------
_MIN_TITLE_LEN = 8
_NAV_SKIP = {"首页", "上一页", "下一页", "末页", "更多", ">>", "<<", "...", "返回", "查看更多"}
_NEXT_PAGE_TEXTS = {"下一页", "下页", "后页", "后一页", "next", ">", "»"}


def _root_domain(host: str) -> str:
    """mee.gov.cn / www.mee.gov.cn / hbdc.mee.gov.cn → mee.gov.cn"""
    parts = host.lower().split(".")
    # Chinese .gov.cn: take last 3 parts  (mee.gov.cn)
    if len(parts) >= 3 and parts[-1] == "cn" and parts[-2] == "gov":
        return ".".join(parts[-3:])
    return host.lower()

# createPageHTML(totalPages, currentPage, "basename", "ext") — embedded in an HTML comment
_CREATE_PAGE_RE = re.compile(
    r'createPageHTML\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*"([^"]+)"\s*,\s*"([^"]+)"\s*\)'
)
# document.write pager (MEM etc.): var currentPage = 0; ... var countPage = 25
_JS_PAGER_CUR = re.compile(r'var\s+currentPage\s*=\s*(\d+)')
_JS_PAGER_CNT = re.compile(r'var\s+countPage\s*=\s*(\d+)')
# Matches .../index_3.html or .../list_3.shtml style URLs
_INDEX_N_RE = re.compile(r'/(index|list)_(\d+)\.(s?html?)$', re.I)


def _find_next_page_url(tree, current_url: str, base_url: str) -> str | None:
    """Detect the next-page URL from pagination controls on a gov.cn list page.

    Strategies (in order):
    1. Explicit next-page anchor text (下一页, next, etc.)
    2. createPageHTML(total, current, base, ext) in an HTML comment
    3. index_N.html / list_N.html URL increment pattern
    """
    # 1. Explicit next-page link
    for a in tree.xpath("//a[@href]"):
        text = (a.text_content() or "").strip()
        href = a.get("href", "")
        if not href or href.startswith("javascript") or href.startswith("#"):
            continue
        if text in _NEXT_PAGE_TEXTS or "下一" in text:
            url = urljoin(current_url, href)
            if url != current_url:
                return url

    # 2. createPageHTML(total, current, basename, ext) — HTML comment or <script> tag
    for node in [*tree.xpath("//comment()"), *tree.xpath("//script")]:
        try:
            text = node.text_content()
        except (TypeError, ValueError):
            text = str(node)  # lxml comment nodes expose text via str()
        m = _CREATE_PAGE_RE.search(text or "")
        if m:
            total_pages = int(m.group(1))
            current_page = int(m.group(2))
            basename, ext = m.group(3), m.group(4)
            next_page = current_page + 1
            if next_page < total_pages:
                return urljoin(current_url, f"{basename}_{next_page}.{ext}")
            return None  # already on the last page

    # 2b. document.write pager: var currentPage = N (0-based) / var countPage = M,
    #     writing links like index_1.shtml (MEM and other TRS variants)
    for node in tree.xpath("//script"):
        text = node.text_content() or ""
        m_cur = _JS_PAGER_CUR.search(text)
        m_cnt = _JS_PAGER_CNT.search(text)
        if m_cur and m_cnt:
            cur, cnt = int(m_cur.group(1)), int(m_cnt.group(1))
            if cur + 1 >= cnt:
                return None  # last page
            ext = "shtml" if "shtml" in text else "html"
            return urljoin(current_url, f"index_{cur + 1}.{ext}")

    # 3. index_N.html / list_N.html URL increment (.gov.cn convention)
    path = current_url.split("?")[0]
    m = _INDEX_N_RE.search(path)
    if m:
        name, num, ext = m.group(1), int(m.group(2)), m.group(3)
        return _INDEX_N_RE.sub(f"/{name}_{num + 1}.{ext}", path)
    # First page: directory or explicit index.html → try index_1.html
    if path.endswith("/") or re.search(r'/index\.s?html?$', path, re.I):
        base_path = path.rstrip("/")
        if "." in path.rsplit("/", 1)[-1]:
            base_path = path.rsplit("/", 1)[0]
        return base_path + "/index_1.html"

    return None


def _extract_articles(tree, base_url: str, max_items: int = 200) -> list[dict]:
    """
    Extract article (title, link, published) from a typical gov.cn list page.

    base_url MUST be the URL of the page being parsed (not the site root):
    gov.cn list pages use page-relative hrefs like ./202606/t20260625_x.html,
    which resolve wrongly against the domain root.

    Tries two strategies:
    1. <ul><li> items with a link and an associated date
    2. Table rows with a link and a date cell
    """
    from urllib.parse import urlparse
    base_root = _root_domain(urlparse(base_url).netloc)

    results: list[dict] = []
    seen_hrefs: set[str] = set()

    def _add(title: str, href: str, date: str) -> None:
        title = title.strip()
        # Strip a date glued to the title's end (list items without separators)
        title = re.sub(r'[\s(（]*\d{4}\s*[-/年]\s*\d{1,2}\s*[-/月]\s*(\d{1,2})?\s*日?[)）]?\s*$',
                       '', title).strip()
        if not title or len(title) < _MIN_TITLE_LEN:
            return
        if title in _NAV_SKIP or any(nav in title for nav in _NAV_SKIP):
            return
        if not href or href.startswith("javascript") or href.startswith("#"):
            return
        link = urljoin(base_url, href)
        # Skip links that point to a completely different ministry/domain
        link_root = _root_domain(urlparse(link).netloc)
        if base_root and link_root and link_root != base_root:
            return
        if link in seen_hrefs:
            return
        seen_hrefs.add(link)
        if not date:
            date = _date_from_href(href)
        results.append({"title": title, "link": link, "published": date, "summary": ""})

    # --- Strategy 1: <ul><li> list items ---
    for li in tree.xpath("//ul//li[.//a[@href]]"):
        links = li.xpath(".//a[@href]")
        if not links:
            continue
        a = links[0]
        title = a.text_content()
        href = a.get("href", "")
        li_text = li.text_content()
        date = (_parse_date(li_text)
                or _date_from_text_and_href(li_text, href)
                or _date_from_href(href))
        _add(title, href, date)

    if results:
        return results[:max_items]

    # --- Strategy 2: table rows ---
    for tr in tree.xpath("//table//tr[.//a[@href]]"):
        links = tr.xpath(".//a[@href]")
        if not links:
            continue
        a = links[0]
        title = a.text_content()
        href = a.get("href", "")
        tds = tr.xpath(".//td")
        date = ""
        for td in reversed(tds):
            date = _parse_date(td.text_content())
            if date:
                break
        if not date:
            tr_text = tr.text_content()
            date = (_date_from_text_and_href(tr_text, href)
                    or _date_from_href(href))
        _add(title, href, date)

    return results[:max_items]


# ---------------------------------------------------------------------------
# Per-target fetch
# ---------------------------------------------------------------------------
def scrape_target(target: dict, timeout: int = 20) -> dict:
    """Fetch one ministry page and return a store_feed_result()-compatible dict."""
    name = target["name"]
    url = target["url"]
    base = target["base"]
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout)
        resp.raise_for_status()
        # lxml handles GBK/UTF-8 automatically via the encoding declared in the page
        tree = lhtml.fromstring(resp.content)
        articles = _extract_articles(tree, resp.url or url)
        return {
            "source": name,
            "source_cn": target.get("name_cn", ""),
            "category": target.get("category", "ministry"),
            "doc_type": target.get("doc_type"),
            "feed_url": url,
            "description": target.get("description", ""),
            "entries": articles,
            "ok": True,
        }
    except Exception as exc:
        return {
            "source": name,
            "source_cn": target.get("name_cn", ""),
            "category": target.get("category", "ministry"),
            "doc_type": target.get("doc_type"),
            "feed_url": url,
            "entries": [],
            "ok": False,
            "error": str(exc),
        }


def scrape_target_paginated(
    target: dict,
    max_pages: int = 50,
    known_links: set | None = None,
    page_delay: float = 1.0,
    timeout: int = 20,
) -> dict:
    """
    Fetch one ministry with full pagination support.

    max_pages   — hard ceiling on pages to fetch (use 100+ for initial full fetch,
                  3-5 for incremental runs)
    known_links — set of URLs already in the per-ministry DB; when every article on
                  a page is already known, pagination stops (incremental mode)
    page_delay  — seconds between page requests to avoid hammering the server
    """
    name = target["name"]
    url = target["url"]
    base = target["base"]
    all_articles: list[dict] = []
    seen_links: set[str] = set()
    current_url = url
    pages_fetched = 0

    try:
        for page_num in range(1, max_pages + 1):
            resp = requests.get(current_url, headers=HEADERS, timeout=timeout)
            # 404/410 on any page after the first means we've passed the end
            if page_num > 1 and resp.status_code in (404, 410):
                break
            resp.raise_for_status()
            tree = lhtml.fromstring(resp.content)
            articles = _extract_articles(tree, resp.url or current_url, max_items=200)
            pages_fetched = page_num

            if not articles:
                break

            new_on_page = 0
            for art in articles:
                link = art.get("link", "")
                if link in seen_links:
                    continue
                seen_links.add(link)
                art["page_num"] = page_num
                if known_links and link in known_links:
                    continue  # already stored — don't add to result, but keep paginating
                all_articles.append(art)
                new_on_page += 1

            # Incremental stop: if the whole page was already known we've caught up
            if known_links is not None and new_on_page == 0:
                break

            next_url = _find_next_page_url(tree, current_url, base)
            if not next_url:
                break
            current_url = next_url
            if page_num < max_pages:
                time.sleep(page_delay)

        return {
            "source": name,
            "source_cn": target.get("name_cn", ""),
            "category": target.get("category", "ministry"),
            "doc_type": target.get("doc_type"),
            "feed_url": target["url"],
            "description": target.get("description", ""),
            "entries": all_articles,
            "pages_fetched": pages_fetched,
            "ok": True,
        }
    except Exception as exc:
        return {
            "source": name,
            "source_cn": target.get("name_cn", ""),
            "category": target.get("category", "ministry"),
            "doc_type": target.get("doc_type"),
            "feed_url": target["url"],
            "entries": all_articles,
            "pages_fetched": pages_fetched,
            "ok": False,
            "error": str(exc),
        }


def scrape_all(
    targets: list[dict] | None = None,
    max_workers: int = 4,
    timeout: int = 20,
    paginate: bool = False,
    max_pages: int = 3,
    max_pages_by_source: dict | None = None,
    known_links_by_source: dict | None = None,
    page_delay: float = 1.0,
) -> list[dict]:
    """
    Fetch all ministry targets.

    paginate=False  — quick single-page fetch (original behaviour, for dashboard refresh)
    paginate=True   — full paginated fetch; use known_links_by_source for incremental stop
    """
    if targets is None:
        targets = TARGETS

    results = []
    if not paginate:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(scrape_target, t, timeout): t for t in targets}
            for fut in as_completed(futures):
                results.append(fut.result())
    else:
        # Paginated: sequential per target to respect page_delay rate-limiting
        # but concurrent across targets up to max_workers
        known = known_links_by_source or {}
        per_source = max_pages_by_source or {}
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(
                    scrape_target_paginated,
                    t,
                    per_source.get(t["name"], max_pages),
                    known.get(t["name"]),
                    page_delay,
                    timeout,
                ): t
                for t in targets
            }
            for fut in as_completed(futures):
                results.append(fut.result())
    return results
