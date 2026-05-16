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
    {
        "name": "NDRC — News Releases",
        "name_cn": "国家发展改革委-新闻发布",
        "category": "central_government",
        "url": "https://www.ndrc.gov.cn/xwdt/xwfb/",
        "base": "https://www.ndrc.gov.cn",
        "description": "NDRC economic planning news and press releases",
    },
    {
        "name": "MOFCOM — Policy Releases",
        "name_cn": "商务部-政策文件",
        "category": "ministry",
        "url": "http://www.mofcom.gov.cn/zcfb/",
        "base": "http://www.mofcom.gov.cn",
        "description": "Ministry of Commerce trade policy and regulatory releases",
    },
    {
        "name": "SASAC — SOE News",
        "name_cn": "国务院国有资产监督管理委员会-新闻",
        "category": "regulator",
        "url": "http://www.sasac.gov.cn/n2588025/n2588124/",
        "base": "http://www.sasac.gov.cn",
        "description": "SASAC state-owned enterprise supervision news",
    },
    {
        "name": "MFA — Press Briefings",
        "name_cn": "外交部-发言人答问",
        "category": "ministry",
        "url": "https://www.mfa.gov.cn/wjdt_674879/fyrbt_674889/",
        "base": "https://www.mfa.gov.cn",
        "description": "Ministry of Foreign Affairs spokesperson press briefings",
    },
    {
        "name": "Ministry of Finance — News",
        "name_cn": "财政部-财政新闻",
        "category": "ministry",
        "url": "http://www.mof.gov.cn/zhengwuxinxi/",
        "base": "http://www.mof.gov.cn",
        "description": "Ministry of Finance fiscal policy and financial news",
    },
    {
        "name": "MIIT — Industrial Policy",
        "name_cn": "工业和信息化部-工作动态",
        "category": "ministry",
        "url": "https://www.miit.gov.cn/xwdt/",
        "base": "https://www.miit.gov.cn",
        "description": "MIIT industrial and information technology policy updates",
    },
    {
        "name": "State Council — Latest Policies",
        "name_cn": "国务院-最新政策",
        "category": "central_government",
        "url": "https://www.gov.cn/zhengce/",
        "base": "https://www.gov.cn",
        "description": "State Council top-level policy releases and regulations",
    },
    {
        "name": "SAMR — Market Regulation News",
        "name_cn": "市场监督管理总局-新闻",
        "category": "regulator",
        "url": "https://www.samr.gov.cn/xw/",
        "base": "https://www.samr.gov.cn",
        "description": "State Administration for Market Regulation news and enforcement",
    },

    # -------------------------------------------------------------------------
    # Financial Regulators
    # -------------------------------------------------------------------------
    {
        "name": "PBOC — Policy Communications",
        "name_cn": "中国人民银行-沟通交流",
        "category": "regulator",
        "url": "http://www.pbc.gov.cn/rmyh/108976/index.html",
        "base": "http://www.pbc.gov.cn",
        "description": "People's Bank of China monetary policy communications and press releases",
    },
    {
        "name": "NFRA — Banking & Insurance Supervision",
        "name_cn": "国家金融监督管理总局-最新动态",
        "category": "regulator",
        "url": "https://www.nfra.gov.cn/cn/view/pages/ItemList.html?itemPid=923&itemId=4115&itemUrl=ItemList.html",
        "base": "https://www.nfra.gov.cn",
        "description": "National Financial Regulatory Administration banking and insurance supervision updates",
    },
    {
        "name": "CSRC — Securities Regulation",
        "name_cn": "中国证监会-最新动态",
        "category": "regulator",
        "url": "http://www.csrc.gov.cn/csrc/c101954/",
        "base": "http://www.csrc.gov.cn",
        "description": "China Securities Regulatory Commission capital markets policy and enforcement",
    },
    {
        "name": "SAFE — FX Policy",
        "name_cn": "国家外汇管理局-通知公告",
        "category": "regulator",
        "url": "http://www.safe.gov.cn/safe/",
        "base": "http://www.safe.gov.cn",
        "description": "State Administration of Foreign Exchange FX policy announcements and capital controls",
    },
    # GAC (customs.gov.cn) and MPS (mps.gov.cn) block all external traffic — tracked
    # in registry.yaml but cannot be directly scraped from outside China.

    # -------------------------------------------------------------------------
    # Security & Strategy
    # -------------------------------------------------------------------------
    {
        "name": "MSS — National Security Notices",
        "name_cn": "国家安全部-警示提示",
        "category": "ministry",
        "url": "https://www.12339.gov.cn/",
        "base": "https://www.12339.gov.cn",
        "description": "Ministry of State Security national security warnings and notices",
    },

    # -------------------------------------------------------------------------
    # Industry & Technology
    # -------------------------------------------------------------------------
    {
        "name": "MOST — Science & Technology Policy",
        "name_cn": "科学技术部-科技报告",
        "category": "ministry",
        "url": "https://www.most.gov.cn/kjbgz/",
        "base": "https://www.most.gov.cn",
        "description": "Ministry of Science and Technology R&D policy, tech regulations, and innovation funding",
    },
    {
        "name": "CAC — Internet & AI Regulation",
        "name_cn": "国家互联网信息办公室",
        "category": "regulator",
        "url": "https://www.cac.gov.cn/",
        "base": "https://www.cac.gov.cn",
        "description": "Cyberspace Administration of China internet, data governance, and AI regulation",
    },
    {
        "name": "NEA — Energy Policy",
        "name_cn": "国家能源局-政府信息公开",
        "category": "regulator",
        "url": "http://www.nea.gov.cn/zfxxgk/",
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
        "url": "https://www.mee.gov.cn/ywdt/ywdtjj/",
        "base": "https://www.mee.gov.cn",
        "description": "Ministry of Ecology and Environment carbon policy and environmental standards",
    },
    {
        "name": "MOFCOM — Free Trade Zone Announcements",
        "name_cn": "商务部-新闻发布",
        "category": "ministry",
        "url": "http://www.mofcom.gov.cn/xwfb/",
        "base": "http://www.mofcom.gov.cn",
        "description": "Ministry of Commerce FTZ and trade-related news releases (separate from zcfb policy docs)",
    },
    {
        "name": "MARA — Agriculture Policy",
        "name_cn": "农业农村部-新闻",
        "category": "ministry",
        "url": "http://www.moa.gov.cn/xw/",
        "base": "http://www.moa.gov.cn",
        "description": "Ministry of Agriculture and Rural Affairs agricultural policy and rural development",
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
    m = _HREF_DATE.search(href)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    # Try /YYYYMM/ path segment → YYYY-MM-01
    m2 = re.search(r'/(\d{6})/', href)
    if m2:
        s = m2.group(1)
        return f"{s[:4]}-{s[4:6]}-01"
    return ""


# ---------------------------------------------------------------------------
# HTML article extractor
# ---------------------------------------------------------------------------
_MIN_TITLE_LEN = 8
_NAV_SKIP = {"首页", "上一页", "下一页", "末页", "更多", ">>", "<<", "...", "返回", "查看更多"}
_NEXT_PAGE_TEXTS = {"下一页", "下页", "后页", "后一页", "next", ">", "»"}


def _find_next_page_url(tree, current_url: str, base_url: str) -> str | None:
    """Detect the next-page URL from pagination controls on a gov.cn list page."""
    for a in tree.xpath("//a[@href]"):
        text = (a.text_content() or "").strip()
        href = a.get("href", "")
        if not href or href.startswith("javascript") or href.startswith("#"):
            continue
        if text in _NEXT_PAGE_TEXTS or "下一" in text:
            url = urljoin(current_url, href)
            if url != current_url:
                return url
    return None


def _extract_articles(tree, base_url: str, max_items: int = 200) -> list[dict]:
    """
    Extract article (title, link, published) from a typical gov.cn list page.

    Tries two strategies:
    1. <ul><li> items with a link and an associated date
    2. Table rows with a link and a date cell
    Falls back to any link with a date-like pattern in a plausible context.
    """
    results: list[dict] = []
    seen_hrefs: set[str] = set()

    def _add(title: str, href: str, date: str) -> None:
        title = title.strip()
        if not title or len(title) < _MIN_TITLE_LEN:
            return
        if title in _NAV_SKIP or any(nav in title for nav in _NAV_SKIP):
            return
        if not href or href.startswith("javascript") or href.startswith("#"):
            return
        link = urljoin(base_url, href)
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
        date = _parse_date(li_text) or _date_from_href(href)
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
            date = _date_from_href(href)
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
        articles = _extract_articles(tree, base)
        return {
            "source": name,
            "source_cn": target.get("name_cn", ""),
            "category": target.get("category", "ministry"),
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
            resp.raise_for_status()
            tree = lhtml.fromstring(resp.content)
            articles = _extract_articles(tree, base, max_items=200)
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
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(
                    scrape_target_paginated,
                    t,
                    max_pages,
                    known.get(t["name"]),
                    page_delay,
                    timeout,
                ): t
                for t in targets
            }
            for fut in as_completed(futures):
                results.append(fut.result())
    return results
