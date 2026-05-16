"""
Direct HTML scraper for Chinese ministry and government body announcement pages.

Most .gov.cn sites are publicly accessible from outside China without a proxy.
Supplements the RSS-based monitor with scraped content where no RSS is available.

Results are stored to the same `items` table via store_feed_result(), so they
appear in the dashboard and are available for the Policy Advisor.
"""

import re
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


def _extract_articles(tree, base_url: str, max_items: int = 25) -> list[dict]:
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


def scrape_all(targets: list[dict] | None = None, max_workers: int = 6,
               timeout: int = 20) -> list[dict]:
    """Fetch all ministry targets concurrently."""
    if targets is None:
        targets = TARGETS
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(scrape_target, t, timeout): t for t in targets}
        for fut in as_completed(futures):
            results.append(fut.result())
    return results
