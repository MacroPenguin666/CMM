"""
MOFCOM scraper — fetches active policy documents from China's Ministry of Commerce.

Source: http://www.mofcom.gov.cn
Approach: requests + BeautifulSoup, targeting the policy-disclosure sections.

Note: mofcom.gov.cn is only reliably reachable from within China or via a
China-accessible network. The scraper fails gracefully when the site is unreachable.
"""

import hashlib
import logging
import re
import time

import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

BASE_URL = "http://www.mofcom.gov.cn"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "zh-CN,zh;q=0.9",
}
TIMEOUT = 20
MAX_PAGES = 5  # pages to fetch per section

# Sections to scrape: (url_path, doc_type_label, hierarchy_level)
SECTIONS = [
    ("/article/b/g/", "部门规章", 4),
    ("/article/b/c/", "规范性文件", 5),
    ("/article/i/ck/", "通知", 6),
    ("/article/i/jh/", "意见", 6),
    ("/zwgk/zcfb/", "政策发布", 5),
]

# Date patterns found in Chinese gov document listings
DATE_PATTERNS = [
    re.compile(r"(\d{4})[-/年](\d{1,2})[-/月](\d{1,2})"),
    re.compile(r"(\d{4})[-/](\d{1,2})"),
]


def _get(url: str) -> BeautifulSoup | None:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "lxml")
    except requests.exceptions.Timeout:
        log.warning("MOFCOM timeout fetching %s (site may be unreachable outside China)", url)
        return None
    except requests.exceptions.ConnectionError:
        log.warning("MOFCOM connection error for %s", url)
        return None
    except Exception as e:
        log.warning("MOFCOM fetch error %s: %s", url, e)
        return None


def _extract_date(text: str) -> str:
    for pat in DATE_PATTERNS:
        m = pat.search(text)
        if m:
            year = m.group(1)
            month = f"{int(m.group(2)):02d}"
            return f"{year}-{month}"
    return ""


def _make_doc_id(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()[:20]


def _scrape_section(path: str, doc_type: str, max_pages: int = MAX_PAGES) -> list[dict]:
    """Scrape one MOFCOM section listing (multiple pages)."""
    docs = []
    for page in range(1, max_pages + 1):
        url = f"{BASE_URL}{path}" if page == 1 else f"{BASE_URL}{path}?page={page}"
        soup = _get(url)
        if not soup:
            break

        # Common MOFCOM list patterns: <ul class="..."><li><a>title</a><span>date</span></li>
        found_items = False
        for li in soup.select("ul li, .list-item, .news-list li, table tr"):
            link = li.find("a", href=True)
            if not link:
                continue
            title = link.get_text(strip=True)
            href = link.get("href", "")
            if not title or len(title) < 4:
                continue

            # Resolve relative URLs
            if href.startswith("/"):
                href = BASE_URL + href
            elif not href.startswith("http"):
                href = f"{BASE_URL}{path}{href}"

            # Extract date from the list item text
            item_text = li.get_text(" ", strip=True)
            issue_date = _extract_date(item_text)

            doc_id = _make_doc_id(href)
            docs.append({
                "doc_id": doc_id,
                "title": title,
                "doc_type": doc_type,
                "issue_date": issue_date,
                "url": href,
                "status": "active",
            })
            found_items = True

        if not found_items:
            log.debug("MOFCOM %s page %d: no list items found", path, page)
            break

        time.sleep(0.5)

    return docs


def fetch_mofcom() -> list[dict]:
    """
    Scrape active policy documents from MOFCOM.
    Returns list of dicts matching the mofcom_docs schema.
    Falls back to empty list if site is unreachable.
    """
    # Quick connectivity check
    test = _get(BASE_URL)
    if not test:
        log.warning(
            "MOFCOM unreachable — skipping. "
            "This is expected when running outside China. "
            "Connect via a China-accessible network to populate MOFCOM data."
        )
        return []

    all_docs = []
    for path, doc_type, _ in SECTIONS:
        log.info("MOFCOM: scraping %s (%s)", path, doc_type)
        docs = _scrape_section(path, doc_type)
        log.info("MOFCOM: %s → %d documents", doc_type, len(docs))
        all_docs.extend(docs)

    # Deduplicate by doc_id (keep first occurrence)
    seen = set()
    unique = []
    for doc in all_docs:
        if doc["doc_id"] not in seen:
            seen.add(doc["doc_id"])
            unique.append(doc)

    log.info("MOFCOM: %d unique documents total", len(unique))
    return unique
