"""
Full-text fetcher for ministry policy announcement pages.

Stage 2 of the policy pipeline: takes pending rows from policy_docs (discovered
by ministry_scraper list scraping) and fetches each article page, extracting
the main document text and the official document number (文号).

Swarm model: parallel ACROSS domains (one worker thread per domain), strictly
serial WITHIN a domain with a politeness delay — gov.cn sites rate-limit and
occasionally ban aggressive clients.
"""

import logging
import queue
import re
import threading
import time
from collections import defaultdict
from urllib.parse import urlparse

import requests
from lxml import html as lhtml

from backend.fetchers.ministry_scraper import HEADERS, _parse_date

log = logging.getLogger("policy_content")

# ---------------------------------------------------------------------------
# Article text extraction
# ---------------------------------------------------------------------------

# Known main-content containers on .gov.cn sites, tried in order.
_CONTENT_XPATHS = [
    '//div[contains(@class,"TRS_Editor")]',          # TRS CMS (NDRC, MEE, MIIT, ...)
    '//div[@id="zoom"]',                             # gov.cn, MOF
    '//div[contains(@class,"pages_content")]',       # gov.cn policy pages
    '//div[@id="UCAP-CONTENT"]',                     # gov.cn zhengce
    '//div[contains(@class,"article-content")]',
    '//div[contains(@class,"article_con")]',
    '//div[contains(@class,"artcon")]',
    '//div[contains(@class,"content_body")]',
    '//td[contains(@class,"b12c")]',                 # legacy MOFCOM
    '//div[contains(@class,"detail_content")]',
    '//div[@id="content"]',
    '//div[contains(@class,"gsj_htmlcon_bot")]',     # SAMR
    '//div[contains(@class,"trs_editor_view")]',
]

_STRIP_TAGS = ("script", "style", "noscript", "iframe")

# 文号 patterns: 发改运行〔2026〕123号 | 商务部公告2026年第25号 | 国办发〔2026〕7号
_DOC_NUMBER_PATTERNS = [
    re.compile(r'[一-鿿]{2,8}[〔\[［]\s*(?:19|20)\d{2}\s*[〕\]］]\s*第?\s*\d{1,4}\s*号'),
    re.compile(r'[一-鿿]{2,10}(?:19|20)\d{2}年第\s*\d{1,4}\s*号'),
]

_CJK = re.compile(r'[一-鿿]')
_WS = re.compile(r'[ \t　\xa0]+')
_MANY_NL = re.compile(r'\n{3,}')

_BINARY_EXT = (".pdf", ".doc", ".docx", ".xls", ".xlsx", ".zip", ".rar", ".ofd", ".wps")

MIN_TEXT_CHARS = 100  # minimum non-link CJK chars for a block to count as content

_BLOCK_TAGS = {"p", "div", "li", "tr", "td", "h1", "h2", "h3", "h4", "h5",
               "h6", "br", "section", "article", "table", "ul", "ol"}


def _clean_text(text: str) -> str:
    lines = [_WS.sub(" ", ln).strip() for ln in text.splitlines()]
    return _MANY_NL.sub("\n\n", "\n".join(lines)).strip()


def _iter_text(el):
    """Document-order text with newlines at block-element boundaries
    (text_content() glues blocks together, merging title/文号/body into one line)."""
    tag = el.tag if isinstance(el.tag, str) else ""
    if tag in _STRIP_TAGS:
        return
    if tag in _BLOCK_TAGS:
        yield "\n"
    if el.text:
        yield el.text
    for child in el:
        yield from _iter_text(child)
        if child.tail:
            yield child.tail
    if tag in _BLOCK_TAGS:
        yield "\n"


def _node_text(node) -> str:
    return _clean_text("".join(_iter_text(node)))


def _drop_junk(node):
    for tag in _STRIP_TAGS:
        for el in node.findall(f".//{tag}"):
            el.getparent().remove(el)
    return node


def _content_cjk(el) -> int:
    """CJK characters in el's subtree, with anchor text discounted — nav menus
    and link lists are nearly all anchors, article bodies nearly none."""
    total = len(_CJK.findall(el.text_content() or ""))
    linked = sum(len(_CJK.findall(a.text_content() or ""))
                 for a in el.findall(".//a"))
    return total - 2 * linked


def _largest_text_block(tree) -> str:
    """Fallback: the block with the most non-link CJK characters, preferring
    deeper (more specific) nodes over page-level wrappers."""
    best_el, best_score = None, 0
    for el in tree.xpath("//div | //td | //article | //section"):
        score = _content_cjk(el)
        if score < MIN_TEXT_CHARS:
            continue
        # Prefer nodes whose direct children are mostly text/paragraphs, i.e.
        # nodes not much larger than the biggest of their child blocks.
        child_max = 0
        for child in el:
            if child.tag in ("div", "td", "article", "section", "table"):
                child_max = max(child_max, _content_cjk(child))
        if child_max > 0.85 * score:
            continue  # a child holds nearly all the text — descend instead
        if score > best_score:
            best_score = score
            best_el = el
    return _node_text(best_el) if best_el is not None else ""


def _find_doc_number(text: str) -> str:
    """Find the 文号. Prefer a short standalone line (its usual layout);
    fall back to an in-text match."""
    for line in text.splitlines()[:40]:
        line = line.strip()
        if not line or len(line) > 40:
            continue
        for pat in _DOC_NUMBER_PATTERNS:
            m = pat.search(line)
            if m and len(m.group(0)) >= len(line) - 12:
                return m.group(0).strip()
    # In-text fallback stays near the top of the document — doc numbers deeper
    # in the body are usually references to OTHER documents.
    for pat in _DOC_NUMBER_PATTERNS:
        m = pat.search(text[:600])
        if m:
            return m.group(0).strip()
    return ""


def extract_article(content: bytes, url: str = "") -> dict:
    """Extract {text, doc_number, published} from an article page.

    Returns text='' when no meaningful content block is found.
    """
    tree = lhtml.fromstring(content)
    _drop_junk(tree)

    text = ""
    known_container = None
    for xp in _CONTENT_XPATHS:
        nodes = tree.xpath(xp)
        if not nodes:
            continue
        if known_container is None:
            known_container = nodes[0]
        if _content_cjk(nodes[0]) >= MIN_TEXT_CHARS:
            text = _node_text(nodes[0])
            break
    if not text:
        text = _largest_text_block(tree)
    if not text and known_container is not None:
        # A known content container exists but is short or link-heavy (e.g. a
        # table of approved standards) — accept it if minimally substantial.
        candidate = _node_text(known_container)
        if len(_CJK.findall(candidate)) >= 30:
            text = candidate

    doc_number = _find_doc_number(text) if text else ""

    # Published date from page metadata text (成文日期/发布日期 rows, header spans)
    published = ""
    page_text = tree.text_content()[:5000]
    m = re.search(r'(?:成文日期|发布日期|发文日期|发布时间)[:：\s]*([\d年月日/\-\.\s]{8,14})',
                  page_text)
    if m:
        published = _parse_date(m.group(1))

    return {"text": text, "doc_number": doc_number, "published": published}


# ---------------------------------------------------------------------------
# Swarm fetcher — parallel across domains, serial + delayed within a domain
# ---------------------------------------------------------------------------

def fetch_one(url: str, timeout: int = 25, session: requests.Session | None = None) -> dict:
    """Fetch a single article URL. Returns a result dict for update_policy_content()."""
    if url.lower().split("?")[0].endswith(_BINARY_EXT):
        return {"status": "binary", "http_status": None, "text": "",
                "doc_number": "", "published": "", "error": ""}
    try:
        getter = session or requests
        resp = getter.get(url, headers=HEADERS, timeout=timeout)
        if resp.status_code != 200:
            return {"status": "error", "http_status": resp.status_code, "text": "",
                    "doc_number": "", "published": "",
                    "error": f"HTTP {resp.status_code}"}
        ctype = resp.headers.get("Content-Type", "")
        if ctype and "html" not in ctype and "text" not in ctype:
            return {"status": "binary", "http_status": 200, "text": "",
                    "doc_number": "", "published": "", "error": ""}
        art = extract_article(resp.content, url)
        status = "ok" if art["text"] else "empty"
        return {"status": status, "http_status": 200, "text": art["text"],
                "doc_number": art["doc_number"], "published": art["published"],
                "error": ""}
    except Exception as exc:
        return {"status": "error", "http_status": None, "text": "",
                "doc_number": "", "published": "",
                "error": f"{type(exc).__name__}: {exc}"[:300]}


def fetch_contents(rows: list, on_result, max_domains: int = 6,
                   per_request_delay: float = 1.0, timeout: int = 25,
                   max_consecutive_errors: int = 10) -> dict:
    """
    Fetch article content for `rows` (each with .id/["id"] and .url/["url"]).

    on_result(row, result) is called on the CALLER's thread for every finished
    fetch (safe for SQLite writes). A domain worker aborts its queue after
    max_consecutive_errors in a row (site down / blocking us).

    Returns {"done": n, "by_status": {...}}.
    """
    by_domain: dict[str, list] = defaultdict(list)
    for row in rows:
        by_domain[urlparse(row["url"]).netloc].append(row)

    out_q: queue.Queue = queue.Queue()
    sem = threading.Semaphore(max_domains)
    n_workers = len(by_domain)

    def worker(domain: str, items: list):
        with sem:
            session = requests.Session()
            consecutive = 0
            for i, row in enumerate(items):
                if consecutive >= max_consecutive_errors:
                    log.warning(f"  {domain}: {consecutive} consecutive errors — "
                                f"skipping remaining {len(items) - i} items")
                    break
                result = fetch_one(row["url"], timeout=timeout, session=session)
                consecutive = consecutive + 1 if result["status"] == "error" else 0
                out_q.put((row, result))
                time.sleep(per_request_delay)
        out_q.put(None)  # this worker is done

    threads = [threading.Thread(target=worker, args=(d, items), daemon=True)
               for d, items in by_domain.items()]
    for t in threads:
        t.start()

    done = 0
    by_status: dict[str, int] = defaultdict(int)
    finished_workers = 0
    while finished_workers < n_workers:
        item = out_q.get()
        if item is None:
            finished_workers += 1
            continue
        row, result = item
        on_result(row, result)
        done += 1
        by_status[result["status"]] += 1
        if done % 50 == 0:
            log.info(f"  ...{done} fetched ({dict(by_status)})")

    for t in threads:
        t.join(timeout=5)
    return {"done": done, "by_status": dict(by_status)}
