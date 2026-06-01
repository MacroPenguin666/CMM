"""
NPC Observer scraper — fetches all 14th NPC bills and their legislative histories.

Source: https://npcobserver.com/legislation/14th/
Server-rendered WordPress (Gutenberg table blocks) — plain requests + BeautifulSoup.

Page structure:
  - 6 Gutenberg <table> blocks, each a section (New Laws Cat I/II/Unlisted,
    Amendments Cat I/II/Unlisted)
  - Each row has 3 cells: emoji | English title (with <a>) | Chinese name
  - Emoji: 🟢=passed, 🟠=pending, 🔴=shelved, 🔵=consultation; no emoji=other

Detail page structure (per bill):
  - <li> items inside a "Legislative History & Text" nested list
  - Event lines: "NPCSC deliberation – round #1: Oct. 20 – 24, 2023"
  - Also sub-items: "Public consultation: Dec. 29, 2023 – Jan. 27, 2024"
"""

import hashlib
import logging
import re
import time

import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

BASE_URL = "https://npcobserver.com"
LISTING_URL = f"{BASE_URL}/legislation/14th/"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; ChinaPolicyMonitor/1.0)"}
TIMEOUT = 20

EMOJI_STATUS = {
    "🟢": "passed",
    "🟠": "pending",
    "🔴": "shelved",
    "🔵": "consultation",
}

# Normalise event description → event_type key
EVENT_TYPE_PATTERNS = [
    (re.compile(r"deliberation.*round\s*#?3|third\s+read|3rd\s+read", re.I), "third_reading"),
    (re.compile(r"deliberation.*round\s*#?2|second\s+read|2nd\s+read", re.I), "second_reading"),
    (re.compile(r"deliberation.*round\s*#?1|first\s+read|1st\s+read", re.I), "first_reading"),
    (re.compile(r"public\s+consult|comment\s+period", re.I), "consultation"),
    (re.compile(r"explanation|introduced|draft\s+publish", re.I), "draft"),
    (re.compile(r"presidential\s+order|pass(?:ed)?|adopt|enact|promulg", re.I), "passed"),
    (re.compile(r"effective|tak.*effect|entry.*force", re.I), "effective"),
]

MONTH_MAP = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04", "may": "05", "jun": "06",
    "jul": "07", "aug": "08", "sep": "09", "oct": "10", "nov": "11", "dec": "12",
}

# Finds any month name in text (standalone lookup)
_MONTH_RE = re.compile(
    r"\b(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
    r"Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\b",
    re.I,
)
# Finds any 4-digit year (2000-2099)
_YEAR_RE = re.compile(r"\b(20\d{2})\b")


def _get(url: str) -> BeautifulSoup | None:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "lxml")
    except Exception as e:
        log.warning("NPC Observer fetch failed %s: %s", url, e)
        return None


def _extract_date(text: str) -> str:
    """
    Return the earliest (year, month) date found in text as 'YYYY-MM'.

    Strategy: collect all month mentions and all 4-digit years in the text,
    then pair each month with the nearest year that follows it (or the only
    year in the text if there's just one). Returns the chronologically earliest
    YYYY-MM found.

    Handles all of these formats correctly:
      "Apr. 14 – May 14, 2023"          → "2023-04"
      "Nov. 8 – Dec. 7, 2024"           → "2024-11"
      "Oct. 20 – 24, 2023"              → "2023-10"
      "NPCSC deliberation – round #2: Dec. 21 – 25, 2024"  → "2024-12"
      "Nov. 4 – 8, 2024"                → "2024-11"
    """
    # Collect (position, month_abbr) for every month mention
    months = [(m.start(), m.group(1)[:3].lower()) for m in _MONTH_RE.finditer(text)]
    # Collect (position, year) for every year mention
    years = [(m.start(), m.group(1)) for m in _YEAR_RE.finditer(text)]

    if not months or not years:
        return ""

    # For each month, find the nearest year that comes AFTER it (or the last year overall)
    pairs = []
    for mpos, mabbr in months:
        # First year that starts after this month position
        after = [y for ypos, y in years if ypos > mpos]
        year = after[0] if after else years[-1][1]
        pairs.append((int(year), int(MONTH_MAP[mabbr]), mabbr, year))

    pairs.sort()
    _, _, month_abbr, year = pairs[0]
    return f"{year}-{MONTH_MAP[month_abbr]}"


def _normalise_event_type(text: str) -> str:
    for pattern, etype in EVENT_TYPE_PATTERNS:
        if pattern.search(text):
            return etype
    return "other"


def _scrape_bill_events(url: str) -> list[dict]:
    """
    Fetch a bill detail page and extract legislative timeline events.

    Two sources on each page:
      1. "Legislative History & Text" <li> — contains sub-<li> for each deliberation
         round and public consultation.  This is the primary source.
      2. "Legislative Records" <li> — contains sub-<li> items like
         "Explanation (Nov. 4, 2024)". The Explanation date marks when the draft
         was submitted for first reading, so we map it to a 'draft' event.

    Deduplication: after extracting (event_type, event_date, description), we
    deduplicate by (event_type, event_date) pair so nested sub-items don't repeat.
    """
    soup = _get(url)
    if not soup:
        return []

    content = soup.find("div", class_="entry-content") or soup.find("article")
    if not content:
        return []

    events = []

    # ---- Pass 1: legislative history <li> sub-items ----
    for li in content.find_all("li"):
        text = li.get_text(" ", strip=True)
        if "legislative history" in text.lower() and len(text) < 1000:
            for sub in li.find_all("li"):
                sub_text = sub.get_text(" ", strip=True)
                if len(sub_text) > 400:
                    continue
                event_date = _extract_date(sub_text)
                if not event_date:
                    continue
                event_type = _normalise_event_type(sub_text)
                events.append({
                    "event_type": event_type,
                    "event_date": event_date,
                    "description": sub_text[:300],
                })
            break

    # ---- Pass 2: Explanation date from Legislative Records → 'draft' ----
    for li in content.find_all("li"):
        text = li.get_text(" ", strip=True)
        if "legislative records" in text.lower() and len(text) < 1000:
            for sub in li.find_all("li"):
                sub_text = sub.get_text(" ", strip=True)
                # "Explanation (Nov. 4, 2024)" — marks when bill was submitted
                if re.match(r"explanation\s*\(", sub_text, re.I):
                    event_date = _extract_date(sub_text)
                    if event_date:
                        events.append({
                            "event_type": "draft",
                            "event_date": event_date,
                            "description": sub_text[:200],
                        })
                    break
            break

    # ---- Deduplicate by (event_type, event_date) ----
    seen = set()
    unique = []
    for e in events:
        key = (e["event_type"], e["event_date"])
        if key not in seen:
            seen.add(key)
            unique.append(e)

    unique.sort(key=lambda e: e["event_date"] or "9999")
    return unique


# Maps the heading text preceding each table to a category label
_CATEGORY_MAP = [
    ("New Laws",             "Category I",   "New Law — Cat I"),
    ("New Laws",             "Category II",  "New Law — Cat II"),
    ("New Laws",             "Unlisted",     "New Law — Unlisted"),
    ("Amendments",           "Category I",   "Amendment — Cat I"),
    ("Amendments",           "Category II",  "Amendment — Cat II"),
    ("Amendments",           "Unlisted",     "Amendment — Unlisted"),
]
_TABLE_CATEGORIES = [label for _, _, label in _CATEGORY_MAP]


def _scrape_listing() -> list[dict]:
    """
    Parse the 14th NPC legislation listing page using Gutenberg table blocks.

    Captures ALL rows — including bills without a detail-page link (plan-stage
    only). For those, bill_id is derived from the title instead of the URL.
    Also captures: title_cn (Chinese name), category (table section label).
    Returns list of bill dicts.
    """
    soup = _get(LISTING_URL)
    if not soup:
        log.error("NPC Observer listing page unreachable")
        return []

    content = soup.find("div", class_="entry-content")
    if not content:
        log.warning("NPC Observer: could not find entry-content div")
        return []

    bills = []
    tables = content.find_all("table")
    for table_idx, table in enumerate(tables):
        category = _TABLE_CATEGORIES[table_idx] if table_idx < len(_TABLE_CATEGORIES) else "Unknown"

        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue

            # Cell 0: emoji status
            emoji_cell = cells[0].get_text(strip=True)
            status = "other"
            for emoji, s in EMOJI_STATUS.items():
                if emoji in emoji_cell:
                    status = s
                    break

            # Cell 1: English title + optional link
            title_cell = cells[1]
            link = title_cell.find("a", href=True)
            if link:
                title = link.get_text(strip=True)
                href = link.get("href", "")
                if not href.startswith("http"):
                    href = ""
            else:
                title = title_cell.get_text(strip=True)
                href = ""

            if not title:
                continue

            # Cell 2: Chinese name (optional)
            title_cn = cells[2].get_text(strip=True) if len(cells) > 2 else ""

            # Stable ID: prefer URL hash; fall back to title hash for plan-only bills
            id_source = href if href else title
            bill_id = hashlib.md5(id_source.encode()).hexdigest()[:16]

            bills.append({
                "bill_id": bill_id,
                "title": title,
                "title_cn": title_cn or None,
                "status": status,
                "category": category,
                "url": href or None,
                "date_introduced": "",
            })

    log.info("NPC Observer: found %d bills on listing page (%d without detail page)",
             len(bills), sum(1 for b in bills if not b.get("url")))
    return bills


def fetch_npc_bills() -> list[tuple[dict, list[dict]]]:
    """
    Scrape NPC Observer legislation listing + detail pages.

    Returns list of (bill_dict, [event_dict, ...]).
    Detail pages are fetched for ALL bills that have a URL so that passed laws
    also get their full legislative history.
    """
    bills = _scrape_listing()
    if not bills:
        return []

    results = []
    for bill in bills:
        events = []
        if bill.get("url"):
            time.sleep(0.4)
            events = _scrape_bill_events(bill["url"])
            log.info(
                "NPC Observer: %s — %d events [%s]",
                bill["title"][:60],
                len(events),
                bill["status"],
            )

        results.append((bill, events))

    log.info("NPC Observer: processed %d bills total", len(results))
    return results
