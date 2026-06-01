"""
Scan top academic journals for China-related publications.

Uses the CrossRef API (no auth required) to fetch recent articles by journal
ISSN, filters general economics journals for China-relevant content, and
stores results in data/feeds.db.

Usage:
    python academic.py              # fetch new articles, store to DB
    python academic.py --show       # show latest stored articles
    python academic.py --summary    # per-journal summary
    python academic.py --json       # output as JSON
"""

import argparse
import json
import logging
import re
import sqlite3
from datetime import datetime, timezone

import requests

from policy_monitor.storage import DB_DIR, DB_PATH

log = logging.getLogger("academic")

CROSSREF_API = "https://api.crossref.org/journals/{issn}/works"

# ---------------------------------------------------------------------------
# Journal configuration
# ---------------------------------------------------------------------------

JOURNALS = [
    {
        "name": "The China Quarterly",
        "issn": "0305-7410",
        "china_only": True,
    },
    {
        "name": "American Economic Review",
        "issn": "0002-8282",
        "china_only": False,
    },
    {
        "name": "Journal of Political Economy",
        "issn": "0022-3808",
        "china_only": False,
    },
    {
        "name": "Quarterly Journal of Economics",
        "issn": "0033-5533",
        "china_only": False,
    },
    {
        "name": "China & World Economy",
        "issn": "1749-124X",
        "china_only": True,
    },
]

# Keywords for filtering general economics journals
CHINA_KEYWORDS = [
    # Country / region
    "china", "chinese", "beijing", "shanghai", "shenzhen", "guangdong",
    "hong kong", "taiwan", "tibet", "xinjiang", "macau",
    # Pinyin / proper nouns
    "renminbi", "rmb", "yuan", "pboc", "ccp", "prc",
    # Institutions
    "people's bank", "state council", "politburo",
    "national people's congress",
    # Economic concepts tied to China
    "belt and road", "bri", "one belt", "special economic zone",
    "hukou", "gaokao", "soe", "state-owned enterprise",
    "great firewall", "social credit",
    # Trade / geopolitical context
    "us-china", "sino-american", "sino-us", "u.s.-china",
]

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

ACADEMIC_SCHEMA = """
CREATE TABLE IF NOT EXISTS academic_articles (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    journal     TEXT NOT NULL,
    title       TEXT NOT NULL,
    authors     TEXT,
    link        TEXT,
    doi         TEXT,
    published   TEXT,
    abstract    TEXT,
    china_match TEXT,
    fetched_at  TEXT NOT NULL,
    UNIQUE(journal, title, link)
);

CREATE TABLE IF NOT EXISTS academic_votes (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id  INTEGER NOT NULL REFERENCES academic_articles(id),
    vote        INTEGER NOT NULL,  -- +1 upvote, -1 downvote
    voted_at    TEXT NOT NULL,
    UNIQUE(article_id)             -- one vote per article
);
"""


def get_academic_db() -> sqlite3.Connection:
    DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.executescript(ACADEMIC_SCHEMA)
    return conn


# ---------------------------------------------------------------------------
# China relevance filter
# ---------------------------------------------------------------------------

def _is_china_related(title: str, abstract: str) -> str | None:
    """Return matched keywords string, or None if not China-related."""
    matches = []
    title_lower = (title or "").lower()
    abstract_lower = (abstract or "").lower()
    for kw in CHINA_KEYWORDS:
        if kw in title_lower:
            matches.append(f"title:{kw}")
        elif kw in abstract_lower:
            matches.append(f"abstract:{kw}")
    return "; ".join(matches) if matches else None


# ---------------------------------------------------------------------------
# CrossRef fetching
# ---------------------------------------------------------------------------

def _safe_fetch_journal(journal: dict, rows: int = 50, timeout: int = 30) -> list[dict]:
    """Fetch recent articles from one journal via CrossRef API."""
    name = journal["name"]
    issn = journal["issn"]
    url = CROSSREF_API.format(issn=issn)
    try:
        resp = requests.get(
            url,
            params={
                "rows": rows,
                "sort": "published",
                "order": "desc",
                "select": "title,author,DOI,published-print,published-online,abstract,URL",
            },
            timeout=timeout,
            headers={
                "User-Agent": "ChinaPolicyMonitor/1.0 (https://github.com; mailto:contact@example.com)",
            },
        )
        resp.raise_for_status()
        data = resp.json()
        items = data.get("message", {}).get("items", [])

        articles = []
        for item in items:
            title = item.get("title", [""])[0] if item.get("title") else ""
            # Strip HTML from title
            title = re.sub(r"<[^>]+>", "", title).strip()
            if not title:
                continue

            # Authors
            authors_list = item.get("author", [])
            authors = "; ".join(
                f"{a.get('family', '')}, {a.get('given', '')}".strip(", ")
                for a in authors_list
            )

            # DOI and link
            doi = item.get("DOI", "")
            link = item.get("URL", "")
            if not link and doi:
                link = f"https://doi.org/{doi}"

            # Published date (prefer print, fall back to online)
            published = ""
            for date_key in ("published-print", "published-online"):
                date_parts = item.get(date_key, {}).get("date-parts", [[]])
                if date_parts and date_parts[0]:
                    parts = date_parts[0]
                    published = "-".join(str(p).zfill(2) for p in parts)
                    break

            # Abstract (may contain JATS XML tags)
            abstract = item.get("abstract", "") or ""
            abstract = re.sub(r"<[^>]+>", "", abstract).strip()[:1000]

            articles.append({
                "journal": name,
                "title": title,
                "authors": authors,
                "link": link,
                "doi": doi,
                "published": published,
                "abstract": abstract,
            })

        log.info(f"  OK   {name}: {len(articles)} articles")
        return articles
    except Exception as exc:
        log.warning(f"  FAIL {name}: {type(exc).__name__}: {str(exc)[:120]}")
        return []


def fetch_academic() -> tuple[list[dict], int, int]:
    """Fetch all journals. Returns (articles, ok_count, fail_count)."""
    all_articles = []
    ok = 0
    fail = 0

    for journal in JOURNALS:
        raw = _safe_fetch_journal(journal)
        if raw:
            ok += 1
        else:
            fail += 1
            continue

        if journal["china_only"]:
            for a in raw:
                a["china_match"] = "all"
            all_articles.extend(raw)
        else:
            for a in raw:
                match = _is_china_related(a["title"], a["abstract"])
                if match:
                    a["china_match"] = match
                    all_articles.append(a)

    return all_articles, ok, fail


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

def store_articles(conn: sqlite3.Connection, articles: list[dict]) -> int:
    """Store articles to DB. Returns count of new articles inserted."""
    now = datetime.now(timezone.utc).isoformat()
    inserted = 0
    for a in articles:
        try:
            conn.execute(
                "INSERT OR IGNORE INTO academic_articles "
                "(journal, title, authors, link, doi, published, abstract, china_match, fetched_at) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    a["journal"],
                    a["title"],
                    a.get("authors", ""),
                    a.get("link", ""),
                    a.get("doi", ""),
                    a.get("published", ""),
                    a.get("abstract", ""),
                    a.get("china_match", ""),
                    now,
                ),
            )
            inserted += 1
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    return inserted


# ---------------------------------------------------------------------------
# Query helpers (for dashboard)
# ---------------------------------------------------------------------------

def get_recent_articles(
    conn: sqlite3.Connection, limit: int = 50, journal: str = "", q: str = "",
    ranked: bool = False,
) -> list[dict]:
    """Return recent academic articles, with optional filters.

    If ranked=True, order by learned preference score instead of date.
    """
    if ranked:
        return _get_ranked_articles(conn, limit=limit, journal=journal, q=q)

    query = """
        SELECT a.id, a.journal, a.title, a.authors, a.link, a.doi,
               a.published, a.abstract, a.china_match, a.fetched_at,
               COALESCE(v.vote, 0) as vote
        FROM academic_articles a
        LEFT JOIN academic_votes v ON v.article_id = a.id
        WHERE 1=1
    """
    params: list = []
    if journal:
        query += " AND a.journal = ?"
        params.append(journal)
    if q:
        query += " AND (a.title LIKE ? OR a.abstract LIKE ? OR a.authors LIKE ?)"
        params.extend([f"%{q}%", f"%{q}%", f"%{q}%"])
    query += " ORDER BY a.published DESC, a.id DESC LIMIT ?"
    params.append(limit)
    cur = conn.execute(query, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_journal_summary(conn: sqlite3.Connection) -> list[dict]:
    """Aggregate articles by journal."""
    cur = conn.execute("""
        SELECT journal, COUNT(*) as count, MAX(published) as latest
        FROM academic_articles
        GROUP BY journal
        ORDER BY count DESC
    """)
    return [
        {"journal": r[0], "count": r[1], "latest": r[2]}
        for r in cur.fetchall()
    ]


# ---------------------------------------------------------------------------
# Voting & preference learning
# ---------------------------------------------------------------------------

def cast_vote(conn: sqlite3.Connection, article_id: int, vote: int) -> dict:
    """Record or update a vote (+1/-1) for an article. vote=0 removes it."""
    now = datetime.now(timezone.utc).isoformat()
    if vote == 0:
        conn.execute("DELETE FROM academic_votes WHERE article_id = ?", (article_id,))
    else:
        conn.execute(
            "INSERT INTO academic_votes (article_id, vote, voted_at) VALUES (?, ?, ?) "
            "ON CONFLICT(article_id) DO UPDATE SET vote = excluded.vote, voted_at = excluded.voted_at",
            (article_id, 1 if vote > 0 else -1, now),
        )
    conn.commit()
    row = conn.execute(
        "SELECT COALESCE(v.vote, 0) FROM academic_articles a "
        "LEFT JOIN academic_votes v ON v.article_id = a.id WHERE a.id = ?",
        (article_id,),
    ).fetchone()
    return {"article_id": article_id, "vote": row[0] if row else 0}


def get_preferences(conn: sqlite3.Connection) -> dict:
    """Compute learned preference weights from vote history.

    Returns journal weights and keyword weights, each as avg vote score.
    These are used by _get_ranked_articles to score new/unvoted articles.
    """
    # Journal preferences: avg vote per journal
    journal_weights = {}
    cur = conn.execute("""
        SELECT a.journal, AVG(v.vote) as avg_vote, COUNT(*) as n
        FROM academic_votes v
        JOIN academic_articles a ON a.id = v.article_id
        GROUP BY a.journal
    """)
    for r in cur.fetchall():
        journal_weights[r[0]] = {"avg_vote": r[1], "n": r[2]}

    # Keyword preferences: for each china_match keyword, avg vote
    keyword_weights = {}
    cur = conn.execute("""
        SELECT a.china_match, v.vote
        FROM academic_votes v
        JOIN academic_articles a ON a.id = v.article_id
        WHERE a.china_match IS NOT NULL AND a.china_match != '' AND a.china_match != 'all'
    """)
    for match_str, vote in cur.fetchall():
        for part in match_str.split("; "):
            kw = part.split(":")[-1] if ":" in part else part
            if kw not in keyword_weights:
                keyword_weights[kw] = {"total": 0, "n": 0}
            keyword_weights[kw]["total"] += vote
            keyword_weights[kw]["n"] += 1

    for kw, d in keyword_weights.items():
        d["avg_vote"] = d["total"] / d["n"] if d["n"] else 0

    # Author preferences: avg vote per author last name
    author_weights = {}
    cur = conn.execute("""
        SELECT a.authors, v.vote
        FROM academic_votes v
        JOIN academic_articles a ON a.id = v.article_id
        WHERE a.authors IS NOT NULL AND a.authors != ''
    """)
    for authors_str, vote in cur.fetchall():
        for author in authors_str.split("; "):
            last_name = author.split(",")[0].strip().lower()
            if last_name:
                if last_name not in author_weights:
                    author_weights[last_name] = {"total": 0, "n": 0}
                author_weights[last_name]["total"] += vote
                author_weights[last_name]["n"] += 1

    for a, d in author_weights.items():
        d["avg_vote"] = d["total"] / d["n"] if d["n"] else 0

    return {
        "journal": journal_weights,
        "keyword": keyword_weights,
        "author": author_weights,
    }


def _score_article(article: dict, prefs: dict) -> float:
    """Compute a preference score for an article based on learned weights.

    Score = explicit_vote (if any) * 10
          + journal_preference
          + avg(matching keyword preferences)
          + avg(matching author preferences)
    """
    score = 0.0

    # Explicit vote dominates
    explicit = article.get("vote", 0)
    if explicit:
        score += explicit * 10

    # Journal preference
    jp = prefs["journal"].get(article.get("journal", ""), {})
    if jp:
        score += jp["avg_vote"] * 2

    # Keyword preference
    match_str = article.get("china_match", "") or ""
    if match_str and match_str != "all":
        kw_scores = []
        for part in match_str.split("; "):
            kw = part.split(":")[-1] if ":" in part else part
            kp = prefs["keyword"].get(kw, {})
            if kp:
                kw_scores.append(kp["avg_vote"])
        if kw_scores:
            score += sum(kw_scores) / len(kw_scores)

    # Author preference
    authors_str = article.get("authors", "") or ""
    if authors_str:
        author_scores = []
        for author in authors_str.split("; "):
            last_name = author.split(",")[0].strip().lower()
            ap = prefs["author"].get(last_name, {})
            if ap:
                author_scores.append(ap["avg_vote"])
        if author_scores:
            score += sum(author_scores) / len(author_scores)

    return score


def _get_ranked_articles(
    conn: sqlite3.Connection, limit: int = 50, journal: str = "", q: str = ""
) -> list[dict]:
    """Return articles ordered by learned preference score."""
    prefs = get_preferences(conn)

    query = """
        SELECT a.id, a.journal, a.title, a.authors, a.link, a.doi,
               a.published, a.abstract, a.china_match, a.fetched_at,
               COALESCE(v.vote, 0) as vote
        FROM academic_articles a
        LEFT JOIN academic_votes v ON v.article_id = a.id
        WHERE 1=1
    """
    params: list = []
    if journal:
        query += " AND a.journal = ?"
        params.append(journal)
    if q:
        query += " AND (a.title LIKE ? OR a.abstract LIKE ? OR a.authors LIKE ?)"
        params.extend([f"%{q}%", f"%{q}%", f"%{q}%"])
    cur = conn.execute(query, params)
    cols = [d[0] for d in cur.description]
    articles = [dict(zip(cols, row)) for row in cur.fetchall()]

    # Score and sort
    for a in articles:
        a["_score"] = _score_article(a, prefs)
    articles.sort(key=lambda a: (a["_score"], a.get("published", "")), reverse=True)

    # Strip internal score, return top N
    for a in articles[:limit]:
        a["score"] = round(a.pop("_score"), 2)
    return articles[:limit]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Fetch academic publications on China")
    parser.add_argument("--show", action="store_true", help="Show latest articles from DB")
    parser.add_argument("--summary", action="store_true", help="Per-journal summary")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args()

    conn = get_academic_db()

    if args.summary:
        summary = get_journal_summary(conn)
        total = sum(s["count"] for s in summary)
        if args.json:
            print(json.dumps({"total": total, "journals": summary}, indent=2))
        else:
            print(f"Total articles: {total} across {len(summary)} journals\n")
            print(f"{'Journal':<35} {'Count':>6} {'Latest':<20}")
            print("-" * 65)
            for s in summary:
                print(f"{s['journal']:<35} {s['count']:>6} {s['latest'] or '—':<20}")
        conn.close()
        return

    if args.show:
        articles = get_recent_articles(conn, limit=20)
        if args.json:
            print(json.dumps(articles, indent=2))
        else:
            for a in articles:
                print(f"[{a['journal']}] {a['published'] or '—'}")
                print(f"  {a['title']}")
                if a["authors"]:
                    print(f"  by {a['authors'][:80]}")
                if a["link"]:
                    print(f"  {a['link']}")
                print()
        conn.close()
        return

    # Default: fetch and store
    log.info("Fetching academic journals via CrossRef...")
    articles, ok, fail = fetch_academic()
    inserted = store_articles(conn, articles)
    total = conn.execute("SELECT COUNT(*) FROM academic_articles").fetchone()[0]
    log.info(f"Done: {ok} journals OK, {fail} failed, {len(articles)} China-relevant, "
             f"{inserted} new, {total} total in DB")
    conn.close()


if __name__ == "__main__":
    main()
