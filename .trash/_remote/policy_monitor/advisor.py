"""
Policy Advisor — generates structured policy briefs for European government officials.

Retrieves relevant items from the local SQLite DB using keyword search, then
calls Claude (claude-sonnet-4-6) to produce a structured memo.

When ANTHROPIC_API_KEY is not set, returns a source-listing stub so the UI
can still be used and the integration can be activated later.
"""

import os
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

DB_DIR = Path(__file__).parent.parent / "data"
DB_PATH = DB_DIR / "feeds.db"
ACADEMIC_DB_PATH = DB_DIR / "academic.db"

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

_STOP_WORDS = {
    "a", "an", "the", "and", "or", "of", "in", "on", "at", "to", "for",
    "with", "is", "are", "was", "were", "be", "by", "from", "that", "this",
    "what", "how", "why", "when", "where", "who", "which", "about", "china",
    "chinese",
}

SYSTEM_PROMPT = """You are a senior China policy analyst advising European government officials.
Your role is to produce concise, actionable policy briefs based on recent Chinese policy developments.

Guidelines:
- Write for a senior civil servant or minister — clear, direct, no jargon
- Ground every claim in the provided source material; do not invent facts
- Focus on concrete implications for European governments and SMEs
- Be specific about sectors, timelines, and recommended actions
- Tone: authoritative but not alarmist

Output format (use exactly these section headers):
## Executive Summary
(2-3 sentences — the most important thing the reader needs to know)

## Recent Policy Developments
(numbered list of the most significant developments from the sources, each with source name and date)

## Implications for European Governments
(bullet points grouped by theme: trade, industrial policy, regulatory/diplomatic)

## Recommended Actions
- Short-term (0-6 months): ...
- Medium-term (6-18 months): ...

## Key Sources
(brief list of the primary sources used)"""


@dataclass
class PolicyBrief:
    topic: str
    generated_at: str
    days: int
    source_count: int
    stub: bool
    content: str
    sources: list[dict] = field(default_factory=list)
    error: str = ""


def _keywords(query: str) -> list[str]:
    words = query.lower().split()
    return [w.strip(".,;:!?()") for w in words if w not in _STOP_WORDS and len(w) > 2]


def retrieve_context(query: str, days: int = 90, max_items: int = 25) -> list[dict]:
    """Return relevant items from feeds.db and academic.db via keyword search."""
    keywords = _keywords(query)
    if not keywords:
        keywords = query.lower().split()[:5]

    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    items: list[dict] = []

    # --- Policy news (feeds.db) ---
    if DB_PATH.exists():
        conn = sqlite3.connect(str(DB_PATH))
        try:
            like_clauses = " OR ".join(
                ["(title LIKE ? OR summary LIKE ?)"] * len(keywords)
            )
            params: list = []
            for kw in keywords:
                params += [f"%{kw}%", f"%{kw}%"]
            params.append(cutoff)
            params.append(max_items)

            cur = conn.execute(
                f"SELECT source, category, title, link, published, summary FROM items "
                f"WHERE ({like_clauses}) AND published >= ? "
                f"ORDER BY published DESC LIMIT ?",
                params,
            )
            cols = [d[0] for d in cur.description]
            for row in cur.fetchall():
                d = dict(zip(cols, row))
                d["type"] = "news"
                items.append(d)
        finally:
            conn.close()

    # --- Academic articles ---
    if ACADEMIC_DB_PATH.exists():
        try:
            conn = sqlite3.connect(str(ACADEMIC_DB_PATH))
            like_clauses = " OR ".join(
                ["(title LIKE ? OR abstract LIKE ?)"] * len(keywords)
            )
            params = []
            for kw in keywords:
                params += [f"%{kw}%", f"%{kw}%"]
            params.append(cutoff)
            params.append(max(5, max_items // 5))

            cur = conn.execute(
                f"SELECT journal as source, 'academic' as category, title, link, published, abstract as summary "
                f"FROM academic_articles "
                f"WHERE ({like_clauses}) AND published >= ? "
                f"ORDER BY published DESC LIMIT ?",
                params,
            )
            cols = [d[0] for d in cur.description]
            for row in cur.fetchall():
                d = dict(zip(cols, row))
                d["type"] = "academic"
                items.append(d)
            conn.close()
        except Exception:
            pass

    seen: set[str] = set()
    unique: list[dict] = []
    for item in items:
        key = item.get("title", "")[:80]
        if key not in seen:
            seen.add(key)
            unique.append(item)

    return unique[:max_items]


def _format_context(items: list[dict]) -> str:
    lines = []
    for i, item in enumerate(items, 1):
        date = (item.get("published") or "")[:10]
        source = item.get("source", "Unknown")
        title = item.get("title", "")
        summary = (item.get("summary") or "")[:300]
        link = item.get("link", "")
        lines.append(f"[{i}] {source} ({date})\nTitle: {title}\nSummary: {summary}\nURL: {link}\n")
    return "\n".join(lines)


def _stub_brief(topic: str, items: list[dict], days: int) -> str:
    lines = [
        f"## Executive Summary",
        f"",
        f"*AI analysis is not enabled. Set ANTHROPIC_API_KEY in your .env file to activate.*",
        f"",
        f"The following {len(items)} source(s) were retrieved for the topic **\"{topic}\"** "
        f"from the last {days} days. Add your API key to generate a full policy brief.",
        f"",
        f"## Retrieved Sources",
        f"",
    ]
    for i, item in enumerate(items, 1):
        date = (item.get("published") or "")[:10]
        source = item.get("source", "Unknown")
        title = item.get("title", "")
        link = item.get("link", "")
        url_part = f" — [{link}]({link})" if link else ""
        lines.append(f"{i}. **[{source}]** {title} ({date}){url_part}")
    if not items:
        lines.append("*No matching sources found in the database for this topic and time range.*")
    return "\n".join(lines)


def _call_claude(topic: str, context: str) -> str:
    try:
        import anthropic
    except ImportError:
        return "*anthropic package not installed. Run: pip install anthropic*"

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    user_message = (
        f"Generate a policy brief for European government officials on the following topic:\n\n"
        f"**Topic:** {topic}\n\n"
        f"**Source material ({len(context.splitlines())} lines from the last 90 days):**\n\n"
        f"{context}"
    )
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_message}],
    )
    return message.content[0].text


def generate_brief(topic: str, days: int = 90) -> PolicyBrief:
    """Retrieve context and produce a PolicyBrief. Stubs if no API key."""
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    items = retrieve_context(topic, days=days)
    is_stub = not ANTHROPIC_API_KEY.strip()

    if is_stub:
        content = _stub_brief(topic, items, days)
    else:
        try:
            ctx = _format_context(items)
            content = _call_claude(topic, ctx)
        except Exception as exc:
            content = f"*Error generating brief: {exc}*"

    sources = [
        {
            "source": item.get("source", ""),
            "title": item.get("title", ""),
            "link": item.get("link", ""),
            "published": (item.get("published") or "")[:10],
            "type": item.get("type", "news"),
        }
        for item in items
    ]

    return PolicyBrief(
        topic=topic,
        generated_at=now,
        days=days,
        source_count=len(items),
        stub=is_stub,
        content=content,
        sources=sources,
    )
