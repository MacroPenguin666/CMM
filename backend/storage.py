"""
Local SQLite storage for fetched policy items.

Database: data/cmm.db — single consolidated DB for the whole project.
"""

import json
import re
import sqlite3
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Project paths — single source of truth. The data/ directory is deliberately
# kept OUTSIDE backend/ (portable code, separate data). Every module must
# import these constants instead of recomputing Path(__file__).parent... —
# the parent-chain length differs by file depth and is easy to miscount,
# which previously spawned a stray backend/data/ shadow folder.
# ---------------------------------------------------------------------------
DATA_DIR = Path(__file__).parent.parent / "data"   # CMM/data (repo-root level)
DB_DIR = DATA_DIR                                   # backward-compat alias
DB_PATH = DATA_DIR / "cmm.db"
LOG_DIR = DATA_DIR / "logs"
RAW_DIR = DATA_DIR / "raw"
REFERENCE_DIR = DATA_DIR / "reference"


def get_conn() -> sqlite3.Connection:
    DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

MINISTRY_SCHEMA = """
CREATE TABLE IF NOT EXISTS articles (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source      TEXT NOT NULL,
    title       TEXT NOT NULL,
    link        TEXT UNIQUE,
    published   TEXT,
    summary     TEXT,
    category    TEXT,
    page_num    INTEGER DEFAULT 1,
    fetched_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_art_source    ON articles(source);
CREATE INDEX IF NOT EXISTS idx_art_published ON articles(published);
"""


def _ministry_table_schema(table: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS "{table}" (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source      TEXT NOT NULL,
    title       TEXT NOT NULL,
    link        TEXT UNIQUE,
    published   TEXT,
    summary     TEXT,
    category    TEXT,
    doc_type    TEXT,
    page_num    INTEGER DEFAULT 1,
    fetched_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS "idx_{table}_published" ON "{table}"(published);
CREATE INDEX IF NOT EXISTS "idx_{table}_source"    ON "{table}"(source);
"""

SCHEMA = """
CREATE TABLE IF NOT EXISTS items (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source      TEXT NOT NULL,
    source_cn   TEXT,
    category    TEXT,
    title       TEXT NOT NULL,
    link        TEXT,
    published   TEXT,
    summary     TEXT,
    fetched_at  TEXT NOT NULL,
    UNIQUE(source, title, link)
);

CREATE TABLE IF NOT EXISTS fetch_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source      TEXT NOT NULL,
    feed_url    TEXT,
    ok          INTEGER NOT NULL,
    error       TEXT,
    item_count  INTEGER DEFAULT 0,
    fetched_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS batch_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at      TEXT NOT NULL,
    completed_at    TEXT,
    sources_run     TEXT,
    sources_ok      TEXT,
    sources_failed  TEXT,
    status          TEXT DEFAULT 'running'
);
"""


def get_db() -> sqlite3.Connection:
    conn = get_conn()
    conn.executescript(SCHEMA)
    return conn


def store_feed_result(conn: sqlite3.Connection, result: dict) -> int:
    """Store a feed fetch result. Returns number of new items inserted."""
    now = datetime.utcnow().isoformat()

    conn.execute(
        "INSERT INTO fetch_log (source, feed_url, ok, error, item_count, fetched_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (
            result["source"],
            result.get("feed_url", ""),
            1 if result["ok"] else 0,
            result.get("error"),
            len(result.get("entries", [])),
            now,
        ),
    )

    new_count = 0
    for entry in result.get("entries", []):
        try:
            conn.execute(
                "INSERT OR IGNORE INTO items "
                "(source, source_cn, category, title, link, published, summary, fetched_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    result["source"],
                    result.get("source_cn", ""),
                    result.get("category", ""),
                    entry.get("title", ""),
                    entry.get("link", ""),
                    entry.get("published", ""),
                    entry.get("summary", ""),
                    now,
                ),
            )
            new_count += conn.total_changes  # rough proxy
        except sqlite3.IntegrityError:
            pass

    conn.commit()
    return new_count


def get_recent_items(conn: sqlite3.Connection, limit: int = 50) -> list[dict]:
    """Return the most recently fetched items."""
    cur = conn.execute(
        "SELECT source, source_cn, category, title, link, published, summary, fetched_at "
        "FROM items ORDER BY fetched_at DESC, id DESC LIMIT ?",
        (limit,),
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_item_count(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM items").fetchone()[0]


# ---------------------------------------------------------------------------
# Generic helpers used by data-source modules (ecb, destatis, global_macro…)
# ---------------------------------------------------------------------------

def upsert_df(conn: sqlite3.Connection, df, table: str, pk_cols: list[str]) -> int:
    """Upsert a pandas DataFrame into a SQLite table.  Returns row count written."""
    import pandas as pd
    try:
        existing = pd.read_sql_query(f'SELECT * FROM "{table}"', conn)
        merged = pd.concat([existing, df]).drop_duplicates(subset=pk_cols, keep="last")
    except Exception:
        merged = df
    merged.to_sql(table, conn, if_exists="replace", index=False)
    conn.commit()
    return len(df)


def get_latest_date(conn: sqlite3.Connection, table: str, date_col: str = "date",
                    default: str = "1990-01-01") -> str:
    """Return MAX(date_col) from table, or *default* if the table is empty/missing."""
    try:
        row = conn.execute(f'SELECT MAX("{date_col}") FROM "{table}"').fetchone()
        return row[0] if row and row[0] else default
    except Exception:
        return default


def get_latest_quarter(conn: sqlite3.Connection, table: str, period_col: str = "period",
                       default: str = "2000-Q1") -> str:
    """Return MAX period as a 'YYYY-QN' string, falling back to *default*."""
    import pandas as pd
    try:
        row = conn.execute(f'SELECT MAX("{period_col}") FROM "{table}"').fetchone()
        if row and row[0]:
            d = pd.Timestamp(str(row[0])[:10])
            q = (d.month - 1) // 3 + 1
            return f"{d.year}-Q{q}"
        return default
    except Exception:
        return default


def get_start_year(conn: sqlite3.Connection, table: str, date_col: str = "period",
                   lookback: int = 1, default: int = 2019) -> int:
    """Return the year to start fetching from (latest stored year minus lookback)."""
    try:
        row = conn.execute(f'SELECT MAX("{date_col}") FROM "{table}"').fetchone()
        if row and row[0]:
            year = int(str(row[0])[:4])
            return max(default, year - lookback)
        return default
    except Exception:
        return default


def _ministry_slug(name: str) -> str:
    """'NDRC — News Releases' → 'ndrc'  |  'Ministry of Finance — News' → 'ministry_of_finance'"""
    part = name.split("—")[0].strip()
    return re.sub(r'[^a-z0-9]+', '_', part.lower()).strip('_')[:40]


# ---------------------------------------------------------------------------
# Ministry tables (one per ministry slug) — stored in cmm.db
# ---------------------------------------------------------------------------

def get_ministries_db() -> sqlite3.Connection:
    """Open cmm.db for ministry table writes."""
    return get_conn()


def ensure_ministry_table(conn: sqlite3.Connection, source_name: str) -> str:
    """Create the per-ministry table if missing. Returns the table name (slug)."""
    table = _ministry_slug(source_name)
    conn.executescript(_ministry_table_schema(table))
    return table


def get_ministry_known_links_t(conn: sqlite3.Connection, table: str) -> set:
    cur = conn.execute(f'SELECT link FROM "{table}" WHERE link IS NOT NULL')
    return {row[0] for row in cur.fetchall()}


def get_ministry_article_count_t(conn: sqlite3.Connection, table: str) -> int:
    try:
        return conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    except sqlite3.OperationalError:
        return 0


def get_ministry_article_count_by_source(conn: sqlite3.Connection, table: str, source: str) -> int:
    """Count articles for a specific source within a table (for per-source full/incremental decision)."""
    try:
        return conn.execute(
            f'SELECT COUNT(*) FROM "{table}" WHERE source = ?', (source,)
        ).fetchone()[0]
    except sqlite3.OperationalError:
        return 0


def store_ministry_result_t(conn: sqlite3.Connection, table: str, result: dict) -> int:
    """Insert articles into a named table. Returns count of newly inserted rows."""
    now = datetime.utcnow().isoformat()
    inserted = 0
    # Migrate: add doc_type column if this is an older table
    try:
        conn.execute(f'ALTER TABLE "{table}" ADD COLUMN doc_type TEXT')
        conn.commit()
    except sqlite3.OperationalError:
        pass  # column already exists
    for entry in result.get("entries", []):
        try:
            conn.execute(
                f'INSERT OR IGNORE INTO "{table}" '
                '(source, title, link, published, summary, category, doc_type, page_num, fetched_at) '
                'VALUES (?,?,?,?,?,?,?,?,?)',
                (
                    result["source"],
                    entry.get("title", ""),
                    entry.get("link") or None,
                    entry.get("published", ""),
                    entry.get("summary", ""),
                    result.get("category", ""),
                    result.get("doc_type") or entry.get("doc_type"),
                    entry.get("page_num", 1),
                    now,
                ),
            )
            inserted += conn.total_changes
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    return inserted


# ---------------------------------------------------------------------------
# policy_docs — unified full-text policy announcement table (AI-analysis core)
# ---------------------------------------------------------------------------

POLICY_DOCS_SCHEMA = """
CREATE TABLE IF NOT EXISTS policy_docs (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    ministry           TEXT NOT NULL,
    source             TEXT NOT NULL,
    source_cn          TEXT,
    category           TEXT,
    doc_type           TEXT,
    title              TEXT NOT NULL,
    url                TEXT NOT NULL UNIQUE,
    doc_number         TEXT,
    published          TEXT,
    summary            TEXT,
    full_text          TEXT,
    text_len           INTEGER,
    instrument_type    TEXT,
    fetch_status       TEXT NOT NULL DEFAULT 'pending',
    http_status        INTEGER,
    error              TEXT,
    discovered_at      TEXT NOT NULL DEFAULT (datetime('now')),
    content_fetched_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_policy_docs_ministry
    ON policy_docs (ministry, published DESC);
CREATE INDEX IF NOT EXISTS idx_policy_docs_status ON policy_docs (fetch_status);
CREATE INDEX IF NOT EXISTS idx_policy_docs_source ON policy_docs (source);
"""


def domain_slug(url: str) -> str:
    """https://www.ndrc.gov.cn/... → 'ndrc' | https://zwgk.mct.gov.cn/... → 'mct'
    | https://www.gov.cn/... → 'gov' (State Council)"""
    from urllib.parse import urlparse
    parts = urlparse(url).netloc.lower().split(".")
    if len(parts) >= 3 and parts[-1] == "cn" and parts[-2] == "gov":
        return "gov" if parts[-3] == "www" else parts[-3]
    return parts[-2] if len(parts) >= 2 else (parts[0] if parts else "unknown")


def _ensure_column(conn: sqlite3.Connection, table: str, col: str, decl: str) -> None:
    """Add a column to an existing table if it's missing (idempotent migration)."""
    cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    if col not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")
        conn.commit()


def get_policy_docs_db() -> sqlite3.Connection:
    conn = get_conn()
    conn.executescript(POLICY_DOCS_SCHEMA)
    # instrument_type is a later addition — add the column (existing DBs) before
    # indexing it, so the index build never races an absent column.
    _ensure_column(conn, "policy_docs", "instrument_type", "TEXT")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_policy_docs_instrument "
                 "ON policy_docs (instrument_type)")
    conn.commit()
    return conn


# Chinese policy instrument vocabulary. Titles put the operative instrument LAST
# ('关于印发《XX办法》的通知' IS a 通知 transmitting an 办法), so classification picks
# the token whose match ENDS furthest right, with the longest token winning ties
# (so 办法 beats the 法 nested inside it, 命令 beats a bare 令, etc.).
_INSTRUMENT_TOKENS = {
    "通知": "通知", "通告": "通告", "公告": "公告", "公报": "公报",
    "意见": "意见", "办法": "办法", "规定": "规定", "规则": "规则",
    "规划": "规划", "纲要": "纲要", "方案": "方案", "决定": "决定",
    "决议": "决议", "批复": "批复", "条例": "条例", "细则": "细则",
    "标准": "标准", "目录": "目录", "命令": "令", "令": "令",
    "复函": "函", "函": "函", "公示": "公示", "报告": "报告", "法": "法",
    "答记者问": "答问", "记者会": "答问", "答问": "答问",
}

# A trailing parenthetical is almost always the 文号 or an annotation, not the
# instrument — strip it so it can't shadow the real head noun
# (e.g. '…的意见(发改法规规〔2022〕1117号)' → '…的意见').
_TRAILING_PAREN = re.compile(r'[\(（【\[][^)）】\]]*[\)）】\]]\s*$')
_TRAILING_PUNCT = '》」』】）)"”\'’ 　'


def classify_instrument(title: str, doc_number: str = "") -> str | None:
    """Infer the policy instrument type (通知/公告/令/意见/法/答问/…) from a title.

    Chinese titles put the operative instrument LAST, so the token whose match
    ends furthest right wins (longest token breaks ties → 办法 beats the 法 in it).
    Bare 法 (law) counts only when it TERMINATES the title, otherwise it fires on
    法治/法规/执法 mid-title. Returns the canonical token, or None for news items.
    """
    if not title:
        return None
    t = title.strip()
    prev = None
    while prev != t:                       # peel nested/repeated trailing parens
        prev = t
        t = _TRAILING_PAREN.sub("", t).strip()
    ends_with_law = t.rstrip(_TRAILING_PUNCT).endswith("法")

    best_key: tuple[int, int] | None = None
    best_val: str | None = None
    for token, canonical in _INSTRUMENT_TOKENS.items():
        if token == "法" and not ends_with_law:
            continue
        idx = t.rfind(token)
        if idx == -1:
            continue
        key = (idx + len(token), len(token))  # (end position, token length)
        if best_key is None or key > best_key:
            best_key, best_val = key, canonical
    return best_val


def backfill_instrument_types(conn: sqlite3.Connection) -> int:
    """(Re)classify instrument_type for every policy_docs row. Idempotent."""
    rows = conn.execute("SELECT id, title FROM policy_docs").fetchall()
    for row in rows:
        conn.execute(
            "UPDATE policy_docs SET instrument_type = ? WHERE id = ?",
            (classify_instrument(row["title"] or ""), row["id"]),
        )
    conn.commit()
    return len(rows)


def get_policy_known_links(conn: sqlite3.Connection, source: str | None = None) -> set:
    if source:
        cur = conn.execute("SELECT url FROM policy_docs WHERE source = ?", (source,))
    else:
        cur = conn.execute("SELECT url FROM policy_docs")
    return {row[0] for row in cur.fetchall()}


def get_policy_doc_count(conn: sqlite3.Connection, source: str | None = None) -> int:
    if source:
        return conn.execute(
            "SELECT COUNT(*) FROM policy_docs WHERE source = ?", (source,)
        ).fetchone()[0]
    return conn.execute("SELECT COUNT(*) FROM policy_docs").fetchone()[0]


def insert_policy_metadata(conn: sqlite3.Connection, result: dict) -> int:
    """Insert discovery-stage rows (fetch_status='pending'). Returns newly inserted count."""
    ministry = domain_slug(result.get("feed_url", ""))
    inserted = 0
    for entry in result.get("entries", []):
        link = entry.get("link")
        if not link:
            continue
        title = entry.get("title", "")
        cur = conn.execute(
            "INSERT OR IGNORE INTO policy_docs "
            "(ministry, source, source_cn, category, doc_type, title, url, "
            " published, summary, instrument_type) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                ministry,
                result["source"],
                result.get("source_cn", ""),
                result.get("category", ""),
                result.get("doc_type"),
                title,
                link,
                entry.get("published", ""),
                entry.get("summary", ""),
                classify_instrument(title),
            ),
        )
        inserted += cur.rowcount
    conn.commit()
    return inserted


def get_pending_policy_docs(conn: sqlite3.Connection, limit: int | None = None,
                            ministry: str | None = None,
                            retry_errors: bool = False) -> list[sqlite3.Row]:
    statuses = "('pending','error')" if retry_errors else "('pending')"
    q = (f"SELECT id, ministry, source, title, url, published FROM policy_docs "
         f"WHERE fetch_status IN {statuses}")
    params: list = []
    if ministry:
        q += " AND ministry = ?"
        params.append(ministry)
    q += " ORDER BY published DESC"
    if limit:
        q += " LIMIT ?"
        params.append(limit)
    return conn.execute(q, params).fetchall()


def update_policy_content(conn: sqlite3.Connection, doc_id: int, *, status: str,
                          full_text: str = "", doc_number: str = "",
                          published: str = "", http_status: int | None = None,
                          error: str = "") -> None:
    sets = ["fetch_status = ?", "content_fetched_at = datetime('now')",
            "http_status = ?", "error = ?"]
    params: list = [status, http_status, error or None]
    if full_text:
        sets += ["full_text = ?", "text_len = ?"]
        params += [full_text, len(full_text)]
    if doc_number:
        sets.append("doc_number = ?")
        params.append(doc_number)
    conn.execute(
        f"UPDATE policy_docs SET {', '.join(sets)} WHERE id = ?",
        params + [doc_id],
    )
    if published:
        conn.execute(
            "UPDATE policy_docs SET published = ? WHERE id = ? "
            "AND (published IS NULL OR published = '')",
            (published, doc_id),
        )


def get_policy_fetch_stats(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT ministry, fetch_status, COUNT(*) AS n, "
        "       COALESCE(SUM(text_len), 0) AS chars "
        "FROM policy_docs GROUP BY ministry, fetch_status ORDER BY ministry"
    ).fetchall()


def get_ministry_db(source_name: str) -> sqlite3.Connection:
    """Open cmm.db for the given ministry source (legacy alias for get_ministries_db)."""
    conn = get_conn()
    conn.executescript(MINISTRY_SCHEMA)
    return conn


def get_ministry_known_links(conn: sqlite3.Connection) -> set:
    """Return all article links already stored — used for incremental pagination stop."""
    cur = conn.execute("SELECT link FROM articles WHERE link IS NOT NULL")
    return {row[0] for row in cur.fetchall()}


def get_ministry_article_count(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]


def store_ministry_result(conn: sqlite3.Connection, result: dict) -> int:
    """Store paginated scrape result into a per-ministry DB. Returns new article count."""
    now = datetime.utcnow().isoformat()
    inserted = 0
    for entry in result.get("entries", []):
        try:
            conn.execute(
                "INSERT OR IGNORE INTO articles "
                "(source, title, link, published, summary, category, page_num, fetched_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (
                    result["source"],
                    entry.get("title", ""),
                    entry.get("link") or None,
                    entry.get("published", ""),
                    entry.get("summary", ""),
                    result.get("category", ""),
                    entry.get("page_num", 1),
                    now,
                ),
            )
            inserted += conn.total_changes
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    return inserted


def get_fetch_stats(conn: sqlite3.Connection) -> dict:
    """Summary stats from fetch_log."""
    row = conn.execute(
        "SELECT COUNT(*), SUM(ok), SUM(item_count) FROM fetch_log"
    ).fetchone()
    return {
        "total_fetches": row[0],
        "successful": row[1] or 0,
        "total_items_seen": row[2] or 0,
    }
