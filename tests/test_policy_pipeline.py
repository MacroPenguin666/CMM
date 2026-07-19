"""
Tests for the ministry policy pipeline: list discovery → full-text extraction
→ policy_docs storage. Fixtures under tests/fixtures/ are real pages saved
from ministry sites on 2026-07-07 (see meta.json for source URLs).
"""

import json
from pathlib import Path
from urllib.parse import urlparse

import pytest
from lxml import html as lhtml

from backend.fetchers.ministry_scraper import _extract_articles, _parse_date
from backend.fetchers.policy_content import (_find_doc_number, extract_article,
                                             fetch_one)

FIXTURES = Path(__file__).parent / "fixtures"
META = json.loads((FIXTURES / "meta.json").read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Stage 1 — list-page discovery
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("slug", list(META))
def test_list_extraction(slug):
    meta = META[slug]
    tree = lhtml.fromstring((FIXTURES / f"list_{slug}.html").read_bytes())
    articles = _extract_articles(tree, meta["list_url"])

    assert len(articles) >= 8, f"{slug}: too few entries"
    dated = sum(1 for a in articles if a["published"])
    assert dated >= 8, f"{slug}: expected at least 8 dated entries, got {dated}"
    for a in articles:
        assert a["link"].startswith("http")
        assert len(a["title"]) >= 8


def test_links_resolve_against_page_url_not_domain_root():
    """Regression: gov.cn hrefs like ./202606/t...html must resolve against the
    list-page URL — resolving against the domain root produced dead links."""
    meta = META["ndrc"]
    tree = lhtml.fromstring((FIXTURES / "list_ndrc.html").read_bytes())
    articles = _extract_articles(tree, meta["list_url"])
    section_path = urlparse(meta["list_url"]).path  # /xxgk/zcfb/tz/
    in_section = [a for a in articles
                  if urlparse(a["link"]).path.startswith(section_path)]
    assert in_section, "page-relative hrefs should resolve into the section path"
    # The bug resolved ./202606/t...html to domain root: /202606/t...html
    for a in articles:
        assert not urlparse(a["link"]).path.startswith("/20"), a["link"]


# ---------------------------------------------------------------------------
# Stage 2 — article full-text extraction
# ---------------------------------------------------------------------------

# slugs whose fixture is a real article page (the others are index/nav pages
# that must extract as empty rather than as navigation garbage)
ARTICLE_SLUGS = ["ndrc", "ministry", "mem"]
INDEX_SLUGS = ["mofcom", "mct"]


@pytest.mark.parametrize("slug", ARTICLE_SLUGS)
def test_article_extraction(slug):
    art = extract_article((FIXTURES / f"article_{slug}.html").read_bytes(),
                          META[slug]["article_url"])
    assert len(art["text"]) >= 100, f"{slug}: no text extracted"
    assert art["doc_number"], f"{slug}: 文号 not found"
    assert "〔" in art["doc_number"] or "年第" in art["doc_number"]
    # No navigation garbage
    for nav in ("政府信息公开指南", "网站地图", "上一页"):
        assert nav not in art["text"][:200]


@pytest.mark.parametrize("slug", INDEX_SLUGS)
def test_index_pages_extract_empty(slug):
    """Category/index pages linked from list pages must not yield nav text."""
    art = extract_article((FIXTURES / f"article_{slug}.html").read_bytes(),
                          META[slug]["article_url"])
    assert art["text"] == ""


def test_doc_number_patterns():
    cases = {
        "国家发展改革委关于做好X工作的通知\n发改能源〔2026〕884号\n各省...": "发改能源〔2026〕884号",
        "商务部公告2026年第25号\n根据规定...": "商务部公告2026年第25号",
        "应急〔2026〕60号": "应急〔2026〕60号",
        "国办发[2025]7号文件": "国办发[2025]7号",
        "完全没有文号的普通新闻内容": "",
    }
    for text, expected in cases.items():
        assert _find_doc_number(text) == expected


def test_date_parse():
    assert _parse_date("2026年6月25日") == "2026-06-25"
    assert _parse_date("发布时间：2026-06-25") == "2026-06-25"
    assert _parse_date("2026/6/5") == "2026-06-05"


def test_fetch_one_skips_binary_urls():
    result = fetch_one("https://www.example.gov.cn/attachment/P020260101.pdf")
    assert result["status"] == "binary"


# ---------------------------------------------------------------------------
# Storage round-trip (temp DB)
# ---------------------------------------------------------------------------

def test_policy_docs_storage_roundtrip(tmp_path, monkeypatch):
    import backend.storage as st
    monkeypatch.setattr(st, "DB_DIR", tmp_path)
    monkeypatch.setattr(st, "DB_PATH", tmp_path / "test.db")

    conn = st.get_policy_docs_db()
    result = {
        "source": "NDRC — Notices", "source_cn": "国家发展改革委-通知",
        "category": "central_government", "doc_type": "通知",
        "feed_url": "https://www.ndrc.gov.cn/xxgk/zcfb/tz/",
        "entries": [
            {"title": "关于印发某规划的通知", "link": "https://www.ndrc.gov.cn/xxgk/zcfb/tz/202606/t1.html",
             "published": "2026-06-25", "summary": ""},
            {"title": "另一个通知标题内容", "link": "https://www.ndrc.gov.cn/xxgk/zcfb/tz/202606/t2.html",
             "published": "", "summary": ""},
        ],
    }
    assert st.insert_policy_metadata(conn, result) == 2
    assert st.insert_policy_metadata(conn, result) == 0        # idempotent
    assert st.get_policy_doc_count(conn, source="NDRC — Notices") == 2
    assert st.get_policy_known_links(conn) == {
        "https://www.ndrc.gov.cn/xxgk/zcfb/tz/202606/t1.html",
        "https://www.ndrc.gov.cn/xxgk/zcfb/tz/202606/t2.html",
    }

    pending = st.get_pending_policy_docs(conn)
    assert len(pending) == 2
    assert pending[0]["ministry"] == "ndrc"

    doc = next(r for r in pending if r["url"].endswith("t2.html"))
    st.update_policy_content(conn, doc["id"], status="ok",
                             full_text="正文" * 200, doc_number="发改〔2026〕1号",
                             published="2026-06-20", http_status=200)
    conn.commit()
    row = conn.execute("SELECT * FROM policy_docs WHERE id = ?", (doc["id"],)).fetchone()
    assert row["fetch_status"] == "ok"
    assert row["text_len"] == 400
    assert row["doc_number"] == "发改〔2026〕1号"
    assert row["published"] == "2026-06-20"          # filled because it was empty
    assert len(st.get_pending_policy_docs(conn)) == 1

    # error rows only reappear with retry_errors
    other = st.get_pending_policy_docs(conn)[0]
    st.update_policy_content(conn, other["id"], status="error", error="HTTP 500",
                             http_status=500)
    conn.commit()
    assert st.get_pending_policy_docs(conn) == []
    assert len(st.get_pending_policy_docs(conn, retry_errors=True)) == 1

    # published set at discovery must not be overwritten by page-derived date
    row2 = conn.execute("SELECT published FROM policy_docs WHERE url LIKE '%t1.html'").fetchone()
    assert row2["published"] == "2026-06-25"

    stats = st.get_policy_fetch_stats(conn)
    assert sum(r["n"] for r in stats) == 2
    conn.close()


def test_domain_slug():
    from backend.storage import domain_slug
    assert domain_slug("https://www.ndrc.gov.cn/xxgk/") == "ndrc"
    assert domain_slug("https://zwgk.mct.gov.cn/zfxxgkml/") == "mct"
    assert domain_slug("http://www.pbc.gov.cn/rmyh/") == "pbc"
    assert domain_slug("https://www.gov.cn/zhengce/") == "gov"   # State Council


def test_classify_instrument():
    from backend.storage import classify_instrument
    cases = {
        # nested: a 通知 that transmits an 办法 is a 通知 (rightmost instrument wins)
        "关于印发《XX管理办法》的通知": "通知",
        "国家发展改革委关于印发《节能行动方案》的通知": "通知",
        "国家发展改革委令 第55号": "令",
        "商务部令2026年第2号": "令",
        "关于促进民营经济发展的意见": "意见",
        "危险化学品安全管理条例": "条例",
        "商务部公告2026年第25号": "公告",
        # bare 法 (law) must not be shadowed, and 办法 must beat the 法 inside it
        "中华人民共和国对外贸易法": "法",
        "生产安全事故应急预案管理办法": "办法",
        # bare 法 fires ONLY at title-end: 法治/法规 mid-title must not classify as 法
        "应急管理部2025年法治政府建设年度报告": "报告",
        "关于执行招标投标法规制度的若干意见(发改法规规〔2022〕1117号)": "意见",
        # MFA spokesperson briefings
        "外交部发言人林剑主持例行记者会": "答问",
        "2026年7月8日外交部发言人就相关问题答记者问": "答问",
        # plain news → unclassified
        "李强主持召开国务院常务会议": None,
        "": None,
    }
    for title, expected in cases.items():
        assert classify_instrument(title) == expected, title


def test_insert_classifies_instrument(tmp_path, monkeypatch):
    import backend.storage as st
    monkeypatch.setattr(st, "DB_PATH", tmp_path / "test.db")
    conn = st.get_policy_docs_db()
    st.insert_policy_metadata(conn, {
        "source": "NDRC — Orders", "feed_url": "https://www.ndrc.gov.cn/xxgk/zcfb/fzggwl/",
        "doc_type": "发展改革委令",
        "entries": [
            {"title": "国家发展改革委令 第55号", "link": "https://www.ndrc.gov.cn/a/t1.html"},
            {"title": "关于印发《某某办法》的通知", "link": "https://www.ndrc.gov.cn/a/t2.html"},
            {"title": "李强主持召开国务院常务会议", "link": "https://www.ndrc.gov.cn/a/t3.html"},
        ],
    })
    got = dict(conn.execute(
        "SELECT title, instrument_type FROM policy_docs").fetchall())
    assert got["国家发展改革委令 第55号"] == "令"
    assert got["关于印发《某某办法》的通知"] == "通知"
    assert got["李强主持召开国务院常务会议"] is None

    # backfill re-derives the same values idempotently
    conn.execute("UPDATE policy_docs SET instrument_type = NULL")
    assert st.backfill_instrument_types(conn) == 3
    assert conn.execute(
        "SELECT instrument_type FROM policy_docs WHERE title LIKE '%令%'"
    ).fetchone()[0] == "令"
    conn.close()
