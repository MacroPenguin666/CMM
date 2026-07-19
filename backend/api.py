"""
China Policy Monitor — Dashboard

Local web UI with map landing page, news feeds, financial data, and source tracking.

Usage:
    python dashboard.py              # start on http://localhost:5001
    python dashboard.py --port 8080  # custom port
"""

import argparse
import hmac
import json
import sqlite3
import time
from collections import defaultdict
from datetime import datetime
from functools import wraps
from pathlib import Path

from flask import Flask, jsonify, request

from backend.config import ADMIN_TOKEN
from backend.storage import DATA_DIR, DB_PATH, get_db, get_fetch_stats, get_item_count, get_recent_items
from backend.fetchers.financial import get_financial_db, get_latest_snapshots, get_series
from backend.fetchers.macro_dash import build_payload
from backend.fetchers.bruegel import (
    get_bruegel_db, get_bruegel_snapshots, get_bruegel_series, get_bruegel_indicators,
    get_provincial_data, get_provincial_indicators,
)
from backend.fetchers.macro import (
    get_macro_db, get_macro_series, get_macro_variables, get_stored_version,
    VARIABLE_META, CATEGORIES,
)
from backend.fetchers.academic import get_academic_db, get_recent_articles, get_journal_summary, cast_vote, get_preferences
from backend.fetchers.regulations import (
    get_regulations_db, get_mofcom_docs, get_npc_bills,
    get_regulations_stats, HIERARCHY_LABELS,
)
from backend.sources.loader import (
    get_all_sources,
    get_direct_feeds,
    get_rsshub_feeds,
    get_wechat_accounts,
    load_registry,
)

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Guard for cost-bearing / write routes (advisor brief, academic vote).
# Fails closed: with no CMM_ADMIN_TOKEN configured, these routes are disabled
# rather than left open, since this dashboard is otherwise served publicly.
# ---------------------------------------------------------------------------
_rate_limit_hits: dict[str, list[float]] = defaultdict(list)


def _rate_limited(key: str, max_calls: int, window_seconds: int) -> bool:
    now = time.monotonic()
    hits = _rate_limit_hits[key]
    hits[:] = [t for t in hits if now - t < window_seconds]
    if len(hits) >= max_calls:
        return True
    hits.append(now)
    return False


def require_admin(max_calls: int = 10, window_seconds: int = 3600):
    def decorator(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            if not ADMIN_TOKEN:
                return jsonify({"error": "endpoint disabled: set CMM_ADMIN_TOKEN to enable"}), 403
            supplied = request.headers.get("X-Admin-Token", "")
            if not hmac.compare_digest(supplied, ADMIN_TOKEN):
                return jsonify({"error": "unauthorized"}), 401
            client_ip = request.headers.get("CF-Connecting-IP", request.remote_addr or "unknown")
            if _rate_limited(f"{fn.__name__}:{client_ip}", max_calls, window_seconds):
                return jsonify({"error": "rate limit exceeded"}), 429
            return fn(*args, **kwargs)
        return wrapped
    return decorator


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------

@app.route("/api/news")
def api_news():
    db = get_db()
    limit = max(0, min(request.args.get("limit", 50, type=int), 1000))
    category = request.args.get("category", "")
    q = request.args.get("q", "")
    source = request.args.get("source", "")

    query = "SELECT source, source_cn, category, title, link, published, summary, fetched_at FROM items WHERE 1=1"
    params = []
    if category:
        query += " AND category = ?"
        params.append(category)
    if source:
        query += " AND source = ?"
        params.append(source)
    if q:
        query += " AND (title LIKE ? OR summary LIKE ?)"
        params.extend([f"%{q}%", f"%{q}%"])
    query += " ORDER BY fetched_at DESC, id DESC LIMIT ?"
    params.append(limit)

    cur = db.execute(query, params)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    db.close()
    return jsonify(rows)


@app.route("/api/news/sources")
def api_news_sources():
    db = get_db()
    cur = db.execute(
        "SELECT source, category, COUNT(*) as count FROM items GROUP BY source ORDER BY count DESC"
    )
    rows = [{"source": r[0], "category": r[1], "count": r[2]} for r in cur.fetchall()]
    db.close()
    return jsonify(rows)


@app.route("/api/news/stats")
def api_news_stats():
    db = get_db()
    stats = get_fetch_stats(db)
    stats["total_items"] = get_item_count(db)
    cur = db.execute("SELECT MIN(fetched_at), MAX(fetched_at) FROM items")
    row = cur.fetchone()
    stats["first_fetch"] = row[0]
    stats["last_fetch"] = row[1]
    db.close()
    return jsonify(stats)


@app.route("/api/financial/snapshots")
def api_financial_snapshots():
    conn = get_financial_db()
    snapshots = get_latest_snapshots(conn)
    if request.args.get("sparklines") == "1":
        for s in snapshots:
            cur = conn.execute(
                "SELECT date, value FROM financial_series WHERE indicator = ? "
                "ORDER BY date DESC LIMIT 30",
                (s["indicator"],),
            )
            pts = [{"d": r[0], "v": r[1]} for r in cur.fetchall()]
            pts.reverse()
            s["spark"] = pts
    conn.close()
    return jsonify(snapshots)


@app.route("/api/financial/series/<indicator>")
def api_financial_series(indicator):
    limit = max(0, min(request.args.get("limit", 90, type=int), 5000))
    conn = get_financial_db()
    data = get_series(conn, indicator, limit)
    conn.close()
    return jsonify(data)


@app.route("/api/financial/indicators")
def api_financial_indicators():
    conn = get_financial_db()
    cur = conn.execute(
        "SELECT DISTINCT indicator, category FROM financial_series ORDER BY category, indicator"
    )
    rows = [{"indicator": r[0], "category": r[1]} for r in cur.fetchall()]
    conn.close()
    return jsonify(rows)


@app.route("/api/bruegel/snapshots")
def api_bruegel_snapshots():
    conn = get_bruegel_db()
    snapshots = get_bruegel_snapshots(conn)
    # Optionally include sparkline data
    if request.args.get("sparklines") == "1":
        for s in snapshots:
            cur = conn.execute(
                "SELECT date, value FROM bruegel_series WHERE indicator = ? "
                "ORDER BY date DESC LIMIT 30",
                (s["indicator"],),
            )
            pts = [{"d": r[0], "v": r[1]} for r in cur.fetchall()]
            pts.reverse()
            s["spark"] = pts
    conn.close()
    return jsonify(snapshots)


@app.route("/api/bruegel/series/<indicator>")
def api_bruegel_series(indicator):
    limit = max(0, min(request.args.get("limit", 180, type=int), 5000))
    conn = get_bruegel_db()
    data = get_bruegel_series(conn, indicator, limit)
    conn.close()
    return jsonify(data)


@app.route("/api/bruegel/indicators")
def api_bruegel_indicators():
    conn = get_bruegel_db()
    rows = get_bruegel_indicators(conn)
    conn.close()
    return jsonify(rows)


@app.route("/api/bruegel/provincial")
def api_bruegel_provincial():
    """Provincial data with YoY change. ?indicator=GDP&year=2024"""
    conn = get_bruegel_db()
    indicator = request.args.get("indicator", "")
    year = request.args.get("year", 0, type=int)
    data = get_provincial_data(conn, indicator=indicator, year=year)
    # Attach previous year values for YoY comparison
    if year and indicator:
        prev = get_provincial_data(conn, indicator=indicator, year=year - 1)
        prev_map = {d["province"]: d["value"] for d in prev}
        for d in data:
            pv = prev_map.get(d["province"])
            if pv and pv != 0:
                d["change_pct"] = round((d["value"] - pv) / abs(pv) * 100, 2)
            else:
                d["change_pct"] = None
    conn.close()
    return jsonify(data)


@app.route("/api/bruegel/provincial/indicators")
def api_bruegel_provincial_indicators():
    """Available provincial indicators with year ranges."""
    conn = get_bruegel_db()
    rows = get_provincial_indicators(conn)
    conn.close()
    return jsonify(rows)


@app.route("/api/sources/all")
def api_sources_all():
    """Return ALL sources from every section, with sync status from DB."""
    reg = load_registry()

    # Gather all sources from all registry sections
    all_sources = []

    # Standard list sections
    for section in ["central_government", "ministries", "regulators", "party_bodies",
                    "judiciary", "state_media", "open_resources", "direct_feeds"]:
        items = reg.get(section, [])
        if isinstance(items, list):
            for s in items:
                s["_section"] = section
                all_sources.append(s)

    # WeChat accounts
    social = reg.get("social_media", {})
    for w in social.get("wechat_accounts", []):
        all_sources.append({
            "name": w["name"],
            "name_cn": w["name"],
            "url": f"wechat://{w.get('wechat_id', '')}",
            "category": "wechat",
            "content_types": ["wechat"],
            "_section": "wechat",
            "notes": w.get("notes", ""),
        })

    # Intermediary services
    for svc in social.get("intermediary_services", []):
        all_sources.append({
            "name": svc["name"],
            "url": svc.get("url", ""),
            "category": "service",
            "_section": "intermediary",
            "notes": svc.get("notes", ""),
        })

    # Financial data sources (AKShare)
    financial_sources = [
        {"name": "SHIBOR (Interbank Rates)", "name_cn": "上海银行间同业拆放利率",
         "url": "akshare://shibor", "category": "financial", "_section": "akshare",
         "content_types": ["bond", "rates"], "notes": "Via AKShare → CFETS"},
        {"name": "China Gov Bond Yield Curve", "name_cn": "中债国债收益率曲线",
         "url": "akshare://bond_yield", "category": "financial", "_section": "akshare",
         "content_types": ["bond"], "notes": "Via AKShare → ChinaBond"},
        {"name": "SSE Composite Index", "name_cn": "上证综合指数",
         "url": "akshare://index_000001", "category": "financial", "_section": "akshare",
         "content_types": ["equity"], "notes": "Via AKShare → East Money"},
        {"name": "SZSE Component Index", "name_cn": "深证成份指数",
         "url": "akshare://index_399001", "category": "financial", "_section": "akshare",
         "content_types": ["equity"], "notes": "Via AKShare → East Money"},
        {"name": "CSI 300 Index", "name_cn": "沪深300",
         "url": "akshare://index_000300", "category": "financial", "_section": "akshare",
         "content_types": ["equity"], "notes": "Via AKShare → East Money"},
        {"name": "ChiNext Index", "name_cn": "创业板指数",
         "url": "akshare://index_399006", "category": "financial", "_section": "akshare",
         "content_types": ["equity"], "notes": "Via AKShare → East Money"},
        {"name": "USD/CNH Exchange Rate", "name_cn": "美元/离岸人民币",
         "url": "akshare://forex_usdcnh", "category": "financial", "_section": "akshare",
         "content_types": ["fx"], "notes": "Via AKShare → East Money"},
        {"name": "EUR/CNH Exchange Rate", "name_cn": "欧元/离岸人民币",
         "url": "akshare://forex_eurcnh", "category": "financial", "_section": "akshare",
         "content_types": ["fx"], "notes": "Via AKShare → East Money"},
        {"name": "China CPI (Monthly)", "name_cn": "CPI月率",
         "url": "akshare://macro_cpi", "category": "financial", "_section": "akshare",
         "content_types": ["macro"], "notes": "Via AKShare → Jin10"},
        {"name": "China PMI", "name_cn": "制造业PMI",
         "url": "akshare://macro_pmi", "category": "financial", "_section": "akshare",
         "content_types": ["macro"], "notes": "Via AKShare → Jin10"},
        {"name": "China Exports YoY", "name_cn": "出口同比",
         "url": "akshare://trade_exports", "category": "financial", "_section": "akshare",
         "content_types": ["trade"], "notes": "Via AKShare → Jin10"},
        {"name": "China Imports YoY", "name_cn": "进口同比",
         "url": "akshare://trade_imports", "category": "financial", "_section": "akshare",
         "content_types": ["trade"], "notes": "Via AKShare → Jin10"},
        {"name": "China Trade Balance", "name_cn": "贸易差额",
         "url": "akshare://trade_balance", "category": "financial", "_section": "akshare",
         "content_types": ["trade"], "notes": "Via AKShare → Jin10"},
    ]
    all_sources.extend(financial_sources)

    # Global Macro Database source
    all_sources.append({
        "name": "Global Macro Database", "name_cn": "全球宏观数据库",
        "url": "https://www.globalmacrodata.com", "category": "financial",
        "_section": "gmd", "content_types": ["macro", "historical"],
        "notes": "75 annual macro variables for China (1640-2030). Müller, Xu, Lehbib & Chen (2025)."
    })

    # Bruegel China Economic Database source
    all_sources.append({
        "name": "Bruegel China Economic Database", "name_cn": "布鲁盖尔中国经济数据库",
        "url": "https://www.bruegel.org/dataset/china-economic-database",
        "category": "financial", "_section": "bruegel",
        "content_types": ["macro", "financial", "structural", "eu_china"],
        "notes": "48+ indicator categories. Macro, finance, structural, and EU-China data from Bruegel."
    })

    # Build sync status from DB
    sync_map = {}  # source_name → last_fetched_at
    try:
        db = get_db()
        cur = db.execute(
            "SELECT source, MAX(fetched_at) as last_sync, COUNT(*) as items "
            "FROM items GROUP BY source"
        )
        for row in cur.fetchall():
            sync_map[row[0]] = {"last_sync": row[1], "items": row[2], "type": "news"}

        # Fetch log entries
        cur = db.execute(
            "SELECT source, MAX(fetched_at) as last_sync, SUM(ok) as ok_count "
            "FROM fetch_log GROUP BY source"
        )
        for row in cur.fetchall():
            name = row[0]
            if name not in sync_map:
                sync_map[name] = {"last_sync": row[1], "items": 0, "type": "news"}
            sync_map[name]["ok_count"] = row[2]
        db.close()
    except Exception:
        pass

    # Financial sync status
    try:
        conn = get_financial_db()
        cur = conn.execute(
            "SELECT indicator, MAX(fetched_at) as last_sync, COUNT(*) as points "
            "FROM financial_snapshots GROUP BY indicator"
        )
        fin_indicator_to_source = {
            "SHIBOR_ON": "SHIBOR (Interbank Rates)",
            "CGB_10Y": "China Gov Bond Yield Curve",
            "SSE_Composite": "SSE Composite Index",
            "SZSE_Component": "SZSE Component Index",
            "CSI_300": "CSI 300 Index",
            "ChiNext": "ChiNext Index",
            "USD_CNH": "USD/CNH Exchange Rate",
            "EUR_CNH": "EUR/CNH Exchange Rate",
            "CPI_MoM": "China CPI (Monthly)",
            "PMI": "China PMI",
            "Exports_YoY": "China Exports YoY",
            "Imports_YoY": "China Imports YoY",
            "Trade_Balance": "China Trade Balance",
        }
        for row in cur.fetchall():
            source_name = fin_indicator_to_source.get(row[0])
            if source_name:
                sync_map[source_name] = {"last_sync": row[1], "items": row[2], "type": "financial"}

        # Also check series data
        cur = conn.execute(
            "SELECT indicator, MAX(fetched_at) as last_sync, COUNT(*) as points "
            "FROM financial_series GROUP BY indicator"
        )
        indicator_to_source = {
            "SHIBOR_ON": "SHIBOR (Interbank Rates)", "SHIBOR_1W": "SHIBOR (Interbank Rates)",
            "SHIBOR_1M": "SHIBOR (Interbank Rates)", "SHIBOR_3M": "SHIBOR (Interbank Rates)",
            "SHIBOR_6M": "SHIBOR (Interbank Rates)", "SHIBOR_1Y": "SHIBOR (Interbank Rates)",
            "CGB_YIELD_10年": "China Gov Bond Yield Curve", "CGB_YIELD_1年": "China Gov Bond Yield Curve",
            "SSE_Composite": "SSE Composite Index", "SZSE_Component": "SZSE Component Index",
            "CSI_300": "CSI 300 Index", "ChiNext": "ChiNext Index",
            "USD_CNH": "USD/CNH Exchange Rate", "EUR_CNH": "EUR/CNH Exchange Rate",
            "CPI_MoM": "China CPI (Monthly)", "PMI": "China PMI",
            "Exports_YoY": "China Exports YoY", "Imports_YoY": "China Imports YoY",
            "Trade_Balance": "China Trade Balance",
        }
        for row in cur.fetchall():
            source_name = indicator_to_source.get(row[0])
            if source_name and source_name not in sync_map:
                sync_map[source_name] = {"last_sync": row[1], "items": row[2], "type": "financial"}
        conn.close()
    except Exception:
        pass

    # Bruegel sync status
    try:
        conn = get_bruegel_db()
        row = conn.execute(
            "SELECT MAX(last_fetched), SUM(row_count) FROM bruegel_meta"
        ).fetchone()
        if row and row[0]:
            sync_map["Bruegel China Economic Database"] = {
                "last_sync": row[0], "items": int(row[1] or 0), "type": "bruegel"
            }
        conn.close()
    except Exception:
        pass

    # GMD sync status
    try:
        conn = get_macro_db()
        version = get_stored_version(conn)
        if version:
            row = conn.execute(
                "SELECT fetched_at FROM macro_versions ORDER BY id DESC LIMIT 1"
            ).fetchone()
            total_pts = conn.execute("SELECT COUNT(*) FROM macro_series").fetchone()[0]
            sync_map["Global Macro Database"] = {
                "last_sync": row[0] if row else None, "items": total_pts, "type": "macro"
            }
        conn.close()
    except Exception:
        pass

    # Attach sync status to each source
    for s in all_sources:
        name = s.get("name", "")
        si = sync_map.get(name, {})
        s["last_sync"] = si.get("last_sync")
        s["sync_items"] = si.get("items", 0)
        s["sync_type"] = si.get("type", "")

        # Determine feed type
        has_rsshub = bool(s.get("rsshub_routes"))
        has_direct = s.get("verified") or s.get("_section") == "direct_feeds"
        is_financial = s.get("_section") == "akshare"
        is_wechat = s.get("_section") == "wechat"
        is_gmd = s.get("_section") == "gmd"
        is_bruegel = s.get("_section") == "bruegel"

        if is_bruegel:
            s["feed_type"] = "bruegel"
        elif is_gmd:
            s["feed_type"] = "gmd"
        elif is_financial:
            s["feed_type"] = "akshare"
        elif is_wechat:
            s["feed_type"] = "wechat"
        elif has_direct:
            s["feed_type"] = "direct_rss"
        elif has_rsshub:
            s["feed_type"] = "rsshub"
        else:
            s["feed_type"] = "manual"

    # Summary counts
    synced = sum(1 for s in all_sources if s.get("last_sync"))
    total = len(all_sources)

    return jsonify({
        "total": total,
        "synced": synced,
        "not_synced": total - synced,
        "sources": all_sources,
    })


@app.route("/api/overview")
def api_overview():
    """Summary stats for the landing page."""
    db = get_db()
    stats = get_fetch_stats(db)
    item_count = get_item_count(db)
    cur = db.execute("SELECT MAX(fetched_at) FROM items")
    last = cur.fetchone()[0]

    # Recent headlines
    cur = db.execute(
        "SELECT source, title, link, fetched_at FROM items ORDER BY fetched_at DESC, id DESC LIMIT 8"
    )
    headlines = [{"source": r[0], "title": r[1], "link": r[2], "fetched_at": r[3]} for r in cur.fetchall()]

    # Source counts by category
    cur = db.execute("SELECT category, COUNT(DISTINCT source) FROM items GROUP BY category")
    cats = {r[0]: r[1] for r in cur.fetchall()}
    db.close()

    # Financial summary
    fin_count = 0
    fin_snaps = []
    try:
        conn = get_financial_db()
        fin_count = conn.execute("SELECT COUNT(*) FROM financial_series").fetchone()[0]
        fin_snaps = get_latest_snapshots(conn)
        conn.close()
    except Exception:
        pass

    # Bruegel summary
    bruegel_count = 0
    try:
        bconn = get_bruegel_db()
        bruegel_count = bconn.execute("SELECT COUNT(*) FROM bruegel_series").fetchone()[0]
        bconn.close()
    except Exception:
        pass

    reg = load_registry()
    return jsonify({
        "news_items": item_count,
        "news_sources_active": len(cats),
        "financial_points": fin_count,
        "bruegel_points": bruegel_count,
        "financial_snapshots": fin_snaps,
        "total_sources": len(get_all_sources(reg)),
        "last_fetch": last,
        "headlines": headlines,
        "categories": cats,
    })


@app.route("/api/overview/macro")
def api_overview_macro():
    """Macro-strip widgets: key China macro series with latest values."""
    conn = get_financial_db()
    try:
        widgets = build_payload(conn)
    finally:
        conn.close()
    return jsonify({"widgets": widgets})


# ---------------------------------------------------------------------------
# Pipeline status
# ---------------------------------------------------------------------------

@app.route("/api/pipeline/status")
def api_pipeline_status():
    """Report batch pipeline run status and realtime data freshness."""
    db = get_db()

    # Latest batch run
    batch_info = {"last_run": None, "status": None, "sources_ok": [], "sources_failed": []}
    row = db.execute(
        "SELECT started_at, completed_at, sources_ok, sources_failed, status "
        "FROM batch_runs ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if row:
        batch_info = {
            "last_run": row[0],
            "completed_at": row[1],
            "sources_ok": json.loads(row[2]) if row[2] else [],
            "sources_failed": json.loads(row[3]) if row[3] else [],
            "status": row[4],
        }

    # News freshness
    news_last = db.execute("SELECT MAX(fetched_at) FROM items").fetchone()[0]
    db.close()

    # Flight/ship counts
    flights_count = 0
    flights_last = None
    try:
        from backend.fetchers.flights import get_flights_db
        fconn = get_flights_db()
        r = fconn.execute("SELECT COUNT(*), MAX(updated_at) FROM flight_positions").fetchone()
        flights_count, flights_last = r[0] or 0, r[1]
        fconn.close()
    except Exception:
        pass

    ships_count = 0
    ships_last = None
    try:
        from backend.fetchers.ships import get_ships_db
        sconn = get_ships_db()
        r = sconn.execute("SELECT COUNT(*), MAX(updated_at) FROM ship_positions").fetchone()
        ships_count, ships_last = r[0] or 0, r[1]
        sconn.close()
    except Exception:
        pass

    return jsonify({
        "batch": batch_info,
        "realtime": {
            "news_last_item": news_last,
            "flights_count": flights_count,
            "flights_last_update": flights_last,
            "ships_count": ships_count,
            "ships_last_update": ships_last,
        },
    })


@app.route("/api/refresh/status")
def api_refresh_status():
    """Auto-refresh scheduler status: per-group last run / next due / running."""
    try:
        from backend.auto_refresh import ensure_table, get_status, _state
        from backend.storage import get_conn
        conn = get_conn()
        ensure_table(conn)
        groups = get_status(conn)
        conn.close()
        alive = _state["thread"] is not None and _state["thread"].is_alive()
        return jsonify({"scheduler_alive": alive, "groups": groups})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# GeoJSON endpoints — served locally to avoid CORS
# ---------------------------------------------------------------------------
@app.route("/api/geo/prefectures")
def api_geo_prefectures():
    return app.response_class(
        response=open(DATA_DIR / "reference" / "china_prefectures.json", "rb").read(),
        mimetype="application/json",
        headers={"Cache-Control": "public, max-age=86400"},
    )

@app.route("/api/geo/provinces")
def api_geo_provinces():
    return app.response_class(
        response=open(DATA_DIR / "reference" / "china_provinces.json", "rb").read(),
        mimetype="application/json",
        headers={"Cache-Control": "public, max-age=86400"},
    )

# ---------------------------------------------------------------------------
# Commodity markets (copper prices, production, trade)
# ---------------------------------------------------------------------------

@app.route("/api/commodities")
def api_commodities():
    """Commodity markets blob (all materials: prices, production/refining by
    country, trade per HS code) from data/commodities.json (schema 2)."""
    try:
        from backend.fetchers.commodities import get_commodities_data
        return jsonify(get_commodities_data())
    except Exception as e:
        return jsonify({"error": str(e)})

# ---------------------------------------------------------------------------
# Dissent data endpoints
# ---------------------------------------------------------------------------

@app.route("/api/dissent/summary")
def api_dissent_summary():
    """Province-level dissent event counts. ?year=2024 for specific year."""
    try:
        from backend.fetchers.dissent import get_dissent_db
        conn = get_dissent_db()
        year = request.args.get("year", "")

        where = "WHERE province != ''"
        params = []
        if year:
            where += " AND substr(date_start,1,4) = ?"
            params.append(year)

        cur = conn.execute(f"""
            SELECT province, province_id, COUNT(*) as count,
                   MIN(date_start) as earliest, MAX(date_start) as latest
            FROM dissent_events {where}
            GROUP BY province ORDER BY count DESC
        """, params)
        summary = [
            {"province": r[0], "province_id": r[1], "count": r[2], "earliest": r[3], "latest": r[4]}
            for r in cur.fetchall()
        ]

        count_where = "" if not year else "WHERE substr(date_start,1,4) = ?"
        count_params = [] if not year else [year]
        total = conn.execute(f"SELECT COUNT(*) FROM dissent_events {count_where}", count_params).fetchone()[0]

        # Available years
        years = [r[0] for r in conn.execute(
            "SELECT DISTINCT substr(date_start,1,4) FROM dissent_events "
            "WHERE date_start != '' AND date_start != '-' ORDER BY 1 DESC"
        ).fetchall()]

        conn.close()
        return jsonify({"total": total, "provinces": summary, "years": years})
    except Exception as e:
        return jsonify({"total": 0, "provinces": [], "years": [], "error": str(e)})


@app.route("/api/dissent/events")
def api_dissent_events():
    """Return dissent events. ?province=Guangdong&limit=50"""
    try:
        from backend.fetchers.dissent import get_dissent_db, get_recent_events
        conn = get_dissent_db()
        province = request.args.get("province", "")
        limit = max(0, min(request.args.get("limit", 50, type=int), 1000))
        events = get_recent_events(conn, limit=limit, province=province)
        conn.close()
        return jsonify(events)
    except Exception as e:
        return jsonify([])


@app.route("/api/dissent/province_detail")
def api_dissent_province_detail():
    """Issue/mode breakdown for a province. ?province=Guangdong&year=2024"""
    try:
        from backend.fetchers.dissent import get_dissent_db
        conn = get_dissent_db()
        province = request.args.get("province", "")
        year = request.args.get("year", "")
        if not province:
            return jsonify({"error": "province required"})

        yw = " AND substr(date_start,1,4) = ?" if year else ""
        yp = [province, year] if year else [province]

        issues = conn.execute(
            "SELECT issue, COUNT(*) as cnt FROM dissent_events "
            "WHERE province = ? AND issue != ''" + yw + " GROUP BY issue ORDER BY cnt DESC LIMIT 8",
            yp,
        ).fetchall()

        modes = conn.execute(
            "SELECT mode, COUNT(*) as cnt FROM dissent_events "
            "WHERE province = ? AND mode != ''" + yw + " GROUP BY mode ORDER BY cnt DESC LIMIT 5",
            yp,
        ).fetchall()

        stats = conn.execute(
            "SELECT MIN(date_start), MAX(date_start), COUNT(*) FROM dissent_events WHERE province = ?" + yw,
            yp,
        ).fetchone()

        recent = conn.execute(
            "SELECT COUNT(*) FROM dissent_events WHERE province = ? AND date_start >= date('now', '-90 days')",
            (province,),
        ).fetchone()

        # Previous year issue shares for YoY trend arrows
        prev_issues = {}
        cur_total = stats[2] if stats[2] else 0
        if year:
            prev_year = str(int(year) - 1)
            prev_total_row = conn.execute(
                "SELECT COUNT(*) FROM dissent_events WHERE province = ? AND substr(date_start,1,4) = ?",
                (province, prev_year),
            ).fetchone()
            prev_total = prev_total_row[0] if prev_total_row else 0
            if prev_total > 0:
                for r in conn.execute(
                    "SELECT issue, COUNT(*) FROM dissent_events "
                    "WHERE province = ? AND issue != '' AND substr(date_start,1,4) = ? GROUP BY issue",
                    (province, prev_year),
                ).fetchall():
                    prev_issues[r[0]] = r[1] / prev_total

        issues_out = []
        for r in issues:
            share = r[1] / cur_total if cur_total > 0 else 0
            prev_share = prev_issues.get(r[0])
            if prev_share is None:
                trend = "new"
            elif share > prev_share + 0.02:
                trend = "up"
            elif share < prev_share - 0.02:
                trend = "down"
            else:
                trend = "flat"
            issues_out.append({
                "issue": r[0], "count": r[1],
                "share": round(share * 100, 1),
                "prev_share": round((prev_share or 0) * 100, 1),
                "trend": trend,
            })

        conn.close()
        return jsonify({
            "province": province,
            "year": year,
            "issues": issues_out,
            "modes": [{"mode": r[0], "count": r[1]} for r in modes],
            "earliest": stats[0],
            "latest": stats[1],
            "total": stats[2],
            "recent_90d": recent[0],
        })
    except Exception as e:
        return jsonify({"error": str(e)})


@app.route("/api/dissent/historical")
def api_dissent_historical():
    """Yearly trends, top issues/modes across all years, province rankings per year."""
    try:
        from backend.fetchers.dissent import get_dissent_db
        conn = get_dissent_db()

        # Yearly totals
        yearly = conn.execute(
            "SELECT substr(date_start,1,4) as yr, COUNT(*) FROM dissent_events "
            "WHERE date_start != '' AND date_start != '-' GROUP BY yr ORDER BY yr"
        ).fetchall()

        # Top issues all-time
        issues = conn.execute(
            "SELECT issue, COUNT(*) as cnt FROM dissent_events "
            "WHERE issue != '' GROUP BY issue ORDER BY cnt DESC LIMIT 15"
        ).fetchall()

        # Top modes all-time
        modes = conn.execute(
            "SELECT mode, COUNT(*) as cnt FROM dissent_events "
            "WHERE mode != '' GROUP BY mode ORDER BY cnt DESC LIMIT 10"
        ).fetchall()

        # Province ranking per year
        prov_yearly = conn.execute(
            "SELECT substr(date_start,1,4) as yr, province, COUNT(*) as cnt "
            "FROM dissent_events WHERE date_start != '' AND date_start != '-' AND province != '' "
            "GROUP BY yr, province ORDER BY yr, cnt DESC"
        ).fetchall()

        # Group by year
        by_year = {}
        for yr, prov, cnt in prov_yearly:
            by_year.setdefault(yr, []).append({"province": prov, "count": cnt})

        # Issues per year (top 5 per year)
        issues_yearly = conn.execute(
            "SELECT substr(date_start,1,4) as yr, issue, COUNT(*) as cnt "
            "FROM dissent_events WHERE date_start != '' AND date_start != '-' AND issue != '' "
            "GROUP BY yr, issue ORDER BY yr, cnt DESC"
        ).fetchall()
        issues_by_year = {}
        for yr, issue, cnt in issues_yearly:
            lst = issues_by_year.setdefault(yr, [])
            if len(lst) < 8:
                lst.append({"issue": issue, "count": cnt})

        conn.close()
        return jsonify({
            "yearly": [{"year": r[0], "count": r[1]} for r in yearly],
            "top_issues": [{"issue": r[0], "count": r[1]} for r in issues],
            "top_modes": [{"mode": r[0], "count": r[1]} for r in modes],
            "provinces_by_year": by_year,
            "issues_by_year": issues_by_year,
        })
    except Exception as e:
        return jsonify({"error": str(e)})


# ---------------------------------------------------------------------------
# Real-time tracking endpoints (flights & ships)
# ---------------------------------------------------------------------------

_flight_cache = {"data": [], "ts": 0, "refreshing": False}
_ship_cache = {"data": [], "ts": 0, "refreshing": False}


@app.route("/api/flights/positions")
def api_flight_positions():
    """Current flight positions over China (OpenSky Network).

    Returns cached/DB positions immediately and refreshes live data in a
    background thread, so the request never blocks on the external API.
    The frontend polls every 30s and picks up fresh data on the next tick.
    """
    import time
    import threading
    try:
        from backend.fetchers.flights import (
            _load_credentials,
            fetch_flight_positions,
            get_current_flights,
            get_flights_db,
            store_flight_positions,
        )
        u, p = _load_credentials()
        max_age = 60 if (u and p) else 900
        now = time.time()

        # Hydrate from DB on first call so there is always something to serve.
        if _flight_cache["ts"] == 0 and not _flight_cache["data"]:
            conn = get_flights_db()
            _flight_cache["data"] = get_current_flights(conn)
            conn.close()

        if now - _flight_cache["ts"] >= max_age and not _flight_cache["refreshing"]:
            _flight_cache["refreshing"] = True

            def _refresh():
                try:
                    positions = fetch_flight_positions(u, p)
                    conn = get_flights_db()
                    store_flight_positions(conn, positions)
                    conn.close()
                    _flight_cache["data"] = positions
                except Exception:
                    pass
                finally:
                    _flight_cache["ts"] = time.time()
                    _flight_cache["refreshing"] = False

            threading.Thread(target=_refresh, daemon=True).start()

        return jsonify(_flight_cache["data"])
    except Exception:
        return jsonify([])


@app.route("/api/ships/positions")
def api_ship_positions():
    """Current ship positions around China (AISHub or AISStream).

    Returns cached/DB positions immediately and refreshes live data in a
    background thread (the AISStream listen can take ~10s), so the request
    never blocks. The frontend polls every 30s for fresh positions.
    """
    import time
    import threading
    try:
        from backend.fetchers.ships import (
            _load_api_key,
            cleanup_stale,
            fetch_ship_positions,
            get_current_ships,
            get_ships_db,
            store_ship_positions,
        )
        if not _load_api_key():
            return jsonify([])

        now = time.time()

        # Hydrate from DB on first call so there is always something to serve.
        if _ship_cache["ts"] == 0 and not _ship_cache["data"]:
            conn = get_ships_db()
            _ship_cache["data"] = get_current_ships(conn)
            conn.close()

        if now - _ship_cache["ts"] >= 60 and not _ship_cache["refreshing"]:
            _ship_cache["refreshing"] = True

            def _refresh():
                try:
                    positions = fetch_ship_positions(duration_seconds=10)
                    conn = get_ships_db()
                    store_ship_positions(conn, positions)
                    cleanup_stale(conn)
                    _ship_cache["data"] = get_current_ships(conn)
                    conn.close()
                except Exception:
                    pass
                finally:
                    _ship_cache["ts"] = time.time()
                    _ship_cache["refreshing"] = False

            threading.Thread(target=_refresh, daemon=True).start()

        return jsonify(_ship_cache["data"])
    except Exception:
        return jsonify([])


# ---------------------------------------------------------------------------
# Macro data endpoints (Global Macro Database)
# ---------------------------------------------------------------------------

@app.route("/api/macro/variables")
def api_macro_variables():
    """List available macro variables with metadata and year ranges."""
    try:
        conn = get_macro_db()
        variables = get_macro_variables(conn)
        version = get_stored_version(conn)
        conn.close()
        return jsonify({"version": version, "categories": CATEGORIES, "variables": variables})
    except Exception as e:
        return jsonify({"version": None, "categories": {}, "variables": [], "error": str(e)})


@app.route("/api/macro/series/<variable>")
def api_macro_series(variable):
    """Time series for one macro variable. ?start=YYYY&end=YYYY"""
    try:
        start = request.args.get("start", type=int)
        end = request.args.get("end", type=int)
        conn = get_macro_db()
        data = get_macro_series(conn, variable, start_year=start, end_year=end)
        meta = VARIABLE_META.get(variable, {"name": variable, "unit": "", "category": "other", "desc": ""})
        version = get_stored_version(conn)
        conn.close()
        return jsonify({"variable": variable, "meta": meta, "version": version, "data": data})
    except Exception as e:
        return jsonify({"variable": variable, "data": [], "error": str(e)})


@app.route("/api/macro/compare")
def api_macro_compare():
    """Multiple variables for overlay. ?vars=rGDP,infl&start=1980&end=2030"""
    try:
        var_str = request.args.get("vars", "")
        variables = [v.strip() for v in var_str.split(",") if v.strip()]
        start = request.args.get("start", type=int)
        end = request.args.get("end", type=int)
        conn = get_macro_db()
        result = {}
        for v in variables[:5]:  # limit to 5 variables
            data = get_macro_series(conn, v, start_year=start, end_year=end)
            meta = VARIABLE_META.get(v, {"name": v, "unit": "", "category": "other", "desc": ""})
            result[v] = {"meta": meta, "data": data}
        conn.close()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)})


# ---------------------------------------------------------------------------
# Academic publications
# ---------------------------------------------------------------------------

@app.route("/api/academic/articles")
def api_academic_articles():
    """List recent academic articles. ?limit=50&journal=&q=&ranked=1"""
    try:
        limit = max(0, min(request.args.get("limit", 50, type=int), 1000))
        journal = request.args.get("journal", "")
        q = request.args.get("q", "")
        ranked = request.args.get("ranked", "0") == "1"
        conn = get_academic_db()
        articles = get_recent_articles(conn, limit=limit, journal=journal, q=q, ranked=ranked)
        conn.close()
        return jsonify(articles)
    except Exception as e:
        return jsonify([])


@app.route("/api/academic/vote", methods=["POST"])
@require_admin(max_calls=60, window_seconds=3600)
def api_academic_vote():
    """Cast a vote on an article. Body: {"article_id": N, "vote": 1|-1|0}"""
    try:
        data = request.get_json(force=True)
        article_id = data.get("article_id")
        vote = data.get("vote", 0)
        if article_id is None:
            return jsonify({"error": "article_id required"}), 400
        conn = get_academic_db()
        result = cast_vote(conn, int(article_id), int(vote))
        conn.close()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/academic/preferences")
def api_academic_preferences():
    """Return learned preference weights from vote history."""
    try:
        conn = get_academic_db()
        prefs = get_preferences(conn)
        vote_count = conn.execute("SELECT COUNT(*) FROM academic_votes").fetchone()[0]
        conn.close()
        return jsonify({"vote_count": vote_count, "preferences": prefs})
    except Exception as e:
        return jsonify({"vote_count": 0, "preferences": {}, "error": str(e)})


@app.route("/api/academic/summary")
def api_academic_summary():
    """Per-journal article counts."""
    try:
        conn = get_academic_db()
        summary = get_journal_summary(conn)
        total = conn.execute("SELECT COUNT(*) FROM academic_articles").fetchone()[0]
        conn.close()
        return jsonify({"total": total, "journals": summary})
    except Exception as e:
        return jsonify({"total": 0, "journals": [], "error": str(e)})


@app.route("/api/academic/journals")
def api_academic_journals():
    """Distinct journal names for filter dropdown."""
    try:
        conn = get_academic_db()
        cur = conn.execute("SELECT DISTINCT journal FROM academic_articles ORDER BY journal")
        journals = [r[0] for r in cur.fetchall()]
        conn.close()
        return jsonify(journals)
    except Exception as e:
        return jsonify([])


# ---------------------------------------------------------------------------
# Regulations API
# ---------------------------------------------------------------------------

@app.route("/api/regulations/mofcom")
def api_regulations_mofcom():
    try:
        conn = get_regulations_db()
        rows = get_mofcom_docs(conn)
        conn.close()
        return jsonify(rows)
    except Exception:
        return jsonify([])


@app.route("/api/regulations/npc")
def api_regulations_npc():
    try:
        conn = get_regulations_db()
        rows = get_npc_bills(conn)
        conn.close()
        return jsonify(rows)
    except Exception:
        return jsonify([])


@app.route("/api/regulations/stats")
def api_regulations_stats():
    try:
        conn = get_regulations_db()
        stats = get_regulations_stats(conn)
        conn.close()
        return jsonify(stats)
    except Exception:
        return jsonify({"mofcom_active": 0, "npc_under_reform": 0})


# ---------------------------------------------------------------------------
# Policy Advisor
# ---------------------------------------------------------------------------

@app.route("/api/advisor/brief", methods=["POST"])
@require_admin(max_calls=10, window_seconds=3600)
def api_advisor_brief():
    """Generate a structured policy brief. Body: {topic: str, days: int}"""
    try:
        from backend.fetchers.advisor import generate_brief
        data = request.get_json(force=True) or {}
        topic = (data.get("topic") or "").strip()
        if not topic:
            return jsonify({"error": "topic is required"}), 400
        days = min(int(data.get("days", 90)), 365)
        brief = generate_brief(topic, days=days)
        return jsonify({
            "topic": brief.topic,
            "generated_at": brief.generated_at,
            "days": brief.days,
            "source_count": brief.source_count,
            "stub": brief.stub,
            "content": brief.content,
            "sources": brief.sources,
            "error": brief.error,
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# Dashboard HTML — single-page app
# ---------------------------------------------------------------------------

@app.route("/api/polity")
def api_polity():
    try:
        from backend.fetchers.polity import get_polity_data
        return jsonify(get_polity_data())
    except Exception as e:
        return jsonify({"error": str(e)})


@app.route("/api/polity/meeting-news")
def api_polity_meeting_news():
    try:
        from backend.fetchers.polity import scrape_meeting_news
        items = scrape_meeting_news(max_items=20)
        return jsonify({"items": items})
    except Exception as e:
        return jsonify({"error": str(e), "items": []})


@app.route("/api/polity/calendar")
def api_polity_calendar():
    try:
        from backend.fetchers.polity_calendar import get_calendar_data
        return jsonify(get_calendar_data())
    except Exception as e:
        return jsonify({"error": str(e)})


# ---------------------------------------------------------------------------
# Eurostat — EU-China competitive intelligence
# ---------------------------------------------------------------------------

@app.route("/api/eurostat/datasets")
def api_eurostat_datasets():
    """List all stored Eurostat datasets with metadata and row counts."""
    try:
        from backend.fetchers.eurostat import get_eurostat_db, get_eurostat_datasets, DATASET_META
        conn = get_eurostat_db()
        datasets = get_eurostat_datasets(conn)
        conn.close()
        return jsonify({"datasets": datasets, "meta": DATASET_META})
    except Exception as e:
        return jsonify({"datasets": [], "error": str(e)})


@app.route("/api/eurostat/series")
def api_eurostat_series():
    """Time series for a dataset. ?dataset=ext_lt_intertrd&indicator=BAL_EU27_2020&start=2015"""
    try:
        from backend.fetchers.eurostat import get_eurostat_db, get_eurostat_series
        dataset = request.args.get("dataset", "")
        if not dataset:
            return jsonify({"error": "dataset parameter required"}), 400
        indicator = request.args.get("indicator") or None
        geo = request.args.get("geo") or None
        partner = request.args.get("partner") or None
        nace = request.args.get("nace") or None
        unit = request.args.get("unit") or None
        start_year = request.args.get("start", type=int)
        conn = get_eurostat_db()
        data = get_eurostat_series(conn, dataset, indicator=indicator, geo=geo,
                                   partner=partner, nace=nace, unit=unit,
                                   start_year=start_year)
        conn.close()
        return jsonify({"dataset": dataset, "count": len(data), "data": data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/eurostat/latest")
def api_eurostat_latest():
    """Latest value per indicator for a dataset. ?dataset=ext_lt_intertrd"""
    try:
        from backend.fetchers.eurostat import get_eurostat_db, get_eurostat_latest
        dataset = request.args.get("dataset", "")
        if not dataset:
            return jsonify({"error": "dataset parameter required"}), 400
        conn = get_eurostat_db()
        data = get_eurostat_latest(conn, dataset)
        conn.close()
        return jsonify({"dataset": dataset, "data": data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/eurostat/indicators")
def api_eurostat_indicators():
    """List indicators stored for a dataset with year ranges. ?dataset=ext_lt_intertrd"""
    try:
        from backend.fetchers.eurostat import get_eurostat_db, get_eurostat_indicators
        dataset = request.args.get("dataset", "")
        if not dataset:
            return jsonify({"error": "dataset parameter required"}), 400
        conn = get_eurostat_db()
        data = get_eurostat_indicators(conn, dataset)
        conn.close()
        return jsonify({"dataset": dataset, "indicators": data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/fyp/tech")
def api_fyp_tech():
    """
    15th-FYP Tech Self-Reliance cockpit payload: official plan targets with
    latest actuals, chip-trade series (world totals, supplier HHI, top
    partners), EU27<->CN monthly chip trade, and China's share of EU HS-85
    imports. All data self-updates via the daily batch (`fyp_tech` source).
    """
    try:
        from backend.fetchers.fyp_tech import (
            CHIP_HS4, PLAN_QUALITATIVE, PLAN_TARGETS, TECH_DOMAINS, TECH_HS,
            benchmark_series, eu_hs85_share, eu_monthly_series, get_db,
            hhi_series, indicator_series, publication_series, top_partners,
            world_series,
        )
        conn = get_db()
        indicators = indicator_series(conn)

        targets = []
        for t in PLAN_TARGETS:
            series = indicators.get(t["indicator"], [])
            latest = series[-1] if series else None
            targets.append({**t, "latest": latest, "history": series})

        payload = {
            "targets": targets,
            "qualitative": PLAN_QUALITATIVE,
            "indicators": indicators,
            "domains": TECH_DOMAINS,
            "publications": publication_series(conn),
            "benchmarks": benchmark_series(conn),
            "chip": {
                "hs4_labels": TECH_HS,
                "world": world_series(conn),
                "hhi": hhi_series(conn, flow="M"),
                "partners": {code: {"M": top_partners(conn, code, flow="M"),
                                    "X": top_partners(conn, code, flow="X")}
                             for code in TECH_HS},
            },
            "eu": {
                "monthly": eu_monthly_series(conn),
                "hs85_share": eu_hs85_share(conn),
            },
            "fetched_at": conn.execute(
                "SELECT MAX(fetched_at) FROM fyp_chip_trade").fetchone()[0],
        }
        conn.close()
        return jsonify(payload)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/fyp/demand")
def api_fyp_demand():
    """
    15th-FYP Domestic Demand cockpit payload: the ten 节-level sections of
    Part V (Ch 15–17) with their plan-text points, related policy documents
    from policy_docs, and the macro series backing the sidebar charts
    (household consumption / investment shares of GDP, unemployment,
    government expenditure).
    """
    try:
        from backend.fetchers.fyp_demand import build_payload
        from backend.storage import get_conn
        conn = get_conn()
        payload = build_payload(conn)
        conn.close()
        return jsonify(payload)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/fiscal")
def api_fiscal():
    """
    Fiscal Capacity payload: the computed fiscal-space assessment (flow /
    repayment / funding gauges + traffic-light verdicts), national monthly
    MOF series, LGB issuance & debt stock, daily yield curves & spreads,
    the 31-province cross-section, the LGB maturity wall, and the curated
    reference facts with citations. Sources: MOF (gks/zwgls/yss), ChinaMoney,
    PBOC via AKShare; methodology anchored to GS AFD recipe + ADB fiscal
    rules paper (see fiscal_assess.py docstring).
    """
    try:
        from backend.fetchers.fiscal_assess import build_payload
        from backend.fetchers.fiscal_china import get_fiscal_db
        conn = get_fiscal_db()
        payload = build_payload(conn)
        conn.close()
        return jsonify(payload)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/eurostat/sme-scorecard")
def api_eurostat_sme_scorecard():
    """
    SME competitive-intelligence scorecard:
    EU-China trade flows, R&D investment, labour costs, patent trends.
    """
    try:
        from backend.fetchers.eurostat import get_eurostat_db, get_sme_scorecard
        conn = get_eurostat_db()
        scorecard = get_sme_scorecard(conn)
        conn.close()
        return jsonify(scorecard)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/eurostat/policy-scorecard")
def api_eurostat_policy_scorecard():
    """
    Regulatory / policy-maker scorecard:
    Chinese FDI in EU, trade imbalance trend, R&D intensity gap.
    """
    try:
        from backend.fetchers.eurostat import get_eurostat_db, get_policy_scorecard
        conn = get_eurostat_db()
        scorecard = get_policy_scorecard(conn)
        conn.close()
        return jsonify(scorecard)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ---------------------------------------------------------------------------
# Competitiveness — Eurostat COMEXT monthly EU import shares by HS chapter
# ---------------------------------------------------------------------------

@app.route("/api/competitiveness/products")
def api_competitiveness_products():
    """HS2 chapters available in eurostat_imports, with labels (for dropdown)."""
    try:
        from backend.fetchers.eurostat_trade import list_products
        from backend.storage import get_conn
        conn = get_conn()
        data = list_products(conn)
        conn.close()
        return jsonify({"products": data})
    except Exception as e:
        return jsonify({"products": [], "error": str(e)})


@app.route("/api/competitiveness/shares")
def api_competitiveness_shares():
    """Per-group monthly value + share of total EU imports.
    ?product=85 (or 'all')  &since=2014-01"""
    try:
        from backend.fetchers.eurostat_trade import compute_shares
        from backend.storage import get_conn
        product = request.args.get("product", "all")
        since = request.args.get("since") or None
        conn = get_conn()
        data = compute_shares(conn, product=product, since=since)
        conn.close()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/competitiveness/gain")
def api_competitiveness_gain():
    """Per-group share gain (pp) over a window. ?product=85&window=12"""
    try:
        from backend.fetchers.eurostat_trade import compute_gain
        from backend.storage import get_conn
        product = request.args.get("product", "all")
        window = request.args.get("window", 12, type=int)
        conn = get_conn()
        data = compute_gain(conn, product=product, window=window)
        conn.close()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/competitiveness/gain_matrix")
def api_competitiveness_gain_matrix():
    """One group's share gain (pp) across all HS2 chapters, ranked.
    ?group=China&window=12"""
    try:
        from backend.fetchers.eurostat_trade import compute_gain_matrix
        from backend.storage import get_conn
        group = request.args.get("group", "China")
        window = request.args.get("window", 12, type=int)
        conn = get_conn()
        data = compute_gain_matrix(conn, group=group, window=window)
        conn.close()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# CCP Elites Database
# ---------------------------------------------------------------------------

@app.route("/api/elites/congresses")
def api_elites_congresses():
    try:
        from backend.fetchers.ccp_elites import get_db, get_congresses, get_meta
        conn = get_db()
        congresses = get_congresses(conn)
        meta = get_meta(conn)
        conn.close()
        return jsonify({"congresses": congresses, "meta": meta})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/elites/psc")
def api_elites_psc():
    try:
        from backend.fetchers.ccp_elites import get_db, get_psc_by_congress
        congress = request.args.get("congress", "")
        conn = get_db()
        rows = get_psc_by_congress(conn)
        conn.close()
        if congress:
            rows = [r for r in rows if r["congress"] == congress]
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/elites/politburo")
def api_elites_politburo():
    try:
        from backend.fetchers.ccp_elites import get_db, get_pb_by_congress
        congress = request.args.get("congress") or None
        conn = get_db()
        rows = get_pb_by_congress(conn, congress=congress)
        conn.close()
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/elites/cc")
def api_elites_cc():
    try:
        from backend.fetchers.ccp_elites import get_db, get_cc_by_congress
        congress = request.args.get("congress") or None
        conn = get_db()
        rows = get_cc_by_congress(conn, congress=congress)
        conn.close()
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/elites/search")
def api_elites_search():
    try:
        from backend.fetchers.ccp_elites import get_db, search_person
        q = request.args.get("q", "").strip()
        if not q:
            return jsonify({"error": "q parameter required"}), 400
        conn = get_db()
        result = search_person(conn, q)
        conn.close()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/elites/purge_heatmap")
def api_elites_purge_heatmap():
    """Purges by body tier (PSC/PB/CC/Alt) and year, for heatmap display."""
    try:
        import re
        from backend.fetchers.ccp_elites import get_db

        def extract_year(s):
            if not s: return None
            m = re.search(r"\b(1[89]\d\d|20\d\d)\b", s)
            return int(m.group(1)) if m else None

        BODIES = ["Politburo Standing Committee", "Politburo", "Central Committee", "Central Committee (Alternate)"]

        conn = get_db()
        rows = conn.execute(
            "SELECT name, birth_year, expelled_when, is_psc, is_politburo, is_alternate "
            "FROM ccp_cc_members WHERE (fate='purged' OR expelled='Y') AND expelled_when IS NOT NULL"
        ).fetchall()
        conn.close()

        counts = {}   # year -> body -> int
        namemap = {}  # year -> body -> [str]
        seen = set()

        for r in rows:
            year = extract_year(r["expelled_when"])
            if not year: continue
            key = (r["name"], r["birth_year"])
            if key in seen: continue
            seen.add(key)

            if r["is_psc"] == "Y":       body = "Politburo Standing Committee"
            elif r["is_politburo"] == "Y": body = "Politburo"
            elif r["is_alternate"] == "Y": body = "Central Committee (Alternate)"
            else:                          body = "Central Committee"

            counts.setdefault(year, {}).setdefault(body, 0)
            counts[year][body] += 1
            namemap.setdefault(year, {}).setdefault(body, []).append(r["name"])

        years = sorted(counts.keys())
        max_count = max(c for yr in counts.values() for c in yr.values()) if counts else 1

        cells = [
            {"year": year, "body": body, "count": counts[year][body],
             "names": namemap[year][body]}
            for year in years
            for body in BODIES
            if counts.get(year, {}).get(body, 0) > 0
        ]

        return jsonify({"years": years, "bodies": BODIES, "cells": cells, "max_count": max_count})
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


@app.route("/api/elites/psc_careers")
def api_elites_psc_careers():
    """Career histories + connections for current 20th CC PSC members."""
    try:
        import re
        from backend.fetchers.polity import get_polity_data

        def _cnum(s):
            m = re.match(r"(\d+)", s or "")
            return int(m.group(1)) if m else 99

        pd = get_polity_data()

        from backend.fetchers.ccp_elites import get_db
        conn = get_db()

        psc_20 = conn.execute(
            "SELECT name, birth_year FROM ccp_psc_members WHERE congress='20th CC'"
        ).fetchall()
        birth_years = {r["name"]: r["birth_year"] for r in psc_20}

        careers = {}
        province_map = {}

        for member in pd["psc"]:
            name = member["name"]
            by = birth_years.get(name)

            if by:
                cc_rows = conn.execute(
                    "SELECT congress, is_alternate, is_politburo, is_psc, province, entry_year "
                    "FROM ccp_cc_members WHERE name=? AND birth_year=? ORDER BY congress",
                    (name, by),
                ).fetchall()
                pb_rows = conn.execute(
                    "SELECT congress, is_psc, province FROM ccp_pb_members WHERE name=? AND birth_year=? ORDER BY congress",
                    (name, by),
                ).fetchall()
            else:
                cc_rows, pb_rows = [], []

            psc_rows = conn.execute(
                "SELECT congress, rank, role, province FROM ccp_psc_members WHERE name=? ORDER BY congress",
                (name,),
            ).fetchall()

            traj = {}
            for r in cc_rows:
                cong = r["congress"]
                if r["is_psc"] == "Y":
                    lv = "psc"
                elif r["is_politburo"] == "Y":
                    lv = "pb"
                elif r["is_alternate"] == "Y":
                    lv = "cc_alt"
                else:
                    lv = "cc"
                traj[cong] = {"congress": cong, "level": lv, "province": r["province"], "entry_year": r["entry_year"]}
                if r["province"]:
                    province_map.setdefault(r["province"], set()).add(name)

            for r in pb_rows:
                cong = r["congress"]
                lv = "psc" if r["is_psc"] == "Y" else "pb"
                if cong not in traj:
                    traj[cong] = {"congress": cong, "level": lv, "province": r["province"]}
                elif lv == "psc":
                    traj[cong]["level"] = "psc"

            for r in psc_rows:
                cong = r["congress"]
                prev = traj.get(cong, {})
                traj[cong] = {**prev, "congress": cong, "level": "psc", "role": r["role"], "province": r["province"]}

            sorted_traj = sorted(traj.values(), key=lambda x: _cnum(x["congress"]))
            careers[name] = {"birth_year": by, "trajectory": sorted_traj}

        conn.close()

        shared_provinces = sorted(
            [{"province": p, "members": sorted(list(ns))} for p, ns in province_map.items() if len(ns) > 1],
            key=lambda x: x["province"],
        )

        entry_map = {}
        for name, c in careers.items():
            if c["trajectory"]:
                entry_map.setdefault(c["trajectory"][0]["congress"], []).append(name)
        entry_cohorts = sorted(
            [{"congress": cong, "members": sorted(names)} for cong, names in entry_map.items()],
            key=lambda x: _cnum(x["congress"]),
        )

        by_map = {}
        for member in pd["psc"]:
            name = member["name"]
            by = careers[name].get("birth_year")
            if by:
                by_map.setdefault(by, []).append(name)
        birth_ties = [
            {"year": y, "members": sorted(names)}
            for y, names in sorted(by_map.items())
            if len(names) > 1
        ]

        return jsonify({
            "careers": careers,
            "connections": {
                "provinces": shared_provinces,
                "entry_cohorts": entry_cohorts,
                "birth_ties": birth_ties,
            },
        })
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


# ---------------------------------------------------------------------------
# Trade — UNCTAD bilateral merchandise flows (HS-2 chapters)
# ---------------------------------------------------------------------------

@app.route("/api/trade/meta")
def api_trade_meta():
    """Available years and HS-2 codes stored in unctad_trade.db."""
    try:
        from backend.fetchers.unctad import get_unctad_db, get_available_years, get_hs2_codes, HS2_SECTIONS
        conn = get_unctad_db()
        years    = get_available_years(conn)
        hs_codes = get_hs2_codes(conn)
        conn.close()
        return jsonify({"years": years, "hs_codes": hs_codes, "sections": HS2_SECTIONS})
    except Exception as e:
        return jsonify({"years": [], "hs_codes": [], "sections": [], "error": str(e)})


@app.route("/api/trade/map")
def api_trade_map():
    """
    Per-partner trade values for choropleth world map.
    ?year=2022&flow=X&hs2=all
    flow: X (exports from China) or M (imports to China)
    hs2:  two-digit chapter like '84', or 'all' for totals
    """
    try:
        from backend.fetchers.unctad import get_unctad_db, get_trade_map
        year = request.args.get("year", type=int)
        flow = request.args.get("flow", "X")
        hs2  = request.args.get("hs2", "all")
        if not year:
            return jsonify({"error": "year parameter required"}), 400
        conn = get_unctad_db()
        data = get_trade_map(conn, year, flow, hs2 if hs2 != "all" else None)
        conn.close()
        return jsonify({"year": year, "flow": flow, "hs2": hs2, "count": len(data), "data": data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/trade/top")
def api_trade_top():
    """
    Top N partners by trade value.
    ?year=2022&flow=X&hs2=all&n=20
    """
    try:
        from backend.fetchers.unctad import get_unctad_db, get_top_partners
        year = request.args.get("year", type=int)
        flow = request.args.get("flow", "X")
        hs2  = request.args.get("hs2", "all")
        n    = request.args.get("n", 20, type=int)
        if not year:
            return jsonify({"error": "year parameter required"}), 400
        conn = get_unctad_db()
        data = get_top_partners(conn, year, flow, hs2 if hs2 != "all" else None, n=n)
        conn.close()
        return jsonify({"year": year, "flow": flow, "hs2": hs2, "data": data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Tariffs (trade_stats.db — WITS china_tariffs)
# ---------------------------------------------------------------------------

@app.route("/api/tariffs/china-applied")
def api_tariffs_china_applied():
    """
    China's own tariff rates (as reporter) by HS section group.
    ?year=2023&indicator=MFN-SMPL-AVRG
    indicator: MFN-SMPL-AVRG | MFN-WGHTD-AVRG | AHS-SMPL-AVRG | AHS-WGHTD-AVRG
    Returns only the 16 HS section rows (pattern NN-NN_*), not aggregate rows.
    """
    year      = request.args.get("year", 2023, type=int)
    indicator = request.args.get("indicator", "MFN-SMPL-AVRG")
    try:
        from backend.storage import get_conn
        conn = get_conn()
        rows = conn.execute("""
            SELECT product_code, value FROM china_tariffs
            WHERE reporter_iso='CHN' AND partner_iso='WLD'
              AND year=? AND indicator=?
              AND product_code LIKE '__-__\_%' ESCAPE '\\'
            ORDER BY product_code
        """, (year, indicator)).fetchall()
        avail_years = [r[0] for r in conn.execute(
            "SELECT DISTINCT year FROM china_tariffs WHERE reporter_iso='CHN' ORDER BY year"
        ).fetchall()]
        data = [{"product": r[0], "rate": round(r[1], 2)} for r in rows if r[1] is not None]
        return jsonify({"year": year, "indicator": indicator, "years": avail_years, "data": data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/tariffs/on-china")
def api_tariffs_on_china():
    """
    Tariff rates applied TO China by other countries (Total row only).
    ?year=2023&indicator=MFN-SMPL-AVRG&n=30
    Returns top N reporters sorted by tariff rate descending.
    """
    year      = request.args.get("year", 2023, type=int)
    indicator = request.args.get("indicator", "MFN-SMPL-AVRG")
    n         = request.args.get("n", 30, type=int)
    try:
        from backend.storage import get_conn
        conn = get_conn()
        rows = conn.execute("""
            SELECT reporter_iso, AVG(value) AS avg_rate FROM china_tariffs
            WHERE partner_iso='CHN' AND year=? AND indicator=?
              AND product_code LIKE '__-__\_%' ESCAPE '\\'
              AND value IS NOT NULL
            GROUP BY reporter_iso
            ORDER BY avg_rate DESC LIMIT ?
        """, (year, indicator, n)).fetchall()
        data = [{"reporter": r[0], "rate": round(r[1], 2)} for r in rows if r[1] is not None]
        return jsonify({"year": year, "indicator": indicator, "data": data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Chartbook — Bridgewater-replica charts rebuilt on live public data.
# Raw series live in chartbook_data (cmm.db); transforms are applied here at
# read-time so the stored data stays raw. See backend/fetchers/chartbook_registry.
# ---------------------------------------------------------------------------
from backend.fetchers.chartbook_registry import (  # noqa: E402
    CHARTS as _CB_CHARTS, SECTIONS as _CB_SECTIONS, STATIC as _CB_STATIC,
    FRED_SERIES as _CB_FRED, FREQ_PERIODS as _CB_FREQ,
)

_CB_BY_ID = {c["id"]: c for c in _CB_CHARTS}


def _cb_raw(conn, sid):
    rows = conn.execute(
        "SELECT date, value FROM chartbook_data WHERE series_id=? ORDER BY date", (sid,)
    ).fetchall()
    return [(r[0], r[1]) for r in rows]


def _cb_series_freq(sid):
    return _CB_FRED[sid][0] if sid in _CB_FRED else "A"


def _cb_transform(rows, transform="level", scale=1.0, base=None, freq="M"):
    """rows: ascending [(date, value|None)] -> transformed [(date, value|None)]."""
    vals = [v for _, v in rows]
    out = []
    if transform == "yoy":
        p = _CB_FREQ.get(freq, 12)
        for i, (d, v) in enumerate(rows):
            prev = vals[i - p] if i >= p else None
            out.append((d, (v / prev - 1) * 100 if (v is not None and prev not in (None, 0)) else None))
    elif transform == "mom_chg":
        for i, (d, v) in enumerate(rows):
            prev = vals[i - 1] if i >= 1 else None
            out.append((d, (v - prev) * scale if (v is not None and prev is not None) else None))
    elif transform == "index":
        base_val = None
        for d, v in rows:
            if v is not None and (base is None or d >= base):
                base_val = v
                break
        for d, v in rows:
            out.append((d, (v / base_val - 1) * 100 if (v is not None and base_val) else None))
    else:  # level
        out = [(d, v * scale if v is not None else None) for d, v in rows]
    return out


def _cb_assemble(chart, series_specs):
    """series_specs: [(label, [(date,val)], axis)] -> aligned Chart.js payload."""
    start = chart.get("start")
    dateset = set()
    for _, pairs, _ in series_specs:
        for d, v in pairs:
            if v is not None and (not start or d >= start):
                dateset.add(d)
    labels = sorted(dateset)
    idx = {d: i for i, d in enumerate(labels)}
    datasets, latest = [], []
    for label, pairs, axis in series_specs:
        arr = [None] * len(labels)
        last_d = last_v = None
        for d, v in pairs:
            if v is not None and d in idx:
                arr[idx[d]] = round(v, 3)
                last_d, last_v = d, v
        ds = {"label": label, "data": arr}
        if axis:
            ds["yAxisID"] = axis
        datasets.append(ds)
        if last_v is not None:
            latest.append({"label": label, "value": round(last_v, 2), "date": last_d})
    return {"labels": labels, "datasets": datasets, "latest": latest}


def _cb_merge_ratio(conn, num_sid, den_sid):
    num, den = dict(_cb_raw(conn, num_sid)), dict(_cb_raw(conn, den_sid))
    return [(d, num[d] / den[d] * 100) for d in sorted(num)
            if d in den and num[d] is not None and den[d] not in (None, 0)]


def _cb_build(conn, chart):
    compute = chart.get("compute")
    if compute == "static":
        s = _CB_STATIC[chart["id"]]
        return {"labels": s["labels"],
                "datasets": [{"label": chart.get("unit", ""), "data": s["values"]}],
                "latest": []}
    if compute == "bar_latest":
        labels, data = [], []
        for it in chart["series"]:
            t = _cb_transform(_cb_raw(conn, it["sid"]), it.get("transform", "level"),
                              it.get("scale", 1.0), chart.get("base"), _cb_series_freq(it["sid"]))
            last_v = next((v for d, v in reversed(t) if v is not None), None)
            labels.append(it["label"])
            data.append(round(last_v, 2) if last_v is not None else None)
        return {"labels": labels, "datasets": [{"label": chart.get("unit", ""), "data": data}], "latest": []}
    if compute == "ratio":
        merged = _cb_merge_ratio(conn, chart["series"][0]["sid"], chart["series"][1]["sid"])
        return _cb_assemble(chart, [(chart["series"][0]["label"], merged, None)])
    if compute == "shares":
        specs = [(p["label"], _cb_merge_ratio(conn, p["num"], p["den"]), None) for p in chart["pairs"]]
        return _cb_assemble(chart, specs)
    # default: each series transformed independently
    specs = []
    for it in chart["series"]:
        t = _cb_transform(_cb_raw(conn, it["sid"]), it.get("transform", "level"),
                          it.get("scale", 1.0), chart.get("base"), _cb_series_freq(it["sid"]))
        specs.append((it["label"], t, it.get("axis")))
    return _cb_assemble(chart, specs)


def _cb_meta(c):
    return {k: c.get(k) for k in ("id", "section", "title", "subtitle", "source",
                                  "note", "type", "unit")}


@app.route("/api/chartbook/index")
def api_chartbook_index():
    sections = []
    for key, title, subtitle in _CB_SECTIONS:
        charts = [_cb_meta(c) for c in _CB_CHARTS if c["section"] == key]
        sections.append({"key": key, "title": title, "subtitle": subtitle, "charts": charts})
    return jsonify({"sections": sections})


@app.route("/api/chartbook/chart/<chart_id>")
def api_chartbook_chart(chart_id):
    chart = _CB_BY_ID.get(chart_id)
    if not chart:
        return jsonify({"error": "unknown chart"}), 404
    try:
        from backend.storage import get_conn
        conn = get_conn()
        payload = _cb_build(conn, chart)
        payload.update(_cb_meta(chart))
        return jsonify(payload)
    except Exception as e:
        return jsonify({"error": str(e), **_cb_meta(chart)}), 500


# ---------------------------------------------------------------------------
# Dashboard HTML — served from dashboard.html file
# ---------------------------------------------------------------------------

_DASHBOARD = Path(__file__).parent.parent / "frontend" / "index.html"
_CHARTBOOK = Path(__file__).parent.parent / "frontend" / "chartbook.html"


@app.route("/")
def index():
    return _DASHBOARD.read_text(encoding="utf-8")


@app.route("/chartbook")
def chartbook():
    return _CHARTBOOK.read_text(encoding="utf-8")
