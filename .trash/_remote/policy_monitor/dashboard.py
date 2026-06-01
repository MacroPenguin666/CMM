"""
China Policy Monitor — Dashboard

Local web UI with map landing page, news feeds, financial data, and source tracking.

Usage:
    python dashboard.py              # start on http://localhost:5001
    python dashboard.py --port 8080  # custom port
"""

import argparse
import json
import sqlite3
from datetime import datetime
from pathlib import Path

from flask import Flask, jsonify, request

from policy_monitor.storage import DB_PATH, get_db, get_fetch_stats, get_item_count, get_recent_items
from policy_monitor.financial import get_financial_db, get_latest_snapshots, get_series
from policy_monitor.bruegel import (
    get_bruegel_db, get_bruegel_snapshots, get_bruegel_series, get_bruegel_indicators,
    get_provincial_data, get_provincial_indicators,
)
from policy_monitor.macro import (
    get_macro_db, get_macro_series, get_macro_variables, get_stored_version,
    VARIABLE_META, CATEGORIES,
)
from policy_monitor.academic import get_academic_db, get_recent_articles, get_journal_summary, cast_vote, get_preferences
from policy_monitor.advisor import generate_brief
from policy_monitor.polity import get_polity_data, scrape_meeting_news
from policy_monitor.eurostat import (
    get_eurostat_db, get_eurostat_datasets, get_eurostat_series,
    get_eurostat_latest, get_eurostat_indicators,
    get_sme_scorecard, get_policy_scorecard, DATASET_META,
)
from policy_monitor.sources.loader import (
    get_all_sources,
    get_direct_feeds,
    get_rsshub_feeds,
    get_wechat_accounts,
    load_registry,
)

app = Flask(__name__)

# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------

@app.route("/api/news")
def api_news():
    db = get_db()
    limit = request.args.get("limit", 50, type=int)
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
    limit = request.args.get("limit", 90, type=int)
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
    limit = request.args.get("limit", 180, type=int)
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


# ---------------------------------------------------------------------------
# GeoJSON endpoints — served locally to avoid CORS
# ---------------------------------------------------------------------------
DATA_DIR = Path(__file__).parent.parent / "data"

@app.route("/api/geo/prefectures")
def api_geo_prefectures():
    return app.response_class(
        response=open(DATA_DIR / "china_prefectures.json", "rb").read(),
        mimetype="application/json",
        headers={"Cache-Control": "public, max-age=86400"},
    )

@app.route("/api/geo/provinces")
def api_geo_provinces():
    return app.response_class(
        response=open(DATA_DIR / "china_provinces.json", "rb").read(),
        mimetype="application/json",
        headers={"Cache-Control": "public, max-age=86400"},
    )

# ---------------------------------------------------------------------------
# Dissent data endpoints
# ---------------------------------------------------------------------------

@app.route("/api/dissent/summary")
def api_dissent_summary():
    """Province-level dissent event counts. ?year=2024 for specific year."""
    try:
        from policy_monitor.dissent import get_dissent_db
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
        from policy_monitor.dissent import get_dissent_db, get_recent_events
        conn = get_dissent_db()
        province = request.args.get("province", "")
        limit = request.args.get("limit", 50, type=int)
        events = get_recent_events(conn, limit=limit, province=province)
        conn.close()
        return jsonify(events)
    except Exception as e:
        return jsonify([])


@app.route("/api/dissent/province_detail")
def api_dissent_province_detail():
    """Issue/mode breakdown for a province. ?province=Guangdong&year=2024"""
    try:
        from policy_monitor.dissent import get_dissent_db
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
        from policy_monitor.dissent import get_dissent_db
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

_flight_cache = {"data": [], "ts": 0}
_ship_cache = {"data": [], "ts": 0}


@app.route("/api/flights/positions")
def api_flight_positions():
    """Current flight positions over China (OpenSky Network).

    Fetches live from OpenSky if cache is older than 60s (authenticated)
    or 900s (anonymous).  No background process required.
    """
    import time
    try:
        from policy_monitor.flights import (
            _load_credentials,
            fetch_flight_positions,
            get_current_flights,
            get_flights_db,
            store_flight_positions,
        )
        u, p = _load_credentials()
        max_age = 60 if (u and p) else 900
        now = time.time()

        if now - _flight_cache["ts"] >= max_age:
            try:
                positions = fetch_flight_positions(u, p)
                conn = get_flights_db()
                store_flight_positions(conn, positions)
                conn.close()
                _flight_cache["data"] = positions
                _flight_cache["ts"] = now
            except Exception:
                # On fetch failure, serve stale cache or DB
                conn = get_flights_db()
                _flight_cache["data"] = get_current_flights(conn)
                conn.close()

        return jsonify(_flight_cache["data"])
    except Exception:
        return jsonify([])


@app.route("/api/ships/positions")
def api_ship_positions():
    """Current ship positions around China (AISHub or AISStream).

    Fetches live if cache is older than 60s.
    """
    import time
    try:
        from policy_monitor.ships import (
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
        if now - _ship_cache["ts"] >= 60:
            try:
                positions = fetch_ship_positions(duration_seconds=10)
                conn = get_ships_db()
                store_ship_positions(conn, positions)
                cleanup_stale(conn)
                _ship_cache["data"] = get_current_ships(conn)
                conn.close()
                _ship_cache["ts"] = now
            except Exception:
                conn = get_ships_db()
                _ship_cache["data"] = get_current_ships(conn)
                conn.close()

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
        limit = request.args.get("limit", 50, type=int)
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
# Policy Advisor
# ---------------------------------------------------------------------------

@app.route("/api/advisor/brief", methods=["POST"])
def api_advisor_brief():
    """Generate a structured policy brief. Body: {topic: str, days: int}"""
    try:
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
    return jsonify(get_polity_data())


@app.route("/api/polity/meeting-news")
def api_polity_meeting_news():
    try:
        items = scrape_meeting_news(max_items=20)
    except Exception as e:
        return jsonify({"error": str(e), "items": []})
    return jsonify({"items": items})


# ---------------------------------------------------------------------------
# Eurostat — EU-China competitive intelligence
# ---------------------------------------------------------------------------

@app.route("/api/eurostat/datasets")
def api_eurostat_datasets():
    """List all stored Eurostat datasets with metadata and row counts."""
    try:
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
        dataset = request.args.get("dataset", "")
        if not dataset:
            return jsonify({"error": "dataset parameter required"}), 400
        conn = get_eurostat_db()
        data = get_eurostat_indicators(conn, dataset)
        conn.close()
        return jsonify({"dataset": dataset, "indicators": data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/eurostat/sme-scorecard")
def api_eurostat_sme_scorecard():
    """
    SME competitive-intelligence scorecard:
    EU-China trade flows, R&D investment, labour costs, patent trends.
    """
    try:
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
        conn = get_eurostat_db()
        scorecard = get_policy_scorecard(conn)
        conn.close()
        return jsonify(scorecard)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/")
def index():
    return DASHBOARD_HTML


DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>China Policy Monitor</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/leaflet@1.9.4/dist/leaflet.min.css"/>
<script src="https://cdn.jsdelivr.net/npm/leaflet@1.9.4/dist/leaflet.min.js"></script>
<style>
  :root {
    --bg: #0a0c10; --surface: #13161d; --surface2: #1c2029;
    --border: #262a36; --text: #e1e4eb; --text2: #727889; --text3: #464c5e;
    --accent: #d4483b; --accent2: #e8a838; --green: #3eb370; --blue: #4a9eff;
    --purple: #9d7cf4;
  }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, 'SF Pro Text', 'Inter', 'Helvetica Neue', sans-serif;
         background: var(--bg); color: var(--text); line-height: 1.5; min-height: 100vh; }
  a { color: var(--blue); text-decoration: none; }
  a:hover { text-decoration: underline; }
  ::-webkit-scrollbar { width: 6px; }
  ::-webkit-scrollbar-track { background: var(--bg); }
  ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }

  .header { background: var(--surface); border-bottom: 1px solid var(--border);
            padding: 12px 24px; display: flex; align-items: center; gap: 16px; position: sticky; top: 0; z-index: 100; }
  .header h1 { font-size: 16px; font-weight: 700; letter-spacing: -0.3px; }
  .header h1 span { color: var(--accent); }
  .header .hstats { margin-left: auto; font-size: 11px; color: var(--text2); display: flex; gap: 14px; }
  .header .hstats b { color: var(--text); }

  .tabs { display: flex; gap: 0; background: var(--surface); border-bottom: 1px solid var(--border); padding: 0 24px; position: sticky; top: 45px; z-index: 99; }
  .tab { padding: 10px 18px; font-size: 12px; font-weight: 500; cursor: pointer; color: var(--text2);
         border-bottom: 2px solid transparent; transition: all 0.15s; letter-spacing: 0.2px; }
  .tab:hover { color: var(--text); }
  .tab.active { color: var(--text); border-bottom-color: var(--accent); }

  .wrap { max-width: 1440px; margin: 0 auto; padding: 20px 24px; }
  .panel { display: none; }
  .panel.active { display: block; }

  /* Cards */
  .cards { display: grid; grid-template-columns: repeat(auto-fill, minmax(190px, 1fr)); gap: 10px; margin-bottom: 20px; }
  .card { background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 14px 16px; }
  .card .label { font-size: 10px; color: var(--text2); text-transform: uppercase; letter-spacing: 0.6px; margin-bottom: 4px; }
  .card .value { font-size: 24px; font-weight: 700; font-variant-numeric: tabular-nums; letter-spacing: -0.5px; }
  .card .change { font-size: 11px; margin-top: 2px; }
  .card .change.up { color: var(--accent); }
  .card .change.down { color: var(--green); }
  .card .meta { font-size: 10px; color: var(--text3); margin-top: 4px; }

  /* Spark card layout */
  .spark-card { cursor: pointer; padding: 10px 12px; }
  .spark-card .value { font-size: 16px; font-weight: 700; }
  .spark-card .label { font-size: 9px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .spark-row { display: flex; align-items: center; gap: 8px; margin-top: 4px; }
  .spark-vals { flex: 1; min-width: 0; }
  .sparkline { width: 80px; height: 32px; flex-shrink: 0; }

  /* Category groups */
  .snap-group { margin-bottom: 18px; }
  .snap-group-title { font-size: 11px; font-weight: 700; color: var(--text2); text-transform: uppercase;
    letter-spacing: 0.8px; margin-bottom: 8px; padding-bottom: 4px; border-bottom: 1px solid var(--border); }
  .snap-group-count { font-size: 10px; color: var(--text3); font-weight: 400; margin-left: 4px; }
  .snap-group-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 8px; }

  /* Landing page */
  .landing-grid { display: grid; grid-template-columns: 1fr 380px; gap: 20px; }
  .map-box { background: var(--surface); border: 1px solid var(--border); border-radius: 10px;
             overflow: hidden; position: relative; }
  .map-title { font-size: 12px; font-weight: 600; color: var(--text2); padding: 14px 16px 0 16px;
               letter-spacing: 0.3px; }
  #china-map { width: 100%; height: 560px; }
  /* Override Leaflet tiles for dark theme */
  .leaflet-container { background: var(--surface) !important; }
  .leaflet-control-attribution { font-size: 9px !important; background: rgba(10,12,16,0.8) !important; color: var(--text3) !important; }
  .leaflet-control-attribution a { color: var(--text2) !important; }
  .leaflet-control-zoom a { background: var(--surface) !important; color: var(--text) !important; border-color: var(--border) !important; }
  .leaflet-control-zoom a:hover { background: var(--surface2) !important; }

  .sidebar { display: flex; flex-direction: column; gap: 12px; }
  .sidebar-section { background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 14px; }
  .sidebar-section h3 { font-size: 11px; color: var(--text2); text-transform: uppercase; letter-spacing: 0.5px;
                         margin-bottom: 10px; padding-bottom: 6px; border-bottom: 1px solid var(--border); }
  .headline-item { padding: 6px 0; border-bottom: 1px solid var(--border); }
  .headline-item:last-child { border: none; }
  .headline-item .hl-source { font-size: 9px; color: var(--accent); font-weight: 700; text-transform: uppercase; letter-spacing: 0.3px; }
  .headline-item .hl-title { font-size: 12px; font-weight: 500; margin-top: 1px; }
  .headline-item .hl-title a { color: var(--text); }
  .headline-item .hl-title a:hover { color: var(--blue); }
  .fin-row { display: flex; justify-content: space-between; padding: 5px 0; border-bottom: 1px solid var(--border); font-size: 12px; }
  .fin-row:last-child { border: none; }
  .fin-row .fin-name { color: var(--text2); }
  .fin-row .fin-val { font-weight: 600; font-variant-numeric: tabular-nums; }

  /* Controls */
  .controls { display: flex; gap: 8px; margin-bottom: 14px; flex-wrap: wrap; }
  .controls input, .controls select { background: var(--surface); border: 1px solid var(--border);
    color: var(--text); padding: 7px 11px; border-radius: 6px; font-size: 12px; outline: none; }
  .controls input:focus, .controls select:focus { border-color: var(--blue); }
  .controls input { flex: 1; min-width: 200px; }
  .controls select { min-width: 150px; }

  /* News list */
  .news-list { display: flex; flex-direction: column; gap: 1px; }
  .news-item { background: var(--surface); border: 1px solid var(--border); border-radius: 6px;
               padding: 12px 14px; transition: border-color 0.15s; }
  .news-item:hover { border-color: var(--blue); }
  .news-item .ni-top { display: flex; justify-content: space-between; align-items: center; }
  .news-item .source { font-size: 10px; color: var(--accent); font-weight: 700; text-transform: uppercase; letter-spacing: 0.3px; }
  .news-item .title { font-size: 13px; font-weight: 500; margin: 3px 0; }
  .news-item .summary { font-size: 11px; color: var(--text2); margin-top: 3px;
    display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }
  .news-item .time { font-size: 10px; color: var(--text3); }

  /* Vote controls */
  .vote-col { display: flex; flex-direction: column; align-items: center; gap: 0; margin-right: 12px;
    flex-shrink: 0; user-select: none; }
  .vote-col .vbtn { cursor: pointer; font-size: 16px; line-height: 1; color: var(--text3);
    padding: 2px 4px; border-radius: 3px; transition: color 0.12s; }
  .vote-col .vbtn:hover { color: var(--text); }
  .vote-col .vbtn.up.active { color: var(--accent); }
  .vote-col .vbtn.down.active { color: var(--red, #d4483b); }
  .vote-col .vscore { font-size: 11px; font-weight: 700; color: var(--text2);
    font-variant-numeric: tabular-nums; min-width: 16px; text-align: center; }
  .acad-row { display: flex; align-items: flex-start; }
  .acad-row .news-item { flex: 1; min-width: 0; }

  /* Charts */
  .chart-box { background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 16px; margin-bottom: 14px; }
  .chart-box h3 { font-size: 13px; font-weight: 600; margin-bottom: 10px; }
  canvas { max-height: 280px; }
  .pill { display: inline-block; padding: 3px 9px; font-size: 10px; border-radius: 4px; cursor: pointer;
          background: var(--surface2); color: var(--text2); border: 1px solid var(--border); margin: 2px; }
  .pill:hover { border-color: var(--text2); color: var(--text); }
  .pill.active { background: var(--accent); color: #fff; border-color: var(--accent); }

  /* Sources table */
  .src-table { width: 100%; border-collapse: collapse; font-size: 12px; }
  .src-table th { text-align: left; padding: 8px 10px; border-bottom: 2px solid var(--border);
                  font-size: 10px; color: var(--text3); text-transform: uppercase; letter-spacing: 0.5px;
                  position: sticky; top: 90px; background: var(--bg); z-index: 10; }
  .src-table td { padding: 8px 10px; border-bottom: 1px solid var(--border); vertical-align: top; }
  .src-table tr:hover td { background: var(--surface); }
  .badge { display: inline-block; padding: 2px 7px; border-radius: 3px; font-size: 10px; font-weight: 600; white-space: nowrap; }
  .b-gov { background: rgba(212,72,59,0.12); color: var(--accent); }
  .b-media { background: rgba(74,158,255,0.12); color: var(--blue); }
  .b-reg { background: rgba(232,168,56,0.12); color: var(--accent2); }
  .b-legal { background: rgba(62,179,112,0.12); color: var(--green); }
  .b-fin { background: rgba(157,124,244,0.12); color: var(--purple); }
  .b-wechat { background: rgba(62,179,112,0.12); color: var(--green); }
  .b-svc { background: rgba(114,120,137,0.15); color: var(--text2); }
  .sync-dot { display: inline-block; width: 7px; height: 7px; border-radius: 50%; margin-right: 5px; }
  .sync-ok { background: var(--green); }
  .sync-stale { background: var(--accent2); }
  .sync-never { background: var(--text3); }
  .src-filter-bar { display: flex; gap: 6px; margin-bottom: 14px; flex-wrap: wrap; }

  .loading { text-align: center; padding: 30px; color: var(--text3); }
  .empty { text-align: center; padding: 30px; color: var(--text3); font-style: italic; }

  @media (max-width: 900px) {
    .landing-grid { grid-template-columns: 1fr; }
    .cards { grid-template-columns: repeat(2, 1fr); }
    .snap-group-grid { grid-template-columns: repeat(2, 1fr); }
  }
</style>
</head>
<body>

<div class="header">
  <h1><span>China</span> Policy Monitor</h1>
  <div class="hstats">
    <span>Items <b id="h-items">—</b></span>
    <span>Sources <b id="h-sources">—</b></span>
    <span>Last sync <b id="h-last">—</b></span>
  </div>
</div>

<div class="tabs">
  <div class="tab active" data-panel="overview">Overview</div>
  <div class="tab" data-panel="news">News Feed</div>
  <div class="tab" data-panel="financial">Financial</div>
  <div class="tab" data-panel="sources">All Sources</div>
  <div class="tab" data-panel="history">Historical Context</div>
  <div class="tab" data-panel="academic">Academic</div>
  <div class="tab" data-panel="advisor">Policy Advisor</div>
  <div class="tab" data-panel="polity">Political Structure</div>
</div>

<div class="wrap">

<!-- ====== OVERVIEW ====== -->
<div class="panel active" id="panel-overview">
  <div class="landing-grid">
    <div class="map-box">
      <div class="map-title">People's Republic of China — Prefectural Administrative Divisions</div>
      <div id="china-map"></div>
    </div>
    <div class="sidebar">
      <div class="sidebar-section">
        <h3>Latest Headlines</h3>
        <div id="ov-headlines"><div class="loading">Loading...</div></div>
      </div>
      <div class="sidebar-section">
        <h3>Financial Snapshot</h3>
        <div id="ov-financial"><div class="loading">Loading...</div></div>
      </div>
      <div class="sidebar-section">
        <h3>System Status</h3>
        <div id="ov-status"><div class="loading">Loading...</div></div>
      </div>
    </div>
  </div>
</div>

<!-- ====== NEWS ====== -->
<div class="panel" id="panel-news">
  <div class="controls">
    <input type="text" id="search-input" placeholder="Search headlines... (tariff, Taiwan, AI, trade)">
    <select id="source-filter"><option value="">All Sources</option></select>
    <select id="category-filter">
      <option value="">All Categories</option>
      <option value="state_media">State Media</option>
      <option value="media">Media</option>
      <option value="central_government">Central Gov</option>
      <option value="ministry">Ministries</option>
      <option value="regulator">Regulators</option>
    </select>
  </div>
  <div class="news-list" id="news-list"><div class="loading">Loading...</div></div>
</div>

<!-- ====== FINANCIAL ====== -->
<div class="panel" id="panel-financial">
  <div class="controls" style="margin-bottom:12px">
    <span class="pill active" data-finsrc="all" onclick="setFinSource('all')">All Sources</span>
    <span class="pill" data-finsrc="akshare" onclick="setFinSource('akshare')">AKShare</span>
    <span class="pill" data-finsrc="bruegel" onclick="setFinSource('bruegel')">Bruegel</span>
  </div>
  <div id="snap-cards"><div class="loading">Loading...</div></div>
  <div class="chart-box">
    <h3 id="chart-title">Select an indicator above or below</h3>
    <canvas id="main-chart"></canvas>
  </div>
  <div class="chart-box">
    <h3>All Indicators</h3>
    <div id="ind-list" style="display:flex;flex-wrap:wrap;gap:4px;"></div>
  </div>
</div>

<!-- ====== ALL SOURCES ====== -->
<div class="panel" id="panel-sources">
  <div class="cards" id="src-summary"></div>
  <div class="src-filter-bar" id="src-filters"></div>
  <table class="src-table">
    <thead><tr>
      <th style="width:24px"></th>
      <th>Source</th>
      <th>Chinese</th>
      <th>Category</th>
      <th>Feed Type</th>
      <th>Last Synced</th>
      <th>Items</th>
      <th>URL</th>
    </tr></thead>
    <tbody id="src-body"><tr><td colspan="8" class="loading">Loading...</td></tr></tbody>
  </table>
</div>

<!-- ====== HISTORICAL CONTEXT ====== -->
<div class="panel" id="panel-history">
  <div style="max-width:1100px;margin:0 auto;">
    <!-- Sub-tab selector -->
    <div id="hist-subtabs" style="display:flex;gap:0;margin-bottom:20px;border-bottom:1px solid #1e2230;">
      <div class="hist-sub active" data-sub="macro" style="padding:8px 20px;cursor:pointer;font-size:13px;font-weight:600;color:#e1e4eb;border-bottom:2px solid #d4483b;margin-bottom:-1px;">Macro Economy</div>
      <div class="hist-sub" data-sub="dissent" style="padding:8px 20px;cursor:pointer;font-size:13px;font-weight:600;color:#727889;border-bottom:2px solid transparent;margin-bottom:-1px;">Dissent Monitor</div>
    </div>

    <!-- ---- Macro Economy (GMD) ---- -->
    <div id="hist-panel-macro">
      <p style="color:#727889;font-size:12px;margin-bottom:12px;">Annual data from <a href="https://www.globalmacrodata.com" target="_blank" style="color:#4a9eff;">globalmacrodata.com</a> (Müller et al. 2025). 75 variables, 1640–2030. <span id="gmd-version" style="color:#727889;"></span></p>

      <div id="macro-cats" style="display:flex;gap:6px;margin-bottom:16px;flex-wrap:wrap;"></div>

      <div style="display:grid;grid-template-columns:220px 1fr;gap:16px;margin-bottom:28px;">
        <div class="card" style="padding:12px;max-height:420px;overflow-y:auto;" id="macro-var-list">
          <div class="loading">Loading...</div>
        </div>
        <div class="card" style="padding:16px;">
          <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
            <h3 id="macro-chart-title" style="color:#e1e4eb;font-size:13px;font-weight:600;margin:0;">Select a variable</h3>
            <div style="display:flex;align-items:center;gap:6px;">
              <input type="number" id="macro-year-start" placeholder="1950" value="1950" min="1640" max="2030" style="width:64px;padding:3px 6px;font-size:11px;background:#1e2230;border:1px solid #2a2e3a;border-radius:4px;color:#c8cad0;text-align:center;">
              <span style="color:#727889;font-size:11px;">–</span>
              <input type="number" id="macro-year-end" placeholder="2030" value="2030" min="1640" max="2030" style="width:64px;padding:3px 6px;font-size:11px;background:#1e2230;border:1px solid #2a2e3a;border-radius:4px;color:#c8cad0;text-align:center;">
              <button id="macro-year-apply" style="padding:3px 10px;font-size:11px;background:#d4483b;color:#fff;border:none;border-radius:4px;cursor:pointer;">Apply</button>
            </div>
          </div>
          <canvas id="macro-chart" height="260"></canvas>
          <div id="macro-chart-info" style="color:#727889;font-size:10px;margin-top:6px;"></div>
        </div>
      </div>
    </div>

    <!-- ---- Dissent ---- -->
    <div id="hist-panel-dissent" style="display:none;">
      <p style="color:#727889;font-size:12px;margin-bottom:20px;">All-time data from <a href="https://chinadissent.net/" target="_blank" style="color:#4a9eff;">chinadissent.net</a>. The map overlay shows only the most recent year.</p>

      <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:20px;">
        <div class="card" style="padding:16px;">
          <h3 style="color:#727889;font-size:10px;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:10px;">Events by Year</h3>
          <canvas id="hist-yearly-chart" height="180"></canvas>
        </div>
        <div class="card" style="padding:16px;">
          <h3 style="color:#727889;font-size:10px;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:10px;">Top Issues (All Time)</h3>
          <canvas id="hist-issues-chart" height="180"></canvas>
        </div>
      </div>

      <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:20px;">
        <div class="card" style="padding:16px;">
          <h3 style="color:#727889;font-size:10px;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:10px;">Modes of Dissent (All Time)</h3>
          <div id="hist-modes"></div>
        </div>
        <div class="card" style="padding:16px;">
          <h3 style="color:#727889;font-size:10px;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:10px;">Year-over-Year Comparison</h3>
          <div id="hist-yoy"></div>
        </div>
      </div>

      <div class="card" style="padding:16px;margin-bottom:20px;">
        <h3 style="color:#727889;font-size:10px;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:10px;">Province Rankings by Year</h3>
        <div id="hist-prov-tabs" style="display:flex;gap:6px;margin-bottom:12px;flex-wrap:wrap;"></div>
        <div id="hist-prov-table"></div>
      </div>

      <div class="card" style="padding:16px;">
        <h3 style="color:#727889;font-size:10px;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:10px;">Top Issues by Year</h3>
        <div id="hist-issues-yearly"></div>
      </div>
    </div>
  </div>
</div>

<!-- ====== ACADEMIC ====== -->
<div class="panel" id="panel-academic">
  <div class="controls">
    <input type="text" id="acad-search" placeholder="Search articles... (author, title, keyword)">
    <select id="acad-journal-filter"><option value="">All Journals</option></select>
    <span class="pill active" id="acad-sort-recent" onclick="setAcadSort('recent')">Recent</span>
    <span class="pill" id="acad-sort-ranked" onclick="setAcadSort('ranked')">For You</span>
  </div>
  <div id="acad-summary" style="margin-bottom:12px;font-size:12px;color:var(--text2);"></div>
  <div class="news-list" id="acad-list"><div class="loading">Loading...</div></div>
</div>

<!-- ====== POLICY ADVISOR ====== -->
<div class="panel" id="panel-advisor">
  <div style="max-width:860px;margin:0 auto;">
    <p style="color:var(--text2);font-size:13px;margin-bottom:20px;">
      Generate a structured policy brief for European government officials based on recent Chinese policy developments tracked in this database.
    </p>

    <!-- Input form -->
    <div class="card" style="padding:20px;margin-bottom:20px;">
      <div style="margin-bottom:14px;">
        <label style="font-size:11px;color:var(--text2);text-transform:uppercase;letter-spacing:0.5px;display:block;margin-bottom:6px;">Topic / Question</label>
        <textarea id="adv-topic" rows="3" placeholder="e.g. China EV industrial policy and subsidies&#10;e.g. MIIT semiconductor export controls impact on European manufacturers&#10;e.g. Belt and Road Initiative investment in Eastern Europe"
          style="width:100%;background:var(--surface2);border:1px solid var(--border);color:var(--text);padding:10px 12px;border-radius:6px;font-size:13px;font-family:inherit;outline:none;resize:vertical;"></textarea>
      </div>
      <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap;">
        <div>
          <label style="font-size:11px;color:var(--text2);text-transform:uppercase;letter-spacing:0.5px;display:block;margin-bottom:4px;">Time window</label>
          <select id="adv-days" style="background:var(--surface2);border:1px solid var(--border);color:var(--text);padding:7px 11px;border-radius:6px;font-size:12px;outline:none;">
            <option value="30">Last 30 days</option>
            <option value="90" selected>Last 90 days</option>
            <option value="180">Last 180 days</option>
            <option value="365">Last 12 months</option>
          </select>
        </div>
        <button id="adv-submit" onclick="generateBrief()"
          style="margin-top:16px;padding:8px 22px;background:var(--accent);color:#fff;border:none;border-radius:6px;font-size:13px;font-weight:600;cursor:pointer;transition:opacity 0.15s;">
          Generate Brief
        </button>
        <span id="adv-spinner" style="display:none;color:var(--text3);font-size:12px;">Generating...</span>
      </div>
    </div>

    <!-- Output -->
    <div id="adv-output" style="display:none;">
      <!-- Stub banner -->
      <div id="adv-stub-banner" style="display:none;background:rgba(232,168,56,0.12);border:1px solid rgba(232,168,56,0.35);border-radius:8px;padding:12px 16px;margin-bottom:16px;font-size:12px;color:var(--accent2);">
        <b>AI analysis not enabled.</b> Add <code style="background:rgba(255,255,255,0.08);padding:1px 5px;border-radius:3px;">ANTHROPIC_API_KEY</code> to your <code style="background:rgba(255,255,255,0.08);padding:1px 5px;border-radius:3px;">.env</code> file to generate full policy briefs. Showing retrieved sources only.
      </div>

      <!-- Brief header -->
      <div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:12px;">
        <div>
          <div style="font-size:10px;color:var(--text3);text-transform:uppercase;letter-spacing:0.5px;">Policy Brief</div>
          <div id="adv-brief-topic" style="font-size:18px;font-weight:700;letter-spacing:-0.3px;margin-top:2px;"></div>
        </div>
        <div style="text-align:right;font-size:11px;color:var(--text3);">
          <div id="adv-brief-date"></div>
          <div id="adv-brief-sources" style="margin-top:2px;"></div>
        </div>
      </div>

      <!-- Brief body -->
      <div id="adv-brief-body" class="card" style="padding:24px;line-height:1.7;font-size:13px;"></div>

      <!-- Source list -->
      <div id="adv-source-list" style="margin-top:16px;display:none;">
        <div style="font-size:10px;color:var(--text3);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:8px;">Source Documents</div>
        <div id="adv-sources-body" style="display:flex;flex-direction:column;gap:6px;"></div>
      </div>
    </div>

    <div id="adv-error" style="display:none;color:var(--accent);font-size:13px;margin-top:12px;"></div>
  </div>
</div>

<!-- ====== POLITICAL STRUCTURE ====== -->
<div class="panel" id="panel-polity">
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:20px;" id="polity-top-grid">

    <!-- Hierarchy tree -->
    <div class="card" style="padding:0;overflow:hidden;">
      <div style="padding:14px 16px;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;">
        <span style="font-size:11px;font-weight:700;color:var(--text2);text-transform:uppercase;letter-spacing:0.6px;">Political Hierarchy</span>
        <span style="font-size:10px;color:var(--text3);">Click to expand</span>
      </div>
      <div id="polity-tree" style="padding:12px 16px;max-height:520px;overflow-y:auto;font-size:12px;"></div>
    </div>

    <!-- PSC members -->
    <div>
      <div class="card" style="padding:0;overflow:hidden;margin-bottom:12px;">
        <div style="padding:14px 16px;border-bottom:1px solid var(--border);">
          <span style="font-size:11px;font-weight:700;color:var(--text2);text-transform:uppercase;letter-spacing:0.6px;">Politburo Standing Committee</span>
          <span style="font-size:10px;color:var(--text3);margin-left:8px;">7 members · supreme power</span>
        </div>
        <div id="polity-psc" style="padding:8px 0;"></div>
      </div>

      <!-- Recent meeting news -->
      <div class="card" style="padding:0;overflow:hidden;">
        <div style="padding:14px 16px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:10px;">
          <span style="font-size:11px;font-weight:700;color:var(--text2);text-transform:uppercase;letter-spacing:0.6px;">Recent Meeting News</span>
          <button onclick="refreshMeetingNews()" style="margin-left:auto;padding:3px 10px;background:var(--surface2);border:1px solid var(--border);color:var(--text2);border-radius:4px;font-size:10px;cursor:pointer;">Refresh</button>
        </div>
        <div id="polity-news" style="padding:8px 0;max-height:220px;overflow-y:auto;font-size:12px;">
          <div style="padding:12px 16px;color:var(--text3);">Loading...</div>
        </div>
      </div>
    </div>
  </div>

  <!-- Meeting calendar -->
  <div class="card" style="padding:0;overflow:hidden;margin-bottom:20px;">
    <div style="padding:14px 16px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:16px;">
      <span style="font-size:11px;font-weight:700;color:var(--text2);text-transform:uppercase;letter-spacing:0.6px;">Meeting Calendar</span>
      <div style="display:flex;gap:6px;flex-wrap:wrap;" id="polity-cal-filters">
        <span class="pill active" onclick="setCalFilter('all',this)">All</span>
        <span class="pill" onclick="setCalFilter('party_congress',this)">Party Congress</span>
        <span class="pill" onclick="setCalFilter('plenum',this)">Plenums</span>
        <span class="pill" onclick="setCalFilter('npc',this)">NPC</span>
        <span class="pill" onclick="setCalFilter('cppcc',this)">CPPCC</span>
        <span class="pill" onclick="setCalFilter('cewc',this)">CEWC</span>
      </div>
      <div style="margin-left:auto;display:flex;gap:6px;">
        <span class="pill active" id="cal-show-past" onclick="toggleCalPast()">Past</span>
        <span class="pill active" id="cal-show-upcoming" onclick="toggleCalUpcoming()">Upcoming</span>
      </div>
    </div>
    <div id="polity-calendar" style="padding:0;max-height:380px;overflow-y:auto;"></div>
  </div>

  <!-- Decision-making process -->
  <div class="card" style="padding:0;overflow:hidden;">
    <div style="padding:14px 16px;border-bottom:1px solid var(--border);">
      <span style="font-size:11px;font-weight:700;color:var(--text2);text-transform:uppercase;letter-spacing:0.6px;">How China Makes Decisions</span>
      <span style="font-size:10px;color:var(--text3);margin-left:8px;">Policy decision flow from top to implementation</span>
    </div>
    <div id="polity-process" style="padding:16px;display:grid;grid-template-columns:repeat(3,1fr);gap:12px;"></div>
  </div>
</div>

</div><!-- wrap -->

<script>
// ========== Helpers ==========
const api = p => fetch(p).then(r => r.json());
const esc = s => s ? s.replace(/</g,'&lt;').replace(/>/g,'&gt;') : '';
const ago = iso => {
  if (!iso) return 'never';
  const d = new Date(iso + (iso.includes('Z')||iso.includes('+')?'':'Z'));
  const m = Math.floor((Date.now()-d)/60000);
  if (m < 1) return 'just now';
  if (m < 60) return m+'m ago';
  if (m < 1440) return Math.floor(m/60)+'h ago';
  return Math.floor(m/1440)+'d ago';
};

// ========== Tabs ==========
document.querySelectorAll('.tab').forEach(t => t.addEventListener('click', () => {
  document.querySelectorAll('.tab').forEach(x => x.classList.remove('active'));
  document.querySelectorAll('.panel').forEach(x => x.classList.remove('active'));
  t.classList.add('active');
  document.getElementById('panel-'+t.dataset.panel).classList.add('active');
}));

// ========== OVERVIEW — LEAFLET MAP ==========
let chinaMap = null;

async function initMap() {
  // Initialize Leaflet map centered on China
  chinaMap = L.map('china-map', {
    center: [35.5, 104.5],
    zoom: 4,
    minZoom: 3,
    maxZoom: 10,
    zoomControl: true,
    attributionControl: true,
  });

  // CartoDB Positron (no labels) — clean academic basemap, dark variant
  L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}{r}.png', {
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; <a href="https://carto.com/">CARTO</a>',
    subdomains: 'abcd',
    maxZoom: 12,
  }).addTo(chinaMap);

  // Load prefectural boundaries (served locally — no CORS)
  let prefectures, provinces;
  try {
    const [prefResp, provResp] = await Promise.all([
      fetch('/api/geo/prefectures'),
      fetch('/api/geo/provinces'),
    ]);
    prefectures = await prefResp.json();
    provinces = await provResp.json();
  } catch(e) {
    console.error('GeoJSON load failed:', e);
    return;
  }

  // Prefecture layer — fine boundaries, subtle fill
  const prefLayer = L.geoJSON(prefectures, {
    style: {
      fillColor: '#1a2035',
      fillOpacity: 0.6,
      color: '#2a3050',
      weight: 0.4,
    },
    onEachFeature: (feature, layer) => {
      const name = feature.properties.name || '';
      layer.on({
        mouseover: (e) => {
          e.target.setStyle({ fillColor: '#d4483b', fillOpacity: 0.25, color: '#d4483b', weight: 1.2 });
          // Keep province lines on top (do NOT bringToFront on e.target — doing so
          // causes the browser to fire a synthetic mouseout mid-handler, which resets
          // the style and then fires a second mouseover, leaving the feature stuck red
          // when the cursor finally leaves).
          if (provLayer) provLayer.bringToFront();
          info.update(feature.properties);
        },
        mouseout: (e) => {
          prefLayer.resetStyle(e.target);
          info.update();
        },
      });
    },
  }).addTo(chinaMap);

  // Province layer — thicker boundary lines only (no fill)
  const provLayer = L.geoJSON(provinces, {
    style: {
      fillColor: 'transparent',
      fillOpacity: 0,
      color: '#4a5275',
      weight: 1.5,
    },
    interactive: false,
  }).addTo(chinaMap);

  // City markers
  const cities = [
    { name: 'Beijing 北京', lat: 39.90, lng: 116.40, capital: true },
    { name: 'Shanghai 上海', lat: 31.23, lng: 121.47 },
    { name: 'Guangzhou 广州', lat: 23.13, lng: 113.26 },
    { name: 'Shenzhen 深圳', lat: 22.55, lng: 114.07 },
    { name: 'Chongqing 重庆', lat: 29.56, lng: 106.55 },
    { name: 'Chengdu 成都', lat: 30.57, lng: 104.07 },
    { name: 'Wuhan 武汉', lat: 30.59, lng: 114.30 },
    { name: 'Hangzhou 杭州', lat: 30.27, lng: 120.15 },
    { name: 'Hong Kong 香港', lat: 22.32, lng: 114.17 },
    { name: 'Taipei 台北', lat: 25.03, lng: 121.57 },
  ];
  cities.forEach(c => {
    const color = c.capital ? '#d4483b' : '#4a9eff';
    const r = c.capital ? 5 : 3;
    const marker = L.circleMarker([c.lat, c.lng], {
      radius: r, fillColor: color, fillOpacity: 0.9, color: color, weight: c.capital ? 2 : 1, opacity: 0.5,
    }).addTo(chinaMap);
    marker.bindTooltip(c.name, {
      permanent: false, direction: 'right', offset: [8, 0],
      className: 'city-tooltip',
    });
    // Beijing pulsing ring
    if (c.capital) {
      L.circleMarker([c.lat, c.lng], {
        radius: 10, fillColor: 'transparent', fillOpacity: 0, color: '#d4483b', weight: 1, opacity: 0.3,
      }).addTo(chinaMap);
    }
  });

  // Info control — shows hovered prefecture name
  const info = L.control({ position: 'topright' });
  info.onAdd = function() {
    this._div = L.DomUtil.create('div');
    this._div.style.cssText = 'background:rgba(10,12,16,0.85);color:#e1e4eb;padding:8px 12px;border-radius:6px;font-size:12px;border:1px solid #262a36;min-width:120px;';
    this.update();
    return this._div;
  };
  info.update = function(props) {
    this._div.innerHTML = props
      ? '<span style="color:#d4483b;font-weight:700;font-size:10px;text-transform:uppercase;letter-spacing:0.5px">Prefecture</span><br>'
        + '<strong style="font-size:14px">' + (props.name||'') + '</strong>'
        + (props.adcode ? '<br><span style="color:#727889;font-size:10px">adcode: '+props.adcode+'</span>' : '')
      : '<span style="color:#727889">Hover over a prefecture</span>';
  };
  info.addTo(chinaMap);

  // Add custom CSS for city tooltips
  const style = document.createElement('style');
  style.textContent = '.city-tooltip{background:rgba(10,12,16,0.9)!important;color:#e1e4eb!important;border:1px solid #262a36!important;border-radius:4px!important;font-size:11px!important;font-weight:600!important;padding:4px 8px!important;box-shadow:none!important;}.city-tooltip::before{border-right-color:rgba(10,12,16,0.9)!important;}';
  document.head.appendChild(style);

  // ===== AMTI South China Sea Island Markers =====
  const amtiIslands = [
    { name: 'Cuarteron Reef', lat: 8.884, lng: 112.842, slug: 'cuarteron-reef' },
    { name: 'Fiery Cross Reef', lat: 9.553, lng: 112.890, slug: 'fiery-cross-reef' },
    { name: 'Gaven Reef', lat: 10.206, lng: 114.221, slug: 'gaven-reef' },
    { name: 'Hughes Reef', lat: 9.926, lng: 114.498, slug: 'hughes-reef' },
    { name: 'Johnson Reef', lat: 9.720, lng: 114.280, slug: 'johnson-reef' },
    { name: 'Mischief Reef', lat: 9.904, lng: 115.533, slug: 'mischief-reef' },
    { name: 'Subi Reef', lat: 10.921, lng: 114.080, slug: 'subi-reef' },
    { name: 'Scarborough Shoal', lat: 15.228, lng: 117.764, slug: 'scarborough-shoal' },
    { name: 'Antelope Reef', lat: 16.027, lng: 111.574, slug: 'antelope-reef' },
    { name: 'Bombay Reef', lat: 16.039, lng: 112.476, slug: 'bombay-reef' },
    { name: 'Drummond Island', lat: 16.455, lng: 111.763, slug: 'drummond-island' },
    { name: 'Duncan Islands', lat: 16.481, lng: 111.729, slug: 'duncan-islands' },
    { name: 'Lincoln Island', lat: 16.670, lng: 112.725, slug: 'lincoln-island' },
    { name: 'Middle Island', lat: 16.015, lng: 112.332, slug: 'middle-island' },
    { name: 'Money Island', lat: 16.438, lng: 111.590, slug: 'money-island' },
    { name: 'North Island', lat: 16.955, lng: 112.337, slug: 'north-island' },
    { name: 'North Reef', lat: 17.085, lng: 111.515, slug: 'north-reef' },
    { name: 'Observation Bank', lat: 16.587, lng: 112.640, slug: 'observation-bank' },
    { name: 'Pattle Island', lat: 16.532, lng: 111.592, slug: 'pattle-island' },
    { name: 'Quanfu Island', lat: 16.998, lng: 112.236, slug: 'quanfu-island' },
    { name: 'Robert Island', lat: 16.447, lng: 111.601, slug: 'robert-island' },
    { name: 'South Island', lat: 16.949, lng: 112.336, slug: 'south-island' },
    { name: 'South Sand', lat: 16.523, lng: 111.511, slug: 'south-sand' },
    { name: 'Tree Island', lat: 16.976, lng: 112.266, slug: 'tree-island' },
    { name: 'Triton Island', lat: 15.784, lng: 111.201, slug: 'triton-island' },
    { name: 'West Sand', lat: 16.981, lng: 112.209, slug: 'west-sand' },
    { name: 'Woody Island', lat: 16.834, lng: 112.340, slug: 'woody-island' },
    { name: 'Yagong Island', lat: 16.564, lng: 111.688, slug: 'yagong-island' },
  ];
  const amtiGroup = L.layerGroup();
  amtiIslands.forEach(isl => {
    const m = L.circleMarker([isl.lat, isl.lng], {
      radius: 5, fillColor: '#f5a623', fillOpacity: 0.9, color: '#f5a623', weight: 1.5, opacity: 0.7,
    });
    m.bindTooltip(isl.name, { direction: 'right', offset: [8,0], className: 'city-tooltip' });
    m.on('click', () => window.open('https://amti.csis.org/island-tracker/' + isl.slug + '/', '_blank'));
    m.addTo(amtiGroup);
  });
  amtiGroup.addTo(chinaMap);

  // ===== Flight Positions Layer =====
  const flightGroup = L.layerGroup();
  let flightRefreshTimer = null;

  async function refreshFlights() {
    try {
      const resp = await fetch('/api/flights/positions');
      const flights = await resp.json();
      flightGroup.clearLayers();
      flights.forEach(f => {
        if (f.latitude == null || f.longitude == null) return;
        const hdg = f.heading || 0;
        const icon = L.divIcon({
          className: '',
          html: '<div style="color:#9d7cf4;font-size:14px;transform:rotate(' + hdg + 'deg);text-shadow:0 0 4px rgba(157,124,244,0.5);">&#9992;</div>',
          iconSize: [16, 16],
          iconAnchor: [8, 8],
        });
        const m = L.marker([f.latitude, f.longitude], { icon: icon });
        const alt = f.geo_altitude != null ? Math.round(f.geo_altitude) + 'm' : '?';
        const spd = f.velocity != null ? Math.round(f.velocity * 1.944) + 'kts' : '?';
        m.bindTooltip(
          '<strong>' + esc(f.callsign || f.icao24) + '</strong><br>'
          + esc(f.origin_country || '') + '<br>'
          + 'Alt: ' + alt + ' | Spd: ' + spd,
          { direction: 'right', offset: [10, 0], className: 'city-tooltip' }
        );
        m.addTo(flightGroup);
      });
    } catch(e) { console.warn('Flight refresh failed:', e); }
  }

  // ===== Ship Positions Layer =====
  const shipGroup = L.layerGroup();
  let shipRefreshTimer = null;

  async function refreshShips() {
    try {
      const resp = await fetch('/api/ships/positions');
      const ships = await resp.json();
      shipGroup.clearLayers();
      ships.forEach(s => {
        if (s.latitude == null || s.longitude == null) return;
        const m = L.circleMarker([s.latitude, s.longitude], {
          radius: 3, fillColor: '#3eb370', fillOpacity: 0.85,
          color: '#3eb370', weight: 1, opacity: 0.6,
        });
        const spd = s.sog != null ? Number(s.sog).toFixed(1) + 'kts' : '?';
        const hdg = s.heading != null ? s.heading + '\u00b0' : '?';
        m.bindTooltip(
          '<strong>' + esc(s.ship_name || s.mmsi) + '</strong><br>'
          + 'MMSI: ' + esc(String(s.mmsi)) + '<br>'
          + 'Speed: ' + spd + ' | Hdg: ' + hdg
          + (s.destination ? '<br>Dest: ' + esc(s.destination) : ''),
          { direction: 'right', offset: [8, 0], className: 'city-tooltip' }
        );
        m.addTo(shipGroup);
      });
    } catch(e) { console.warn('Ship refresh failed:', e); }
  }

  // ===== Dissent Events Choropleth Layer =====
  // CDM English province names → GeoJSON Chinese province names
  const provNameMap = {
    'Anhui': '安徽省', 'Beijing': '北京市', 'Chongqing': '重庆市', 'Fujian': '福建省',
    'Gansu': '甘肃省', 'Guangdong': '广东省', 'Guangxi': '广西壮族自治区', 'Guizhou': '贵州省',
    'Hainan': '海南省', 'Hebei': '河北省', 'Heilongjiang': '黑龙江省', 'Henan': '河南省',
    'Hubei': '湖北省', 'Hunan': '湖南省', 'Inner Mongolia': '内蒙古自治区',
    'Jiangsu': '江苏省', 'Jiangxi': '江西省', 'Jilin': '吉林省', 'Liaoning': '辽宁省',
    'Ningxia': '宁夏回族自治区', 'Qinghai': '青海省', 'Shaanxi': '陕西省', 'Shandong': '山东省',
    'Shanghai': '上海市', 'Shanxi': '山西省', 'Sichuan': '四川省', 'Tianjin': '天津市',
    'Tibet': '西藏自治区', 'Xinjiang': '新疆维吾尔自治区', 'Yunnan': '云南省', 'Zhejiang': '浙江省',
    'Hong Kong': '香港特别行政区', 'Macau': '澳门特别行政区', 'Taiwan': '台湾省',
    'Multiple Provinces': null, 'Unknown Province': null, 'Nationwide': null,
  };

  let dissentLayer = null;
  let dissentData = {};
  let dissentDetailCache = {};
  let dissentYear = '';  // most recent year
  try {
    // First fetch to discover available years
    const initResp = await fetch('/api/dissent/summary');
    const initData = await initResp.json();
    if (initData.years && initData.years.length) {
      dissentYear = initData.years[0]; // most recent year
    }
    // Fetch only the most recent year
    const resp = await fetch('/api/dissent/summary?year=' + dissentYear);
    const dsum = await resp.json();
    if (dsum.provinces) {
      dsum.provinces.forEach(p => {
        const geoName = provNameMap[p.province];
        if (geoName) {
          dissentData[geoName] = { count: p.count, enName: p.province, earliest: p.earliest, latest: p.latest };
        }
      });
    }
  } catch(e) { console.warn('Dissent data unavailable:', e); }

  const maxDissent = Math.max(...Object.values(dissentData).map(d => d.count), 1);
  function dissentColor(count) {
    if (!count) return 'transparent';
    const intensity = Math.min(count / maxDissent, 1);
    const r = Math.round(212 + (255-212) * intensity);
    const g = Math.round(72 * (1 - intensity * 0.5));
    const b = Math.round(59 * (1 - intensity * 0.5));
    return `rgba(${r},${g},${b},${0.15 + intensity * 0.55})`;
  }

  // Fetch province detail (issue/mode breakdown) with cache — filtered to current year
  async function getProvinceDetail(enName) {
    const key = enName + ':' + dissentYear;
    if (dissentDetailCache[key]) return dissentDetailCache[key];
    try {
      const resp = await fetch('/api/dissent/province_detail?province=' + encodeURIComponent(enName) + '&year=' + dissentYear);
      const data = await resp.json();
      dissentDetailCache[key] = data;
      return data;
    } catch(e) { return null; }
  }

  // Build rich tooltip content
  function trendArrow(trend) {
    if (trend === 'up') return '<span style="color:#d4483b;font-size:9px;" title="Share increased vs previous year">&#9650;</span>';
    if (trend === 'down') return '<span style="color:#2ecc71;font-size:9px;" title="Share decreased vs previous year">&#9660;</span>';
    if (trend === 'new') return '<span style="color:#f5a623;font-size:9px;" title="New this year">&#9733;</span>';
    return '<span style="color:#727889;font-size:7px;" title="Roughly unchanged">&#9644;</span>';
  }

  function buildDissentTooltip(enName, count, detail) {
    let html = '<div style="min-width:200px;max-width:280px;">'
      + '<strong style="font-size:13px;color:#e1e4eb;">' + esc(enName) + '</strong>'
      + '<span style="float:right;color:#d4483b;font-weight:700;">' + count + '</span>'
      + '<div style="color:#727889;font-size:9px;margin:2px 0 6px;">events in ' + dissentYear + '</div>';
    if (detail) {
      if (detail.recent_90d > 0) {
        html += '<div style="color:#f5a623;font-size:10px;margin-bottom:4px;">' + detail.recent_90d + ' events in last 90 days</div>';
      }
      if (detail.issues && detail.issues.length) {
        html += '<div style="color:#727889;font-size:9px;text-transform:uppercase;letter-spacing:0.5px;margin:4px 0 2px;">Top Issues <span style="font-weight:400;text-transform:none;">(vs ' + (parseInt(dissentYear)-1) + ')</span></div>';
        detail.issues.slice(0, 5).forEach(i => {
          const share = i.share != null ? i.share : Math.round(i.count / count * 100);
          html += '<div style="font-size:10px;margin:2px 0;display:flex;align-items:center;gap:4px;">'
            + trendArrow(i.trend || 'flat')
            + '<span style="color:#c8cad0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;flex:1;max-width:160px;">' + esc(i.issue) + '</span>'
            + '<span style="color:#727889;flex-shrink:0;font-size:9px;">' + i.count + ' (' + share + '%)</span></div>';
        });
      }
      if (detail.modes && detail.modes.length) {
        html += '<div style="color:#727889;font-size:9px;text-transform:uppercase;letter-spacing:0.5px;margin:6px 0 2px;">Modes of Dissent</div>';
        detail.modes.slice(0, 4).forEach(m => {
          html += '<div style="font-size:10px;color:#c8cad0;">' + esc(m.mode) + ' <span style="color:#727889;">(' + m.count + ')</span></div>';
        });
      }
      if (detail.earliest && detail.latest) {
        html += '<div style="color:#727889;font-size:9px;margin-top:5px;">' + detail.earliest + ' — ' + detail.latest + '</div>';
      }
    }
    html += '</div>';
    return html;
  }

  dissentLayer = L.geoJSON(provinces, {
    style: (feature) => {
      const name = feature.properties.name || '';
      const d = dissentData[name];
      const count = d ? d.count : 0;
      return { fillColor: dissentColor(count), fillOpacity: 1, color: 'transparent', weight: 0 };
    },
    onEachFeature: (feature, layer) => {
      const name = feature.properties.name || '';
      const d = dissentData[name];
      const count = d ? d.count : 0;
      const enName = d ? d.enName : name;
      if (count > 0) {
        // Initial simple tooltip — upgraded on hover with detail data
        layer.bindTooltip(`<strong>${esc(enName)}</strong><br>${count} events in ${dissentYear}<br><span style="color:#727889;font-size:9px">hover to load details...</span>`, {
          direction: 'top', className: 'city-tooltip', sticky: true,
        });
        layer.on('mouseover', async () => {
          const detail = await getProvinceDetail(enName);
          layer.setTooltipContent(buildDissentTooltip(enName, count, detail));
        });
        layer.on('click', () => {
          const panel = document.getElementById('panel-dissent-detail');
          if (panel) panel.innerHTML = '<span style="color:#727889">Loading ' + esc(enName) + '...</span>';
          fetch('/api/dissent/events?province=' + encodeURIComponent(enName) + '&limit=30')
            .then(r => r.json())
            .then(evts => {
              if (panel) panel.innerHTML = '<div style="font-weight:700;margin-bottom:6px;color:#d4483b;">' + esc(enName) + '</div>'
                + (evts.map(e => '<div style="margin-bottom:6px;border-bottom:1px solid #1e2230;padding-bottom:6px;">'
                + '<span style="color:#f5a623;font-size:10px">' + esc(e.date_start||'') + '</span> '
                + '<span style="color:#727889;font-size:10px">' + esc(e.mode||'') + '</span><br>'
                + '<span style="font-size:12px">' + esc(e.description ? e.description.substring(0,200) : e.issue||'') + '</span>'
                + '</div>').join('') || '<span style="color:#727889">No events</span>');
            });
        });
      }
    },
  });
  // Not added to map by default — toggled via control panel

  // ===== Dissent Legend (bottom-right, only visible when layer active) =====
  const dissentLegend = L.control({ position: 'bottomright' });
  dissentLegend.onAdd = function() {
    const div = L.DomUtil.create('div');
    div.id = 'dissent-legend';
    div.style.cssText = 'background:rgba(10,12,16,0.92);color:#e1e4eb;padding:10px 14px;border-radius:6px;font-size:11px;border:1px solid #262a36;display:none;';
    const steps = [0, 0.15, 0.3, 0.5, 0.75, 1.0];
    const labels = ['0', '' + Math.round(maxDissent*0.15), '' + Math.round(maxDissent*0.3), '' + Math.round(maxDissent*0.5), '' + Math.round(maxDissent*0.75), '' + maxDissent];
    let gradient = '';
    steps.forEach((s, i) => {
      const cnt = Math.round(s * maxDissent);
      const col = dissentColor(cnt || 0);
      gradient += '<div style="display:flex;align-items:center;margin:2px 0;">'
        + '<span style="display:inline-block;width:18px;height:12px;background:' + (cnt === 0 ? '#1a2035' : col) + ';border-radius:2px;margin-right:6px;border:1px solid #262a36;"></span>'
        + '<span style="color:#727889;">' + labels[i] + (i === steps.length-1 ? '+' : '') + '</span></div>';
    });
    div.innerHTML = '<div style="font-weight:700;font-size:9px;text-transform:uppercase;letter-spacing:0.5px;color:#727889;margin-bottom:4px;">Dissent Events — ' + dissentYear + '</div>'
      + '<div style="color:#727889;font-size:9px;margin-bottom:6px;">China Dissent Monitor (most recent year)</div>'
      + gradient
      + '<div style="color:#727889;font-size:9px;margin-top:6px;">Click province for details</div>';
    L.DomEvent.disableClickPropagation(div);
    return div;
  };
  dissentLegend.addTo(chinaMap);

  // ===== Layer Control Panel (left side) =====
  const layerPanel = L.control({ position: 'topleft' });
  layerPanel.onAdd = function() {
    const div = L.DomUtil.create('div');
    div.style.cssText = 'background:rgba(10,12,16,0.92);color:#e1e4eb;padding:12px 14px;border-radius:6px;font-size:12px;border:1px solid #262a36;min-width:170px;';
    div.innerHTML = '<div style="font-weight:700;font-size:10px;text-transform:uppercase;letter-spacing:0.5px;color:#727889;margin-bottom:8px;">Map Layers</div>'
      + '<label style="display:block;margin-bottom:6px;cursor:pointer;"><input type="checkbox" id="tog-amti" checked style="margin-right:6px;accent-color:#f5a623;">SCS Islands (AMTI)</label>'
      + '<label style="display:block;margin-bottom:6px;cursor:pointer;"><input type="checkbox" id="tog-dissent" style="margin-right:6px;accent-color:#d4483b;">Dissent Events (CDM)</label>'
      + '<label style="display:block;margin-bottom:6px;cursor:pointer;"><input type="checkbox" id="tog-flights" style="margin-right:6px;accent-color:#9d7cf4;">Flights (OpenSky)</label>'
      + '<label style="display:block;margin-bottom:6px;cursor:pointer;"><input type="checkbox" id="tog-ships" style="margin-right:6px;accent-color:#3eb370;">Ships (AIS)</label>'
      + '<label style="display:block;margin-bottom:6px;cursor:pointer;"><input type="checkbox" id="tog-pref" checked style="margin-right:6px;accent-color:#4a9eff;">Prefecture Boundaries</label>'
      + '<label style="display:block;margin-bottom:6px;cursor:pointer;"><input type="checkbox" id="tog-prov" checked style="margin-right:6px;accent-color:#4a5275;">Province Outlines</label>'
      + '<label style="display:block;cursor:pointer;"><input type="checkbox" id="tog-econ" style="margin-right:6px;accent-color:#e8a838;">Economic Data (Bruegel)</label>'
      + '<div id="econ-controls" style="display:none;margin-top:8px;padding-top:8px;border-top:1px solid #262a36;">'
      + '<select id="econ-indicator" style="width:100%;margin-bottom:4px;background:#1a1e2a;color:#e1e4eb;border:1px solid #262a36;border-radius:4px;padding:3px 6px;font-size:11px;"></select>'
      + '<select id="econ-year" style="width:100%;background:#1a1e2a;color:#e1e4eb;border:1px solid #262a36;border-radius:4px;padding:3px 6px;font-size:11px;"></select>'
      + '</div>'
      + '<div id="panel-dissent-detail" style="margin-top:10px;max-height:250px;overflow-y:auto;font-size:11px;"></div>';
    L.DomEvent.disableClickPropagation(div);
    L.DomEvent.disableScrollPropagation(div);
    return div;
  };
  layerPanel.addTo(chinaMap);

  // Toggle handlers
  document.getElementById('tog-amti').addEventListener('change', function() {
    this.checked ? chinaMap.addLayer(amtiGroup) : chinaMap.removeLayer(amtiGroup);
  });
  document.getElementById('tog-dissent').addEventListener('change', function() {
    const legend = document.getElementById('dissent-legend');
    if (this.checked) {
      chinaMap.addLayer(dissentLayer);
      dissentLayer.bringToBack();
      prefLayer.bringToFront();
      provLayer.bringToFront();
      if (legend) legend.style.display = 'block';
    } else {
      chinaMap.removeLayer(dissentLayer);
      if (legend) legend.style.display = 'none';
      const dp = document.getElementById('panel-dissent-detail');
      if (dp) dp.innerHTML = '';
    }
  });
  document.getElementById('tog-flights').addEventListener('change', async function() {
    if (this.checked) {
      chinaMap.addLayer(flightGroup);
      await refreshFlights();
      flightRefreshTimer = setInterval(refreshFlights, 30000);
    } else {
      chinaMap.removeLayer(flightGroup);
      if (flightRefreshTimer) { clearInterval(flightRefreshTimer); flightRefreshTimer = null; }
    }
  });
  document.getElementById('tog-ships').addEventListener('change', async function() {
    if (this.checked) {
      chinaMap.addLayer(shipGroup);
      await refreshShips();
      shipRefreshTimer = setInterval(refreshShips, 30000);
    } else {
      chinaMap.removeLayer(shipGroup);
      if (shipRefreshTimer) { clearInterval(shipRefreshTimer); shipRefreshTimer = null; }
    }
  });
  document.getElementById('tog-pref').addEventListener('change', function() {
    this.checked ? chinaMap.addLayer(prefLayer) : chinaMap.removeLayer(prefLayer);
  });
  document.getElementById('tog-prov').addEventListener('change', function() {
    this.checked ? chinaMap.addLayer(provLayer) : chinaMap.removeLayer(provLayer);
  });

  // ===== Economic Data Choropleth (Bruegel Provincial) =====
  const geoToEn={};Object.entries(provNameMap).forEach(([en,cn])=>{if(cn)geoToEn[cn]=en;});
  let econLayer=null,econData={},econMax=1,econIndicator='GDP',econYear=2024,econUnit='';

  function econColor(v){if(v==null)return'transparent';const t=Math.min(v/econMax,1);
    return'rgba('+Math.round(232*t+30*(1-t))+','+Math.round(168*t+20*(1-t))+','+Math.round(56*(1-t))+','+(0.12+t*0.6)+')';}

  function fmtEcon(v,u){if(v==null)return'\u2014';
    if(u==='100M_yuan')return(v/10000).toFixed(1)+' T \u00a5';
    if(u==='10K_people')return(v/100).toFixed(1)+'M';
    if(u==='1000_USD')return'$'+(v/1000000).toFixed(1)+'B';
    return Math.abs(v)>=1e6?(v/1e6).toFixed(1)+'M':Math.abs(v)>=1e3?(v/1e3).toFixed(1)+'K':Number(v).toFixed(1);}

  let econChange={};

  function econArrow(pct){
    if(pct==null)return'';
    if(pct>0.5)return'<span style="color:#2ecc71;font-size:10px">&#9650;</span> <span style="color:#2ecc71;font-size:11px">+'+pct.toFixed(1)+'%</span>';
    if(pct<-0.5)return'<span style="color:#d4483b;font-size:10px">&#9660;</span> <span style="color:#d4483b;font-size:11px">'+pct.toFixed(1)+'%</span>';
    return'<span style="color:#727889;font-size:8px">&#9644;</span> <span style="color:#727889;font-size:11px">'+pct.toFixed(1)+'%</span>';}

  function buildEconLayer(){
    if(econLayer)chinaMap.removeLayer(econLayer);
    econLayer=L.geoJSON(provinces,{
      style:f=>{const en=geoToEn[f.properties.name||''];return{fillColor:econColor(en?econData[en]:null),fillOpacity:1,color:'transparent',weight:0};},
      onEachFeature:(f,layer)=>{const en=geoToEn[f.properties.name||''];const val=en?econData[en]:null;
        if(en){const chg=econChange[en];
          layer.bindTooltip('<div style="min-width:160px"><strong style="font-size:13px;color:#e1e4eb">'+esc(en)+'</strong>'
          +'<div style="display:flex;align-items:baseline;gap:8px;margin:4px 0">'
          +'<span style="color:#e8a838;font-size:16px;font-weight:700">'+fmtEcon(val,econUnit)+'</span>'
          +(chg!=null?'<span>'+econArrow(chg)+'</span>':'')
          +'</div>'
          +'<div style="color:#727889;font-size:10px">'+esc(econIndicator)+' ('+econYear+')'+(chg!=null?' vs '+(econYear-1):'')+'</div></div>',
          {direction:'top',className:'city-tooltip',sticky:true});}}
    });
    if(document.getElementById('tog-econ').checked){chinaMap.addLayer(econLayer);econLayer.bringToBack();prefLayer.bringToFront();provLayer.bringToFront();}
    updateEconLegend();}

  async function loadEconData(){
    const resp=await fetch('/api/bruegel/provincial?indicator='+encodeURIComponent(econIndicator)+'&year='+econYear);
    const data=await resp.json();econData={};econChange={};
    data.forEach(d=>{econData[d.province]=d.value;if(d.change_pct!=null)econChange[d.province]=d.change_pct;});
    if(data.length)econUnit=data[0].unit||'';
    const vals=Object.values(econData).filter(v=>v!=null);
    econMax=vals.length?Math.max(...vals):1;buildEconLayer();}

  async function initEconControls(){
    const resp=await fetch('/api/bruegel/provincial/indicators');
    const indicators=await resp.json();
    const indSel=document.getElementById('econ-indicator'),yearSel=document.getElementById('econ-year');
    indSel.innerHTML=indicators.map(i=>'<option value="'+esc(i.indicator)+'"'+(i.indicator===econIndicator?' selected':'')+'>'+esc(i.indicator)+' ('+i.unit.replace(/_/g,' ')+')</option>').join('');
    if(indicators.length){const info=indicators.find(i=>i.indicator===econIndicator)||indicators[0];
      const years=[];for(let y=info.max_year;y>=info.min_year;y--)years.push(y);
      yearSel.innerHTML=years.map(y=>'<option value="'+y+'"'+(y===econYear?' selected':'')+'>'+y+'</option>').join('');}
    indSel.addEventListener('change',()=>{econIndicator=indSel.value;loadEconData();});
    yearSel.addEventListener('change',()=>{econYear=parseInt(yearSel.value);loadEconData();});}

  const econLegend=L.control({position:'bottomright'});
  econLegend.onAdd=function(){const div=L.DomUtil.create('div');div.id='econ-legend';
    div.style.cssText='background:rgba(10,12,16,0.92);color:#e1e4eb;padding:10px 14px;border-radius:6px;font-size:11px;border:1px solid #262a36;display:none;';
    L.DomEvent.disableClickPropagation(div);return div;};
  econLegend.addTo(chinaMap);

  function updateEconLegend(){const div=document.getElementById('econ-legend');if(!div)return;
    const steps=[0,0.2,0.4,0.6,0.8,1.0];let g='';
    steps.forEach(s=>{const val=s*econMax;
      g+='<div style="display:flex;align-items:center;margin:2px 0">'
        +'<span style="display:inline-block;width:18px;height:12px;background:'+(val===0?'#1a2035':econColor(val))+';border-radius:2px;margin-right:6px;border:1px solid #262a36"></span>'
        +'<span style="color:#727889">'+fmtEcon(val,econUnit)+'</span></div>';});
    div.innerHTML='<div style="font-weight:700;font-size:9px;text-transform:uppercase;letter-spacing:0.5px;color:#e8a838;margin-bottom:4px">'+esc(econIndicator)+' \u2014 '+econYear+'</div>'
      +'<div style="color:#727889;font-size:9px;margin-bottom:6px">Bruegel Provincial Profiles</div>'+g;}

  document.getElementById('tog-econ').addEventListener('change',function(){
    const controls=document.getElementById('econ-controls'),legend=document.getElementById('econ-legend');
    if(this.checked){controls.style.display='block';
      if(!econLayer){initEconControls().then(()=>loadEconData());}
      else{chinaMap.addLayer(econLayer);econLayer.bringToBack();prefLayer.bringToFront();provLayer.bringToFront();}
      if(legend)legend.style.display='block';
    }else{controls.style.display='none';if(econLayer)chinaMap.removeLayer(econLayer);if(legend)legend.style.display='none';}
  });

  console.log('Map rendered:', prefectures.features.length, 'prefectures,', provinces.features.length, 'provinces,', amtiIslands.length, 'AMTI islands');
}

async function loadOverview() {
  const ov = await api('/api/overview');

  document.getElementById('h-items').textContent = ov.news_items || 0;
  document.getElementById('h-sources').textContent = ov.total_sources || 0;
  document.getElementById('h-last').textContent = ago(ov.last_fetch);

  // Headlines
  const hl = document.getElementById('ov-headlines');
  if (ov.headlines && ov.headlines.length) {
    hl.innerHTML = ov.headlines.map(h => `
      <div class="headline-item">
        <span class="hl-source">${esc(h.source)}</span>
        <div class="hl-title"><a href="${esc(h.link)}" target="_blank">${esc(h.title)}</a></div>
      </div>`).join('');
  } else { hl.innerHTML = '<div class="empty">No news yet</div>'; }

  // Financial
  const fin = document.getElementById('ov-financial');
  if (ov.financial_snapshots && ov.financial_snapshots.length) {
    fin.innerHTML = ov.financial_snapshots.map(s => {
      const v = s.unit === 'points' ? Number(s.latest_value).toFixed(0) : Number(s.latest_value).toFixed(4);
      return `<div class="fin-row"><span class="fin-name">${esc(s.indicator.replace(/_/g,' '))}</span><span class="fin-val">${v} ${esc(s.unit)}</span></div>`;
    }).join('');
  } else { fin.innerHTML = '<div class="empty">Run: python financial.py</div>'; }

  // Status
  document.getElementById('ov-status').innerHTML = `
    <div class="fin-row"><span class="fin-name">News items</span><span class="fin-val">${ov.news_items}</span></div>
    <div class="fin-row"><span class="fin-name">Financial data points</span><span class="fin-val">${ov.financial_points}</span></div>
    <div class="fin-row"><span class="fin-name">Bruegel data points</span><span class="fin-val">${ov.bruegel_points||0}</span></div>
    <div class="fin-row"><span class="fin-name">Active news sources</span><span class="fin-val">${ov.news_sources_active}</span></div>
    <div class="fin-row"><span class="fin-name">Total registered sources</span><span class="fin-val">${ov.total_sources}</span></div>
    <div class="fin-row"><span class="fin-name">Last fetch</span><span class="fin-val">${ago(ov.last_fetch)}</span></div>
  `;
}

initMap();
loadOverview();

// ========== NEWS ==========
let debounce;
const loadNews = async () => {
  const q = document.getElementById('search-input').value;
  const src = document.getElementById('source-filter').value;
  const cat = document.getElementById('category-filter').value;
  const p = new URLSearchParams({limit:100});
  if (q) p.set('q',q); if (src) p.set('source',src); if (cat) p.set('category',cat);
  const items = await api('/api/news?'+p);
  const el = document.getElementById('news-list');
  if (!items.length) { el.innerHTML='<div class="empty">No items found</div>'; return; }
  el.innerHTML = items.map(i=>`
    <div class="news-item">
      <div class="ni-top"><span class="source">${esc(i.source)}</span><span class="time">${ago(i.fetched_at)}</span></div>
      <div class="title"><a href="${esc(i.link)}" target="_blank">${esc(i.title)}</a></div>
      ${i.summary?'<div class="summary">'+esc(i.summary)+'</div>':''}
    </div>`).join('');
};
document.getElementById('search-input').addEventListener('input',()=>{clearTimeout(debounce);debounce=setTimeout(loadNews,300);});
document.getElementById('source-filter').addEventListener('change',loadNews);
document.getElementById('category-filter').addEventListener('change',loadNews);
api('/api/news/sources').then(s=>{const sel=document.getElementById('source-filter');s.forEach(x=>{const o=document.createElement('option');o.value=x.source;o.textContent=x.source+' ('+x.count+')';sel.appendChild(o);});});

// Lazy-load news on tab click
document.querySelector('[data-panel="news"]').addEventListener('click',()=>{loadNews();},{once:true});

// ========== FINANCIAL ==========
let chart = null;
let finSource = 'all';
let finDataCache = {akshare:[], bruegel:[]};
let indDataCache = {akshare:[], bruegel:[]};

function setFinSource(src) {
  finSource = src;
  document.querySelectorAll('[data-finsrc]').forEach(p => p.classList.toggle('active', p.dataset.finsrc === src));
  renderSnaps();
  renderInds();
}

const catLabels = {
  banking:'Banking',consumption:'Consumption',eu_china:'EU-China',external:'External',
  finance:'Aggregate Finance',financial:'Financial Markets',fiscal:'Fiscal',green:'Green Transition',
  investment:'Investment',macro:'Macro',monetary:'Monetary Policy',production:'Production',
  real_estate:'Real Estate',structural:'Structural',trade:'Trade',bond:'Bonds',equity:'Equity',fx:'FX'
};

function drawSparkline(canvas, points, color) {
  if (!points || points.length < 2) return;
  const ctx = canvas.getContext('2d');
  const w = canvas.width = canvas.offsetWidth * 2;
  const h = canvas.height = canvas.offsetHeight * 2;
  ctx.scale(2, 2);
  const cw = w/2, ch = h/2;
  const vals = points.map(p => p.v);
  const mn = Math.min(...vals), mx = Math.max(...vals);
  const range = mx - mn || 1;
  const pad = 1;
  ctx.beginPath();
  vals.forEach((v, i) => {
    const x = pad + (i / (vals.length - 1)) * (cw - pad*2);
    const y = ch - pad - ((v - mn) / range) * (ch - pad*2);
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  });
  ctx.strokeStyle = color;
  ctx.lineWidth = 1.2;
  ctx.lineJoin = 'round';
  ctx.stroke();
  // fill under
  const lastX = pad + (cw - pad*2);
  ctx.lineTo(lastX, ch);
  ctx.lineTo(pad, ch);
  ctx.closePath();
  ctx.fillStyle = color + '15';
  ctx.fill();
}

function renderSnaps() {
  const el = document.getElementById('snap-cards');
  let s = [];
  if (finSource === 'all') s = [...finDataCache.akshare, ...finDataCache.bruegel];
  else s = finDataCache[finSource] || [];
  if(!s.length){el.innerHTML='<div class="empty">No data yet</div>';return;}

  // Group by category
  const groups = {};
  s.forEach(x => {
    const cat = x.category || 'other';
    (groups[cat] = groups[cat] || []).push(x);
  });

  let html = '';
  Object.keys(groups).sort().forEach(cat => {
    const items = groups[cat];
    const label = catLabels[cat] || cat.replace(/_/g,' ').replace(/\b\w/g,c=>c.toUpperCase());
    html += `<div class="snap-group">
      <div class="snap-group-title">${label} <span class="snap-group-count">${items.length}</span></div>
      <div class="snap-group-grid">`;
    items.forEach((x,idx) => {
      const v = x.unit==='points'?Number(x.latest_value).toFixed(0):Number(x.latest_value).toFixed(4);
      const cc = x.change>0?'up':x.change<0?'down':'';
      const ct = x.change!==null&&x.change!==undefined?(x.change>0?'+':'')+Number(x.change).toFixed(2):'';
      const src = x.source==='bruegel'?'B':'A';
      const srcColor = x.source==='bruegel'?'#e8a838':'#4a9eff';
      const hasSpark = x.spark && x.spark.length >= 2;
      const cardId = 'spark-'+cat+'-'+idx;
      html += `<div class="card spark-card" onclick="drawChart('${x.indicator}','${x.source||'akshare'}')">
        <div class="label">${esc(x.indicator.replace(/^BRU_/,'').replace(/_/g,' '))}<span style="font-size:8px;color:${srcColor};margin-left:4px;font-weight:700">${src}</span></div>
        <div class="spark-row"><div class="spark-vals"><div class="value">${v}</div>
        ${ct?'<div class="change '+cc+'">'+ct+'</div>':''}
        <div class="meta">${esc(x.data_date)}</div></div>
        ${hasSpark?'<canvas class="sparkline" id="'+cardId+'" data-color="'+srcColor+'"></canvas>':''}</div></div>`;
    });
    html += '</div></div>';
  });
  el.innerHTML = html;

  // Draw sparklines after DOM insertion
  s.forEach((x, gi) => {
    if (!x.spark || x.spark.length < 2) return;
    const cat = x.category || 'other';
    const idx = groups[cat].indexOf(x);
    const canvas = document.getElementById('spark-'+cat+'-'+idx);
    if (canvas) drawSparkline(canvas, x.spark, canvas.dataset.color);
  });
}

const loadSnaps = async()=>{
  const [ak, bru] = await Promise.all([
    api('/api/financial/snapshots?sparklines=1'),
    api('/api/bruegel/snapshots?sparklines=1')
  ]);
  finDataCache.akshare = (ak||[]).map(x=>({...x, source:'akshare'}));
  finDataCache.bruegel = (bru||[]).map(x=>({...x, source:'bruegel'}));
  renderSnaps();
};

window.drawChart = async(ind, src)=>{
  const endpoint = (src==='bruegel'||ind.startsWith('BRU_')) ? '/api/bruegel/series/' : '/api/financial/series/';
  const d = await api(endpoint+encodeURIComponent(ind)+'?limit=180');
  if(!d.length)return; d.reverse();
  const label = ind.replace(/^BRU_/,'').replace(/_/g,' ');
  document.getElementById('chart-title').textContent=label;
  if(chart)chart.destroy();
  const color = (src==='bruegel'||ind.startsWith('BRU_')) ? '#e8a838' : '#d4483b';
  chart=new Chart(document.getElementById('main-chart'),{type:'line',data:{labels:d.map(x=>x.date),
    datasets:[{label:ind,data:d.map(x=>x.value),borderColor:color,backgroundColor:color+'10',
    borderWidth:1.5,pointRadius:0,fill:true,tension:0.3}]},
    options:{responsive:true,plugins:{legend:{display:false}},
    scales:{x:{ticks:{color:'#727889',maxTicksLimit:10,font:{size:9}},grid:{color:'#262a36'}},
    y:{ticks:{color:'#727889',font:{size:9}},grid:{color:'#262a36'}}},interaction:{intersect:false,mode:'index'}}});
};

function renderInds() {
  const el = document.getElementById('ind-list');
  let inds = [];
  if (finSource === 'all') inds = [...indDataCache.akshare, ...indDataCache.bruegel];
  else inds = indDataCache[finSource] || [];
  if(!inds.length){el.innerHTML='<span class="empty">No data</span>';return;}
  const cats={};inds.forEach(i=>(cats[i.category]=cats[i.category]||[]).push(i));
  el.innerHTML=Object.entries(cats).sort((a,b)=>a[0].localeCompare(b[0])).map(([c,is])=>
    `<div style="margin-bottom:6px;width:100%"><span style="color:var(--text3);font-size:10px;text-transform:uppercase">${c}: </span>`+
    is.map(i=>{
      const src = i.source||'akshare';
      const label = i.indicator.replace(/^BRU_/,'').replace(/_/g,' ');
      const dotColor = src==='bruegel'?'#e8a838':'#4a9eff';
      return `<span class="pill" onclick="drawChart('${i.indicator}','${src}')" title="${i.indicator}"><span style="color:${dotColor};margin-right:2px">&#9679;</span>${label}</span>`;
    }).join('')+'</div>'
  ).join('');
}

const loadInds = async()=>{
  const [akInds, bruInds] = await Promise.all([
    api('/api/financial/indicators'),
    api('/api/bruegel/indicators')
  ]);
  indDataCache.akshare = (akInds||[]).map(x=>({...x, source:'akshare'}));
  indDataCache.bruegel = (bruInds||[]).map(x=>({...x, source:'bruegel'}));
  renderInds();
};
document.querySelector('[data-panel="financial"]').addEventListener('click',()=>{loadSnaps();loadInds();},{once:true});

// ========== ALL SOURCES ==========
let allSourcesData = null;
let activeFilter = '';

function renderSources(filter) {
  if (!allSourcesData) return;
  const sources = filter ? allSourcesData.sources.filter(s => (s.feed_type||'') === filter || (s.category||'') === filter || (s._section||'') === filter) : allSourcesData.sources;
  const badgeMap = {central_government:'gov',ministry:'gov',party:'gov',state_media:'media',media:'media',
    regulator:'reg',judiciary:'legal',legal_database:'legal',financial:'fin',wechat:'wechat',service:'svc'};
  const body = document.getElementById('src-body');
  body.innerHTML = sources.map(s => {
    const bc = badgeMap[s.category]||'gov';
    const synced = !!s.last_sync;
    const stale = synced && (Date.now()-new Date(s.last_sync+'Z'))>86400000*2;
    const dotClass = synced?(stale?'sync-stale':'sync-ok'):'sync-never';
    const syncText = synced?ago(s.last_sync):'<span style="color:var(--text3)">never</span>';
    const ft = s.feed_type||'—';
    const ftBadge = ft==='direct_rss'?'<span class="badge b-media">Direct RSS</span>':
      ft==='rsshub'?'<span class="badge b-reg">RSSHub</span>':
      ft==='akshare'?'<span class="badge b-fin">AKShare</span>':
      ft==='wechat'?'<span class="badge b-wechat">WeChat</span>':
      ft==='manual'?'<span class="badge b-svc">Manual</span>':'<span class="badge b-svc">'+ft+'</span>';
    const url = s.url||'';
    const urlDisplay = url.length>50?url.slice(0,50)+'...':url;
    return `<tr>
      <td><span class="sync-dot ${dotClass}"></span></td>
      <td><strong>${esc(s.name||'')}</strong>${s.notes?'<br><span style="font-size:10px;color:var(--text3)">'+esc(s.notes)+'</span>':''}</td>
      <td>${esc(s.name_cn||'')}</td>
      <td><span class="badge b-${bc}">${esc(s.category||'other')}</span></td>
      <td>${ftBadge}</td>
      <td>${syncText}</td>
      <td style="font-variant-numeric:tabular-nums">${s.sync_items||'—'}</td>
      <td><a href="${esc(url)}" target="_blank" style="font-size:11px">${esc(urlDisplay)}</a></td>
    </tr>`;
  }).join('');
}

async function loadAllSources() {
  allSourcesData = await api('/api/sources/all');
  const d = allSourcesData;

  // Summary cards
  document.getElementById('src-summary').innerHTML = `
    <div class="card"><div class="label">Total Sources</div><div class="value">${d.total}</div></div>
    <div class="card"><div class="label" style="color:var(--green)">Synced</div><div class="value" style="color:var(--green)">${d.synced}</div></div>
    <div class="card"><div class="label" style="color:var(--text3)">Not Yet Synced</div><div class="value">${d.not_synced}</div></div>
  `;

  // Filter pills
  const types = {};
  d.sources.forEach(s => { const t=s.feed_type||'other'; types[t]=(types[t]||0)+1; });
  const cats = {};
  d.sources.forEach(s => { const c=s.category||'other'; cats[c]=(cats[c]||0)+1; });
  let pills = `<span class="pill ${activeFilter?'':'active'}" onclick="filterSrc('')">All (${d.total})</span>`;
  for (const [t,n] of Object.entries(types).sort((a,b)=>b[1]-a[1])) {
    pills += `<span class="pill ${activeFilter===t?'active':''}" onclick="filterSrc('${t}')">${t.replace(/_/g,' ')} (${n})</span>`;
  }
  document.getElementById('src-filters').innerHTML = pills;

  renderSources(activeFilter);
}

window.filterSrc = (f) => {
  activeFilter = f;
  document.querySelectorAll('#src-filters .pill').forEach(p => p.classList.remove('active'));
  // Re-render
  loadAllSources();
};

document.querySelector('[data-panel="sources"]').addEventListener('click',()=>{loadAllSources();},{once:true});

// ========== HISTORICAL CONTEXT — sub-tab switching ==========
let histDissentLoaded = false;
document.querySelectorAll('.hist-sub').forEach(tab => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.hist-sub').forEach(t => {
      t.style.color = '#727889'; t.style.borderBottomColor = 'transparent';
    });
    tab.style.color = '#e1e4eb'; tab.style.borderBottomColor = '#d4483b';
    document.getElementById('hist-panel-macro').style.display = tab.dataset.sub === 'macro' ? '' : 'none';
    document.getElementById('hist-panel-dissent').style.display = tab.dataset.sub === 'dissent' ? '' : 'none';
    if (tab.dataset.sub === 'dissent' && !histDissentLoaded) { histDissentLoaded = true; loadHistory(); }
  });
});

// ========== MACRO ECONOMY (GMD) ==========
let macroChart = null;
let macroVarData = null;
let macroActiveCat = null;
let macroCurrentVar = null;

async function loadMacro() {
  const resp = await api('/api/macro/variables');
  if (!resp || resp.error || !resp.variables.length) {
    document.getElementById('macro-var-list').innerHTML = '<div style="color:#727889;font-size:11px;">No macro data yet. Run: python macro.py</div>';
    return;
  }
  macroVarData = resp;

  if (resp.version) {
    document.getElementById('gmd-version').textContent = 'v' + resp.version;
  }

  const catsDiv = document.getElementById('macro-cats');
  const cats = resp.categories || {};
  const catKeys = [...new Set(resp.variables.map(v => v.category))];
  catsDiv.innerHTML = catKeys.map(k =>
    '<div class="macro-cat-pill" data-cat="' + k + '" style="padding:4px 12px;border-radius:4px;cursor:pointer;font-size:11px;background:#1e2230;color:#727889;">'
    + esc(cats[k] || k) + '</div>'
  ).join('');

  catsDiv.querySelectorAll('.macro-cat-pill').forEach(p => {
    p.addEventListener('click', () => {
      macroActiveCat = macroActiveCat === p.dataset.cat ? null : p.dataset.cat;
      catsDiv.querySelectorAll('.macro-cat-pill').forEach(x =>
        x.style.cssText = 'padding:4px 12px;border-radius:4px;cursor:pointer;font-size:11px;'
          + (x.dataset.cat === macroActiveCat ? 'background:#d4483b;color:#fff;' : 'background:#1e2230;color:#727889;')
      );
      renderMacroVarList();
    });
  });

  renderMacroVarList();
  const first = resp.variables.find(v => v.variable === 'rGDP') || resp.variables[0];
  if (first) loadMacroSeries(first.variable);
}

function renderMacroVarList() {
  const listDiv = document.getElementById('macro-var-list');
  const vars = macroVarData.variables.filter(v => !macroActiveCat || v.category === macroActiveCat);
  listDiv.innerHTML = vars.map(v =>
    '<div class="macro-var-item" data-var="' + v.variable + '" style="padding:6px 8px;border-radius:4px;cursor:pointer;margin-bottom:2px;font-size:11px;color:#c8cad0;transition:background 0.15s;" onmouseover="this.style.background=\'#1e2230\'" onmouseout="if(!this.classList.contains(\'active\'))this.style.background=\'transparent\'">'
    + '<div style="font-weight:600;">' + esc(v.name) + '</div>'
    + '<div style="color:#727889;font-size:9px;">' + v.min_year + '–' + v.max_year + ' · ' + esc(v.unit) + '</div>'
    + '</div>'
  ).join('');
  listDiv.querySelectorAll('.macro-var-item').forEach(el => {
    el.addEventListener('click', () => loadMacroSeries(el.dataset.var));
  });
}

async function loadMacroSeries(variable) {
  macroCurrentVar = variable;
  const startYr = document.getElementById('macro-year-start').value || 1950;
  const endYr = document.getElementById('macro-year-end').value || 2030;
  const resp = await api('/api/macro/series/' + variable + '?start=' + startYr + '&end=' + endYr);
  if (!resp || !resp.data || !resp.data.length) return;

  const meta = resp.meta || {};
  const currentYear = new Date().getFullYear();
  const labels = resp.data.map(d => d.year);
  const values = resp.data.map(d => d.value);
  const projIdx = labels.findIndex(y => y > currentYear);

  document.getElementById('macro-chart-title').textContent = (meta.name || variable) + ' (' + (meta.unit || '') + ')';

  if (macroChart) macroChart.destroy();
  macroChart = new Chart(document.getElementById('macro-chart'), {
    type: 'line',
    data: {
      labels: labels,
      datasets: [{
        label: meta.name || variable,
        data: values,
        borderColor: '#d4483b',
        backgroundColor: 'rgba(212,72,59,0.08)',
        pointRadius: labels.length > 60 ? 0 : 2,
        pointHoverRadius: 4,
        borderWidth: 1.5,
        fill: true,
        tension: 0.15,
        segment: {
          borderColor: ctx => (projIdx > 0 && ctx.p1DataIndex >= projIdx) ? 'rgba(212,72,59,0.4)' : '#d4483b',
          borderDash: ctx => (projIdx > 0 && ctx.p1DataIndex >= projIdx) ? [4, 3] : undefined,
        },
      }]
    },
    options: {
      responsive: true,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: ctx => {
              const yr = labels[ctx.dataIndex];
              const suffix = yr > currentYear ? ' (projection)' : '';
              return (meta.name || variable) + ': ' + ctx.parsed.y.toLocaleString(undefined, {maximumFractionDigits: 2}) + suffix;
            }
          }
        }
      },
      scales: {
        y: { ticks: { color: '#727889' }, grid: { color: '#1e2230' } },
        x: { ticks: { color: '#727889', maxTicksLimit: 15 }, grid: { display: false } },
      },
    },
  });

  const info = (meta.desc || '') + (projIdx > 0 ? ' | Dashed = IMF/WB projections (' + labels[projIdx] + '–' + labels[labels.length-1] + ')' : '');
  document.getElementById('macro-chart-info').textContent = info;

  document.querySelectorAll('.macro-var-item').forEach(el => {
    const isActive = el.dataset.var === variable;
    el.style.background = isActive ? '#1e2230' : 'transparent';
    el.style.borderLeft = isActive ? '2px solid #d4483b' : 'none';
  });
}

// Year range apply button
document.getElementById('macro-year-apply').addEventListener('click', () => {
  if (macroCurrentVar) loadMacroSeries(macroCurrentVar);
});
document.getElementById('macro-year-start').addEventListener('keydown', e => {
  if (e.key === 'Enter' && macroCurrentVar) loadMacroSeries(macroCurrentVar);
});
document.getElementById('macro-year-end').addEventListener('keydown', e => {
  if (e.key === 'Enter' && macroCurrentVar) loadMacroSeries(macroCurrentVar);
});

// ========== HISTORICAL CONTEXT (Dissent) ==========
async function loadHistory() {
  const data = await api('/api/dissent/historical');
  if (!data || data.error) return;

  // --- Yearly bar chart ---
  const yCtx = document.getElementById('hist-yearly-chart');
  if (yCtx && data.yearly) {
    new Chart(yCtx, {
      type: 'bar',
      data: {
        labels: data.yearly.map(y => y.year),
        datasets: [{
          label: 'Events',
          data: data.yearly.map(y => y.count),
          backgroundColor: data.yearly.map((y, i) =>
            i === data.yearly.length - 1 ? 'rgba(212,72,59,0.8)' : 'rgba(212,72,59,0.4)'
          ),
          borderColor: '#d4483b',
          borderWidth: 1,
          borderRadius: 4,
        }]
      },
      options: {
        responsive: true,
        plugins: { legend: { display: false } },
        scales: {
          y: { ticks: { color: '#727889' }, grid: { color: '#1e2230' } },
          x: { ticks: { color: '#727889' }, grid: { display: false } },
        },
      },
    });
  }

  // --- Top issues horizontal bar ---
  const iCtx = document.getElementById('hist-issues-chart');
  if (iCtx && data.top_issues) {
    const top10 = data.top_issues.slice(0, 10);
    new Chart(iCtx, {
      type: 'bar',
      data: {
        labels: top10.map(i => i.issue.length > 28 ? i.issue.substring(0,25) + '...' : i.issue),
        datasets: [{
          data: top10.map(i => i.count),
          backgroundColor: 'rgba(245,166,35,0.6)',
          borderColor: '#f5a623',
          borderWidth: 1,
          borderRadius: 3,
        }]
      },
      options: {
        indexAxis: 'y',
        responsive: true,
        plugins: { legend: { display: false } },
        scales: {
          x: { ticks: { color: '#727889' }, grid: { color: '#1e2230' } },
          y: { ticks: { color: '#c8cad0', font: { size: 10 } }, grid: { display: false } },
        },
      },
    });
  }

  // --- Modes of dissent ---
  const modesDiv = document.getElementById('hist-modes');
  if (modesDiv && data.top_modes) {
    const maxMode = data.top_modes[0]?.count || 1;
    modesDiv.innerHTML = data.top_modes.map(m => {
      const pct = Math.round(m.count / maxMode * 100);
      return '<div style="margin-bottom:8px;">'
        + '<div style="display:flex;justify-content:space-between;font-size:11px;margin-bottom:2px;">'
        + '<span style="color:#c8cad0;">' + esc(m.mode) + '</span>'
        + '<span style="color:#727889;">' + m.count + '</span></div>'
        + '<div style="background:#1e2230;border-radius:3px;height:6px;overflow:hidden;">'
        + '<div style="background:#4a9eff;width:' + pct + '%;height:100%;border-radius:3px;"></div>'
        + '</div></div>';
    }).join('');
  }

  // --- Year-over-year comparison ---
  const yoyDiv = document.getElementById('hist-yoy');
  if (yoyDiv && data.yearly && data.yearly.length >= 2) {
    let html = '<table style="width:100%;font-size:11px;border-collapse:collapse;">'
      + '<tr style="color:#727889;border-bottom:1px solid #1e2230;"><th style="text-align:left;padding:4px;">Year</th><th style="text-align:right;padding:4px;">Events</th><th style="text-align:right;padding:4px;">Change</th></tr>';
    data.yearly.forEach((y, i) => {
      let change = '';
      if (i > 0) {
        const prev = data.yearly[i-1].count;
        const diff = y.count - prev;
        const pctC = prev > 0 ? Math.round(diff / prev * 100) : 0;
        const color = diff > 0 ? '#d4483b' : diff < 0 ? '#2ecc71' : '#727889';
        change = '<span style="color:' + color + ';">' + (diff > 0 ? '+' : '') + diff + ' (' + (diff > 0 ? '+' : '') + pctC + '%)</span>';
      }
      const isLatest = i === data.yearly.length - 1;
      html += '<tr style="border-bottom:1px solid #1e2230;' + (isLatest ? 'color:#f5a623;' : 'color:#c8cad0;') + '">'
        + '<td style="padding:4px;">' + y.year + (isLatest ? ' (ongoing)' : '') + '</td>'
        + '<td style="text-align:right;padding:4px;">' + y.count + '</td>'
        + '<td style="text-align:right;padding:4px;">' + change + '</td></tr>';
    });
    html += '</table>';
    if (data.yearly.length >= 2) {
      const latest = data.yearly[data.yearly.length - 1];
      const prev = data.yearly[data.yearly.length - 2];
      // Extrapolate current year if partial
      const now = new Date();
      const dayOfYear = Math.floor((now - new Date(now.getFullYear(),0,0)) / 86400000);
      const projected = Math.round(latest.count / dayOfYear * 365);
      if (dayOfYear < 350) {
        html += '<div style="color:#727889;font-size:10px;margin-top:8px;">Projected ' + latest.year + ' total (at current pace): ~<strong style="color:#f5a623;">' + projected + '</strong> events</div>';
      }
    }
    yoyDiv.innerHTML = html;
  }

  // --- Province rankings by year ---
  const provTabs = document.getElementById('hist-prov-tabs');
  const provTable = document.getElementById('hist-prov-table');
  if (provTabs && provTable && data.provinces_by_year) {
    const years = Object.keys(data.provinces_by_year).sort();
    function showProvYear(yr) {
      const provs = data.provinces_by_year[yr] || [];
      const top15 = provs.slice(0, 15);
      const maxP = top15[0]?.count || 1;
      provTable.innerHTML = '<table style="width:100%;font-size:11px;border-collapse:collapse;">'
        + '<tr style="color:#727889;border-bottom:1px solid #1e2230;"><th style="text-align:left;padding:4px;">#</th><th style="text-align:left;padding:4px;">Province</th><th style="text-align:right;padding:4px;">Events</th><th style="padding:4px;width:40%;"></th></tr>'
        + top15.map((p, i) => {
          const pct = Math.round(p.count / maxP * 100);
          return '<tr style="border-bottom:1px solid #1e2230;color:#c8cad0;"><td style="padding:4px;color:#727889;">' + (i+1) + '</td>'
            + '<td style="padding:4px;">' + esc(p.province) + '</td>'
            + '<td style="text-align:right;padding:4px;">' + p.count + '</td>'
            + '<td style="padding:4px;"><div style="background:#1e2230;border-radius:3px;height:5px;overflow:hidden;">'
            + '<div style="background:#d4483b;width:' + pct + '%;height:100%;border-radius:3px;"></div></div></td></tr>';
        }).join('')
        + '</table>';
      provTabs.querySelectorAll('.yr-tab').forEach(t => t.style.cssText = t.dataset.yr === yr
        ? 'padding:4px 10px;border-radius:4px;cursor:pointer;font-size:11px;background:#d4483b;color:#fff;'
        : 'padding:4px 10px;border-radius:4px;cursor:pointer;font-size:11px;background:#1e2230;color:#727889;');
    }
    provTabs.innerHTML = years.map(yr =>
      '<div class="yr-tab" data-yr="' + yr + '" style="padding:4px 10px;border-radius:4px;cursor:pointer;font-size:11px;background:#1e2230;color:#727889;">' + yr + '</div>'
    ).join('');
    provTabs.querySelectorAll('.yr-tab').forEach(t => t.addEventListener('click', () => showProvYear(t.dataset.yr)));
    showProvYear(years[years.length - 1]);
  }

  // --- Issues by year ---
  const issuesYearlyDiv = document.getElementById('hist-issues-yearly');
  if (issuesYearlyDiv && data.issues_by_year) {
    const years = Object.keys(data.issues_by_year).sort();
    let html = '<div style="display:grid;grid-template-columns:repeat(' + years.length + ',1fr);gap:12px;">';
    years.forEach(yr => {
      const issues = data.issues_by_year[yr] || [];
      html += '<div><div style="color:#f5a623;font-size:12px;font-weight:700;margin-bottom:6px;">' + yr + '</div>';
      issues.forEach((iss, i) => {
        html += '<div style="font-size:10px;color:' + (i === 0 ? '#e1e4eb' : '#727889') + ';margin-bottom:3px;">'
          + '<span style="color:#d4483b;font-weight:700;margin-right:4px;">' + iss.count + '</span>'
          + esc(iss.issue.length > 30 ? iss.issue.substring(0,27) + '...' : iss.issue) + '</div>';
      });
      html += '</div>';
    });
    html += '</div>';
    issuesYearlyDiv.innerHTML = html;
  }
}

document.querySelector('[data-panel="history"]').addEventListener('click',()=>{loadMacro();},{once:true});

// ========== ACADEMIC ==========
let acadDebounce;
let acadSort = 'recent'; // 'recent' or 'ranked'

function setAcadSort(mode) {
  acadSort = mode;
  document.getElementById('acad-sort-recent').classList.toggle('active', mode === 'recent');
  document.getElementById('acad-sort-ranked').classList.toggle('active', mode === 'ranked');
  loadAcademic();
}

function acadVote(articleId, direction, el) {
  // direction: 1 (up) or -1 (down)
  const row = el.closest('.acad-row');
  const upBtn = row.querySelector('.vbtn.up');
  const downBtn = row.querySelector('.vbtn.down');
  const scoreEl = row.querySelector('.vscore');
  const currentVote = parseInt(row.dataset.vote || '0');

  // Toggle: clicking same direction again removes the vote
  const newVote = currentVote === direction ? 0 : direction;

  fetch('/api/academic/vote', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({article_id: articleId, vote: newVote})
  })
  .then(r => r.json())
  .then(data => {
    row.dataset.vote = data.vote;
    upBtn.classList.toggle('active', data.vote === 1);
    downBtn.classList.toggle('active', data.vote === -1);
    scoreEl.textContent = data.vote > 0 ? '+1' : data.vote < 0 ? '-1' : '·';
    scoreEl.style.color = data.vote > 0 ? 'var(--accent)' : data.vote < 0 ? 'var(--red, #d4483b)' : 'var(--text3)';
  });
}

function loadAcademic() {
  const q = document.getElementById('acad-search').value;
  const journal = document.getElementById('acad-journal-filter').value;
  const el = document.getElementById('acad-list');
  el.innerHTML = '<div class="loading">Loading...</div>';
  const ranked = acadSort === 'ranked' ? '&ranked=1' : '';
  api('/api/academic/articles?limit=100&journal=' + encodeURIComponent(journal) + '&q=' + encodeURIComponent(q) + ranked)
    .then(items => {
      if (!items.length) { el.innerHTML = '<div class="loading">No articles found.</div>'; return; }
      el.innerHTML = items.map(a => {
        const v = a.vote || 0;
        const scoreLabel = v > 0 ? '+1' : v < 0 ? '-1' : '\u00b7';
        const scoreColor = v > 0 ? 'var(--accent)' : v < 0 ? 'var(--red, #d4483b)' : 'var(--text3)';
        return `
        <div class="acad-row" data-vote="${v}">
          <div class="vote-col">
            <span class="vbtn up ${v===1?'active':''}" onclick="acadVote(${a.id},1,this)">\u25b2</span>
            <span class="vscore" style="color:${scoreColor}">${scoreLabel}</span>
            <span class="vbtn down ${v===-1?'active':''}" onclick="acadVote(${a.id},-1,this)">\u25bc</span>
          </div>
          <div class="news-item">
            <div class="ni-top">
              <span class="source">${esc(a.journal)}</span>
              <span class="time">${a.published ? esc(a.published) : ago(a.fetched_at)}${a.score != null ? ' &middot; score ' + a.score : ''}</span>
            </div>
            <div class="title"><a href="${esc(a.link)}" target="_blank">${esc(a.title)}</a></div>
            ${a.authors ? '<div class="summary" style="font-style:italic;">' + esc(a.authors) + '</div>' : ''}
            ${a.abstract ? '<div class="summary">' + esc(a.abstract.length > 300 ? a.abstract.substring(0,297) + '...' : a.abstract) + '</div>' : ''}
            ${a.china_match && a.china_match !== 'all' ? '<div style="font-size:9px;color:var(--accent);margin-top:2px;">Match: ' + esc(a.china_match) + '</div>' : ''}
          </div>
        </div>`;
      }).join('');
    });
}

// Populate journal filter dropdown
api('/api/academic/journals').then(journals => {
  const sel = document.getElementById('acad-journal-filter');
  journals.forEach(j => {
    const o = document.createElement('option');
    o.value = j; o.textContent = j;
    sel.appendChild(o);
  });
});

// Load summary stats
api('/api/academic/summary').then(s => {
  const el = document.getElementById('acad-summary');
  if (s.total) {
    el.innerHTML = 'Total: <b>' + s.total + '</b> articles across <b>' + s.journals.length + '</b> journals';
  }
});

document.getElementById('acad-search').addEventListener('input', () => { clearTimeout(acadDebounce); acadDebounce = setTimeout(loadAcademic, 300); });
document.getElementById('acad-journal-filter').addEventListener('change', loadAcademic);
document.querySelector('[data-panel="academic"]').addEventListener('click', () => { loadAcademic(); }, {once: true});

// ========== POLICY ADVISOR ==========
function mdToHtml(md) {
  // Minimal markdown renderer: headers, bold, bullets, line breaks
  return md
    .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
    .replace(/^## (.+)$/gm, '<h3 style="font-size:13px;font-weight:700;color:var(--text);margin:18px 0 6px;padding-bottom:4px;border-bottom:1px solid var(--border);">$1</h3>')
    .replace(/\*\*(.+?)\*\*/g, '<b>$1</b>')
    .replace(/`(.+?)`/g, '<code style="background:rgba(255,255,255,0.08);padding:1px 5px;border-radius:3px;font-size:12px;">$1</code>')
    .replace(/^\- (.+)$/gm, '<div style="padding:2px 0 2px 14px;position:relative;"><span style="position:absolute;left:0;color:var(--accent);">•</span>$1</div>')
    .replace(/^\d+\. (.+)$/gm, '<div style="padding:2px 0;">$1</div>')
    .replace(/\[([^\]]+)\]\(([^)]+)\)/g, '<a href="$2" target="_blank">$1</a>')
    .replace(/\n\n/g, '<div style="height:8px;"></div>')
    .replace(/\n/g, '<br>');
}

async function generateBrief() {
  const topic = document.getElementById('adv-topic').value.trim();
  if (!topic) { alert('Please enter a topic.'); return; }
  const days = parseInt(document.getElementById('adv-days').value);

  document.getElementById('adv-submit').disabled = true;
  document.getElementById('adv-spinner').style.display = 'inline';
  document.getElementById('adv-output').style.display = 'none';
  document.getElementById('adv-error').style.display = 'none';

  try {
    const resp = await fetch('/api/advisor/brief', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({topic, days}),
    });
    const data = await resp.json();

    if (data.error && !data.content) {
      document.getElementById('adv-error').textContent = 'Error: ' + data.error;
      document.getElementById('adv-error').style.display = 'block';
      return;
    }

    // Stub banner
    document.getElementById('adv-stub-banner').style.display = data.stub ? 'block' : 'none';

    // Header
    document.getElementById('adv-brief-topic').textContent = data.topic;
    document.getElementById('adv-brief-date').textContent = 'Generated: ' + (data.generated_at || '');
    document.getElementById('adv-brief-sources').textContent = data.source_count + ' source' + (data.source_count !== 1 ? 's' : '') + ' reviewed · last ' + data.days + ' days';

    // Body
    document.getElementById('adv-brief-body').innerHTML = mdToHtml(data.content || '');

    // Source list
    const sourcesBody = document.getElementById('adv-sources-body');
    sourcesBody.innerHTML = '';
    if (data.sources && data.sources.length > 0) {
      data.sources.forEach(s => {
        const div = document.createElement('div');
        div.className = 'news-item';
        div.style.cssText = 'padding:8px 12px;';
        const titleHtml = s.link
          ? `<a href="${esc(s.link)}" target="_blank">${esc(s.title)}</a>`
          : esc(s.title);
        div.innerHTML = `<div class="ni-top"><span class="source">${esc(s.source)}</span><span class="time">${esc(s.published)}</span></div><div class="title">${titleHtml}</div>`;
        sourcesBody.appendChild(div);
      });
      document.getElementById('adv-source-list').style.display = 'block';
    } else {
      document.getElementById('adv-source-list').style.display = 'none';
    }

    document.getElementById('adv-output').style.display = 'block';
  } catch(e) {
    document.getElementById('adv-error').textContent = 'Request failed: ' + e.message;
    document.getElementById('adv-error').style.display = 'block';
  } finally {
    document.getElementById('adv-submit').disabled = false;
    document.getElementById('adv-spinner').style.display = 'none';
  }
}

// Submit on Ctrl+Enter in the textarea
document.getElementById('adv-topic').addEventListener('keydown', e => {
  if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) generateBrief();
});

// ========== POLITICAL STRUCTURE ==========
let polityData = null;
let calFilter = 'all';
let calShowPast = true;
let calShowUpcoming = true;

const NODE_COLORS = {
  root: 'var(--text2)', party: 'var(--accent)', psc: 'var(--accent)',
  politburo: '#e8503b', committee: 'var(--accent2)', congress: 'var(--accent2)',
  military_cmd: 'var(--purple)', discipline: 'var(--purple)', secretariat: 'var(--blue)',
  state_gov: 'var(--blue)', legislature: 'var(--blue)', cabinet: 'var(--blue)',
  advisory: 'var(--text2)', judiciary: 'var(--text2)',
  military: 'var(--purple)', service: 'var(--purple)',
  official: 'var(--green)', officials: 'var(--green)', ministries: 'var(--green)',
};

const EVENT_COLORS = {
  party_congress: 'var(--accent)', plenum: 'var(--accent2)',
  npc: 'var(--blue)', cppcc: 'var(--text2)', cewc: 'var(--green)',
  politburo_regular: '#e8503b', psc_regular: 'var(--accent)',
};

const EVENT_LABELS = {
  party_congress: 'Party Congress', plenum: 'Plenum', npc: 'NPC',
  cppcc: 'CPPCC', cewc: 'CEWC', politburo_regular: 'Politburo', psc_regular: 'PSC',
};

async function loadPolity() {
  if (polityData) return;
  polityData = await api('/api/polity');
  renderTree();
  renderPSC();
  renderCalendar();
  renderProcess();
  refreshMeetingNews();
}

// --- Hierarchy tree ---
function renderTree() {
  const el = document.getElementById('polity-tree');
  el.innerHTML = renderNode(polityData.structure, 0);
  el.querySelectorAll('.pt-header').forEach(h => {
    h.addEventListener('click', () => {
      const children = h.nextElementSibling;
      if (children) {
        const open = children.style.display !== 'none';
        children.style.display = open ? 'none' : '';
        h.querySelector('.pt-arrow').textContent = open ? '▶' : '▼';
      }
    });
  });
}

function renderNode(node, depth) {
  const color = NODE_COLORS[node.type] || 'var(--text2)';
  const hasChildren = node.children && node.children.length > 0;
  const indent = depth * 14;
  const arrow = hasChildren ? '<span class="pt-arrow" style="font-size:8px;margin-right:5px;color:var(--text3);">▼</span>' : '<span style="display:inline-block;width:13px;"></span>';
  let html = `<div style="margin-bottom:3px;">
    <div class="pt-header" style="display:flex;align-items:flex-start;padding:5px 6px;padding-left:${indent + 6}px;border-radius:5px;cursor:${hasChildren ? 'pointer' : 'default'};transition:background 0.1s;" onmouseover="this.style.background='var(--surface2)'" onmouseout="this.style.background=''">
      ${arrow}
      <div style="min-width:0;">
        <span style="font-weight:600;color:${color};">${esc(node.name)}</span>
        ${node.name_cn ? `<span style="font-size:10px;color:var(--text3);margin-left:6px;">${esc(node.name_cn)}</span>` : ''}
        ${node.meeting_freq ? `<span style="font-size:9px;background:var(--surface2);border:1px solid var(--border);border-radius:3px;padding:1px 5px;margin-left:6px;color:var(--text3);">${esc(node.meeting_freq)}</span>` : ''}
        <div style="font-size:11px;color:var(--text2);margin-top:2px;line-height:1.4;">${esc(node.desc || '')}</div>
        ${node.members_note ? `<div style="font-size:10px;color:var(--text3);margin-top:1px;">${esc(node.members_note)}</div>` : ''}
      </div>
    </div>`;
  if (hasChildren) {
    html += `<div class="pt-children">` + node.children.map(c => renderNode(c, depth + 1)).join('') + `</div>`;
  }
  html += `</div>`;
  return html;
}

// --- PSC members ---
function renderPSC() {
  const el = document.getElementById('polity-psc');
  const psc = findNode(polityData.structure, 'psc');
  if (!psc || !psc.members) return;
  el.innerHTML = psc.members.map(m => `
    <div style="display:flex;align-items:flex-start;padding:7px 16px;border-bottom:1px solid var(--border);">
      <div style="width:22px;height:22px;border-radius:50%;background:var(--surface2);border:2px solid var(--accent);display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:700;color:var(--accent);flex-shrink:0;margin-right:10px;margin-top:1px;">${m.rank}</div>
      <div>
        <div style="font-weight:600;font-size:12px;">${esc(m.name)} <span style="font-size:10px;color:var(--text3);font-weight:400;">${esc(m.name_cn)}</span></div>
        <div style="font-size:10px;color:var(--text2);margin-top:1px;">${m.roles.map(r => esc(r)).join(' · ')}</div>
      </div>
    </div>`).join('');
}

function findNode(node, id) {
  if (node.id === id) return node;
  for (const c of (node.children || [])) {
    const r = findNode(c, id);
    if (r) return r;
  }
  return null;
}

// --- Calendar ---
function setCalFilter(f, el) {
  calFilter = f;
  document.querySelectorAll('#polity-cal-filters .pill').forEach(p => p.classList.remove('active'));
  el.classList.add('active');
  renderCalendar();
}

function toggleCalPast() {
  calShowPast = !calShowPast;
  document.getElementById('cal-show-past').classList.toggle('active', calShowPast);
  renderCalendar();
}

function toggleCalUpcoming() {
  calShowUpcoming = !calShowUpcoming;
  document.getElementById('cal-show-upcoming').classList.toggle('active', calShowUpcoming);
  renderCalendar();
}

function renderCalendar() {
  const el = document.getElementById('polity-calendar');
  let events = polityData.calendar.filter(e => {
    if (calFilter !== 'all' && e.type !== calFilter) return false;
    if (e.status === 'past' && !calShowPast) return false;
    if (e.status === 'upcoming' && !calShowUpcoming) return false;
    return true;
  });

  if (!events.length) { el.innerHTML = '<div style="padding:16px;color:var(--text3);font-size:12px;">No events match filters.</div>'; return; }

  const today = new Date().toISOString().slice(0,10);
  el.innerHTML = events.map(ev => {
    const color = EVENT_COLORS[ev.type] || 'var(--text2)';
    const label = EVENT_LABELS[ev.type] || ev.type;
    const isPast = ev.status === 'past';
    const isApprox = ev.approximate;
    const daysFrom = ev.start ? Math.round((new Date(ev.start) - new Date(today)) / 86400000) : null;
    let daysStr = '';
    if (daysFrom !== null) {
      if (daysFrom === 0) daysStr = 'Today';
      else if (daysFrom > 0) daysStr = `in ${daysFrom}d`;
      else daysStr = `${-daysFrom}d ago`;
    }
    return `<div style="display:flex;align-items:flex-start;padding:10px 16px;border-bottom:1px solid var(--border);opacity:${isPast ? '0.7' : '1'};">
      <div style="width:3px;min-height:36px;border-radius:2px;background:${color};margin-right:12px;flex-shrink:0;margin-top:2px;"></div>
      <div style="flex:1;min-width:0;">
        <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
          <span style="font-size:9px;font-weight:700;color:${color};text-transform:uppercase;letter-spacing:0.5px;">${label}</span>
          ${isApprox ? '<span style="font-size:9px;color:var(--text3);background:var(--surface2);border:1px solid var(--border);border-radius:3px;padding:1px 4px;">~approx</span>' : ''}
          ${!isPast ? `<span style="font-size:10px;font-weight:600;color:${color};margin-left:auto;">${daysStr}</span>` : ''}
        </div>
        <div style="font-weight:600;font-size:12px;margin-top:2px;">${esc(ev.name)}</div>
        <div style="font-size:10px;color:var(--text3);margin-top:1px;">${esc(ev.name_cn || '')}</div>
        <div style="font-size:11px;color:var(--text2);margin-top:3px;">${esc(ev.significance || '')}</div>
      </div>
      <div style="text-align:right;flex-shrink:0;margin-left:12px;font-size:10px;color:var(--text3);">
        ${ev.start ? `<div>${esc(ev.start)}</div>` : ''}
        ${ev.end && ev.end !== ev.start ? `<div>${esc(ev.end)}</div>` : ''}
        ${isPast ? '<div style="color:var(--text3);margin-top:2px;">past</div>' : ''}
      </div>
    </div>`;
  }).join('');
}

// --- Decision process ---
function renderProcess() {
  const el = document.getElementById('polity-process');
  const proc = polityData.decision_process;
  const typeColors = {
    psc: 'var(--accent)', politburo: '#e8503b', committee: 'var(--accent2)',
    legislature: 'var(--blue)', ministries: 'var(--green)', feedback: 'var(--purple)',
  };
  el.innerHTML = proc.map(p => {
    const color = typeColors[p.type] || 'var(--text2)';
    return `<div style="background:var(--surface2);border:1px solid var(--border);border-radius:8px;padding:14px;position:relative;">
      <div style="position:absolute;top:-1px;left:-1px;width:28px;height:28px;border-radius:7px 0 7px 0;background:${color};display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:800;color:#fff;">${p.step}</div>
      <div style="margin-left:24px;margin-bottom:6px;">
        <div style="font-weight:700;font-size:12px;color:${color};">${esc(p.name)}</div>
        <div style="font-size:10px;color:var(--text3);">${esc(p.body_cn)}</div>
      </div>
      <div style="font-size:11px;font-weight:600;color:var(--text2);margin-bottom:4px;border-top:1px solid var(--border);padding-top:6px;">${esc(p.body)}</div>
      <div style="font-size:11px;color:var(--text2);line-height:1.5;margin-bottom:8px;">${esc(p.desc)}</div>
      <div style="display:flex;flex-wrap:wrap;gap:4px;">
        ${p.examples.map(ex => `<span style="font-size:9px;background:var(--surface);border:1px solid var(--border);border-radius:3px;padding:2px 6px;color:var(--text3);">${esc(ex)}</span>`).join('')}
      </div>
    </div>`;
  }).join('');
}

// --- Meeting news ---
async function refreshMeetingNews() {
  const el = document.getElementById('polity-news');
  el.innerHTML = '<div style="padding:12px 16px;color:var(--text3);">Fetching from Xinhua...</div>';
  try {
    const data = await api('/api/polity/meeting-news');
    const items = data.items || [];
    if (!items.length) {
      el.innerHTML = '<div style="padding:12px 16px;color:var(--text3);font-size:11px;">No recent meeting news found.</div>';
      return;
    }
    el.innerHTML = items.map(item => `
      <div style="padding:7px 16px;border-bottom:1px solid var(--border);">
        <div style="display:flex;gap:6px;align-items:center;margin-bottom:2px;">
          <span style="font-size:9px;font-weight:700;color:var(--accent);text-transform:uppercase;">${esc(item.body)}</span>
          ${item.date ? `<span style="font-size:9px;color:var(--text3);">${esc(item.date)}</span>` : ''}
        </div>
        <div style="font-size:11px;">${item.link ? `<a href="${esc(item.link)}" target="_blank" style="color:var(--text);">${esc(item.title)}</a>` : esc(item.title)}</div>
      </div>`).join('');
  } catch(e) {
    el.innerHTML = `<div style="padding:12px 16px;color:var(--text3);font-size:11px;">Could not load: ${esc(e.message)}</div>`;
  }
}

document.querySelector('[data-panel="polity"]').addEventListener('click', () => { loadPolity(); }, {once: true});

</script>
</body>
</html>"""


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="China Policy Monitor Dashboard")
    parser.add_argument("--port", type=int, default=5001, help="Port to run on")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to")
    args = parser.parse_args()

    print(f"Dashboard: http://{args.host}:{args.port}")
    app.run(host=args.host, port=args.port, debug=True)
