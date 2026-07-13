"""
Source registry for the raw ingestion pipeline.

Maps each source id to its fetcher module and a cadence group. Modules are
imported lazily by ``_run.py`` (so a yfinance run doesn't import akshare, etc.).

Cadence groups:
    discover  — expensive structure crawl, run occasionally (e.g. weekly)
    batch     — full pull on a daily schedule
    realtime  — short snapshot, polled on a tight interval by ``_run.py --realtime``
"""

# source id -> dict(module, cadence, note)
SOURCES: dict[str, dict] = {
    # --- statistical / economic (batch) ---
    "nbs":         {"module": "fetch_nbs",          "cadence": "batch",    "note": "NBS full tree (national+province+city)"},
    "akshare":     {"module": "fetch_akshare",      "cadence": "batch",    "note": "AKShare China interface sweep"},
    "gmd_macro":   {"module": "fetch_gmd_macro",    "cadence": "batch",    "note": "Global Macro Database, China"},
    "bis":         {"module": "fetch_bis_china",    "cadence": "batch",    "note": "BIS SDMX, China dataflows"},
    "imf":         {"module": "fetch_imf_china",    "cadence": "batch",    "note": "IMF DataMapper, all indicators, China"},
    "yfinance":    {"module": "fetch_yfinance_china","cadence": "batch",   "note": "Yahoo Finance China markets, full history"},
    # --- entities / text (batch) ---
    "ccp_elites":  {"module": "fetch_ccp_elites",   "cadence": "batch",    "note": "CCP elite leadership xlsx"},
    "ministries":  {"module": "fetch_ministries",   "cadence": "batch",    "note": ".gov.cn ministry archives"},
    "regulations": {"module": "fetch_regulations",  "cadence": "batch",    "note": "MOFCOM laws + NPC bills"},
    "dissent":     {"module": "fetch_dissent",      "cadence": "batch",    "note": "China Dissent Monitor"},
    "academic":    {"module": "fetch_academic",     "cadence": "batch",    "note": "CrossRef China scholarship"},
    # --- realtime (poll) ---
    "rss":         {"module": "fetch_rss",          "cadence": "realtime", "note": "policy/news RSS feeds"},
    "flights":     {"module": "fetch_flights",      "cadence": "realtime", "note": "OpenSky China airspace"},
    "ships":       {"module": "fetch_ships",        "cadence": "realtime", "note": "AIS China waters"},
}

# Poll intervals (seconds) for realtime sources used by `_run.py --realtime`.
REALTIME_INTERVALS = {
    "flights": 60,
    "ships":   60,
    "rss":     4 * 3600,
}


def batch_sources() -> list[str]:
    return [s for s, m in SOURCES.items() if m["cadence"] == "batch"]


def realtime_sources() -> list[str]:
    return [s for s, m in SOURCES.items() if m["cadence"] == "realtime"]
