-- CMM unified database schema
-- Single file: data/cmm.db
-- All timestamps: TEXT ISO-8601 via datetime('now')
-- All country codes: ISO-3166 alpha-3
-- All booleans: INTEGER 0/1

-- ─────────────────────────────────────────────────────────────
-- REFERENCE
-- ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS countries (
    iso3         TEXT PRIMARY KEY,
    iso2         TEXT,
    name_en      TEXT,
    name_cn      TEXT,
    region       TEXT,
    income_group TEXT
);

CREATE TABLE IF NOT EXISTS products (
    classification TEXT NOT NULL,   -- 'HS2','HS4','HS6','HS8','ISIC4','SITC','WTO_GROUP'
    code           TEXT NOT NULL,
    description    TEXT,
    parent_code    TEXT,
    PRIMARY KEY (classification, code)
);

CREATE TABLE IF NOT EXISTS tickers (
    ticker   TEXT PRIMARY KEY,
    name     TEXT,
    category TEXT,                  -- 'index','etf','fx','commodity','rate','equity'
    currency TEXT,
    exchange TEXT
);

-- ─────────────────────────────────────────────────────────────
-- TIME SERIES
-- ─────────────────────────────────────────────────────────────

-- All country-level time series: macro, financial rates, labour, fiscal
-- Sources: AKSHARE, BIS, ECB, GMD, IMF_WEO, IMF_FISCAL, NBS, BRUEGEL,
--          DESTATIS, EUROSTAT, ILO
CREATE TABLE IF NOT EXISTS macro_series (
    source       TEXT NOT NULL,
    country_iso3 TEXT NOT NULL,
    indicator    TEXT NOT NULL,
    date         TEXT NOT NULL,     -- 'YYYY' | 'YYYY-MM' | 'YYYY-MM-DD' | 'YYYY-Q1'
    value        REAL,
    unit         TEXT,
    fetched_at   TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (source, country_iso3, indicator, date)
);
CREATE INDEX IF NOT EXISTS idx_macro_country_indicator
    ON macro_series (country_iso3, indicator);
CREATE INDEX IF NOT EXISTS idx_macro_source_date
    ON macro_series (source, date);

-- All bilateral series: trade flows, tariff rates, FDI
-- flow examples: 'export','import','mfn_simple','applied_weighted','fdi_inflow'
-- unit examples: 'USD_MN','USD_BN','PCT'
-- Sources: COMTRADE, UNCTAD, WITS, WTO, OECD_STAN, OECD_FDI, EUROSTAT
CREATE TABLE IF NOT EXISTS bilateral_series (
    source         TEXT NOT NULL,
    reporter_iso3  TEXT NOT NULL,
    partner_iso3   TEXT NOT NULL,
    product_code   TEXT NOT NULL DEFAULT '',   -- '' = total/all products
    classification TEXT NOT NULL DEFAULT '',   -- 'HS2','HS4','WTO_GROUP', etc.
    year           INTEGER NOT NULL,
    flow           TEXT NOT NULL,
    value          REAL,
    unit           TEXT,
    fetched_at     TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (source, reporter_iso3, partner_iso3, product_code, classification, year, flow)
);
CREATE INDEX IF NOT EXISTS idx_bilateral_reporter_partner
    ON bilateral_series (reporter_iso3, partner_iso3, year);
CREATE INDEX IF NOT EXISTS idx_bilateral_source
    ON bilateral_series (source, year);

-- OHLCV market prices — different shape, stays separate
-- Source: YFINANCE
CREATE TABLE IF NOT EXISTS market_prices (
    source     TEXT NOT NULL DEFAULT 'YFINANCE',
    ticker     TEXT NOT NULL,
    date       TEXT NOT NULL,
    open       REAL,
    high       REAL,
    low        REAL,
    close      REAL,
    adj_close  REAL,
    volume     REAL,
    fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (source, ticker, date)
);
CREATE INDEX IF NOT EXISTS idx_prices_ticker_date
    ON market_prices (ticker, date DESC);

-- US HTS tariff schedule — product tree with rates, not a time series
-- Source: USITC
CREATE TABLE IF NOT EXISTS usitc_hts (
    hts_code      TEXT PRIMARY KEY,
    description   TEXT,
    general_rate  TEXT,
    special_rates TEXT,
    other_rate    TEXT,
    unit          TEXT,
    chapter       INTEGER,
    is_section301 INTEGER NOT NULL DEFAULT 0,
    fetched_at    TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_hts_chapter ON usitc_hts (chapter);
CREATE INDEX IF NOT EXISTS idx_hts_301 ON usitc_hts (is_section301);

-- ─────────────────────────────────────────────────────────────
-- DOCUMENTS & NEWS
-- ─────────────────────────────────────────────────────────────

-- News items from RSS feeds and ministry scrapers
-- Sources: RSS, MINISTRIES
CREATE TABLE IF NOT EXISTS news_items (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source          TEXT NOT NULL,
    source_category TEXT,
    title           TEXT NOT NULL,
    url             TEXT,
    published_at    TEXT,
    summary         TEXT,
    fetched_at      TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (source, title, url)
);
CREATE INDEX IF NOT EXISTS idx_news_source_date
    ON news_items (source, published_at DESC);

-- Ministry policy announcements with full document text (AI-analysis core table)
-- Sources: MINISTRY_SCRAPER (discovery) + POLICY_CONTENT (full-text swarm)
CREATE TABLE IF NOT EXISTS policy_docs (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    ministry           TEXT NOT NULL,          -- domain slug: ndrc, mofcom, pbc, ...
    source             TEXT NOT NULL,          -- section, e.g. "NDRC — Notices"
    source_cn          TEXT,
    category           TEXT,                   -- central_government / ministry / regulator
    doc_type           TEXT,                   -- 通知 / 公告 / 部令 / ...
    title              TEXT NOT NULL,
    url                TEXT NOT NULL UNIQUE,
    doc_number         TEXT,                   -- 文号, e.g. 发改运行〔2026〕123号
    published          TEXT,                   -- YYYY-MM-DD
    summary            TEXT,
    full_text          TEXT,
    text_len           INTEGER,
    fetch_status       TEXT NOT NULL DEFAULT 'pending',  -- pending/ok/empty/binary/error
    http_status        INTEGER,
    error              TEXT,
    discovered_at      TEXT NOT NULL DEFAULT (datetime('now')),
    content_fetched_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_policy_docs_ministry
    ON policy_docs (ministry, published DESC);
CREATE INDEX IF NOT EXISTS idx_policy_docs_status ON policy_docs (fetch_status);
CREATE INDEX IF NOT EXISTS idx_policy_docs_source ON policy_docs (source);

-- Formal legal documents: laws, regulations, bills, orders
-- Sources: MOFCOM, NPC
CREATE TABLE IF NOT EXISTS documents (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    source       TEXT NOT NULL,
    doc_id       TEXT UNIQUE,
    title        TEXT NOT NULL,
    title_cn     TEXT,
    doc_type     TEXT,              -- 'law','regulation','bill','notice','order'
    status       TEXT DEFAULT 'active',
    issued_at    TEXT,
    effective_at TEXT,
    expires_at   TEXT,
    url          TEXT,
    fetched_at   TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_docs_source ON documents (source, issued_at DESC);

-- Timeline events attached to a document (readings, amendments, passage)
CREATE TABLE IF NOT EXISTS document_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id      TEXT NOT NULL REFERENCES documents(doc_id),
    event_type  TEXT,
    event_date  TEXT,
    description TEXT
);

-- Academic journal articles
-- Source: CROSSREF
CREATE TABLE IF NOT EXISTS academic_articles (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    source       TEXT NOT NULL,     -- journal identifier
    doi          TEXT UNIQUE,
    title        TEXT NOT NULL,
    authors      TEXT,
    published_at TEXT,
    abstract     TEXT,
    url          TEXT,
    fetched_at   TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_academic_source ON academic_articles (source, published_at DESC);

-- ─────────────────────────────────────────────────────────────
-- EVENTS
-- ─────────────────────────────────────────────────────────────

-- Protest and dissent events
-- Source: DISSENT
CREATE TABLE IF NOT EXISTS dissent_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id      TEXT UNIQUE NOT NULL,
    date_start   TEXT,
    date_end     TEXT,
    country_iso3 TEXT NOT NULL DEFAULT 'CHN',
    province     TEXT,
    location     TEXT,
    mode         TEXT,
    issue        TEXT,
    participants TEXT,
    repression   TEXT,
    fetched_at   TEXT NOT NULL DEFAULT (datetime('now'))
);

-- ─────────────────────────────────────────────────────────────
-- ENTITIES
-- ─────────────────────────────────────────────────────────────

-- CCP leadership — CC, Politburo, PSC unified
-- Source: SINE_CCP (manual import)
CREATE TABLE IF NOT EXISTS ccp_members (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    congress     TEXT NOT NULL,
    body         TEXT NOT NULL,     -- 'CC','PB','PSC'
    rank         INTEGER,
    name_en      TEXT NOT NULL,
    name_cn      TEXT,
    birth_year   INTEGER,
    province     TEXT,
    role         TEXT,
    is_alternate INTEGER NOT NULL DEFAULT 0,
    expelled     INTEGER NOT NULL DEFAULT 0,
    fate         TEXT,
    fetched_at   TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (congress, body, name_en)
);
CREATE INDEX IF NOT EXISTS idx_ccp_congress_body ON ccp_members (congress, body);

-- Monthly HS8-level export data (browser-scraped)
-- Source: CUSTOMS
CREATE TABLE IF NOT EXISTS customs_exports (
    year         INTEGER NOT NULL,
    month        INTEGER NOT NULL,
    hs8_code     TEXT NOT NULL,
    hs_desc      TEXT,
    partner_iso3 TEXT NOT NULL,
    partner_name TEXT,
    value_usd    REAL,
    value_cny    REAL,
    qty          REAL,
    qty_unit     TEXT,
    fetched_at   TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (year, month, hs8_code, partner_iso3)
);
CREATE INDEX IF NOT EXISTS idx_customs_year_month ON customs_exports (year, month);

-- ─────────────────────────────────────────────────────────────
-- REAL-TIME POSITIONS
-- ─────────────────────────────────────────────────────────────

-- Latest known position per entity (upserted on every fetch)
-- Sources: OPENSKY, AISSTREAM
CREATE TABLE IF NOT EXISTS positions_current (
    source      TEXT NOT NULL,
    entity_id   TEXT NOT NULL,      -- ICAO24 (flights) or MMSI (ships)
    entity_name TEXT,
    lat         REAL,
    lon         REAL,
    altitude_m  REAL,
    speed_kts   REAL,
    heading     REAL,
    status      TEXT,
    destination TEXT,
    updated_at  TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (source, entity_id)
);

-- Rolling 30-day history — purge on each fetch:
--   DELETE FROM positions_history WHERE snapshot_at < datetime('now','-30 days')
CREATE TABLE IF NOT EXISTS positions_history (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source      TEXT NOT NULL,
    entity_id   TEXT NOT NULL,
    entity_name TEXT,
    lat         REAL,
    lon         REAL,
    altitude_m  REAL,
    speed_kts   REAL,
    heading     REAL,
    status      TEXT,
    destination TEXT,
    snapshot_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_positions_history_lookup
    ON positions_history (source, entity_id, snapshot_at DESC);
CREATE INDEX IF NOT EXISTS idx_positions_history_ts
    ON positions_history (snapshot_at);

-- ─────────────────────────────────────────────────────────────
-- OPERATIONAL
-- ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fetch_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    source       TEXT NOT NULL,
    ok           INTEGER NOT NULL,  -- 1 = success, 0 = failure
    rows_added   INTEGER DEFAULT 0,
    rows_updated INTEGER DEFAULT 0,
    error        TEXT,
    duration_s   REAL,
    fetched_at   TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_fetch_log_source ON fetch_log (source, fetched_at DESC);

-- Auto-refresh scheduler runs (backend/auto_refresh.py, started by cmm-serve)
CREATE TABLE IF NOT EXISTS auto_refresh_runs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    group_name  TEXT NOT NULL,
    started_at  REAL NOT NULL,   -- unix seconds
    finished_at REAL,
    ok          INTEGER
);
