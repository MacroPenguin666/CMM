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

-- ─────────────────────────────────────────────────────────────
-- CHINA FISCAL CAPACITY (FYP Fiscal subtab)
-- Sources: MOF gks 收支 monthly, MOF zwgls/yss LGB debt reports,
--          AKShare (curves/TSF/PBOC balance), ChinaMoney registry,
--          MOF final accounts + NBS yearbook (annual), curated refs
-- ─────────────────────────────────────────────────────────────

-- MOF monthly 财政收支情况 — cumulative YTD values, 亿元 (yi CNY)
-- Source: gks.mof.gov.cn/tongjishuju
CREATE TABLE IF NOT EXISTS fiscal_national_monthly (
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL,   -- last month of cumulative period
    metric          TEXT NOT NULL,      -- e.g. gpb_rev, gpb_rev_central, gpb_rev_local,
                                        -- gpb_exp, gpb_exp_central, gpb_exp_local,
                                        -- tax_rev, nontax_rev, debt_interest_exp,
                                        -- fund_rev, fund_rev_central, fund_rev_local,
                                        -- land_sale_rev, fund_exp, fund_exp_local, ...
    value_100m      REAL,               -- 亿元 cumulative Jan..month
    yoy_pct         REAL,               -- reported same-period yoy growth %
    source_url      TEXT,
    fetched_at      TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (year, month, metric)
);

-- MOF monthly 地方政府债券发行和债务余额情况 — 亿元 / years / %
-- Source: zwgls.mof.gov.cn/tjsj (2025-12→) + yss.mof.gov.cn dfzgl/sjtj (history)
CREATE TABLE IF NOT EXISTS fiscal_lgb_monthly (
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    metric          TEXT NOT NULL,      -- issue_new, issue_new_general, issue_new_special,
                                        -- issue_refi, issue_refi_general, issue_refi_special,
                                        -- issue_total*, avg_tenor_y, avg_rate_pct,
                                        -- ytd_* variants, principal_repaid_ytd,
                                        -- principal_repaid_by_refi_ytd, interest_paid_ytd,
                                        -- debt_limit, debt_limit_general, debt_limit_special,
                                        -- debt_outstanding, debt_outstanding_general,
                                        -- debt_outstanding_special, stock_avg_tenor_y,
                                        -- stock_avg_rate_pct, ...
    value           REAL,
    source_url      TEXT,
    fetched_at      TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (year, month, metric)
);

-- LGB principal maturity wall by province × year (rebuilt each run)
-- Source: ChinaMoney bond registry (www.chinamoney.com.cn)
CREATE TABLE IF NOT EXISTS fiscal_maturity (
    province        TEXT NOT NULL,      -- CN name, '全国' for aggregate rows
    maturity_year   INTEGER NOT NULL,
    principal_100m  REAL NOT NULL,      -- 亿元 due that year
    n_bonds         INTEGER,
    built_at        TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (province, maturity_year)
);

-- Daily close yield curves (ChinaMoney via AKShare bond_china_close_return)
CREATE TABLE IF NOT EXISTS fiscal_curves_daily (
    curve           TEXT NOT NULL,      -- cgb | lgb_aaa | cpmtn_aa
    date            TEXT NOT NULL,      -- YYYY-MM-DD
    tenor_y         REAL NOT NULL,
    yield_pct       REAL,
    fetched_at      TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (curve, date, tenor_y)
);

-- Monthly monetary/flow context (AKShare): TSF components, fiscal deposits, PSL
CREATE TABLE IF NOT EXISTS fiscal_monetary_monthly (
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    metric          TEXT NOT NULL,      -- tsf_flow, tsf_govbond_flow, tsf_trust_flow,
                                        -- fiscal_deposits, psl_balance, ...
    value_100m      REAL,
    fetched_at      TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (year, month, metric)
);

-- Annual province panel (MOF final accounts + NBS yearbook + LGB reports)
CREATE TABLE IF NOT EXISTS fiscal_province_annual (
    province        TEXT NOT NULL,      -- CN name
    year            INTEGER NOT NULL,
    metric          TEXT NOT NULL,      -- gpb_rev, gpb_exp, transfers_in, gdp_100m,
                                        -- pop_10k, debt_outstanding, debt_limit,
                                        -- bond_interest_paid, ...
    value           REAL,
    source          TEXT,               -- mof_final_accounts | nbs_yearbook | mof_lgb | curated
    fetched_at      TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (province, year, metric)
);

-- Curated reference facts (cited; no public feed exists)
CREATE TABLE IF NOT EXISTS fiscal_reference (
    key             TEXT NOT NULL,      -- e.g. restricted_provinces, lgfv_debt_est,
                                        -- vat_split_history, swap_program, net_land_share,
                                        -- afd_lgfv_bond_net_pct_gdp, quota_YYYY, thresholds
    province        TEXT NOT NULL DEFAULT '',  -- '' for national/scalar entries
    value_json      TEXT NOT NULL,      -- JSON payload
    as_of           TEXT,               -- date the estimate refers to
    citation        TEXT NOT NULL,      -- where the number comes from
    updated_at      TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (key, province)
);
CREATE INDEX IF NOT EXISTS idx_fiscal_nat_metric ON fiscal_national_monthly (metric, year, month);
CREATE INDEX IF NOT EXISTS idx_fiscal_lgb_metric ON fiscal_lgb_monthly (metric, year, month);
CREATE INDEX IF NOT EXISTS idx_fiscal_curves_date ON fiscal_curves_daily (curve, tenor_y, date);
CREATE INDEX IF NOT EXISTS idx_fiscal_prov_metric ON fiscal_province_annual (metric, year);

-- Bond-level LGB registry (ChinaMoney) — feeds fiscal_maturity aggregation
CREATE TABLE IF NOT EXISTS fiscal_lgb_bonds (
    bond_code       TEXT PRIMARY KEY,   -- chinamoney bondDefinedCode
    bond_name       TEXT,
    bond_full_name  TEXT,
    issuer          TEXT,               -- entyFullName as published
    province        TEXT,               -- normalised (单列市 → parent province)
    issue_date      TEXT,
    maturity_date   TEXT,
    amount_100m     REAL,               -- issueAmnt 亿元
    tenor           TEXT,               -- bondPeriod e.g. 30年
    is_special      INTEGER,            -- 专项 in full name
    is_refi         INTEGER,            -- 再融资 in full name
    fetched_at      TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_fiscal_bonds_prov ON fiscal_lgb_bonds (province, maturity_date);

-- Monthly by-province LGB flows (parsed from the MOF report's 附表 PDF annex)
CREATE TABLE IF NOT EXISTS fiscal_lgb_province_monthly (
    province        TEXT NOT NULL,
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    metric          TEXT NOT NULL,      -- issue_new / issue_new_general / issue_new_special
                                        -- issue_refi* / issue_total* (+ _ytd variants),
                                        -- principal_repaid_month/_ytd, interest_paid_month/_ytd
    value_100m      REAL,
    source_url      TEXT,
    fetched_at      TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (province, year, month, metric)
);
CREATE INDEX IF NOT EXISTS idx_fiscal_lgbprov ON fiscal_lgb_province_monthly (metric, year, month);
