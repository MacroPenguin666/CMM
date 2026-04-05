# China Macro Monitor (CMM)

A unified monitoring platform for Chinese macroeconomic data, government policy, financial markets, social dissent, maritime/aviation activity, and academic publications.

## Modules

### Customs Scraper (`customs_scraper/`)

Monthly scraper for the GACC (General Administration of Customs) statistics portal at [stats.customs.gov.cn](http://stats.customs.gov.cn). Captures Chinese export data at the deepest available granularity:

**HS 8-digit commodity code x destination country x month**

Results are stored in SQLite (synced to Google Drive) and refreshed automatically each month via APScheduler.

### Policy Monitor (`policy_monitor/`)

Tracks Chinese government policy signals and macroeconomic indicators across multiple dimensions:

| Module | Source | Data |
|---|---|---|
| `monitor.py` | 11 RSS feeds (Xinhua, People's Daily, China Daily, Global Times, SCMP, MFA, Gov.cn, ECNS) | Government policy announcements, state media editorials |
| `financial.py` | AKShare (SHIBOR, bond yields, FX rates, PMI, CPI/PPI) | Daily/monthly financial time series |
| `dissent.py` | China Dissent Monitor | Protest and dissent events by province, issue, and mode |
| `bruegel.py` | Bruegel China Economic Database | 181 macroeconomic indicators with provincial breakdowns |
| `macro.py` | Global Macro Data | 74 China-specific variables (GDP, trade, demographics) |
| `academic.py` | CrossRef API | China-policy publications from 5 journals (CQ, JAS, TCQ, MEF, CER) |
| `flights.py` | OpenSky Network | Real-time military/government aircraft positions over the SCS and Taiwan Strait |
| `ships.py` | AISStream / AISHub | Real-time vessel positions (coastguard, navy, research ships) |

All data is stored permanently in SQLite with append-only history tables for flights, ships, and macro data.

### Dashboard (`policy_monitor/dashboard.py`)

Flask web dashboard on port 5001 with:

- **News** tab: latest policy items from RSS feeds
- **Financial** tab: SHIBOR, bond yields, FX rates, PMI, CPI/PPI charts
- **Macro Economy** tab: Bruegel + Global Macro Database indicators
- **Academic** tab: recent China-policy journal articles
- **Dissent** tab: historical dissent trends, top issues/modes, province rankings
- **Map** tab: Leaflet map with toggleable layers for:
  - Real-time flight positions (military/government aircraft)
  - Real-time ship positions (coastguard, navy, research vessels)
  - AMTI South China Sea island markers (28 Chinese-occupied features)
  - China Dissent Monitor choropleth (most recent year, province-level)

---

## Project Structure

```
CMM/
├── customs_scraper/              # GACC export data scraper
│   ├── main.py                   # CLI entry point
│   ├── config.py                 # Configuration (env vars, paths)
│   ├── db.py                     # SQLite schema, upserts, checkpoints
│   ├── fetcher.py                # Scrapling: static + Playwright, retry logic
│   ├── parser.py                 # HTML table parsing
│   ├── page_actions.py           # Playwright form interaction
│   ├── orchestrator.py           # Scrape loop with checkpoint/resume
│   ├── scheduler.py              # APScheduler monthly cron
│   ├── bootstrap.py              # One-time HS code + country list fetch
│   └── debug.py                  # --debug-browser mode
│
├── policy_monitor/               # Policy + macro monitoring
│   ├── storage.py                # Shared SQLite DB (data/feeds.db)
│   ├── monitor.py                # RSS feed fetcher
│   ├── financial.py              # AKShare financial data
│   ├── dissent.py                # China Dissent Monitor scraper
│   ├── bruegel.py                # Bruegel economic database
│   ├── macro.py                  # Global Macro Data integration
│   ├── academic.py               # CrossRef journal scanner
│   ├── flights.py                # OpenSky flight tracking
│   ├── ships.py                  # AIS ship tracking
│   ├── dashboard.py              # Flask web dashboard
│   ├── sources/
│   │   ├── registry.yaml         # 50+ government source definitions
│   │   ├── loader.py             # Registry parser, feed URL builder
│   │   └── validate.py           # Feed validation utility
│   └── runners/
│       ├── fetch_all.py          # Full fetch of all sources
│       ├── fetch_news.py         # Hourly news-only fetch
│       ├── fetch_macro.py        # Daily macro fetch (random timing)
│       └── fetch_realtime.py     # Continuous flight/ship tracker
│
├── run_dashboard.py              # Launch dashboard on :5001
├── run_fetch_all.py              # Run all fetchers once
├── run_fetch_news.py             # Run news fetch once
├── run_fetch_macro.py            # Run macro fetch once
├── run_fetch_realtime.py         # Start continuous realtime tracker
│
├── launchd/                      # macOS scheduled job templates
│   ├── com.chinapolicymonitor.news.plist      # Hourly
│   ├── com.chinapolicymonitor.macro.plist     # Daily at 03:00 + random offset
│   └── com.chinapolicymonitor.realtime.plist  # Continuous daemon
│
├── data/                         # Runtime data (gitignored)
│   ├── feeds.db                  # SQLite database (all policy monitor data)
│   ├── config.json               # API keys fallback
│   ├── china_provinces.json      # GeoJSON for map choropleth
│   ├── china_prefectures.json    # GeoJSON for prefectures
│   └── logs/                     # Fetch logs by date
│
├── tests/
│   ├── test_db.py                # 15 tests: customs DB schema, upserts, checkpoints
│   ├── test_parser.py            # 34 tests: HTML parsing, pagination
│   └── fixtures/
│       └── sample_table.html
│
├── pyproject.toml                # Editable install for both packages
├── requirements.txt              # All dependencies
└── .env.example                  # Environment variable reference
```

---

## Setup

```bash
pip install -r requirements.txt
pip install -e .
cp .env.example .env
# Edit .env as needed
```

### Optional API keys (in `.env` or `data/config.json`)

| Variable | Purpose |
|---|---|
| `CUSTOMS_PROXY_URL` | HTTP proxy with Chinese IP for GACC access |
| `OPENSKY_USERNAME` / `OPENSKY_PASSWORD` | OpenSky Network (higher rate limits) |
| `AISSTREAM_API_KEY` | AISStream WebSocket (ship tracking) |
| `AISHUB_USERNAME` | AISHub REST API (alternative ship source) |

All policy monitor data sources work without authentication. API keys are optional and provide better rate limits.

---

## Usage

### Policy Monitor

```bash
# Start the dashboard
python run_dashboard.py

# Fetch all data sources once
python run_fetch_all.py

# Fetch only news feeds
python run_fetch_news.py

# Start continuous flight/ship tracking
python run_fetch_realtime.py
```

### Customs Scraper

```bash
# Scrape a specific month (requires Chinese IP proxy)
python -m customs_scraper.main --year 2024 --month 1

# Resume a partial/failed run
python -m customs_scraper.main --resume <run_id>

# Start monthly scheduler (15th of each month)
python -m customs_scraper.main --schedule

# Open browser for site inspection
python -m customs_scraper.main --debug-browser
```

---

## Scheduled Jobs (macOS)

Three launchd agents handle automated fetching:

| Agent | Schedule | Script |
|---|---|---|
| `com.chinapolicymonitor.news` | Every hour | `run_fetch_news.py` |
| `com.chinapolicymonitor.macro` | Daily at 03:00 + random 0-18h offset | `run_fetch_macro.py` |
| `com.chinapolicymonitor.realtime` | Continuous daemon (KeepAlive) | `run_fetch_realtime.py` |

Install/update:

```bash
cp launchd/*.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.chinapolicymonitor.news.plist
launchctl load ~/Library/LaunchAgents/com.chinapolicymonitor.macro.plist
launchctl load ~/Library/LaunchAgents/com.chinapolicymonitor.realtime.plist
```

---

## Database

Two separate SQLite databases:

**Customs data** (`customs_exports.db` on Google Drive):
- `exports` — one row per (year, month, hs8_code, country_code)
- `scrape_runs` — audit log per run
- `scrape_checkpoints` — resume support

**Policy monitor data** (`data/feeds.db`):
- `items` — RSS feed articles
- `financial_series` / `financial_snapshots` — market data time series
- `dissent_events` / `dissent_provinces` — protest/dissent tracking
- `bruegel_series` / `bruegel_provincial` — Bruegel economic indicators
- `macro_series` / `macro_history` — Global Macro Database (append-only history)
- `academic_articles` — journal publications
- `flight_positions` / `flight_history` — aircraft tracking (live + history)
- `ship_positions` / `ship_history` — vessel tracking (live + history)
- `fetch_log` — fetch audit trail

---

## Tests

```bash
pytest
```

49 tests covering customs scraper DB operations and HTML parsing. No network or proxy required.

---

## Sources

The policy monitor tracks 50+ Chinese government sources defined in `policy_monitor/sources/registry.yaml`, spanning:

- Central government (State Council, NPC, NDRC)
- 20+ ministries (MFA, MOF, MOFCOM, MIIT, MOE, etc.)
- 15+ regulators (PBOC, CSRC, SAMR, CAC, GACC, NBS, etc.)
- Party/discipline bodies (CCDI, CPC Central Committee)
- Judiciary (Supreme Court, Supreme Procuratorate, legal databases)
- State media (Xinhua, People's Daily, China Daily, Global Times, CCTV, Caixin, The Paper)
- 11 verified direct RSS feeds accessible from outside China
