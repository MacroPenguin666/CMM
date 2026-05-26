# CMM — Project Structure

## What this project does
China Policy Monitor (CMM) collects, standardises, and displays data about Chinese government policy, financial indicators, academic publications, trade data, and regulatory activity. It runs as a local web dashboard.

## Folder overview

### `dashboard.html`
The full frontend. Open it by starting the API server (see `live/`) and visiting http://localhost:5001. This is a single HTML file containing all styles and JavaScript.

### `live/`
The Flask API server that feeds live data into the dashboard. Start it with:
    python live/run.py
Contains `api.py` (all API endpoints) and `run.py` (entry point). This is the ONLY code the dashboard depends on at runtime.

### `scripts/`
All data collection and standardisation code. Runs on a schedule (see `scheduler/`). Writes results to `data/feeds.db` and `data/regulations.db`. Completely independent of the dashboard — no Flask, no HTML.

- `scripts/policy_monitor/` — main Python package
  - `scrapers/` — web scrapers (NPC Observer, MOFCOM)
  - `runners/` — scheduled job entry points (fetch_batch, fetch_macro, fetch_news, fetch_realtime)
  - `sources/` — source registry (registry.yaml) and loader
  - `storage.py` — SQLite DB layer (all read/write to data/feeds.db)
  - Other modules: macro, financial, bruegel, regulations, academic, flights, ships, dissent, monitor
- `scripts/customs_scraper/` — China Customs export data scraper

### `data/`
All data storage. Never commit the .db files.

- `data/feeds.db` — canonical SQLite database (news items, fetch logs, financial data, etc.)
- `data/regulations.db` — regulations database (MOFCOM, NPC)
- `data/raw/` — raw fetched files (JSON dumps, CSVs) before processing
- `data/reference/` — static lookup files (china_prefectures.json, china_provinces.json, config.json)
- `data/logs/` — run logs from scheduled jobs

### `outputs/`
Generated exports, charts, and reports. Nothing here is source-controlled.

### `literature/`
Academic PDFs relevant to the project.

### `scheduler/`
macOS launchd .plist files that run the scripts in `scripts/runners/` on a schedule. Install with `launchctl load scheduler/<name>.plist`.

### `tests/`
Test suite. Run with `pytest`.

### `.trash/`
Files removed during the 2026-05-22 restructure. Safe to delete permanently once you're sure nothing is missing.

## How to run

1. Install: `pip install -e .`
2. Start the API server: `python live/run.py`
3. Open browser: http://localhost:5001

## The two types of code

| Folder | Purpose | Dashboard dependency? |
|--------|---------|----------------------|
| `scripts/` | Fetch + standardise data | None |
| `live/` | Serve data to dashboard | Yes — this is the bridge |


## Data pipeline

### Databases

| File | Size | Contents |
|------|------|----------|
| `data/feeds.db` | ~97 MB | **Primary DB** — 36 tables: news items, financial series, Bruegel macro, GMD, BIS, ECB, IMF Fiscal, academic articles, flights, ships, dissent events |
| `data/trade_stats.db` | ~16 MB | Consolidated trade metrics — OECD TiVA, WTO, WITS tariffs (84k rows, 2019–2023), USITC HTS |
| `data/unctad_trade.db` | ~8 MB | UN Comtrade bilateral merchandise trade by HS-2 chapter (China vs ~200 partners, ~8 years) |
| `data/ccp_elites.db` | ~572 KB | Sine CPC leadership DB — CC/PB/PSC members across 7th–20th Party Congress |
| `data/regulations.db` | ~152 KB | MOFCOM active laws + NPC bills under revision |

Note: `data/` is gitignored. Never commit .db files.

---

### External sources → DB mapping

**→ `feeds.db`**
- **100+ RSS/RSSHub/WeChat feeds** (State Council, 12+ ministries, NDRC, regulators) → `items`
- **AKShare API** (SHIBOR, bond yields, SSE/CSI 300, USD/CNH, CPI, PMI, trade balance) → `financial_series`, `financial_snapshots`
- **Bruegel China Economic Dashboard** Excel files (100+ macro/financial indicators, all 31 provinces) → `bruegel_series`, `bruegel_provincial`
- **GMD (Müller et al. 2025)** — 75 annual macro variables, 243 countries, 1640–2030 → `macro_series`
- **BIS** (credit-to-GDP, policy rates, REER) → `bis_*` tables
- **ECB** (yield curves, bank lending survey, CISS) → `ecb_*` tables
- **IMF Fiscal Monitor** (revenue, debt, primary balance) → `imf_fiscal`
- **CrossRef API** (China Quarterly, AER, JPE, QJE) → `academic_articles`
- **OpenSky Network** (live aircraft over China) → `flight_positions`
- **AISHub / AISStream** (live ships around China) → `ship_positions`
- **China Dissent Monitor API** (protests, strikes by province) → `dissent_events`

**→ `unctad_trade.db`**
- **UN Comtrade+ public API** (bilateral merchandise trade by HS-2)

**→ `trade_stats.db`**
- **WITS** (applied bilateral tariffs, HS-6), **OECD TiVA**, **WTO**, **USITC HTS**

**→ `ccp_elites.db`**
- **Sine CPC Elite Database** (static import from cpcleadershipdata.pages.dev)

**→ `regulations.db`**
- **MOFCOM website** scraper (active laws) + **NPC Observer** scraper (bills)

**Note:** Political structure (PSC members, leading groups, state organs) is hardcoded in a Python dict inside `live/api.py` — no DB.

---

### Collection schedule

| Runner | Interval | What it fetches |
|--------|----------|-----------------|
| `fetch_news.py` | 4 hours | All RSS/RSSHub/WeChat |
| `fetch_financial.py` | Daily | AKShare |
| `fetch_bruegel.py` | Monthly | Bruegel Excel |
| `fetch_macro.py` | On-demand | GMD, BIS, ECB, IMF, Eurostat, ILO |
| `fetch_regulations.py` | On-demand | MOFCOM, NPC Observer |
| `fetch_realtime.py` | 60 s | Flights (OpenSky), ships (AIS) |
| `fetch_academic.py` | On-demand | CrossRef journals |
| `fetch_dissent.py` | On-demand | China Dissent Monitor |

---

### Dashboard endpoints → DB mapping

| Dashboard tab / feature | API endpoint | DB |
|-------------------------|-------------|-----|
| News feed | `/api/news` | `feeds.db` |
| Financial indicators | `/api/financial/*` | `feeds.db` |
| Bruegel macro | `/api/bruegel/*` | `feeds.db` |
| Global macro (GMD) | `/api/macro/*` | `feeds.db` |
| Academic papers | `/api/academic/*` | `feeds.db` |
| Trade choropleth | `/api/trade/*` | `unctad_trade.db` |
| Regulations timeline | `/api/regulations/*` | `regulations.db` |
| Political structure | `/api/polity` | hardcoded dict in `api.py` |
| CCP elites / purge heatmap | `/api/elites/*` | `ccp_elites.db` |
| Flights / ships | `/api/flights/*`, `/api/ships/*` | `feeds.db` |
| Dissent events | `/api/dissent/*` | `feeds.db` |
| Pipeline status | `/api/pipeline/status` | `feeds.db` |

| Tariff rates | `/api/tariffs/china-applied`, `/api/tariffs/on-china` | `trade_stats.db` |

---

### API credentials

All keys live in `scripts/policy_monitor/config.py` (loaded from `.env`).

| Service | Used by | Required? |
|---------|---------|-----------|
| Anthropic | `advisor.py` | Optional (stub mode if missing) |
| OpenSky Network | `flights.py` | Optional (anonymous rate-limited) |
| AISStream | `ships.py` | Optional |
| Destatis GENESIS | `destatis.py` | Optional (GAST guest works) |
| UN Comtrade+ | `unctad.py` | None (public API) |
| WITS | `wits.py` | Optional |
| WTO | `wto.py` | Optional |
| IMF DataMapper | `global_macro.py` | None (public API) |
| ECB / Eurostat / BIS | various | None (public APIs) |

---

## GOOD TO KNOW:
 Changes made:
  1. fetch_realtime.py — NEWS_INTERVAL changed from 3600 → 14400 (4 hours)
  2. scheduler/com.chinapolicymonitor.news.plist — StartInterval changed 3600 → 14400, and fixed the broken path reference to
  run_fetch_news.py (file was deleted in the restructure)
  3. Created run_fetch_news.py at top level (was missing, breaking the launchd plist)
  
  To reload launchd so the new interval takes effect, run:
  ! launchctl unload ~/PycharmProjects/CMM/scheduler/com.chinapolicymonitor.news.plist && launchctl load
  ~/PycharmProjects/CMM/scheduler/com.chinapolicymonitor.news.plist
  The realtime daemon will pick up the new interval automatically on its next restart.