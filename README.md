Several data sources are loaded but not all of them (claude skips some)

A local web dashboard that collects, standardises, and displays data about Chinese government policy, financial indicators, trade, regulatory activity, and CCP leadership.

---

## Quick start

```bash
pip install -e .
cmm-serve                      # → http://localhost:5001
cmm-serve --port 8080
cmm-serve --no-refresh         # serve only, skip background fetching
```

`cmm-serve` is the only command you need: it serves everything straight from
`data/cmm.db` immediately, and a built-in scheduler checks in the background
every 15 min whether data is stale and re-fetches it (news 4 h, policies 12 h,
batch 24 h, commodities 7 d — one fetch subprocess at a time, logged to
`data/logs/auto_refresh.log`, status at `/api/refresh/status`).

**Fetch data manually (optional, e.g. while the server is down):**
```bash
cmm-fetch news          # RSS + ministry scrapers (runs every 4 h via launchd)
cmm-fetch policies      # ministry policy docs incl. full text → policy_docs (every 12 h)
cmm-fetch realtime      # flights + ships (continuous daemon)
cmm-fetch batch         # all non-realtime sources (runs daily at 03:00 via launchd)
cmm-fetch macro         # GMD, IMF WEO/Fiscal, NBS, Bruegel
cmm-fetch comtrade      # UN Comtrade bilateral trade
cmm-fetch trade-stats   # WITS tariffs, WTO, USITC HTS, ILO, OECD
cmm-fetch ccp-elites    # import CCP elite leadership xlsx

python -m backend.runners.fetch_commodities              # all-materials commodity tracker
python -m backend.runners.fetch_commodities --no-trade   # fast pass (skip Comtrade backfill)
python -m backend.runners.fetch_commodities --trade-only # resume/top-up trade only
```

---

## Database

All data lives in a single SQLite file: **`data/cmm.db`** (47 tables, ~623K rows).

`data/` is gitignored — the database is distributed separately:

> **Download `cmm.db`:** https://drive.google.com/drive/folders/1GNdPi-mAN2MpnCyA3qwucQidYRdnZqaq?usp=drive_link

Place the downloaded `cmm.db` in `data/` before starting the server.

**To build from scratch** (requires API keys in `.env`):
```bash
python -m backend.bootstrap_db
```

All code connects via `from backend.storage import get_conn`. Core tables:

| Table | Contents |
|-------|----------|
| `macro_series` | Country-level time series (IMF, GMD, BIS, ECB, Bruegel, ILO, Eurostat, Destatis) |
| `bilateral_series` | Bilateral flows (Comtrade, WITS, OECD, WTO) |
| `market_prices` | OHLCV (Yahoo Finance, AKShare) |
| `news_items` | RSS + ministry scraper items |
| `policy_docs` | Ministry policy announcements with **full document text**, 文号, and `instrument_type` (通知/公告/令/意见/法/…) (34 sources, ~19 gov bodies) |
| `documents` / `document_events` | MOFCOM laws + NPC bills + bill timeline events |
| `academic_articles` | CrossRef journals (DOI-deduped) |
| `dissent_events` | China Dissent Monitor protests/strikes |
| `ccp_members` | CC/PB/PSC members across 7th–20th congresses |
| `customs_exports` | HS-8 export data from China Customs |
| `positions_current` / `positions_history` | Live + rolling 30-day flight/ship positions |
| `fetch_log` | Per-run audit log (source, ok, rows, duration) |
| `countries` / `products` / `tickers` | ISO-3, HS/ISIC/WTO tree, instrument metadata |

(Plus legacy tables preserved from the 5-DB migration: `items`, `financial_series`, `bruegel_*`, `bis_*`, `ecb_*`, `imf_fiscal`, `china_tariffs`, `unctad_trade`, `usitc_hts`, etc.)

---

## Folder layout

```
backend/           All Python code
  api.py           Flask server (all routes)
  run.py           Entry point: python -m backend.run  (or: cmm-serve)
  cli.py           Unified fetch CLI (cmm-fetch <command>)
  storage.py       get_conn() + DATA_DIR/LOG_DIR/… → data/
  config.py        All API keys (loaded from .env)
  schema.sql       16-table unified DDL
  bootstrap_db.py  Build fresh cmm.db from scratch
  migrate_db.py    One-off migration tool (5 DBs → cmm.db)
  fetchers/        29 source modules (bis, ecb, bruegel, …)
  scrapers/        mofcom.py, npc_observer.py
  sources/         registry.yaml, loader.py, validate.py
  runners/         fetch_news, fetch_batch, fetch_macro, …
  customs_scraper/ Browser-automation China Customs scraper
  scheduler/       macOS launchd .plist files
  literature/      Reference papers (DSGE / agent-based macro)
  model/           Macro-model landscape notes

frontend/
  index.html       Dashboard (served by Flask at /)

tasks/todo.md      Live task backlog
data/              Gitignored — databases, logs, raw files
  cmm.db           Canonical DB (download from Drive)
.trash/            Retired files (old layout, tests, build artifacts, finished plans)
```

> Fetch from the command line with `cmm-fetch <command>` (see Quick start) or
> `python -m backend.cli <command>` without installing.

---

## Data sources

| Source | Table(s) | Frequency | Auth |
|--------|----------|-----------|------|
| 100+ RSS/RSSHub feeds (ministries, NDRC, …) | items | 4 h | None |
| AKShare (SHIBOR, bonds, SSE/CSI 300, CNH, CPI, PMI) | financial_series, financial_snapshots | Daily | None |
| Bruegel China Economic Dashboard | bruegel_series, bruegel_provincial | Monthly | None |
| GMD (Müller et al. 2025) — 75 macro variables, 243 countries | macro_series | On-demand | None |
| BIS (credit-to-GDP, policy rates, REER) | bis_* | On-demand | None |
| ECB (yield curves, CISS, bank lending survey) | ecb_* | On-demand | None |
| IMF Fiscal Monitor | imf_fiscal | On-demand | None |
| Eurostat | eurostat_* | On-demand | None |
| WITS tariffs (HS-6, 2019–2023) | china_tariffs | On-demand | None |
| UN Comtrade bilateral trade (HS-2) | unctad_trade | On-demand | Optional |
| WTO | wto_* | On-demand | Key needed |
| OECD TiVA | oecd_tiva | On-demand | None |
| USITC HTS product tree | usitc_hts | On-demand | None |
| ILO | ilo_* | On-demand | None |
| NPC Observer (bills) | npc_bills, npc_bill_events | On-demand | None |
| MOFCOM (laws) | mofcom_docs | On-demand | China IP |
| CrossRef (academic journals) | academic_articles | On-demand | None |
| OpenSky Network (live flights) | flight_positions | 60 s | Optional |
| AISStream / AISHub (live ships) | ship_positions | 60 s | Key in data/reference/config.json |
| China Dissent Monitor | dissent_events | On-demand | None |
| Sine CPC Elite Database | ccp_cc/pb/psc_members | On-demand | None |
| Yahoo Finance / AKShare (OHLCV) | financial_series | Daily | None |
| Commodity Markets tab — 53 materials (base/precious/REE/battery/semiconductor/gases): USGS MCS data releases (mine/refinery/smelter production by country), UN Comtrade (annual trade per HS code, resumable backfill), Yahoo + IMF-via-FRED (prices) | data/commodities.json (no sqlite) | On-demand | None |

---

## API keys

All keys in `backend/config.py`, loaded from `.env` at project root.

| Key | Required? | Notes |
|-----|-----------|-------|
| `AISSTREAM_API_KEY` | Optional | Key already in `data/reference/config.json` |
| `COMTRADE_API_KEY` | Optional | Paid; public preview works without key |
| `WTO_API_KEY` | Optional | Free at api.wto.org |
| `DESTATIS_TOKEN` | Optional | Guest access `GAST` works without key |
| `ANTHROPIC_API_KEY` | Optional | `advisor.py` stub mode if missing |
| `OPENSKY_USERNAME/PASSWORD` | Optional | Anonymous rate-limited access works |

All other sources (ECB, BIS, IMF, Eurostat, WITS, OECD, ILO, Comtrade public) require no auth.

---

## Scheduler (macOS launchd) — optional

`cmm-serve` already refreshes news/policies/batch/commodities in the background
while it runs (see Quick start). The launchd daemons are only needed if you want
fetching while the server is **down**, or the continuous realtime flights/ships
poller. Duplicate runs are harmless — all fetchers dedupe on insert.

Four daemons in `backend/scheduler/`. Load with:
```bash
launchctl load ~/PycharmProjects/CMM/backend/scheduler/com.chinapolicymonitor.news.plist
launchctl load ~/PycharmProjects/CMM/backend/scheduler/com.chinapolicymonitor.batch.plist
launchctl load ~/PycharmProjects/CMM/backend/scheduler/com.chinapolicymonitor.realtime.plist
```

| Plist | Command | Schedule |
|-------|---------|----------|
| news | `cmm-fetch news` | Every 4 hours |
| policies | `cmm-fetch policies` | Every 12 hours |
| batch | `cmm-fetch batch` | Daily 03:00 + random 0–18 h offset |
| realtime | `cmm-fetch realtime` | Continuous (KeepAlive) |
| macro | `cmm-fetch macro` | Daily 03:00 (superseded by batch) |
