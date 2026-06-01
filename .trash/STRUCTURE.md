# CMM Repository Structure

Last updated: 2026-06-01 (root cleanup: literature/model → backend/, docs/ folder, deps in pyproject, run_fetch.py + tests + egg-info retired)

```
CMM/
├── backend/                    Python package — all server + fetch code
│   ├── api.py                  Flask app — all /api/* routes + serves frontend/index.html
│   ├── run.py                  Entry point: python -m backend.run [--port N]
│   ├── storage.py              get_conn() → data/cmm.db (single connection factory)
│   ├── config.py               All API keys and constants (loaded from .env)
│   ├── schema.sql              Canonical 16-table DDL for cmm.db
│   ├── bootstrap_db.py         Build fresh cmm.db from scratch (apply schema + run fetchers)
│   ├── migrate_db.py           One-off migration: merged 5 source DBs → cmm.db (2026-05-28)
│   │
│   ├── cli.py                  cmm-fetch entry point (all subcommands)
│   ├── fetchers/               One module per data source
│   │   ├── academic.py         CrossRef journals
│   │   ├── advisor.py          Anthropic-powered policy advisor
│   │   ├── bis.py              BIS (credit-to-GDP, policy rates, REER)
│   │   ├── bruegel.py          Bruegel China Economic Dashboard
│   │   ├── ccp_elites.py       Sine CPC Elite Database
│   │   ├── comtrade.py         UN Comtrade bilateral trade
│   │   ├── destatis.py         Destatis GENESIS (Germany)
│   │   ├── destatis_utils.py
│   │   ├── dissent.py          China Dissent Monitor API
│   │   ├── ecb.py              ECB data portal
│   │   ├── eurostat.py         Eurostat datasets
│   │   ├── flights.py          OpenSky Network live flights
│   │   ├── global_macro.py     GMD, IMF WEO/Fiscal, ILO
│   │   ├── ilo.py              ILO statistics
│   │   ├── monitor.py          Pipeline health checks
│   │   ├── news.py             RSS/RSSHub/WeChat feeds + AKShare
│   │   ├── oecd.py             OECD TiVA
│   │   ├── ships.py            AISStream / AISHub live ships
│   │   ├── wits.py             WITS tariffs (SDMX, public)
│   │   ├── wto.py              WTO statistics API
│   │   └── yfinance.py         Yahoo Finance OHLCV
│   │
│   ├── scrapers/               HTML scrapers (no public API)
│   │   ├── mofcom.py           MOFCOM active laws (requires China IP)
│   │   └── npc_observer.py     NPC Observer bills (WordPress, server-rendered)
│   │
│   ├── sources/                Source registry
│   │   ├── registry.yaml       All RSS/RSSHub feed URLs
│   │   ├── loader.py           Load + validate registry
│   │   └── validate.py
│   │
│   ├── runners/                Entry points dispatched by cli.py (cmm-fetch)
│   │   ├── fetch_news.py
│   │   ├── fetch_batch.py      All non-realtime sources
│   │   ├── fetch_macro.py      GMD, IMF, NBS, Bruegel
│   │   ├── fetch_realtime.py   Flights + ships (continuous)
│   │   ├── fetch_comtrade.py
│   │   ├── fetch_eurostat.py
│   │   ├── fetch_ministries.py
│   │   ├── fetch_trade_stats.py
│   │   ├── fetch_yfinance.py
│   │   └── import_ccp_elites.py
│   │
│   ├── scheduler/              macOS launchd daemons (plists use cmm-fetch)
│   │   ├── com.chinapolicymonitor.news.plist      Every 4 hours
│   │   ├── com.chinapolicymonitor.batch.plist     Daily 03:00
│   │   ├── com.chinapolicymonitor.realtime.plist  Continuous (KeepAlive)
│   │   └── com.chinapolicymonitor.macro.plist     Daily 03:00 (superseded by batch)
│   ├── customs_scraper/        Browser-automation scraper for China Customs export data
│   │   ├── main.py
│   │   ├── orchestrator.py
│   │   ├── fetcher.py
│   │   └── …
│   ├── literature/             Reference papers (DSGE / agent-based macro PDFs)
│   └── model/                  macro_model_landscape.md
│
├── frontend/
│   └── index.html              Single-page dashboard (all CSS + JS inline)
│
│   (no root entry script — use `cmm-fetch <cmd>` / `cmm-serve`, or `python -m backend.cli`)
│
├── docs/                       STRUCTURE.md (this file), TODOS.md, big_restructuring_plan.md
│
├── data/                       GITIGNORED — download cmm.db from Google Drive
│   ├── cmm.db                  Canonical database (47 tables, 623K rows)
│   ├── *.db.bak                Original per-topic DBs (kept as safety copies)
│   ├── reference/              Static lookup files (config.json, province/prefecture JSON)
│   ├── raw/                    Raw fetched files before processing
│   └── logs/                   launchd stdout/stderr logs
│
├── .trash/                     Retired files (old layout, tests/, cmm.egg-info, run_fetch.py) — do not delete
├── README.md                   Quick-start, data sources, API keys
├── CLAUDE.md                   Project instructions (must stay at root)
└── pyproject.toml              Package + dependencies; where=["."], include=["backend*"]
```

## Database schema (data/cmm.db)

All code connects via `from backend.storage import get_conn`.

| Table | Contents |
|-------|----------|
| `macro_series` | Country-level time series (IMF, GMD, BIS, ECB, Bruegel, ILO, Eurostat, Destatis) |
| `bilateral_series` | Bilateral flows (Comtrade, WITS, OECD, WTO) |
| `market_prices` | OHLCV (Yahoo Finance, AKShare) |
| `news_items` | RSS + ministry scraper items |
| `documents` | MOFCOM laws + NPC bills |
| `document_events` | Bill timeline events |
| `academic_articles` | CrossRef journals (DOI-deduped) |
| `dissent_events` | China Dissent Monitor protests/strikes |
| `ccp_members` | CC/PB/PSC members across 7th–20th congresses |
| `customs_exports` | HS-8 export data from China Customs |
| `positions_current` | Live flight/ship positions (upserted) |
| `positions_history` | Rolling 30-day position snapshots |
| `fetch_log` | Per-run audit log (source, ok, rows, duration) |
| `countries` | ISO-3 reference |
| `products` | HS/ISIC/WTO product classification tree |
| `tickers` | Financial instrument metadata |

Plus legacy tables preserved from migration:
`items`, `financial_series`, `financial_snapshots`, `bruegel_series`, `bruegel_provincial`,
`bis_*`, `ecb_*`, `imf_fiscal`, `academic_articles`, `flight_positions`, `ship_positions`,
`dissent_events`, `npc_bills`, `npc_bill_events`, `mofcom_docs`, `china_tariffs`,
`unctad_trade`, `usitc_hts`, `ccp_cc/pb/psc_members`, `fetch_log`