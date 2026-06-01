# CMM Repo Restructuring Plan

## Context

The CMM repo has grown organically: 5 separate SQLite databases (137MB total, 71 tables), code scattered across `scripts/policy_monitor/`, `scripts/customs_scraper/`, `live/`, and 8 top-level `run_*.py` scripts. The goal is:

- **Backend/frontend separation** — one folder for data acquisition, one for the dashboard
- **Single consolidated database** — merge 5 SQLite DBs into one (`data/cmm.db`)
- **Portable code, separate data** — the repo (code only) can be pushed to others; `cmm.db` is distributed separately (e.g. Google Drive) and gitignored
- **Incremental fetching** — only fetch/write missing data (already implemented in most fetchers)

**Distribution model:**
```
Repo (code, gitignored DB) → anyone can clone
data/cmm.db (gitignored)   → distributed separately (Google Drive / direct share)

New user workflow:
  1. git clone CMM
  2. Download cmm.db → place in data/
  3. python run_fetch.py news    (optional: top up with fresh data)
  4. python -m backend.api       (dashboard at localhost:5001)
```

**Data flow (local, always):**
```
Fetchers → data/cmm.db ← Flask API (backend/api.py) ← browser (localhost:5001)
```

No cloud database. No GitHub Actions. No static export step.

---

## Phase 1: Folder Restructure

Reorganize the codebase into `backend/` and `frontend/` with no behavior change.

### Target layout

```
CMM/
├── backend/
│   ├── __init__.py
│   ├── config.py                     # from scripts/policy_monitor/config.py
│   ├── storage.py                    # from scripts/policy_monitor/storage.py
│   │                                 # single get_conn() → data/cmm.db (post Phase 2)
│   ├── api.py                        # from live/api.py (Flask server + all routes)
│   ├── fetchers/                     # 25+ data-source modules
│   │   ├── __init__.py
│   │   ├── financial.py              # AKShare (SHIBOR, bonds, CPI, PMI)
│   │   ├── macro.py                  # Global Macro Database
│   │   ├── bruegel.py                # Bruegel China Economic Dashboard
│   │   ├── academic.py               # CrossRef journals
│   │   ├── bis.py                    # BIS credit, rates, REER
│   │   ├── ecb.py                    # ECB yield curves, lending, CISS
│   │   ├── imf_fiscal.py             # IMF Fiscal Monitor
│   │   ├── destatis.py               # Destatis GENESIS
│   │   ├── eurostat.py               # Eurostat API
│   │   ├── global_macro.py           # IMF DataMapper
│   │   ├── unctad.py                 # UN Comtrade+ bilateral trade
│   │   ├── comtrade.py               # UN Comtrade HS4
│   │   ├── wits.py                   # WITS tariffs
│   │   ├── usitc_hts.py              # USITC harmonized schedule
│   │   ├── oecd_tiva.py              # OECD value added
│   │   ├── dissent.py                # China Dissent Monitor
│   │   ├── flights.py                # OpenSky Network
│   │   ├── ships.py                  # AISStream
│   │   ├── monitor.py                # RSS feed fetcher
│   │   ├── ministry_scraper.py       # Ministry HTML scraper
│   │   ├── regulations.py            # MOFCOM + NPC
│   │   ├── ccp_elites.py             # CPC leadership import
│   │   ├── polity.py                 # Polity regime data
│   │   ├── nbs.py                    # China NBS
│   │   ├── yfinance_data.py          # Yahoo Finance
│   │   ├── ilo.py, wto.py, advisor.py, destatis_utils.py
│   │   └── calendar_fetcher.py
│   ├── scrapers/
│   │   ├── __init__.py
│   │   ├── mofcom.py                 # from scripts/policy_monitor/scrapers/
│   │   └── npc_observer.py
│   ├── sources/
│   │   ├── __init__.py
│   │   ├── registry.yaml             # 100+ feed definitions
│   │   ├── loader.py
│   │   └── validate.py
│   ├── runners/
│   │   ├── __init__.py
│   │   ├── fetch_all.py, fetch_batch.py
│   │   ├── fetch_news.py, fetch_macro.py, fetch_realtime.py
│   │   ├── fetch_comtrade.py, fetch_eurostat.py
│   │   ├── fetch_ministries.py, fetch_trade_stats.py
│   │   ├── fetch_yfinance.py
│   │   └── import_ccp_elites.py
│   └── customs_scraper/              # Separate sub-package (browser automation)
│       └── (all existing customs_scraper files)
│
├── frontend/
│   ├── index.html                    # renamed from dashboard.html (served by Flask)
│   └── geo/
│       ├── china_provinces.json      # from data/reference/
│       └── china_prefectures.json
│
├── run_fetch.py                      # single CLI entry point (replaces 8 scripts)
│
├── data/                             # gitignored entirely
│   ├── cmm.db                        # unified SQLite — distributed separately
│   ├── raw/, reference/, logs/
│
├── scheduler/                        # launchd plists (updated paths)
├── tests/
├── model/, literature/
├── pyproject.toml, requirements.txt
└── CLAUDE.md
```

### Work items

1. Create directory tree: `backend/`, `backend/fetchers/`, `backend/scrapers/`, `backend/sources/`, `backend/runners/`, `backend/customs_scraper/`, `frontend/`, `frontend/geo/`
2. Move files to new locations:
   - `scripts/policy_monitor/X.py` → `backend/fetchers/X.py` for data modules
   - `scripts/policy_monitor/storage.py`, `config.py` → `backend/`
   - `scripts/policy_monitor/scrapers/` → `backend/scrapers/`
   - `scripts/policy_monitor/sources/` → `backend/sources/`
   - `scripts/policy_monitor/runners/` → `backend/runners/`
   - `live/api.py` → `backend/api.py`
   - `dashboard.html` → `frontend/index.html`
   - `data/reference/*.json` → `frontend/geo/`
   - `customs_scraper/` → `backend/customs_scraper/`
3. Update **all imports** across the codebase:
   - `from policy_monitor.storage import ...` → `from backend.storage import ...`
   - `from policy_monitor.config import ...` → `from backend.config import ...`
   - `from policy_monitor.X import ...` → `from backend.fetchers.X import ...`
   - Internal cross-imports within fetchers
   - GeoJSON route in `backend/api.py`: update path to `frontend/geo/`
4. Update `pyproject.toml`: change package discovery to `backend*`
5. Create `run_fetch.py` with argparse subcommands (`news`, `macro`, `batch`, `realtime`, `comtrade`, `eurostat`, `ministries`, `trade-stats`, `yfinance`, `ccp-elites`), replacing the 8 separate `run_fetch_*.py` scripts
6. Update scheduler plists to use new `run_fetch.py` paths

**Critical files to modify:**
- `scripts/policy_monitor/storage.py` — the hub; every fetcher imports from it
- `scripts/policy_monitor/runners/*.py` — import all fetcher modules
- `live/api.py` — imports storage + individual fetcher modules for DB paths
- `pyproject.toml` — package discovery
- All 25+ fetcher modules — internal imports

**Verify:** `python run_fetch.py news` completes; `python -m backend.api` boots the dashboard on localhost:5001; all tabs load.

---

## Phase 2: Database Consolidation

Merge 5 SQLite databases into `data/cmm.db`. This is the **single source of truth** — every fetcher writes to it, every API route reads from it. All table names are already globally unique — no conflicts.

### Tables being merged

| Source DB | Tables | Rows |
|-----------|--------|------|
| feeds.db (97MB) | items, fetch_log, batch_runs, financial_series, financial_snapshots, bruegel_series/meta/provincial, macro_series/versions/history, bis_* (5 tables), ecb_* (6 tables), imf_fiscal, global_macro, dissent_events/provinces, flight_positions/history, ship_positions/history, academic_articles/votes, scrape_runs/checkpoints, exports, + per-ministry tables | ~840K |
| unctad_trade.db (24MB) | unctad_trade, unctad_fetch_log | 149K |
| trade_stats.db (16MB) | china_tariffs, unctad_series, usitc_hts | 85K |
| ccp_elites.db (572KB) | ccp_cc_members, ccp_pb_members, ccp_psc_members, ccp_elites_meta | 4.7K |
| regulations.db (192KB) | mofcom_docs, npc_bills, npc_bill_events | 556 |

### Work items

1. Write `backend/migrate_db.py`:
   - Creates `data/cmm.db`
   - ATTACHes each source DB
   - Copies each table with `CREATE TABLE ... AS SELECT * FROM attached.table`
   - Recreates indexes
   - Verifies row counts match
   - Renames originals to `.db.bak` (not deleted — per project convention)

2. Update `backend/storage.py`:
   - Single `DB_PATH = DB_DIR / "cmm.db"` (all old per-DB constants removed)
   - `get_conn()` always returns a connection to `cmm.db`

3. Update every module that defines its own DB path to use `storage.get_conn()` instead:
   - `unctad.py` — remove `UNCTAD_DB`
   - `regulations.py` — remove `_REG_DB`
   - `ccp_elites.py` — remove `_DB_PATH`
   - `wits.py`, `usitc_hts.py` — remove `TRADE_STATS_DB`
   - `eurostat.py`, `comtrade.py`, `yfinance_data.py` — same pattern
   - `backend/api.py` — remove all per-DB path references, use `storage.get_conn()` throughout

4. Run migration script

**Verify:** `sqlite3 data/cmm.db ".tables"` shows all ~63 tables; row counts match originals; `python run_fetch.py batch` writes to `cmm.db`; dashboard loads all 8 tabs.

---

## Phase 3: Distribution Setup

Make the repo shareable code-only, with `cmm.db` distributed as a separate artifact.

### Gitignore

`data/` is already gitignored. Confirm `cmm.db` and all `.db` files are excluded:

```gitignore
# Data — distributed separately
data/
*.db
*.db.bak
```

The repo contains only code, config, and static assets. No data is committed.

### DB distribution

Upload `cmm.db` to the project Google Drive folder:
```
https://drive.google.com/drive/folders/1GNdPi-mAN2MpnCyA3qwucQidYRdnZqaq
```

Add a direct download link to `README.md` so that anyone who clones the repo can get the data in one step.

### Bootstrap script (`backend/bootstrap_db.py`)

For users who want to build their own `cmm.db` from scratch instead of downloading:

```python
# Runs all fetchers in sequence to populate a fresh cmm.db
# Requires API keys defined in backend/config.py
```

This lets the repo be fully self-contained for users who have their own API keys.

### `README.md` quick-start section

```
## Quick start

1. git clone <repo>
2. pip install -e .
3. Download data/cmm.db from [Google Drive link] → place in data/
   OR: python backend/bootstrap_db.py   (builds from scratch, needs API keys)
4. python -m backend.api
   → Dashboard at http://localhost:5001
```

**Verify:** Clone repo to a clean directory, download cmm.db, run `python -m backend.api` — all tabs load.

---

## Phase 4: Cleanup

1. Move old files to `.trash/` (per project convention — never delete):
   - `scripts/` → `.trash/scripts/`
   - `live/` → `.trash/live/`
   - `run_fetch_*.py` (the 8 individual scripts) → `.trash/`
   - `data/*.db.bak` → `.trash/`
2. Update `.gitignore`: confirm `data/`, `*.db`, logs excluded
3. Update scheduler plists to reference new paths
4. Update `CLAUDE.md` and `STRUCTURE.md`
5. Move `_remote/` to `.trash/_remote/`

---

## What is NOT in scope

- Supabase or any external cloud database
- GitHub Pages or any static site export
- GitHub Actions
- Any dual-write or sync mechanism
- Any public API or hosted version of the dashboard

The dashboard is always local. The database is always local. Distribution happens via file share, not cloud infra.

---

## Verification Checklist

- [ ] Phase 1: `python run_fetch.py news` works; `python -m backend.api` boots dashboard at localhost:5001
- [ ] Phase 2: `sqlite3 data/cmm.db ".tables"` shows ~63 tables; all tabs load from cmm.db; no module references a non-cmm.db path
- [ ] Phase 3: `git status` shows no `.db` files tracked; README has download link; bootstrap script runs clean on a fresh checkout
- [ ] Phase 4: `scripts/` and `live/` gone from active tree; scheduler plists point to new paths
