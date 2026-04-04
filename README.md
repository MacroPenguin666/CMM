# China Macro Monitor (CMM)

## Next Steps (TODOs)

These require a Chinese IP or proxy (`CUSTOMS_PROXY_URL`):

1. **Bootstrap HS codes + country codes from the site**
   ```
   CUSTOMS_PROXY_URL=http://... python -m customs_scraper.main --bootstrap-hs-codes
   ```
   Implement `bootstrap.py` after inspecting the site. Writes `data/hs8_codes.csv` and `data/countries.csv`.

2. **Inspect the real site and update selectors**
   ```
   CUSTOMS_PROXY_URL=http://... python -m customs_scraper.main --debug-browser
   ```
   - Check DevTools Network tab for a JSON API (XHR/Fetch calls when submitting the query form). If found, replace `DynamicFetcher` with a simple HTTP POST — much faster.
   - Otherwise update selectors in `customs_scraper/page_actions.py` (form interaction) and `customs_scraper/parser.py` (results table).
   - Save real HTML to `tests/fixtures/sample_table.html` and run `pytest` to validate.

3. **Run one month end-to-end**
   ```
   CUSTOMS_PROXY_URL=http://... python -m customs_scraper.main --year 2024 --month 1
   ```
   Verify DB is written, tune `SCRAPE_DELAY_SECONDS`.

4. **Start the monthly scheduler**
   ```
   CUSTOMS_PROXY_URL=http://... python -m customs_scraper.main --schedule
   ```
   Runs automatically on the 15th of each month (previous month's data).

---

## Overview

CMM is a monitoring application for Chinese macroeconomic data. The first module is a monthly scraper for the GACC (General Administration of Customs of China) statistics portal ([stats.customs.gov.cn](http://stats.customs.gov.cn)), capturing all Chinese export data at the deepest available granularity:

**HS 8-digit commodity code × destination country × month**

Results are stored in SQLite (synced to Google Drive) and refreshed automatically each month via APScheduler.

---

## Project Structure

```
china_macro_monitor/
├── customs_scraper/
│   ├── main.py           # CLI entry point
│   ├── config.py         # All configuration (env vars, paths)
│   ├── db.py             # SQLite schema, upserts, run tracking, checkpoints
│   ├── fetcher.py        # Scrapling wrapper: static + Playwright browser, retry logic
│   ├── parser.py         # HTML table → row dicts (selectors are stubs, update after site inspection)
│   ├── page_actions.py   # Playwright async functions for form interaction (stubs)
│   ├── orchestrator.py   # Main scrape loop with checkpoint/resume support
│   ├── scheduler.py      # APScheduler monthly cron (15th of each month)
│   ├── bootstrap.py      # One-time fetch of HS code + country lists from site (stub)
│   └── debug.py          # --debug-browser mode for manual site inspection
├── data/
│   ├── hs8_codes.csv     # Seed list of HS 8-digit codes (populate via --bootstrap-hs-codes)
│   └── countries.csv     # GACC country codes (populate via --bootstrap-hs-codes)
├── tests/
│   ├── test_db.py        # 15 tests: schema, upserts, run tracking, checkpointing
│   ├── test_parser.py    # 34 tests: parsing helpers, row extraction, pagination detection
│   └── fixtures/
│       └── sample_table.html  # Synthetic HTML fixture (replace with real captured HTML)
├── .env.example          # All configurable env vars with descriptions
└── requirements.txt
```

---

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env
# Edit .env — set DB_PATH and CUSTOMS_PROXY_URL at minimum
```

**Database** is auto-created on first run. On macOS, it defaults to the first detected Google Drive folder at `~/Library/CloudStorage/GoogleDrive-*/My Drive/CMM/customs_exports.db`. Override with `DB_PATH` in `.env`.

---

## Usage

```bash
# Scrape a specific month (backfill)
python -m customs_scraper.main --year 2024 --month 1

# Resume a partial/failed run
python -m customs_scraper.main --resume <run_id>

# Start monthly scheduler (blocks, fires on 15th of each month)
python -m customs_scraper.main --schedule

# Open browser for site inspection (needs proxy)
python -m customs_scraper.main --debug-browser

# Fetch HS code + country lists from site (needs proxy, run once)
python -m customs_scraper.main --bootstrap-hs-codes
```

---

## Configuration

All settings via environment variables (see `.env.example`):

| Variable | Default | Description |
|---|---|---|
| `DB_PATH` | Auto-detected Google Drive path | SQLite database location |
| `CUSTOMS_PROXY_URL` | _(none)_ | HTTP proxy with Chinese IP for site access |
| `SCRAPE_DELAY_SECONDS` | `2.0` | Polite delay between requests |
| `SCRAPE_MAX_RETRIES` | `5` | Retries per fetch with exponential backoff |
| `SCRAPE_HEADLESS` | `true` | Run Playwright in headless mode |
| `SCRAPE_USE_DYNAMIC` | `true` | Use Playwright (vs. static HTTP) |
| `QUERY_ENDPOINT` | `http://stats.customs.gov.cn/indexEn` | Query form URL (update after site inspection) |
| `SCHEDULER_DAY` | `15` | Day of month to run |
| `SCHEDULER_HOUR` | `8` | Hour to run (Asia/Shanghai timezone) |

---

## Database Schema

Three tables in the SQLite database:

**`exports`** — one row per `(year, month, hs8_code, country_code)`:
- `hs8_code`, `hs_description` — 8-digit commodity code and description
- `country_code`, `country_name` — GACC destination country
- `export_value_usd`, `export_value_cny`, `export_qty`, `export_qty_unit`
- Upsert-safe: re-running the same month updates existing rows

**`scrape_runs`** — audit log per run: status, row counts, timing, errors

**`scrape_checkpoints`** — tracks which HS codes completed within a run, enabling `--resume` after crashes without re-fetching completed data

---

## Scraping Design

**Iteration**: Queries one HS 8-digit code at a time, expecting all destination countries in a single paginated result set (~9,000 queries per month). Falls back to HS × country iteration if the site requires a country selection.

**Estimated runtime**: ~5 hours per month at 2s/request with 9,000 HS codes.

**Anti-bot**: Uses scrapling's built-in fingerprint spoofing. Persistent Playwright session reuses one browser across all queries (faster, less detectable than per-query browser startup). Exponential backoff on failures.

**Resume**: If the run is interrupted, `--resume <run_id>` skips all checkpointed HS codes and continues from where it stopped.

---

## Tests

```bash
pip install lxml pytest
pytest tests/
```

49 tests, no scrapling or network required. The parser tests use a synthetic HTML fixture and a minimal scrapling API shim backed by lxml.
