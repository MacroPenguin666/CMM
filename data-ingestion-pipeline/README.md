# data-ingestion-pipeline

A **raw, exhaustive China-data landing layer** that runs in parallel to the main
`backend/` → `data/cmm.db` → dashboard pipeline. Where `backend/fetchers/`
transform a lossy subset of each source into SQLite for the dashboard, this
pipeline captures **everything a source publishes on China, raw and un-transformed,
as append-only parquet with ingestion timestamps and full revision (vintage)
history** — a durable bronze layer to feed the KITE / DSGE modelling work.

It does **not** touch `data/cmm.db`. The two pipelines coexist.

## Layout

```
01_raw_fetchers/        # one fetch_*.py per source + shared _ modules
  _raw_store.py         #   the spine: append-only parquet writer, provenance, vintages
  _registry.py          #   source id -> module + cadence group
  _run.py               #   orchestrator CLI
  fetch_<source>.py     #   14 fetchers (see table below)
02_inputs/              # landing zone (created on first run)
  <source>/<dataset>/ingest_date=YYYY-MM-DD/<run_id>.parquet
  <source>/_manifest.json
```

## How storage works (`_raw_store.py`)

* **Append-only.** Every run writes a *new* part file; nothing is overwritten.
  A value the source later revises is appended as a new row with a later
  `_ingested_at`. The full vintage history is the union of all parts.
* **Provenance on every row:** `_source`, `_dataset`, `_ingested_at` (UTC ISO),
  `_run_id`, `_endpoint`.
* **Latest-value is read-time:** `store.latest_view(source, dataset, key_cols)`
  collapses to the most-recently-ingested row per key without mutating the parts.
* **Manifest:** `02_inputs/<source>/_manifest.json` records last run id, status,
  and per-dataset row counts (the parquet-layer analogue of `cmm.db`'s
  `fetch_log`).
* Mixed-type scraped columns are stringified on a write-retry so a raw landing
  never fails on Arrow type inference.

## Running

```bash
cd /Users/sd/PycharmProjects/CMM
python data-ingestion-pipeline/01_raw_fetchers/_run.py --list      # registered sources
python data-ingestion-pipeline/01_raw_fetchers/_run.py imf         # one source
python data-ingestion-pipeline/01_raw_fetchers/_run.py --all       # all batch sources
python data-ingestion-pipeline/01_raw_fetchers/_run.py --realtime  # poll flights/ships/rss
python data-ingestion-pipeline/01_raw_fetchers/_run.py --realtime --once

# fetchers also run standalone, with their own flags:
python data-ingestion-pipeline/01_raw_fetchers/fetch_nbs.py --discover
python data-ingestion-pipeline/01_raw_fetchers/fetch_nbs.py --limit 20   # smoke test
```

Read any dataset back:

```python
import sys; sys.path.insert(0, "data-ingestion-pipeline/01_raw_fetchers")
import _raw_store as store
df  = store.read_dataset("imf", "series")               # all vintages
cur = store.latest_view("imf", "series", ["indicator", "year"])  # newest per key
```

## Sources (14, China-only)

Trade databases (Comtrade/UNCTAD/WITS/WTO/USITC/OECD-TiVA) and pure-EU sources
(Eurostat/Destatis/Bruegel/ECB) are intentionally **excluded**.

| source | what's pulled (raw, in full) | cadence | reachable outside China |
|--------|------------------------------|---------|--------------------------|
| `nbs` | All 8 EasyQuery DBs (national/province/city × annual/quarterly/monthly), every leaf, full history | batch | ✗ (China network) |
| `akshare` | Every no-arg `macro_china_*` interface + curated bond/FX/index, full frames | batch | ✗ (throttles non-CN IPs) |
| `gmd_macro` | Global Macro DB China: ~75 vars, 1640→2030, version-stamped | batch | ✓ |
| `bis` | BIS SDMX China: WS_TC, WS_EER, WS_CBPOL, WS_DSR, WS_SPP (raw CSV) | batch | ✓ |
| `imf` | IMF DataMapper: **all** indicators, China slice, full years | batch | ✓ |
| `yfinance` | China indices/HK names/ETFs/CNY-CNH FX/commodities, `period="max"` | batch | ✓ |
| `ccp_elites` | Sine CPC Elite Leadership xlsx, every sheet raw | batch | ✓ |
| `ministries` | Deep-paginated .gov.cn archives (NDRC/MOFCOM/PBOC/MIIT/…) | batch | ✗ |
| `regulations` | MOFCOM active laws (✗) + NPC Observer bills & events (✓) | batch | partial |
| `dissent` | China Dissent Monitor full event DB + provinces | batch | ✗ (host geo/avail) |
| `academic` | CrossRef China scholarship, full cursor pagination | batch | ✓ |
| `rss` | All entries from every registry feed, per poll | realtime | ✓ |
| `flights` | OpenSky China-bbox aircraft snapshot, per poll | realtime | ✓ |
| `ships` | AIS China-bbox vessel snapshot, per poll | realtime | ✓ |

Each fetcher **reuses** the acquisition/parse helpers already in
`backend/fetchers/*` (tree crawl, feed registry, ministry paginator, ticker list,
SDMX helper) and only widens the scope + redirects storage to parquet.

## Known caveats / to verify on a China-capable network

* `nbs`, `akshare`, `ministries`, `dissent`, MOFCOM half of `regulations` are
  geo-blocked from non-China networks; they are written defensively (failures
  land in `_manifest.json` with `status: error/partial`) and verified by
  construction, but need a live run from inside China to populate fully.
* NBS quarterly period codes (`YYYYA..YYYYD`) follow the documented EasyQuery
  convention; confirm on the first successful live run.

## Scheduling (follow-up)

`_run.py` is launchd-ready but no `.plist` files are added yet (to honour the
"two folders only" constraint). To wire it like the existing `backend/scheduler/`
jobs, point a launchd job's `ProgramArguments` at
`_run.py --all` (daily) and another at `_run.py --realtime` (KeepAlive daemon).
