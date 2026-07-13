# CMM — Master Task List

> **Priority (set 2026-06-01):** make the **database comprehensive first** — fill the
> remaining data sources in §2–§8 below. **Presentation/dashboard (§9) is deferred**
> until the data layer is complete. Models (§10) are medium-term, after the data exists.

---

## ▶ ACTIVE: cmm-serve auto-refresh — zero manual steps (plan, 2026-07-10)

Goal: `cmm-serve` serves from `cmm.db` immediately (two-tier structure kept) and fires
staleness-checked background fetches itself — no launchctl/cmm-fetch needed while it runs.
Design approved by user: built-in scheduler, realtime stays on-request, commodities weekly
fast pass.

- [x] `backend/auto_refresh.py` — daemon thread, 15-min check loop; groups: news 4 h,
      policies 12 h, batch 24 h (tracked in new `auto_refresh_runs` table), commodities 7 d
      (tracked via `data/commodities.json` mtime); one subprocess at a time, most-stale
      first; output → `data/logs/auto_refresh.log`; 2 h per-run timeout.
- [x] `schema.sql`: `auto_refresh_runs` table.
- [x] `run.py`: start scheduler in `main()` (not on api import); added `--no-refresh` escape hatch.
- [x] `api.py`: `/api/refresh/status` — per-group last run / next due / running.
- [x] Tests: staleness logic (temp DB, fake mtime), no real subprocesses — 7 new, 47 total pass.
- [x] Verify: cmm-serve starts, `/` instant, status endpoint live, scheduler fires stale groups.
- [x] README: quick start = just `cmm-serve`; launchd section marked optional. Review here.

### Review (2026-07-10)
Done, TDD (tests written first, watched fail). Live-verified on port 5099: `/` served
in 3 ms and `/api/news` + `/api/commodities` answered instantly **while** the first
news fetch ran; news completed in 60 s, was recorded in `auto_refresh_runs` (ok=1),
status flipped to not-due and the scheduler moved on to policies. Commodities was
correctly judged fresh via file mtime (2 d < 7 d). Two-tier structure preserved:
endpoints always read the DB; fetching is subprocesses (`python -m backend.cli …`)
so a crashed/hung fetcher (2 h timeout) can't take the server down. launchd now
optional (only for fetching while the server is down / realtime daemon); duplicate
runs harmless since fetchers dedupe on insert. Note: `fetch_news` prints a
deprecation warning (realtime daemon also does news hourly) but works fine for this
4 h cadence — that warning predates this change.

---

## ▶ ACTIVE: Commodity Markets — expand copper tracker to all materials (plan, 2026-07-09)

Goal: track **every Table A material** from `materials_unique_index.md` (~47 dashboard
entries; REEs as one basket entry; Kr+Xe combined as in the index) with, per material:
**producing countries, refining countries, traded quantity, and prices.** Autonomous
(user requested); all sources keyless.

**Sources (validated 2026-07-09)**
- Production/refining by country: USGS MCS **2026** single CSV (ScienceBase item
  `69837e43b66b01367d7ec7c7`, 8,886 rows, 127 commodities, years 2024+2025e; mine /
  refinery / smelter stages) + MCS **2025** world CSV (item `6798fd34d34ea8c18376e8ee`,
  adds 2023, wide format). BGS API is dead (404). Copper keeps its static 2020–2022 tail.
- Trade: UN Comtrade public preview (as today), per-material HS codes (6-digit works,
  tested 283691 → 373 rows). Combined X,M calls truncate at 500 rows — keep per-flow.
- Prices: Yahoo daily (HG=F, ALI=F, GC=F, SI=F, PL=F, PA=F); FRED monthly IMF series
  (PCOPP/PALUM/PNICK/PZINC/PLEAD/PTIN/PIORECR + USDM); implied USD/kg unit value from
  Comtrade world exports (value/netWgt) as universal fallback for everything else.

**Tasks**
- [x] `backend/fetchers/materials.py` — registry: slug → name/symbol/category/sourcing
      flag+note, USGS match rules (2026 + 2025 formats), HS codes, FRED id, Yahoo ticker.
- [x] `backend/fetchers/commodities.py` — generalize: USGS CSV parsers (both formats,
      value cleaning W/—/commas, country renames), per-material price fetchers, shared
      per-HS-code trade fetch (incremental/resumable, merges existing JSON), schema v2:
      `{materials: {slug: {…, production.stages, trade_codes}}, trade.commodities}`.
- [x] Copper static 2020–2022 merged in so its history doesn't regress.
- [x] `backend/api.py` — endpoint unchanged (serves blob); docstring update.
- [x] Frontend: material selector (grouped dropdown) driving KPI/prices/production/trade
      sections; conditional rendering for missing blocks; implied-unit-value chart when
      no market price; trade pills per material's HS codes.
- [x] Tests: registry sanity + USGS parser fixtures + merge logic (offline) — 21 new,
      40 total passing.
- [x] Run: fast pass (USGS + prices) done for all 53 materials; background Comtrade
      backfill running (resumable; ~67 codes × 2 flows × 4 years, incremental writes).
- [x] Verify in headless browser both themes; README update; review section here.

### Review (2026-07-09)
Copper tracker expanded to **53 materials** (every Table A entry of
`materials_unique_index.md`; the 10 REEs as one basket entry — no element-level
country data exists; Kr+Xe combined as in the index). Per material: producing
countries + refining/smelting countries (USGS MCS 2026 + 2025 data-release CSVs,
2023–2025e, parsed dynamically — no more hardcoded tables; copper keeps its
static 2020–22 tail), annual world trade quantity+value per HS code
(UN Comtrade, top exporters/importers + trend), and prices (6 Yahoo daily
tickers, 7 IMF-via-FRED monthly series, and an **implied trade price** —
world export value ÷ net weight — for the ~40 materials with no quoted market).
7 materials have a prod_note instead of production data (Hf, Ir, Ru, Ge,
HP-quartz, Cl, Ar — pure byproducts/untracked; note says which host to look at).
Frontend: one material dropdown (6 category optgroups) drives KPIs, prices,
per-stage production charts, and trade — sections render conditionally.
Partial-trade-year heuristic made weight-aware (a value crash with volumes
holding, e.g. cobalt 2023, is a price move, not missing filings).
Verified headless (light+dark, 5 materials, copper/cobalt/lithium spot-checked
against USGS/Comtrade published values); only console error is the
pre-existing `/api/overview` 500 (flagged earlier, unrelated).
**Backfill complete (same day):** all 69 HS codes fetched, 0 failed calls —
every material now has trade data (data/commodities.json, 629 KB). Future
top-ups (new trade years, USGS revisions) via
`python -m backend.runners.fetch_commodities` (add `--trade-only` /
`--no-trade` to scope).

---

## Commodity Markets tab (copper) — port from bridgewater/copper_dashboard — ✅ DONE

Decisions: restyle charts to match CMM tokens; **copy** (leave bridgewater original in place).
Copper data is keyless (Yahoo HG=F, FRED PCOPPUSDM, UN Comtrade, static USGS table).

**Backend**
- [x] `backend/fetchers/commodities.py` — port `fetch_data.py`: `build_data()`, `refresh()` writes
      `data/commodities.json`, `get_commodities_data()` reads it. Stdlib only.
- [x] `backend/api.py` — `/api/commodities` serves the blob ({} if absent).
- [x] `backend/runners/fetch_commodities.py` — CLI runner mirroring the others (LOG_DIR logging).
- [x] Seeded `data/commodities.json` (copied bridgewater's identical-schema data.json; fresh, dated 2026-07-08).

**Frontend (`frontend/index.html`)**
- [x] Tab button `data-panel="commodities"` → "Commodity Markets" + `#panel-commodities`.
- [x] KPI row uses CMM `.cards`/`.card`; Prices/Production/Trade in CMM `.chart-box`; range/commodity via `.pill`.
- [x] Chart-only bits (legend, tooltip, tables) scoped under `#panel-commodities` with `cm-` classes, CMM tokens.
- [x] IIFE (no global leakage) SVG chart engine; palette reads CMM CSS vars; gains green / losses red.
- [x] Lazy-init on first tab activation; resize + `data-theme` MutationObserver redraw.

**Verify** — done via headless Chromium (Playwright in a scratchpad venv, port 5077):
- [x] `/api/commodities` → 200 with valid JSON (1891 daily pts through 2026-07-08).
- [x] Tab renders 4 KPI cards + all 7 chart SVGs; 4 range pills, 2 commodity pills, 7-item mine legend.
- [x] Range/commodity toggles work; renders correctly in BOTH light and dark (screenshots captured).
- [x] No page/console errors from the commodities code; no id or CSS/JS collisions with existing tabs.

### Review
Done. Copper dashboard integrated as a native "Commodity Markets" tab, restyled to CMM tokens
(reuses `.cards`/`.card`/`.chart-box`/`.pill`; charts recoloured to `--blue/--green/--accent2/--purple/--accent`).
Bridgewater original left untouched. Backend follows CMM's fetcher/runner/endpoint pattern; data is a single
`data/commodities.json` blob (no sqlite needed). Refresh anytime with `python -m backend.runners.fetch_commodities`.

**Note (pre-existing, out of scope):** `/api/overview` returns 500 — `sqlite3.OperationalError: no such column:
item_count` in the local `cmm.db`. Unrelated to this work; surfaced during verification. Flag for a separate fix.

---

## 1. Infrastructure — ✅ COMPLETE

Backend/frontend split, single consolidated `data/cmm.db` (47 tables), scheduler on
`cmm-fetch`, storage paths centralised in `backend/storage.py`, dashboard boots and all
endpoints verified 2026-06-01. (Old `scripts/`/`live/`/`feeds.db` layout retired to `.trash/`.)

---

# ▶ FOCUS: make the database comprehensive (§2–§8)

## 2. Data — Trade Flows

- [x] UN Comtrade — bilateral trade, HS6 (`comtrade.py`)
- [x] UNCTAD — bilateral trade + trade-in-services (`unctad.py`)
- [x] WITS/MAcMap — applied bilateral tariff rates 2019–2023 (`wits.py`)
- [ ] BACI (CEPII) — HS6 bilateral flows, 200 countries; one-time bulk download
- [ ] GACC monthly customs — China exports/imports by HS2 + partner; customs.gov.cn *(requires China network)*

---

## 3. Data — Tariffs & Trade Policy

- [x] USITC HTS — US tariffs on Chinese goods (`usitc_hts.py`)
- [x] WTO notifications — MFN and applied rate changes (`wto.py`)
- [ ] EU trade defence measures — anti-dumping/CVDs on Chinese goods; trade.ec.europa.eu
- [ ] UNCTAD TRAINS NTBs — non-tariff barrier notifications by HS chapter
- [ ] MOFCOM export controls / retaliatory tariffs *(requires China network)*

---

## 4. Data — Input-Output Structure

- [x] OECD TiVA / STAN — trade in value added, GVC integration (`oecd_tiva.py`)
- [ ] OECD ICIO tables — inter-industry linkages by country and sector
- [ ] WIOD — 43 countries × 56 sectors; free download; evaluate before GTAP
- [ ] China NBS I-O tables — benchmark 2022; one-time download from NBS
- [ ] GTAP — 141 countries × 65 sectors; ~$2k academic license; defer until WIOD assessed

---

## 5. Data — Global Macro & Fiscal

- [x] BIS — credit aggregates, REER, policy rates (`bis.py`)
- [x] IMF Fiscal Monitor — government revenue, expenditure, primary balance, debt (`imf_fiscal.py`)
- [x] ECB — bank lending survey, balance sheet, yield curve (`ecb.py`)
- [x] Eurostat — EU macro aggregates (`eurostat.py`)
- [x] Destatis — German national accounts (`destatis.py`)
- [x] ILO — labour statistics (`ilo.py`)
- [ ] IMF IFS — quarterly time series (GDP, CPI, trade, BOP) for all countries
- [ ] World Bank WDI — taxes, government spending, revenue; World Development Indicators API
- [ ] OECD.Stat — balance sheets (national accounts), business demography, labour stats

---

## 6. Data — China Macro (DSGE calibration targets)

- [x] NBS — industrial production, PMI, GDP components (`nbs.py`)
- [x] yfinance — CNY/USD spot rate, equity indices (`yfinance_data.py`)
- [ ] PBoC — M2, total social financing, bank loans; pboc.gov.cn
- [ ] PBoC BOP — quarterly current account; pboc.gov.cn
- [ ] SAFE — monthly FX reserves; safe.gov.cn
- [ ] CFETS RMB basket index — weekly; pboc.gov.cn

---

## 7. Data — Microdata (low priority, license required)

- [ ] ECB HFCS — household finance and consumption survey; apply via ECB Research Data Centre
- [ ] Compustat — firm and bank balance sheets / income statements; institutional license

---

## 8. Data — Freight & Shipping

- [ ] SCFI — weekly Shanghai containerised freight index; sse.net.cn
- [ ] Baltic Dry Index — daily; free via FRED
- [ ] Shanghai Port TEU throughput — monthly; sipg.com.cn

---

# ⏸ DEFERRED until data is comprehensive

## 9. Presentation / Dashboard

- [ ] Eurostat tab — run `eurostat.py` fetcher first, then build UI
- [ ] energy-monitor idea (from `ideas/`) — investigate feasibility

---

## 10. Models (medium-term — after the data layer is complete)

- [ ] MT-1: Trade policy model — NQTM/KITE-style (BACI + WITS + WIOD inputs)
- [ ] MT-2: China macro model — NK-SOE DSGE (PBoC rule, dual labour, LGFVs; Dynare vs Python TBD)

---

# 11. Ministry policy-scraper swarm → policy_docs core table (plan, 2026-07-07)

Goal: scrape policy announcements from major Chinese government ministries into a single
AI-analysis-ready table in `data/cmm.db`, with **full document text** (the existing
`ministry_scraper` captures only title/link/date into per-ministry tables).

## Design (autonomous — user requested full autonomy)
- **Approach:** extend the proven `backend/fetchers/ministry_scraper.py` list scraper
  (not a rewrite): two-stage pipeline.
  - *Stage 1 — discovery:* paginated list scrape (existing code) → upsert metadata rows
    into new `policy_docs` table, `fetch_status='pending'`.
  - *Stage 2 — content swarm:* new `backend/fetchers/policy_content.py` — thread pool
    parallel **across domains**, serial + delayed **within a domain** (polite); generic
    gov.cn article extractor (TRS_Editor / #zoom / .article-content / largest-block
    fallback); extracts 文号 (doc number) + full text → updates row.
- **Core table `policy_docs`** (cmm.db): ministry, source, title, url UNIQUE, doc_number,
  doc_type, category, published, summary, full_text, text_len, fetch_status
  (pending/ok/error/skip), http_status, error, fetched_at, content_fetched_at.
- **New runner** `backend/runners/fetch_policies.py` (`cmm-fetch policies`), flags:
  `--discover-only`, `--content-only`, `--limit N`, `--ministry SLUG`, `--full`.
- **Expand TARGETS** with major ministries currently missing (verify live first):
  MOHRSS, MOE, MOT, MOHURD, MNR, MWR, NHC, MCA, MOJ, MEM, NHSA, MCT, STA (tax),
  PBOC 条法司 policy files, State Council 政策文件库. (NFRA/GAC/MPS known blocked.)
- **Tests:** `tests/` with saved-HTML fixtures for extractors + parsing; live smoke
  test opt-in via marker.

## Tasks
- [x] Verify live accessibility of candidate new ministry list pages; add working ones to TARGETS
      (MEM ×2 + MCT ×2 added; MOHRSS/MOE/MOT/MOHURD/MNR/MWR/NHC/MCA/MOJ/NHSA/STA/MOD
      geo-block external traffic — noted in TARGETS comments)
- [x] `policy_docs` schema in schema.sql + storage helpers
- [x] `policy_content.py`: article text + doc-number extractor + domain-aware swarm fetcher
- [x] `fetch_policies.py` runner + CLI wiring (`cmm-fetch policies`) + launchd plist (12 h)
- [x] Tests (fixtures + unit) passing — 17 tests, tests/test_policy_pipeline.py
- [x] Live iteration: discovery run + content sample run; per-source success report; fix worst extractors
- [x] Review section + README update

## Review (2026-07-07)
Full production run: **5,021 documents discovered** across 26 reachable sources;
first 3,000 content-fetched → **2,656 full texts (~5.3 M chars)**, 400 `empty`
(verified legitimate: NDRC 一图读懂 infographics, MEE 视频丨 video news, PDF-wrapper
budget pages), 1 binary, 1 error. Remainder drained by a follow-up `--content-only`
run; future top-ups via `cmm-fetch policies` (12 h launchd plist added — needs
`launchctl load`, and plists still carry old `/Users/sd/` paths).
Per-ministry (post-run): ndrc 2367, mfa 1000, mof 519, mem 500, mee 403, mct 63,
mofcom 61, moa 49, nea 45, gov 14.
8 sources failed discovery with timeouts this run (MIIT/SAMR/PBOC/CSRC/SAFE/MOST/
CAC/SASAC) — intermittent, retried on every incremental run.
AI-analysis entry point: `SELECT title, doc_number, published, full_text FROM
policy_docs WHERE fetch_status='ok' AND ministry='ndrc' ORDER BY published DESC`.

**Fixed along the way:** pre-existing link-resolution bug in `ministry_scraper._extract_articles`
(relative hrefs joined against domain root instead of list-page URL → dead links like
`ndrc.gov.cn/202606/t...` for every page-relative source; historical per-ministry tables
contain such dead links). Also: JS `document.write` pagination support (MEM-style,
`index_N.shtml`), titles with glued dates cleaned.

**Added (2026-07-08): `instrument_type` column.** `policy_docs.instrument_type` classifies
each doc from its title into 通知/公告/令/意见/法/办法/规划/答问/报告/… via
`storage.classify_instrument()` (rightmost-longest token match; strips trailing 文号 parens;
bare 法 only at title-end so 法治/法规 don't misfire). Applied at insert time and backfilled
over all 5,037 rows (`storage.backfill_instrument_types()`); migration auto-adds the column +
index in `get_policy_docs_db()`. Distribution: 答问 1204, 通知 1033, (news/other) 913,
公告 704, 令 200, 意见 198, 办法 178, … 法 50 (all genuine 中华人民共和国…法). Tests added
in `tests/test_policy_pipeline.py` (19 pass). Query e.g.
`SELECT title, published FROM policy_docs WHERE instrument_type='令' ORDER BY published DESC`.

---

# 12. Ingest pycharm_archive + Zombies data into cmm.db (plan, 2026-06-03)

Goal: pull as much *primary/curated* data as possible from `~/Documents/pycharm_archive/`
(includes the Zombies projects) into the single `data/cmm.db` — **without** creating duplicates.
Synergy: the archive's `data_raw/` already holds **WB WDI/WGI/DBI, OECD PISA, ILO, IMF findex,
PWT 11.0** — directly ticks several §5 TODOs.

## Two hard constraints found during recon
1. **iCloud-offloaded.** Every large file is a dataless placeholder (`blocks=0`). The tree is
   **82 GB logical, ~2 GB resident**. Must `brctl download` before reading; downloads are
   currently slow/stalling (needs iCloud online, possibly Finder "Download Now").
2. **2.6 GB free disk** (volume 82% full). Can't materialize 82 GB at once.
→ Ingestion must be **streaming** (download→ingest→`brctl evict`, disk-budget aware) and
   **selective** (curated/primary only; never the 82 GB of figures/outputs/caches/dupes).

## Principles
- One DB only (`storage.get_conn()` → `data/cmm.db`); no new .db files.
- Namespacing so nothing collides with native tables: `repo_*` (from central_repo.db),
  `zomb_*` (zombie/firm/bank/tribunal panels), `tfp_*` (TFP & city/province), `ext_*` (raw
  reference: WB/OECD/ILO/PWT/Ember/IMF).
- Provenance cols on every import: `_src_file`, `_src_sheet`, `_ingested_at`.
- Dedup via `ingest_manifest(src_path, sha256, size, table, n_rows, ingested_at, status)` —
  re-runs skip already-loaded files; overlaps with native tables reconciled, not duplicated.
- Snapshot `cmm.db` → `cmm.db.pre_ingest.bak` first; all imports additive + namespaced (reversible).

## Phases
- [ ] **P0 Infra:** confirm iCloud downloads progress; add `backend/ingest/` (materialize.py,
      manifest.py, loaders.py); create `ingest_manifest`; snapshot cmm.db.
- [ ] **P1 Inventory:** walk both trees, classify each data file primary/curated/intermediate/noise
      into `ingest_manifest`; **user reviews keep-list before bulk download.**
- [ ] **P2 central_repo.db (1.13 GB, crown jewel):** download → ATTACH → list tables/schemas →
      copy non-overlapping tables as `repo_*` (reconcile macro/WB/BIS/trade overlaps) → evict.
- [ ] **P3 Curated panels:** Zombies_fixed/data/aggregated/*.parquet → `zomb_*`;
      firm_bank_year_panel → `zomb_firm_bank_year`; TFPZombies/data/*.csv → `tfp_*`;
      central_cities_panel + regional_rd_panel_clean → `tfp_*`.
- [ ] **P4 Raw sources:** data_raw keepers → `ext_*` (WB, OECD PISA, ILO, IMF findex, PWT 11.0,
      Ember, GEM, EPLEX, zombie_data*.xlsx, Datasets/China/Zombies/2021–2024.xlsx);
      data_processed pickles (all_tfp, zombie_fin, global_macro, tfp components) → `tfp_*`/`zomb_*`.
      Check merged.csv / yombie2_dataframes.pkl for unique cols before ingesting (likely derived joins).
- [ ] **P5 Reconcile:** cross-check `ext_*` vs native (global_macro, bis_*, imf_fiscal, china_tariffs);
      drop true overlaps, keep new coverage; collapse near-identical city panels.
- [ ] **P6 Verify:** per-table row/null report + 3-value spot-checks vs source; confirm get_conn()
      + dashboard boot; update README inventory; review section here; delete this section when done.

## NOT ingested
`__pycache__`, `.venv`, `.git`, `.idea`, `.opencode`, `*backup*`, `*trash*`, `_archive`,
`outputs/`, `figures/`, `Graphs/`, `Heatmaps/`, `*.png/.html/.docx/.pdf/.pptx`, regression
result dumps, duplicate xlsx of CSVs already ingested.

## Decisions needed before P2
- Scope: curated-only vs include raw reference (`ext_*`)?
- If iCloud keeps stalling: you pre-download keep-list folders in Finder, or I run a retry loop.

---

## Open items carried over from archived June tree (added 2026-07-13)
- [ ] Policy Event Calendar
- [ ] Detailed 15-year tracker
- [ ] Trade simulations using KITE (requires R + KITE package from Hinz, Mahlkow, Wanner — see backend/models/KITE/)

### Review — archive reconciliation (2026-07-13)
Merged the stranded June 4–26 work from ~/Documents/pycharm_archive/PycharmProjects/CMM
into the active repo. Two commits: 21cbb20 (July work safety commit), 8b7ed22 (June
restore: FYP tab, competitiveness, chartbook, eurostat-trade, KITE notes, research dirs).
Three-way merged index.html/api.py/cli.py/README (base 9f2ce5b); dashboard stays at
frontend/index.html. Verified live: all 11 tabs + Chartbook link served, /api/fyp/tech,
/api/competitiveness/*, /api/chartbook/*, /chartbook all return data; July endpoints
(/api/commodities, /api/refresh/status) intact; 47 tests pass; inline JS parse-checked.
