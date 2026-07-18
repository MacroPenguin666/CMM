# CMM — Master Task List

> **Priority (set 2026-06-01):** make the **database comprehensive first** — fill the
> remaining data sources in §2–§8 below. **Presentation/dashboard (§9) is deferred**
> until the data layer is complete. Models (§10) are medium-term, after the data exists.

---

## ▶ ACTIVE: Overview macro strip — always-current key macrodata widgets (plan, 2026-07-15)

Goal: top strip of small widgets on the Overview tab — GDP growth (quarterly), CPI/PPI,
exports/imports, gov deficit, gov debt & repayments, GDP composition, SHIBOR — each with
an adjustable timeseries and a highlighted latest value, kept current by auto-refresh.
Design approved by user (placement: Overview top strip; new akshare fetchers + aggregated
endpoint; global range selector with per-widget override).

**Data map (verified in cmm.db 2026-07-15):** SHIBOR daily current (financial_series);
CPI/PPI/trade monthly via bruegel_series through 2026-05 (fallback); akshare CPI/trade
fetchers stalled Sep 2025 (jin10-calendar endpoints died) — replace with NBS/eastmoney
tables; no quarterly GDP anywhere → new fetcher; MOF fiscal_national_monthly (gpb_rev/
gpb_exp/debt_interest_exp, cumulative YTD, through 2026-05); annual macro_series
(gen_govdebt_GDP incl. forecast years, cons/inv/exports/imports _GDP shares);
fiscal_maturity = LGB principal due per year (全国 aggregate).

### Task 1 — new/fixed akshare fetchers (backend/fetchers/financial.py)
- [ ] Tests first (tests/test_macro_dash.py): parse fixture DataFrames (monkeypatch `ak`)
      → rows/snapshots for: `GDP_YoY` (quarterly, ak.macro_china_gdp), `CPI_YoY`
      (monthly, ak.macro_china_cpi), `PPI_YoY` (monthly, ak.macro_china_ppi),
      `Exports_YoY`/`Imports_YoY` (monthly, ak.macro_china_hgjck customs table,
      replacing the dead jin10 fetch_trade)
- [ ] Implement fetch_gdp_quarterly / fetch_cpi_yoy / fetch_ppi_yoy / new fetch_trade;
      register in fetch_all_financial() (auto-refresh "batch" group then covers them —
      no auto_refresh.py change needed); rows use existing (indicator, category, date,
      value, unit) convention, full history not .tail(36)
- [ ] Run `cmm-fetch financial` once for real; verify new indicators land in
      financial_series with current dates

### Task 2 — widget registry + payload (backend/fetchers/macro_dash.py, new)
- [ ] Tests first: seeded temp sqlite DB → build_payload(conn) returns 8 widgets:
      gdp_yoy (Q), cpi_ppi (M, 2 lines, bruegel fallback when financial_series empty),
      trade (M, exports+imports yoy), fiscal_balance (M: YTD cum. gpb_rev−gpb_exp per
      month across years + latest annual gen_govdef_GDP actual as secondary stat),
      debt (A: gen_govdebt_GDP actuals only, forecast years excluded + debt_interest_exp
      YTD secondary), repayments (A: fiscal_maturity 全国 principal due by year),
      gdp_comp (A: cons_GDP, inv_GDP, net exports = exports_GDP−imports_GDP),
      shibor (D: all 6 tenors, default 3M)
- [ ] Payload shape per widget: {key, title, unit, freq, series:[{name, points:[[date,v]]}],
      latest:{date, value}, secondary?:{label, value, date}} — full history (frontend
      slices ranges client-side, no refetch)
- [ ] Implement WIDGETS registry + build_payload

### Task 3 — API endpoint (backend/api.py)
- [ ] Test: flask test client GET /api/overview/macro → 200, all 8 widget keys present
- [ ] Implement route (single get_db-style connection, jsonify payload)

### Task 4 — frontend macro strip (frontend/index.html)
- [ ] Read dataviz skill before chart code
- [ ] CSS + HTML: `macro-strip` responsive card grid at top of panel-overview (above
      map+sidebar); remove sidebar "Financial Snapshot" section (replaced by strip)
- [ ] JS: loadMacroStrip() → one fetch; per card: title, big latest value + date +
      colored Δ vs previous point, small Chart.js line; global 1Y/3Y/5Y/10Y/Max selector,
      per-widget override; SHIBOR tenor toggle; register charts for theme-switch update
- [ ] Verify headless (light+dark, no console errors) + visual check

### Task 5 — verification + close-out
- [ ] pytest full suite; serve dashboard, confirm strip renders with live data and
      latest values match DB; README dashboard section touch-up if needed
- [ ] Review section here; delete plan when done

## FYP Domestic Demand subtab — outline ingest + interactive cockpit — ✅ DONE (2026-07-14)

Goal: (1) store the official 15th FYP outline (十五五规划纲要, gov.cn 2026-03-13) full text
in `policy_docs` via the existing pipeline, reproducibly (seeded, so bootstrap gets it too);
(2) update the Domestic Demand subtab so it captures every point Part V (Ch 15–17:
大力提振消费 / 扩大有效投资 / 全国统一大市场) makes, with a source link. Plan approved.

- [x] `ministry_scraper.py`: `SEED_DOCS` constant (landmark docs not on list pages)
- [x] `fetch_policies.py`: seed insertion in `discover()` (idempotent via INSERT OR IGNORE)
- [x] Run `--ministry gov`, verify outline row fetch_status=ok with chapter headings in full_text
- [x] Gap analysis: enumerate Ch 15–17 节-level points from stored text vs current subtab copy
- [x] `frontend/index.html`: `demand` entry gets `points` + doc link; generic "What the plan says" card in `fypRenderPanels()`
- [x] Verify: idempotent re-run, pytest tests/test_policy_pipeline.py, dashboard render check

### Review (2026-07-14)
Done and verified live. The full 15th FYP outline (60,175 chars, all 18 篇, TOC + body)
is now in `policy_docs` (**id 7680**, ministry `gov`, source "State Council — Landmark
Documents", fetch_status ok) via a new `SEED_DOCS` mechanism in `ministry_scraper.py` —
landmark docs that never appear on scraped list pages, inserted by `discover()` each run
(INSERT OR IGNORE on unique url; re-run confirmed 0 new). Bootstrap-from-scratch now
ingests it too, since `bootstrap_db.py` runs the policies runner.

Gap analysis against the stored Part V text (第五篇 建设强大国内市场, Ch 15–17, 3,352 chars):
the old subtab captured only the consumption/GDP headline. Now the subtab carries a
"What the plan says" card with all **10 节-level points** (Ch 15: consumption capacity /
service consumption / goods consumption / consumption environment; Ch 16: government
investment returns / private investment / investment–consumption loop; Ch 17: base
institutions / fair competition / market infrastructure), each written from the stored
Chinese text (not press summaries — e.g. "内卷式竞争" claimed by web coverage is NOT in
Ch 17 §2 and was left out). Desc now cites the plan's actual framing (战略基点, 投资于物和
投资于人) and the Ch-3 headline objective 居民消费率明显提高. Source link → gov.cn full
text; renderer is generic (`p.points`) so other subtabs can be filled in later.
Verified: 48/48 tests pass; headless Chromium — card renders in light+dark, all 10
points + link present, other subtabs unchanged, no console errors.

### Review — interactive cockpit (same day, user request)
The static card was replaced by a **radial cockpit like Tech Self-Reliance**: hub
(headline 居民消费率 objective) + all 10 sections as clickable chapter-colored nodes
(Ch15 amber / Ch16 blue / Ch17 purple), zoom + slide-in sidebar per node.
- `backend/fetchers/fyp_demand.py` (new): SECTIONS registry (10 节 — en/cn names,
  glyph, plan-text points moved here from the frontend, per-section doc keywords),
  `related_docs()` over policy_docs, `build_payload()` (macro series capped at 2025
  to cut GMD projections).
- `backend/api.py`: `GET /api/fyp/demand` — sections+points, related policy docs,
  and GMD series (hcons_GDP / inv_GDP / unemp / govexp_GDP).
- `frontend/index.html`: stage mechanics factored into `fypMakeStage(prefix, opts)`
  (tech converted, behavior identical; single global Esc handler); demand engine
  (`fypLoadDemand`, `fypdHubPanel`/`fypdSectionPanel`, Chart.js line charts). Hub
  sidebar: 2030-objective banner, 41.0%-of-GDP KPI chip, consumption-vs-investment
  chart (1980–2024), preamble, key documents. Section sidebars: plan points, live
  series where held (cap→unemp, gov_inv→govexp_GDP, priv_inv→inv_GDP), related
  policy documents with ministry/date chips. Static "What the plan says" card and
  its generic renderer removed (superseded); demand's Data & Trackability collapsed
  into <details> like tech.
- Tests: `tests/test_fyp_demand.py` (registry sanity, related_docs filtering/order/
  limit, payload shape) — 52/52 pass.
Verified live (headless Chromium, port 5001): 15 functional checks pass — hub+10
nodes, zoom/sidebar/Esc/close, goods sidebar shows 首发经济 points + 6 real doc
links (NDRC 以旧换新 notices, MOFCOM auto-consumption briefing), hub chart renders,
**tech subtab regression-checked** after the factory refactor (6 nodes, semi panel,
4 charts, Esc); light+dark screenshots; no local request failures.

---

## Demand cockpit v2 — per-section policies, goals, status-vs-data — ✅ DONE (2026-07-14)

Each of the 10 demand sections gets: implementing-policy milestones (dated, sourced),
concrete targets (tech-style cards incl. max_level/trend), and live status series
(bruegel_series/macro_series — no new fetchers). Plan approved.

- [x] Research pass: verify every seeded number + source URL (policy_docs texts + web) —
      retail 47.15/48.79/50.12万亿 (23/24/25) → 60万亿 target; services share 42.6 (2020) →
      46.1 (2024) = 46.1 (2025, flat); private-FAI share 50.1 (2024) → ~49.7 (2025, −6.4% yoy);
      logistics/GDP 14.4→14.1→13.9 vs 13.5 (2027); negative list 151→117→106; unemployment
      2026-06 5.0% vs ≤5.5% target; trade-in bonds 1500→3000→2500亿 (+625亿 first 2026 batch);
      育儿补贴 3600元/90bn; 职业伤害保障试点扩围 2025-07; 离境退税 500→200 + 即买即退 (+367% users);
      民营经济促进法 2025-05-20 + nuclear stakes to 20%; 实施办法 66 situations; AUCL rev 2025-06;
      新型政策性金融工具 5000亿 deployed 2025-10-31 (incl 消费基础设施); 专项债自审自发 (国办 2024-12);
      一单制一箱制 (2023-08; multimodal +15.6%/+16.5% 2024); GDP 2025 +5.0% = income +5.0%
- [x] `fyp_demand.py`: MILESTONES / TARGETS / STATUS_SERIES / FACTS + reading logic + payload
- [x] `frontend/index.html`: fypdTargetCard (min/max/trend), sidebar reorg, monthly charts
- [x] Tests: registry sanity, reading logic, payload shape
- [x] Verify: pytest, payload curl, headless (goods 60tn card, infra 13.5%, priv_inv trend, tech regression, themes)

### Review — v2 (2026-07-14)
Every demand section now answers "what's been done and where does it stand":
- **Backend** (`fyp_demand.py`): 26 curated policy milestones (all dated + officially
  sourced; one deliberately open — 统一大市场建设条例 pending), 7 targets joined to data
  with a testable `target_reading()` (min_level/max_level/trend; on/off/mixed/met/n-a
  with pace notes), 17 static fact chips, and live monthly status series pulled from
  the auto-refreshed `bruegel_series` (retail/car/box-office/flights/FAI/PPI, trimmed
  ≥2019, monthly last-obs; tolerant of a missing table on fresh bootstraps).
- **Readings computed from the data** (server-side, unit-tested): retail 60万亿 →
  n/a (baseline year; needs ≈3.7%/yr), 居民消费率 → n/a (no post-2024 GMD reading),
  services share → **mixed** (flat at 46.1%), private-FAI share → **off track**
  (50.1→49.7, private FAI −6.4% in 2025), negative list → on track (151→106),
  logistics/GDP → **on track** (14.1→13.9 vs 13.5 by 2027, 33% gap closed at 33%
  elapsed), unemployment → met (5.0% vs ≤5.5%).
- **Frontend**: sidebars reorganised to Where-it-stands (target cards with progress
  bars/trend arrows + reading badges, fact chips, live chart) → What's-been-done
  (milestone timeline with links) → plan text (collapsed) → related docs; hub gets
  the retail target card + 58.8% contribution chip. `fypdLineChart` generalised to
  monthly date series with label-union.
- **Verified**: 82/82 tests; payload 96 KB; 18 headless checks pass (incl. tech
  regression + dark theme); on-screen readings cross-checked against DB/source values.

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

# 13. FYP "Fiscal Capacity" subtab — self-updating fiscal-space assessment (spec + plan, 2026-07-14)

Goal: 9th FYP subtab (**Fiscal Capacity**) that renders a *computed, constantly-updating
assessment* — not tables — of China's fiscal space at the national, provincial and
(via the cascading lens) municipal level, answering three questions per level:
**(a)** how much money is available (trailing cash flows + cash on hand + unused quota),
**(b)** how repayment pressures are developing, **(c)** how funding costs are developing.
Approach 2 approved by user: full live-fetcher build, maximum public depth.

## Methodology (anchored to two references)
- **GS "China H2 Fiscal Outlook" (2025-08-06), Exhibit 16 recipe** → Gauge A
  ("augmented fiscal balance lite", 12mma % of GDP, center/local split):
  effective on-budget deficit (expenditure − revenue, general budget — NOT the official
  deficit) + LGSB + CGSB + policy-bank bonds + PSL + trust loans + **net** land-sales
  financing (documented net-share assumption; gross overstates ~3×) + debt-resolution
  bonds. LGFV bond net issuance = curated estimate (only WIND-sourced item; ≈0 since
  2024 per GS). Plus **fiscal-space-remaining tracker**: cumulative bond issuance as %
  of full-year quota (excl. refinancing-for-swap; vs 2020-24 average pace) + fiscal
  deposits at PBOC (level + yoy).
- **ADB Fiscal Rules paper (EAWP 251113) descriptive framework** → Gauges B & C
  (not in the GS brief; deliberate extension): interest/revenue as the pressure gauge,
  special-debt share, LGB maturity wall (12/24/36 m, by province, from bond registry),
  refinancing share of gross issuance; funding costs via daily close curves — CGB 10Y,
  LGB(AAA) spread over CGB, CP/MTN(AA) credit spread as public chengtou/LGFV proxy —
  with z-scores vs 3-y history. Provincial dispersion is the point (debt/GDP vs income,
  12 restricted provinces flagged); municipal tier = cascading lens (province→city
  transmission: transfer pass-through, LGFV footprint, swap tracker), no fake city data.
- **Assessment block** at top: per level (center / localities / worst-decile provinces)
  an auto-generated timestamped verdict sentence + traffic light from fixed documented
  thresholds over the three gauges. Drill-down panels below (vertical gap since 1994,
  funding channels, provincial cross-section scatter+heat strip, cascading panel,
  center's balance sheet vs peers). No GDP-impact multiplier (GS-proprietary; false
  precision) — show ΔAFD only.

## Sources (all probed live 2026-07-14)
- ✅ MOF 财政收支 monthly (mof.gov.cn/zhengwuxinxi/caizhengshuju/) — national +
  central/local general budget, tax detail, gov-fund budget (land). Reachable.
- ✅ MOF 地方政府债券发行和债务余额 monthly — issuance, stock, general/special,
  new/refinancing, avg rate & maturity. Reachable (same portal).
- ✅ MOF final accounts (yss.mof.gov.cn) — annual 31-province revenue/expenditure/
  transfers. Reachable.
- ✅ NBS yearbook (www.stats.gov.cn/sj/ndsj/) — provincial GDP/pop denominators.
  Reachable. (data.stats.gov.cn easyquery API is geo-blocked — do NOT use.)
- ✅ AKShare validated calls: `macro_china_czsr` (monthly fiscal revenue, data through
  2026-05), `bond_china_close_return` (curves: 国债 ✓, 地方政府债(AAA) ✓ CYCC84A,
  中短期票据(AA) ✓; full 75-curve map via `bond_china_close_return_map`),
  `macro_china_shrzgm` (TSF components), `macro_china_central_bank_balance`
  (fiscal/government deposits), `macro_china_national_tax_receipts`.
- ✅ ChinaMoney bond registry JSON (`/ags/ms/cm-u-bond-md/BondMarketInfoListEN`,
  398k bonds, pageable) — LGB maturity schedule by province; LGFV issuers visible.
  bondType filter codes need discovery (list view works; detail via bond_info_*_cm).
- ✅ chinabond.com.cn + yield.chinabond.com.cn reachable (upgrade path: true 中债城投
  curve API — best-effort).
- ❌ Blocked (don't re-probe): CELMA, data.stats.gov.cn, cninfo LGB endpoint
  (JSONDecodeError — needs POST/token work, treat as best-effort).
- Curated (`fiscal_reference`, honestly flagged in UI): LGFV interest-bearing-debt
  estimates by province (Shih-Elkobi / IMF Art. IV), 12 restricted provinces, annual
  bond quotas (NPC budget reports), VAT-split history (75/25→50/50 2016), swap-program
  params (2024-28 RMB10-12tn), net-land-share assumption, AFD thresholds.

## Storage (cmm.db, schema.sql)
`fiscal_national_monthly` (budget + fund accounts, central/local), `fiscal_lgb_monthly`
(issuance/stock/rates), `fiscal_maturity` (LGB principal due by province × year, rebuilt
from registry pages), `fiscal_curves_daily` (curve, tenor, yield), `fiscal_monetary_monthly`
(TSF components, fiscal deposits, PSL), `fiscal_province_annual` (rev/exp/transfers/GDP/
debt/interest), `fiscal_reference` (curated key-value + per-province). Gauges computed at
read time in `backend/fetchers/fiscal_assess.py` (pure functions over tables → testable).

## Tasks
- [x] `schema.sql`: 7 fiscal tables + storage helpers
- [x] `backend/fetchers/fiscal_china.py`: MOF monthly 收支 scraper (article-page parser,
      zh headers per ministry_scraper conventions; per-source failure isolation)
- [x] MOF LGB monthly debt-report parser (same module)
- [x] AKShare sub-fetchers: czsr, 3 curves, shrzgm, central-bank balance (fiscal deposits),
      tax receipts
- [x] ChinaMoney registry pager → `fiscal_maturity` (discover LGB bondTypeCode; polite
      paging; province from issuer name)
- [x] Annual: MOF final-accounts tables + NBS yearbook provincial fetch
- [x] `fiscal_reference` seed data (curated, cited inline)
- [x] `fiscal_assess.py`: AFD-lite, quota tracker, gauges A/B/C, z-scores, thresholds →
      traffic lights + verdict sentences (fixed rules, no LLM)
- [x] `fetch_batch.py` group `fiscal` + auto_refresh cadence (daily curves, monthly MOF,
      annual accounts)
- [x] `api.py`: `GET /api/fiscal` (assessment + national + provinces + center + meta
      freshness)
- [x] Frontend: 9th `FYP_PRIORITIES` entry + assessment block + drill-down panels
      (dataviz skill; light+dark)
- [x] Tests: parser fixtures (saved MOF HTML), assessment threshold unit tests,
      registry-pager mock
- [x] Live verify: full fetch run, endpoint check, headless browser both themes; README;
      review section here

### Review (2026-07-15)
Built and live-verified, TDD throughout (102 tests pass; every parser watched fail
first against saved fixtures in tests/fixtures/fiscal/). The subtab is the 9th FYP
tab ("Fiscal Capacity") and renders a computed assessment, not tables: three
traffic-light verdict cards (flow space / repayment pressure / funding costs) with
auto-generated sentences, KPI chips, six charts, the 31-province cross-section
(scatter + table), the cascading/municipal lens card, and a Sources & method
disclosure with thresholds and citations.

**Live numbers at first build (data through 2026-05):** effective deficit 9.4% of
GDP (T12M, both accounts), AFD-lite 11.0% (GS's own 12mma was 11.3% mid-2025 —
sanity ✓), land revenue −28.7% yoy, refinancing share 61% of 2026 issuance (vs 48%
in 2025) → repayment verdict red, 92.6% of the NPC debt limit used, fiscal deposits
RMB 6.0tn (+7.9%), CGB 10Y 1.74% / LGB AAA +8bp. Provincial interest burden ranks
贵州/吉林/甘肃/天津 heaviest — matches the known stress cases; provincial annex sums
cross-check national totals to <0.1%.

**Sources that turned out better than spec'd:** the LGB monthly reports attach a
PDF annex (附表) with BY-PROVINCE issuance (new/refi × general/special) and debt
service (principal + interest, monthly + YTD) — parsed positionally with pdfplumber
(no ruling lines; 合计 row anchors the column grid). MOF moved the LGB series to the
new 债务管理司 site (zwgls.mof.gov.cn, 2025-12→current); yss.mof.gov.cn carries
history. CGB curve history comes in bulk from EastMoney (bond_zh_us_rate, 3y daily).

**Known gaps (documented in the UI):** (1) provincial own rev/exp has NO public
machine-readable source — NBS yearbook tables are JPGs since ~2021, provincial
communiqués are geo-blocked; carried as have:false. (2) LGFV stocks + true chengtou
curve are WIND-only → curated entries in fiscal_reference with citations (AA credit
curve is the live proxy). (3) ChinaMoney rate-limits hard: the 18k-bond registry
backfill accretes over runs (resumable; ~3.9k done at review time), credit-curve
history accretes ~6 windows/run backward to 3y; maturity-wall verdicts stay
suppressed below 50% registry coverage, z-scores label their history depth.
(4) LGB annexes: months re-ingest via retry loop after a clustering bug fix
(rows whose label/numbers straddle a bucket boundary — proximity clustering now,
with a provincial-sums-vs-national regression test).

Wired into fetch_batch as group `fiscal` (daily via auto-refresh batch), runner
`python -m backend.runners.fetch_fiscal` (--full / --registry-backfill), endpoint
`/api/fiscal`. DB concurrency: fiscal connections set busy_timeout=60s and the
registry commits per insert (a transaction across a network call had write-locked
cmm.db for minutes).

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
