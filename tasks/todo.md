# CMM — Master Task List

> **Priority (set 2026-06-01):** make the **database comprehensive first** — fill the
> remaining data sources in §2–§8 below. **Presentation/dashboard (§9) is deferred**
> until the data layer is complete. Models (§10) are medium-term, after the data exists.

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

# 11. Ingest pycharm_archive + Zombies data into cmm.db (plan, 2026-06-03)

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
