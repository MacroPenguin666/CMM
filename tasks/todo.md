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
