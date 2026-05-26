# CMM — TODO

---

## Short-term: Aggregate Data

Goal: get all required data into the database before building models.

### Trade Flows
- [x] UN Comtrade — bilateral trade, HS6 (`comtrade.py`)
- [x] UNCTAD — bilateral trade + trade-in-services (`unctad.py`)
- [x] WITS/MAcMap — applied bilateral tariff rates, 2019–2023 (`wits.py`)
- [ ] BACI (CEPII) — HS6 bilateral flows, 200 countries; one-time bulk download
- [ ] GACC monthly customs — China exports/imports by HS2 + partner; scrape customs.gov.cn

### Tariffs & Trade Policy
- [x] USITC HTS — US tariffs on Chinese goods (`usitc_hts.py`)
- [x] WTO notifications — MFN and applied rate changes (`wto.py`)
- [ ] EU trade defence measures — anti-dumping/CVDs on Chinese goods; trade.ec.europa.eu
- [ ] UNCTAD TRAINS NTBs — non-tariff barrier notifications by HS chapter
- [ ] MOFCOM export controls / retaliatory tariffs — requires China-network access

### Input-Output Structure
- [x] OECD TiVA / STAN — trade in value added, GVC integration (`oecd_tiva.py`)
- [ ] OECD Input-Output tables — inter-industry linkages by country and sector; OECD ICIO tables
- [ ] WIOD — 43 countries × 56 sectors; free download; evaluate before buying GTAP
- [ ] China NBS I-O tables — NBS benchmark (latest: 2022); one-time download
- [ ] GTAP database — 141 countries × 65 sectors; ~$2k academic license; defer until WIOD is assessed

### Global Macro & Fiscal
- [x] BIS — credit aggregates, REER, policy rates (`bis.py`)
- [x] IMF Fiscal Monitor — government revenue, expenditure, primary balance, debt (`imf_fiscal.py`)
- [x] ECB — bank lending survey, balance sheet items, yield curve (`ecb.py`)
- [x] Eurostat — EU macro aggregates (`eurostat.py`)
- [x] Destatis — German national accounts (`destatis.py`)
- [x] ILO — labour statistics (`ilo.py`)
- [ ] IMF IFS — quarterly time series of economic aggregates (GDP, CPI, trade, BOP) for all countries; imf.org/en/Data
- [ ] World Bank — taxes, government spending, government revenue; World Development Indicators API
- [ ] OECD — balance sheets (national accounts), business demography, labour statistics; OECD.Stat API

### China Macro (DSGE calibration targets)
- [x] NBS — industrial production, PMI, GDP components (`nbs.py`)
- [x] yfinance — CNY/USD spot rate, equity indices (`yfinance_data.py`)
- [ ] PBoC — M2, total social financing, bank loans; pboc.gov.cn
- [ ] PBoC BOP — quarterly current account; pboc.gov.cn
- [ ] SAFE — monthly FX reserves; safe.gov.cn
- [ ] CFETS RMB basket index — weekly; pboc.gov.cn

### Microdata — Firms & Households
- [ ] ECB HFCS — Household Finance and Consumption Survey; microdata on consumption, wealth, and debt; available via ECB Research Data Centre (application required)
- [ ] Compustat — firm and bank balance sheets and income statements; S&P Global; requires institutional license

### Freight & Shipping (dashboard indicators, not model inputs)
- [ ] SCFI — weekly Shanghai containerised freight index; sse.net.cn
- [ ] Baltic Dry Index — daily; free via FRED
- [ ] Shanghai Port TEU throughput — monthly; sipg.com.cn

---

## Medium-term: Build Models

### MT-1 — Trade Policy Model (NQTM / KITE-style)
### MT-2 — China Macro Model (DSGE)
