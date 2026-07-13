"""
Chartbook registry — declarative definition of every series and chart.

The "Chartbook" is a standalone dashboard that rebuilds the charts from two
Bridgewater research pieces on the most recent PUBLICLY AVAILABLE data:

  1. "We're All Mercantilists Now"
  2. "The Macro Implications of the AI Capex Boom"

plus two explicit monitoring blocks the user asked for: full US inflation inputs
(CPI/PCE components) and the US labour market overall AND broken down by sector.

Everything is data-driven from this one file:

  * FRED_SERIES  — raw series pulled from FRED's *keyless* CSV endpoint
                   (https://fred.stlouisfed.org/graph/fredgraph.csv?id=ID) — no API key.
  * WB_SERIES    — raw series pulled from the World Bank v2 JSON API (no key).
  * STATIC       — small hand-entered datasets where no live public series exists
                   (clearly labelled "static reference" in the UI).
  * CHARTS       — display definitions. Each chart references series by id and
                   declares how to transform them. The transform engine lives in
                   backend/api.py (so raw values stay raw in the DB).

Bridgewater's charts are images built on proprietary measures; where a chart has
no clean public equivalent (e.g. the GW→capex→GDP decomposition, the AI-vs-ex-AI
debt split) we substitute the closest monitorable public proxy and say so in the
chart's `note`. Nothing here is invented data — every number is fetched live or
cited to a named static source.

Series-id conventions:
  * FRED  : the FRED code verbatim, e.g. "TLMFGCONS".
  * World Bank : "WB:{indicator}:{iso3}", e.g. "WB:NV.IND.MANF.CD:CHN".

Transforms (applied per-series unless the chart sets `compute`):
  * level     — raw value, optionally * `scale`.
  * yoy       — % change vs same period one year ago (periods from frequency).
  * mom_chg   — change vs previous period (level diff, * `scale`).
  * index     — cumulative % change since `base` date: (v / v_base - 1) * 100.

Chart-level `compute` (overrides per-series transform):
  * ratio     — single line = series[0] / series[1] * 100.
  * shares    — one line per item in `pairs` = num / den * 100.
  * bar_latest— one bar per series; height = latest transformed value.
  * static    — read straight from STATIC[chart_id].
"""

# Periods-per-year for the yoy/mom transforms, keyed by frequency code.
FREQ_PERIODS = {"M": 12, "Q": 4, "W": 52, "A": 1, "D": 252}

# ---------------------------------------------------------------------------
# FRED raw series — {id: (frequency, human label)}.  All verified to resolve
# via the keyless CSV endpoint (2026-06).
# ---------------------------------------------------------------------------
FRED_SERIES = {
    # --- Mercantilism ---
    "TLMFGCONS": ("M", "Construction spending: manufacturing ($mn SAAR)"),
    "NETEXP":    ("Q", "Net exports of goods & services ($bn SAAR)"),
    "GDP":       ("Q", "Gross domestic product ($bn SAAR)"),
    "B235RC1Q027SBEA": ("Q", "Federal customs duties received ($bn SAAR)"),
    "IMPGS":     ("Q", "Imports of goods & services ($bn SAAR)"),
    # --- AI capex boom ---
    "A679RC1Q027SBEA": ("Q", "Investment: info-processing equipment & software ($bn)"),
    "A191RL1Q225SBEA": ("Q", "Real GDP growth (% chg, annualised)"),
    "GDPPOT":    ("Q", "Real potential GDP ($bn)"),
    "PNFIC1":    ("Q", "Real nonresidential fixed investment ($bn)"),
    "OPHNFB":    ("Q", "Nonfarm business output per hour (index)"),
    "TLPWRCON":  ("M", "Construction spending: power ($mn SAAR)"),
    "CUSR0000SEHF01": ("M", "CPI: electricity"),
    "PCOPPUSDM": ("M", "Global price of copper ($/mt)"),
    "PALUMUSDM": ("M", "Global price of aluminum ($/mt)"),
    "FRBATLWGT3MMAUMHWGO": ("M", "Atlanta Fed wage growth tracker (%)"),
    "FIXHAI":    ("M", "NAR housing affordability index"),
    "DRCCLACBS": ("Q", "Delinquency rate: credit cards (%)"),
    "DRSFRMACBS":("Q", "Delinquency rate: single-family mortgages (%)"),
    "DRCLACBS":  ("Q", "Delinquency rate: consumer loans (%)"),
    "DRALACBS":  ("Q", "Delinquency rate: all loans (%)"),
    "UNRATE":    ("M", "Unemployment rate (U-3, %)"),
    # --- Inflation inputs ---
    "CPIAUCSL":  ("M", "CPI: all items"),
    "CPILFESL":  ("M", "CPI: core (ex food & energy)"),
    "PCEPI":     ("M", "PCE price index"),
    "PCEPILFE":  ("M", "PCE: core (ex food & energy)"),
    "CPIUFDSL":  ("M", "CPI: food"),
    "CPIENGSL":  ("M", "CPI: energy"),
    "CUSR0000SACL1E": ("M", "CPI: core goods (commodities less food & energy)"),
    "CUSR0000SASLE":  ("M", "CPI: core services (services less energy)"),
    "CUSR0000SAH1":   ("M", "CPI: shelter"),
    "CUSR0000SASL2RS":("M", "CPI: services less rent of shelter (supercore)"),
    "CUSR0000SEHC":   ("M", "CPI: owners' equivalent rent"),
    "CUSR0000SEHA":   ("M", "CPI: rent of primary residence"),
    "CORESTICKM159SFRBATL": ("M", "Atlanta Fed sticky core CPI (yoy %)"),
    "PCETRIM12M159SFRBATL": ("M", "Dallas Fed trimmed-mean PCE (yoy %)"),
    "DGDSRG3M086SBEA": ("M", "PCE: goods"),
    "DSERRG3M086SBEA": ("M", "PCE: services"),
    # --- Labour market ---
    "PAYEMS":    ("M", "Total nonfarm payrolls (000s)"),
    "MANEMP":    ("M", "Manufacturing payrolls (000s)"),
    "USCONS":    ("M", "Construction payrolls (000s)"),
    "CES5000000001": ("M", "Information payrolls (000s)"),
    "USPBS":     ("M", "Professional & business services payrolls (000s)"),
    "USLAH":     ("M", "Leisure & hospitality payrolls (000s)"),
    "USGOVT":    ("M", "Government payrolls (000s)"),
    "USEHS":     ("M", "Education & health services payrolls (000s)"),
    "USTPU":     ("M", "Trade, transport & utilities payrolls (000s)"),
    "USFIRE":    ("M", "Financial activities payrolls (000s)"),
    "USMINE":    ("M", "Mining & logging payrolls (000s)"),
    "U6RATE":    ("M", "U-6 underemployment rate (%)"),
    "CIVPART":   ("M", "Labour force participation rate (%)"),
    "EMRATIO":   ("M", "Employment-population ratio (%)"),
    "JTSJOR":    ("M", "JOLTS job-openings rate (%)"),
    "JTSHIR":    ("M", "JOLTS hires rate (%)"),
    "JTSQUR":    ("M", "JOLTS quits rate (%)"),
    "CES0500000003": ("M", "Avg hourly earnings: total private ($)"),
    "CES3000000003": ("M", "Avg hourly earnings: manufacturing ($)"),
    "CES2000000003": ("M", "Avg hourly earnings: construction ($)"),
    "CES5000000003": ("M", "Avg hourly earnings: information ($)"),
    "CES6000000003": ("M", "Avg hourly earnings: professional & business ($)"),
    "CES7000000003": ("M", "Avg hourly earnings: leisure & hospitality ($)"),
    "CES6500000003": ("M", "Avg hourly earnings: education & health ($)"),
    "CES4000000003": ("M", "Avg hourly earnings: trade/transport/utilities ($)"),
}

# ---------------------------------------------------------------------------
# World Bank raw series — (indicator, iso3, label).  Annual, no API key.
# ---------------------------------------------------------------------------
WB_SERIES = [
    ("NV.IND.MANF.CD", "CHN", "China manufacturing value added"),
    ("NV.IND.MANF.CD", "WLD", "World manufacturing value added"),
    ("NE.CON.PRVT.CD", "CHN", "China household consumption"),
    ("NE.CON.PRVT.CD", "WLD", "World household consumption"),
    ("BN.CAB.XOKA.GD.ZS", "CHN", "China current account (% GDP)"),
    ("BN.CAB.XOKA.GD.ZS", "DEU", "Germany current account (% GDP)"),
    ("BN.CAB.XOKA.GD.ZS", "USA", "United States current account (% GDP)"),
    ("BN.CAB.XOKA.GD.ZS", "JPN", "Japan current account (% GDP)"),
    ("BN.CAB.XOKA.GD.ZS", "KOR", "South Korea current account (% GDP)"),
]

# ---------------------------------------------------------------------------
# Static reference datasets (no live public series exists).
# ---------------------------------------------------------------------------
STATIC = {
    # CSIS "Red Ink: Estimating Chinese Industrial Policy Spending" (DiPippo,
    # Mazzocco, Kennedy, 2022), 2019 estimates, % of GDP — conservative measure.
    "m_indpol": {
        "labels": ["China", "South Korea", "Japan", "United States", "Germany", "France"],
        "values": [1.73, 0.67, 0.50, 0.39, 0.41, 0.55],
    },
}

# ---------------------------------------------------------------------------
# Sections (tab order + titles).
# ---------------------------------------------------------------------------
SECTIONS = [
    ("mercantilism", "We're All Mercantilists Now",
     "Rebuilt on live public data from Bridgewater's mercantilism thesis."),
    ("aicapex", "The AI Capex Boom",
     "Public proxies for Bridgewater's AI-capex macro charts."),
    ("inflation", "US Inflation Inputs",
     "CPI & PCE decomposed into the components that drive the headline."),
    ("labor", "US Labour Market",
     "Employment, wages and openings — overall and by sector."),
]

# ---------------------------------------------------------------------------
# Charts.  `series` items: {sid, label, transform?, scale?, axis?}.
# Chart keys: id, section, title, subtitle, source, note, type, start,
#             unit, compute?, base?, series / pairs.
# ---------------------------------------------------------------------------
CHARTS = [
    # ===================== MERCANTILISM =====================
    {
        "id": "m_mfg_constr", "section": "mercantilism", "type": "line",
        "title": "US manufacturing construction has nearly doubled",
        "subtitle": "Total construction spending: manufacturing, $bn (SAAR)",
        "source": "US Census via FRED — TLMFGCONS", "start": "2002-01-01", "unit": "$bn",
        "note": "CHIPS Act + IRA drove a step-change in factory building since 2021.",
        "series": [{"sid": "TLMFGCONS", "label": "Manufacturing construction", "transform": "level", "scale": 0.001}],
    },
    {
        "id": "m_trade_balance", "section": "mercantilism", "type": "line",
        "title": "The US runs the world's deficit",
        "subtitle": "Net exports of goods & services, $bn (SAAR)",
        "source": "BEA via FRED — NETEXP", "start": "1960-01-01", "unit": "$bn",
        "note": "Persistent US external deficit absorbs the surpluses of mercantilist exporters.",
        "series": [{"sid": "NETEXP", "label": "Net exports", "transform": "level"}],
    },
    {
        "id": "m_tariff", "section": "mercantilism", "type": "line",
        "title": "US effective tariff rate",
        "subtitle": "Federal customs duties as % of imports of goods & services",
        "source": "BEA via FRED — B235RC1Q027SBEA / IMPGS", "start": "1990-01-01", "unit": "%",
        "note": "Proxy for the average applied tariff; rises with the 2018-19 and 2025 tariff rounds.",
        "compute": "ratio",
        "series": [{"sid": "B235RC1Q027SBEA", "label": "Effective tariff rate"}, {"sid": "IMPGS"}],
    },
    {
        "id": "m_china_prod_cons", "section": "mercantilism", "type": "line",
        "title": "China makes far more than it consumes",
        "subtitle": "China as a share of world total, %",
        "source": "World Bank — NV.IND.MANF.CD, NE.CON.PRVT.CD (China ÷ World)",
        "start": "2000", "unit": "% of world",
        "note": "~30% of world manufacturing vs <15% of world consumption — the core imbalance.",
        "compute": "shares",
        "pairs": [
            {"num": "WB:NV.IND.MANF.CD:CHN", "den": "WB:NV.IND.MANF.CD:WLD", "label": "Share of world manufacturing"},
            {"num": "WB:NE.CON.PRVT.CD:CHN", "den": "WB:NE.CON.PRVT.CD:WLD", "label": "Share of world consumption"},
        ],
    },
    {
        "id": "m_imbalances", "section": "mercantilism", "type": "line",
        "title": "Global current-account imbalances",
        "subtitle": "Current account balance, % of GDP",
        "source": "World Bank — BN.CAB.XOKA.GD.ZS", "start": "2000", "unit": "% of GDP",
        "note": "Surplus economies (China, Germany) vs the US deficit.",
        "series": [
            {"sid": "WB:BN.CAB.XOKA.GD.ZS:CHN", "label": "China", "transform": "level"},
            {"sid": "WB:BN.CAB.XOKA.GD.ZS:DEU", "label": "Germany", "transform": "level"},
            {"sid": "WB:BN.CAB.XOKA.GD.ZS:JPN", "label": "Japan", "transform": "level"},
            {"sid": "WB:BN.CAB.XOKA.GD.ZS:KOR", "label": "South Korea", "transform": "level"},
            {"sid": "WB:BN.CAB.XOKA.GD.ZS:USA", "label": "United States", "transform": "level"},
        ],
    },
    {
        "id": "m_indpol", "section": "mercantilism", "type": "bar",
        "title": "Industrial-policy spending: China leads by far",
        "subtitle": "Estimated industrial-policy spending, % of GDP (2019)",
        "source": "CSIS, \"Red Ink\" (2022) — static reference, not live", "unit": "% of GDP",
        "note": "Best available cross-country estimate; conservative measure, 2019. Updated only when CSIS republishes.",
        "compute": "static",
    },
    # ===================== AI CAPEX =====================
    {
        "id": "ai_investment", "section": "aicapex", "type": "line",
        "title": "Investment in info-processing equipment & software",
        "subtitle": "Nonresidential fixed investment, $bn (SAAR)",
        "source": "BEA via FRED — A679RC1Q027SBEA", "start": "1995-01-01", "unit": "$bn",
        "note": "The national-accounts line that captures the AI/data-centre capex surge.",
        "series": [{"sid": "A679RC1Q027SBEA", "label": "Info-processing equip & software", "transform": "level"}],
    },
    {
        "id": "ai_gdp_growth", "section": "aicapex", "type": "line",
        "title": "Growth running above potential",
        "subtitle": "Real GDP growth vs potential, % (annualised)",
        "source": "BEA / CBO via FRED — A191RL1Q225SBEA, GDPPOT", "start": "2012-01-01", "unit": "%",
        "note": "Bridgewater attributes a chunk of above-trend growth to AI capex.",
        "series": [
            {"sid": "A191RL1Q225SBEA", "label": "Real GDP growth", "transform": "level"},
            {"sid": "GDPPOT", "label": "Potential GDP growth", "transform": "yoy"},
        ],
    },
    {
        "id": "ai_construction", "section": "aicapex", "type": "line",
        "title": "Building the AI economy: power & factories",
        "subtitle": "Construction spending, $bn (SAAR)",
        "source": "US Census via FRED — TLPWRCON, TLMFGCONS", "start": "2010-01-01", "unit": "$bn",
        "note": "Power and manufacturing build-out underpinning data centres and chips.",
        "series": [
            {"sid": "TLPWRCON", "label": "Power", "transform": "level", "scale": 0.001},
            {"sid": "TLMFGCONS", "label": "Manufacturing", "transform": "level", "scale": 0.001},
        ],
    },
    {
        "id": "ai_electricity", "section": "aicapex", "type": "line",
        "title": "Electricity prices climbing with AI power demand",
        "subtitle": "CPI electricity, cumulative % change since Jan 2024",
        "source": "BLS via FRED — CUSR0000SEHF01", "start": "2023-01-01", "unit": "% since Jan-2024",
        "base": "2024-01-01",
        "note": "Data-centre load is one driver of rising retail power prices.",
        "series": [{"sid": "CUSR0000SEHF01", "label": "Electricity (CPI)", "transform": "index"}],
    },
    {
        "id": "ai_commodities", "section": "aicapex", "type": "line",
        "title": "Copper & aluminium: the metals of electrification",
        "subtitle": "Cumulative % change since Jan 2024",
        "source": "IMF/World Bank via FRED — PCOPPUSDM, PALUMUSDM", "start": "2021-01-01",
        "unit": "% since Jan-2024", "base": "2024-01-01",
        "note": "AI/data-centre and grid build-out lifts demand for power metals.",
        "series": [
            {"sid": "PCOPPUSDM", "label": "Copper", "transform": "index"},
            {"sid": "PALUMUSDM", "label": "Aluminium", "transform": "index"},
        ],
    },
    {
        "id": "ai_wages", "section": "aicapex", "type": "line",
        "title": "Nominal wage growth",
        "subtitle": "Atlanta Fed wage growth tracker, % (3-mo MA of median)",
        "source": "Atlanta Fed via FRED — FRBATLWGT3MMAUMHWGO", "start": "2005-01-01", "unit": "%",
        "note": "Wage pressure feeds the inflation half of the AI-capex macro story.",
        "series": [{"sid": "FRBATLWGT3MMAUMHWGO", "label": "Wage growth", "transform": "level"}],
    },
    {
        "id": "ai_delinquencies", "section": "aicapex", "type": "line",
        "title": "Household delinquencies by loan type",
        "subtitle": "90+ day delinquency rate, %",
        "source": "Federal Reserve via FRED — DR*ACBS", "start": "2003-01-01", "unit": "%",
        "note": "Bridgewater flags the divergence between an AI-led top line and a softening consumer.",
        "series": [
            {"sid": "DRCCLACBS", "label": "Credit cards", "transform": "level"},
            {"sid": "DRSFRMACBS", "label": "Mortgages", "transform": "level"},
            {"sid": "DRCLACBS", "label": "Consumer loans", "transform": "level"},
            {"sid": "DRALACBS", "label": "All loans", "transform": "level"},
        ],
    },
    {
        "id": "ai_affordability", "section": "aicapex", "type": "line",
        "title": "Housing affordability",
        "subtitle": "NAR housing affordability index (100 = median family just qualifies)",
        "source": "NAR via FRED — FIXHAI", "start": "2005-01-01", "unit": "index",
        "note": "Higher-for-longer rates keep housing badly unaffordable.",
        "series": [{"sid": "FIXHAI", "label": "Affordability index", "transform": "level"}],
    },
    {
        "id": "ai_productivity", "section": "aicapex", "type": "line",
        "title": "Productivity — the AI payoff to watch",
        "subtitle": "Nonfarm business output per hour, yoy %",
        "source": "BLS via FRED — OPHNFB", "start": "2000-01-01", "unit": "yoy %",
        "note": "The J-curve thesis: AI productivity gains show up with a lag.",
        "series": [{"sid": "OPHNFB", "label": "Output per hour", "transform": "yoy"}],
    },
    {
        "id": "ai_inv_vs_unemp", "section": "aicapex", "type": "line",
        "title": "Strong investment, soft labour market",
        "subtitle": "Business investment (yoy %, left) vs unemployment rate (%, right)",
        "source": "BEA / BLS via FRED — PNFIC1, UNRATE", "start": "1990-01-01", "unit": "",
        "note": "Bridgewater expects an unusual divergence: capex up, hiring soft.",
        "series": [
            {"sid": "PNFIC1", "label": "Real business investment (yoy %)", "transform": "yoy", "axis": "y"},
            {"sid": "UNRATE", "label": "Unemployment rate (%)", "transform": "level", "axis": "y1"},
        ],
    },
    # ===================== INFLATION =====================
    {
        "id": "inf_headline_core", "section": "inflation", "type": "line",
        "title": "Headline vs core inflation",
        "subtitle": "CPI & PCE, yoy %",
        "source": "BLS / BEA via FRED", "start": "2015-01-01", "unit": "yoy %",
        "series": [
            {"sid": "CPIAUCSL", "label": "CPI headline", "transform": "yoy"},
            {"sid": "CPILFESL", "label": "CPI core", "transform": "yoy"},
            {"sid": "PCEPI", "label": "PCE headline", "transform": "yoy"},
            {"sid": "PCEPILFE", "label": "PCE core", "transform": "yoy"},
        ],
    },
    {
        "id": "inf_cpi_components", "section": "inflation", "type": "line",
        "title": "What's driving CPI",
        "subtitle": "CPI components, yoy %",
        "source": "BLS via FRED", "start": "2018-01-01", "unit": "yoy %",
        "series": [
            {"sid": "CPIUFDSL", "label": "Food", "transform": "yoy"},
            {"sid": "CPIENGSL", "label": "Energy", "transform": "yoy"},
            {"sid": "CUSR0000SACL1E", "label": "Core goods", "transform": "yoy"},
            {"sid": "CUSR0000SASLE", "label": "Core services", "transform": "yoy"},
            {"sid": "CUSR0000SAH1", "label": "Shelter", "transform": "yoy"},
            {"sid": "CUSR0000SASL2RS", "label": "Supercore (svcs ex shelter)", "transform": "yoy"},
        ],
    },
    {
        "id": "inf_shelter", "section": "inflation", "type": "line",
        "title": "Shelter inflation detail",
        "subtitle": "yoy %",
        "source": "BLS via FRED", "start": "2015-01-01", "unit": "yoy %",
        "series": [
            {"sid": "CUSR0000SAH1", "label": "Shelter", "transform": "yoy"},
            {"sid": "CUSR0000SEHC", "label": "Owners' equivalent rent", "transform": "yoy"},
            {"sid": "CUSR0000SEHA", "label": "Rent of primary residence", "transform": "yoy"},
        ],
    },
    {
        "id": "inf_trend", "section": "inflation", "type": "line",
        "title": "Underlying / trend inflation gauges",
        "subtitle": "yoy %",
        "source": "BLS / Atlanta & Dallas Feds via FRED", "start": "2015-01-01", "unit": "yoy %",
        "series": [
            {"sid": "CPILFESL", "label": "Core CPI", "transform": "yoy"},
            {"sid": "CORESTICKM159SFRBATL", "label": "Sticky core CPI", "transform": "level"},
            {"sid": "PCETRIM12M159SFRBATL", "label": "Trimmed-mean PCE", "transform": "level"},
        ],
    },
    {
        "id": "inf_pce_goods_services", "section": "inflation", "type": "line",
        "title": "PCE: goods vs services",
        "subtitle": "yoy %",
        "source": "BEA via FRED", "start": "2015-01-01", "unit": "yoy %",
        "series": [
            {"sid": "DGDSRG3M086SBEA", "label": "Goods", "transform": "yoy"},
            {"sid": "DSERRG3M086SBEA", "label": "Services", "transform": "yoy"},
        ],
    },
    # ===================== LABOUR =====================
    {
        "id": "lab_payrolls_chg", "section": "labor", "type": "bar",
        "title": "Monthly payroll change",
        "subtitle": "Total nonfarm payrolls, month-over-month change (000s)",
        "source": "BLS via FRED — PAYEMS", "start": "2022-01-01", "unit": "000s / mo",
        "series": [{"sid": "PAYEMS", "label": "Payroll change", "transform": "mom_chg"}],
    },
    {
        "id": "lab_sector_yoy", "section": "labor", "type": "line",
        "title": "Payroll growth by sector",
        "subtitle": "Employment, yoy %",
        "source": "BLS via FRED (CES)", "start": "2018-01-01", "unit": "yoy %",
        "series": [
            {"sid": "MANEMP", "label": "Manufacturing", "transform": "yoy"},
            {"sid": "USCONS", "label": "Construction", "transform": "yoy"},
            {"sid": "CES5000000001", "label": "Information", "transform": "yoy"},
            {"sid": "USPBS", "label": "Professional & business", "transform": "yoy"},
            {"sid": "USLAH", "label": "Leisure & hospitality", "transform": "yoy"},
            {"sid": "USEHS", "label": "Education & health", "transform": "yoy"},
            {"sid": "USTPU", "label": "Trade/transport/utilities", "transform": "yoy"},
            {"sid": "USFIRE", "label": "Financial", "transform": "yoy"},
            {"sid": "USGOVT", "label": "Government", "transform": "yoy"},
            {"sid": "USMINE", "label": "Mining & logging", "transform": "yoy"},
        ],
    },
    {
        "id": "lab_sector_latest", "section": "labor", "type": "bar",
        "title": "Latest sector momentum",
        "subtitle": "Employment growth by sector, latest yoy %",
        "source": "BLS via FRED (CES)", "unit": "yoy %",
        "compute": "bar_latest",
        "series": [
            {"sid": "MANEMP", "label": "Manufacturing", "transform": "yoy"},
            {"sid": "USCONS", "label": "Construction", "transform": "yoy"},
            {"sid": "CES5000000001", "label": "Information", "transform": "yoy"},
            {"sid": "USPBS", "label": "Prof & business", "transform": "yoy"},
            {"sid": "USLAH", "label": "Leisure & hosp.", "transform": "yoy"},
            {"sid": "USEHS", "label": "Education & health", "transform": "yoy"},
            {"sid": "USTPU", "label": "Trade/transp/util", "transform": "yoy"},
            {"sid": "USFIRE", "label": "Financial", "transform": "yoy"},
            {"sid": "USGOVT", "label": "Government", "transform": "yoy"},
            {"sid": "USMINE", "label": "Mining & logging", "transform": "yoy"},
        ],
    },
    {
        "id": "lab_unemployment", "section": "labor", "type": "line",
        "title": "Unemployment: U-3 vs U-6",
        "subtitle": "%",
        "source": "BLS via FRED — UNRATE, U6RATE", "start": "2015-01-01", "unit": "%",
        "series": [
            {"sid": "UNRATE", "label": "U-3 (headline)", "transform": "level"},
            {"sid": "U6RATE", "label": "U-6 (underemployment)", "transform": "level"},
        ],
    },
    {
        "id": "lab_participation", "section": "labor", "type": "line",
        "title": "Participation & employment ratio",
        "subtitle": "%",
        "source": "BLS via FRED — CIVPART, EMRATIO", "start": "2015-01-01", "unit": "%",
        "series": [
            {"sid": "CIVPART", "label": "Labour force participation", "transform": "level"},
            {"sid": "EMRATIO", "label": "Employment-population ratio", "transform": "level"},
        ],
    },
    {
        "id": "lab_jolts", "section": "labor", "type": "line",
        "title": "JOLTS: openings, hires & quits",
        "subtitle": "Rate, %",
        "source": "BLS via FRED — JTSJOR, JTSHIR, JTSQUR", "start": "2015-01-01", "unit": "%",
        "series": [
            {"sid": "JTSJOR", "label": "Job-openings rate", "transform": "level"},
            {"sid": "JTSHIR", "label": "Hires rate", "transform": "level"},
            {"sid": "JTSQUR", "label": "Quits rate", "transform": "level"},
        ],
    },
    {
        "id": "lab_wage_sector", "section": "labor", "type": "line",
        "title": "Wage growth by sector",
        "subtitle": "Average hourly earnings, yoy %",
        "source": "BLS via FRED (CES, all employees)", "start": "2018-01-01", "unit": "yoy %",
        "series": [
            {"sid": "CES0500000003", "label": "Total private", "transform": "yoy"},
            {"sid": "CES3000000003", "label": "Manufacturing", "transform": "yoy"},
            {"sid": "CES2000000003", "label": "Construction", "transform": "yoy"},
            {"sid": "CES5000000003", "label": "Information", "transform": "yoy"},
            {"sid": "CES6000000003", "label": "Professional & business", "transform": "yoy"},
            {"sid": "CES7000000003", "label": "Leisure & hospitality", "transform": "yoy"},
            {"sid": "CES6500000003", "label": "Education & health", "transform": "yoy"},
            {"sid": "CES4000000003", "label": "Trade/transport/utilities", "transform": "yoy"},
        ],
    },
]


def all_series_ids():
    """Every raw series id the fetcher must populate."""
    ids = list(FRED_SERIES.keys())
    ids += [f"WB:{ind}:{iso}" for (ind, iso, _label) in WB_SERIES]
    return ids
