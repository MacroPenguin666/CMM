import os
from dotenv import load_dotenv

load_dotenv()

# =========================
# ANTHROPIC API
# Used by: policy_monitor/advisor.py
# Get one at: https://console.anthropic.com/
# Without this key the advisor will still retrieve and list sources (stub mode)
# =========================
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

# =========================
# OPENSKY NETWORK (flights)
# Used by: policy_monitor/flights.py
# Register at: https://opensky-network.org for higher rate limits (anonymous allowed)
# =========================
OPENSKY_USERNAME = os.getenv("OPENSKY_USERNAME", "")
OPENSKY_PASSWORD = os.getenv("OPENSKY_PASSWORD", "")

# =========================
# AIS STREAM (ships — primary)
# Used by: policy_monitor/ships.py
# Register at: https://aisstream.io
# =========================
AISSTREAM_API_KEY = os.getenv("AISSTREAM_API_KEY", "")

# =========================
# AISHUB (ships — fallback)
# Used by: policy_monitor/ships.py
# Register at: https://aishub.net
# =========================
AISHUB_USERNAME = os.getenv("AISHUB_USERNAME", "")

# =========================
# DESTATIS GENESIS API
# Used by: policy_monitor/destatis.py
# No registration required — guest access (GAST) used automatically when unset.
# Register at genesis.destatis.de only for higher rate limits.
# =========================
DESTATIS_TOKEN    = os.getenv("DESTATIS_TOKEN", "")
DESTATIS_BASE_URL = os.getenv("DESTATIS_BASE_URL", "https://www-genesis.destatis.de/genesisWS/rest/2020/data/table")

# =========================
# FRED API (Federal Reserve)
# Used by: policy_monitor/fred.py
# Get a free key at: https://fred.stlouisfed.org/docs/api/api_key.html
# =========================
FRED_API_KEY = os.getenv("FRED_API_KEY", "")

# =========================
# ECB DATA PORTAL (no auth required)
# Used by: policy_monitor/ecb.py
# Docs: https://data-api.ecb.europa.eu
# =========================
ECB_BASE_URL = os.getenv("ECB_BASE_URL", "https://data-api.ecb.europa.eu/service/data")

# =========================
# IMF DATAMAPPER API (no auth required)
# Used by: policy_monitor/imf.py
# Docs: https://www.imf.org/external/datamapper/api/v1/indicators
# =========================
IMF_BASE_URL = os.getenv("IMF_BASE_URL", "https://www.imf.org/external/datamapper/api/v1")

# =========================
# UN COMTRADE+ API (subscription)
# Used by: policy_monitor/comtrade.py
# Register at: https://comtradedeveloper.un.org
# Auth header: Ocp-Apim-Subscription-Key
# =========================
COMTRADE_API_KEY  = os.getenv("COMTRADE_API_KEY", "")
COMTRADE_BASE_URL = os.getenv("COMTRADE_BASE_URL", "https://comtradeapi.un.org/data/v1/get/C/A/HS")

# =========================
# WITS / UNCTAD TRAINS API (tariff data by HS code)
# Used by: policy_monitor/wits.py
# Register free at: https://wits.worldbank.org → My Account → API Access
# =========================
WITS_API_KEY  = os.getenv("WITS_API_KEY", "")
WITS_BASE_URL = os.getenv("WITS_BASE_URL", "https://wits.worldbank.org/API/V1/SDMX/V21/datasource")

# =========================
# WTO DATA PORTAL API (tariff tracker, dispute settlement)
# Used by: policy_monitor/wto.py
# Register at: https://api.wto.org  (free developer tier)
# Auth header: Ocp-Apim-Subscription-Key
# =========================
WTO_API_KEY  = os.getenv("WTO_API_KEY", "")
WTO_BASE_URL = os.getenv("WTO_BASE_URL", "https://api.wto.org/timeseries/v1")
