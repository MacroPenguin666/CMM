import os
from dotenv import load_dotenv

load_dotenv()

# =========================
# DESTATIS GENESIS API
# Used by: policy_monitor/destatis.py
# Register at: https://www-genesis.destatis.de
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
# WORLD BANK API (no auth required)
# Used by: policy_monitor/worldbank.py
# Docs: https://datahelpdesk.worldbank.org/knowledgebase/articles/898581
# =========================
WB_BASE_URL = os.getenv("WB_BASE_URL", "https://api.worldbank.org/v2/country/all/indicator")

# =========================
# UN COMTRADE+ API (subscription)
# Used by: policy_monitor/comtrade.py
# Register at: https://comtradedeveloper.un.org
# Auth header: Ocp-Apim-Subscription-Key
# =========================
COMTRADE_API_KEY  = os.getenv("COMTRADE_API_KEY", "")
COMTRADE_BASE_URL = os.getenv("COMTRADE_BASE_URL", "https://comtradeapi.un.org/data/v1/get/C/A/HS")
