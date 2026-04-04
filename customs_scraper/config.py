"""
All configuration loaded from environment variables.
Copy .env.example to .env and fill in values before running.
"""
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ── Database ──────────────────────────────────────────────────────────────────

def _detect_gdrive_db_path() -> str:
    """
    Auto-detect Google Drive mount on macOS.
    Looks for ~/Library/CloudStorage/GoogleDrive-*/My Drive/CMM/
    Falls back to ~/customs_exports.db if not found.
    """
    cloudstore = Path.home() / "Library" / "CloudStorage"
    if cloudstore.exists():
        candidates = list(cloudstore.glob("GoogleDrive-*/My Drive/CMM"))
        if candidates:
            db_dir = candidates[0]
            db_dir.mkdir(parents=True, exist_ok=True)
            return str(db_dir / "customs_exports.db")
    return str(Path.home() / "customs_exports.db")


DB_PATH: str = os.getenv("DB_PATH", _detect_gdrive_db_path())

# ── Proxy ─────────────────────────────────────────────────────────────────────

# Placeholder: set to an HTTP proxy with a Chinese IP once you have one.
# e.g. "http://user:pass@proxy-host:3128"
CUSTOMS_PROXY_URL: str | None = os.getenv("CUSTOMS_PROXY_URL") or None

# ── Scraping behaviour ────────────────────────────────────────────────────────

SCRAPE_DELAY_SECONDS: float = float(os.getenv("SCRAPE_DELAY_SECONDS", "2.0"))
SCRAPE_MAX_RETRIES: int = int(os.getenv("SCRAPE_MAX_RETRIES", "5"))
SCRAPE_RETRY_BASE_SECONDS: float = float(os.getenv("SCRAPE_RETRY_BASE_SECONDS", "3.0"))
SCRAPE_HEADLESS: bool = os.getenv("SCRAPE_HEADLESS", "true").lower() == "true"
SCRAPE_USE_DYNAMIC: bool = os.getenv("SCRAPE_USE_DYNAMIC", "true").lower() == "true"

# ── Target site ───────────────────────────────────────────────────────────────

BASE_URL: str = "http://stats.customs.gov.cn"

# Update this after running --debug-browser and inspecting the real query form URL
QUERY_ENDPOINT: str = os.getenv("QUERY_ENDPOINT", f"{BASE_URL}/indexEn")

# ── Scheduler ─────────────────────────────────────────────────────────────────

SCHEDULER_DAY: int = int(os.getenv("SCHEDULER_DAY", "15"))
SCHEDULER_HOUR: int = int(os.getenv("SCHEDULER_HOUR", "8"))
SCHEDULER_TIMEZONE: str = os.getenv("SCHEDULER_TIMEZONE", "Asia/Shanghai")
