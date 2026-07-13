"""
Commodity markets data runner.
Fetches, for every material in backend/fetchers/materials.py: production &
refining by country (USGS MCS data releases), market prices (Yahoo/FRED),
and world trade (UN Comtrade) into data/commodities.json.

Keyless — no credentials required. Comtrade fetches are rate-limited and
resumable: the first full trade backfill takes hours; later runs only top up
recent years. Pass --no-trade for the fast pass (production + prices only).
"""

import logging
import sys
from datetime import datetime

from backend.storage import LOG_DIR

LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_DIR / f"commodities_{datetime.now():%Y-%m-%d}.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("fetch_commodities")


def run(trade=True, base=True):
    from backend.fetchers.commodities import refresh, DATA_PATH

    log.info("Commodity markets fetch — all materials: %s%s",
             "production, prices" if base else "trade top-up only",
             ", trade" if trade and base else "")
    data = refresh(trade=trade, base=base)
    mats = data.get("materials", {})
    n_prod = sum(1 for m in mats.values() if m.get("production"))
    n_px = sum(1 for m in mats.values() if m.get("prices_daily") or m.get("prices_monthly"))
    cmds = data.get("trade", {}).get("commodities", {})
    log.info(f"Done: {len(mats)} materials ({n_prod} with production, {n_px} with market prices), "
             f"{len(cmds)} trade HS codes → {DATA_PATH}")


if __name__ == "__main__":
    run(trade="--no-trade" not in sys.argv,
        base="--trade-only" not in sys.argv)
