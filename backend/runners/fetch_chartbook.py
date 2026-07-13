"""Runner: refresh the Chartbook series (FRED keyless + World Bank) into cmm.db."""

import logging

log = logging.getLogger("runner.chartbook")


def run() -> dict:
    from backend.fetchers.chartbook import run as fetch_chartbook

    log.info("=" * 60)
    log.info("CHARTBOOK — Bridgewater-replica series (FRED + World Bank)")
    log.info("=" * 60)
    result = fetch_chartbook()
    log.info(
        "Chartbook: %d series ok, %d failed, %d rows",
        result["ok"], result["failed"], result["rows"],
    )
    return result
