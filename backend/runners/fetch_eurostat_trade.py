"""Runner: refresh Eurostat COMEXT monthly HS-trade (trailing months)."""

import logging

log = logging.getLogger("runner.eurostat_trade")


def run() -> dict:
    from backend.fetchers.eurostat_trade import fetch_recent

    log.info("=" * 60)
    log.info("EUROSTAT TRADE — EU27 monthly imports by HS2 chapter & partner")
    log.info("=" * 60)
    # ~9-month trailing window: Eurostat publishes with a ~3-month lag and
    # revises recent months, so this always overlaps published data.
    result = fetch_recent(n=9)
    log.info(
        "Eurostat trade: %d/%d months with data, %d rows",
        result["months_with_data"], result["months"], result["rows"],
    )
    return result
