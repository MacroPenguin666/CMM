"""Runner: fetch all Eurostat EU-China competitiveness datasets."""

import logging

log = logging.getLogger("runner.eurostat")


def run() -> list[dict]:
    from policy_monitor.eurostat import fetch_all_eurostat

    log.info("=" * 60)
    log.info("EUROSTAT — EU-China Competitive Intelligence")
    log.info("=" * 60)
    results = fetch_all_eurostat()
    ok = sum(1 for r in results if r["ok"])
    stored = sum(r.get("stored", 0) for r in results)
    log.info("Eurostat: %d/%d datasets OK, %d rows stored", ok, len(results), stored)
    return results
