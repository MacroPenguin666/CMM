"""Runner: refresh 15th-FYP Tech Self-Reliance tracker data.

Incremental by default (recent years / trailing 12 months); backfills
automatically on first run because the tables start empty.
"""

import logging

log = logging.getLogger("runner.fyp_tech")


def run() -> dict:
    from backend.fetchers.fyp_tech import (
        fetch_ai_benchmarks,
        fetch_chip_trade,
        fetch_eu_monthly,
        fetch_publications,
        fetch_wb_rd_intensity,
        get_db,
        seed_indicators,
    )

    log.info("=" * 60)
    log.info("FYP TECH — tech-line trade, EU monthly, publications, indicators")
    log.info("=" * 60)
    conn = get_db()
    seeded = seed_indicators(conn)
    wb = fetch_wb_rd_intensity(conn)
    pubs = fetch_publications(conn)
    bench = fetch_ai_benchmarks(conn)
    eu = fetch_eu_monthly(conn)
    chip = fetch_chip_trade(conn)
    total = conn.execute("SELECT COUNT(*) FROM fyp_chip_trade").fetchone()[0]
    conn.close()
    log.info("FYP tech: %d trade rows upserted (%d total), %d EU-monthly rows, "
             "%d pub rows, %d benchmark rows, %d WB points, %d seeds",
             chip, total, eu, pubs, bench, wb, seeded)
    return {"chip_rows": chip, "eu_rows": eu, "pub_rows": pubs,
            "bench_rows": bench, "wb_rows": wb, "seeded": seeded}
