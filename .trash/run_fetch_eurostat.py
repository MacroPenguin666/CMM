"""CLI entry point: fetch Eurostat EU-China competitive-intelligence datasets."""

import logging

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    from policy_monitor.runners.fetch_eurostat import run
    run()
