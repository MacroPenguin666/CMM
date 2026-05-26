#!/usr/bin/env python3
"""Run the batch data ingestion pipeline (all non-realtime sources)."""
from policy_monitor.runners.fetch_batch import main

if __name__ == "__main__":
    main()
