"""
Validate that source URLs in the registry are reachable.

Usage:
    python -m sources.validate              # check all sources
    python -m sources.validate --feeds      # check only RSSHub feed URLs
    python -m sources.validate --timeout 10 # custom timeout in seconds
"""

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from policy_monitor.sources.loader import get_all_sources, get_rsshub_feeds, load_registry


def check_url(name: str, url: str, timeout: int = 8) -> dict:
    """HEAD-request a URL and return status info."""
    try:
        resp = requests.head(
            url,
            timeout=timeout,
            allow_redirects=True,
            headers={"User-Agent": "ChinaPolicyMonitor/1.0"},
        )
        return {
            "name": name,
            "url": url,
            "status": resp.status_code,
            "ok": resp.status_code < 400,
        }
    except requests.RequestException as exc:
        return {"name": name, "url": url, "status": str(exc), "ok": False}


def validate_sources(sources: list[dict], timeout: int = 8) -> list[dict]:
    """Check reachability of a list of source dicts (must have 'name' and 'url')."""
    results = []
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {
            pool.submit(check_url, s["name"], s["url"], timeout): s
            for s in sources
        }
        for fut in as_completed(futures):
            results.append(fut.result())
    return sorted(results, key=lambda r: (r["ok"], r["name"]))


def main():
    parser = argparse.ArgumentParser(description="Validate source URLs")
    parser.add_argument(
        "--feeds", action="store_true", help="Check RSSHub feed URLs only"
    )
    parser.add_argument(
        "--timeout", type=int, default=8, help="Request timeout in seconds"
    )
    args = parser.parse_args()

    reg = load_registry()
    if args.feeds:
        sources = get_rsshub_feeds(reg)
    else:
        sources = get_all_sources(reg)

    print(f"Checking {len(sources)} URLs (timeout={args.timeout}s)...\n")
    results = validate_sources(sources, timeout=args.timeout)

    ok_count = sum(1 for r in results if r["ok"])
    fail_count = len(results) - ok_count

    for r in results:
        status = "OK" if r["ok"] else "FAIL"
        print(f"  [{status}] {r['name']}: {r['url']} ({r['status']})")

    print(f"\n{ok_count} reachable, {fail_count} unreachable out of {len(results)}")
    sys.exit(0 if fail_count == 0 else 1)


if __name__ == "__main__":
    main()
