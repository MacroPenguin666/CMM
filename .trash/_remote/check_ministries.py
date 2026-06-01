"""
Dry-run diagnostic for every ministry scraper target.

Fetches page 1 of each target, prints what would be stored (titles, dates),
and flags obvious problems. Does NOT write to the database.

Usage:
    python check_ministries.py              # check all targets
    python check_ministries.py ndrc mfa     # filter by name keywords
"""
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.stdout.reconfigure(encoding="utf-8")

from policy_monitor.ministry_scraper import TARGETS, scrape_target

GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
RESET  = "\033[0m"
BOLD   = "\033[1m"


def status_icon(result: dict) -> tuple[str, str]:
    entries = result.get("entries", [])
    n = len(entries)
    err = result.get("error", "")

    if not result.get("ok") or err:
        return RED, f"FAIL  0 items  [{err[:80]}]"

    dated = [e for e in entries if e.get("published")]
    no_date = n - len(dated)

    if n == 0:
        return RED, "FAIL  0 items fetched"

    dates = sorted(e["published"] for e in dated if e.get("published"))
    date_range = f"{dates[0]} .. {dates[-1]}" if dates else "no dates"
    date_warn = f"  ({no_date}/{n} missing dates)" if no_date else ""

    if n < 3:
        color = YELLOW
        verdict = f"WARN  {n} items"
    elif no_date == n:
        color = YELLOW
        verdict = f"WARN  {n} items, all dates missing"
    else:
        color = GREEN
        verdict = f"OK    {n} items"

    return color, f"{verdict}  {date_range}{date_warn}"


def check_one(target: dict) -> dict:
    result = scrape_target(target, timeout=25)
    return result


def main():
    keywords = [k.lower() for k in sys.argv[1:]]
    targets = TARGETS
    if keywords:
        targets = [t for t in TARGETS if any(k in t["name"].lower() for k in keywords)]
        print(f"Filtered to {len(targets)} targets matching: {keywords}\n")

    print(f"Checking {len(targets)} ministry targets (page 1 only, no DB writes)\n")
    print(f"{'Target':<45}  {'URL':<55}  Result")
    print("-" * 165)

    results_map: dict[str, dict] = {}

    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(check_one, t): t for t in targets}
        for future in as_completed(futures):
            t = futures[future]
            try:
                result = future.result()
            except Exception as e:
                result = {"source": t["name"], "entries": [], "ok": False, "error": str(e)}
            results_map[t["name"]] = result

    # Print in original order
    problems = []
    for t in targets:
        result = results_map.get(t["name"], {})
        color, summary = status_icon(result)
        name = t["name"]
        url = t["url"][:53]
        print(f"{color}{name:<45}  {url:<55}  {summary}{RESET}")

        entries = result.get("entries", [])
        for e in entries[:3]:
            date = e.get("published") or "no-date"
            title = (e.get("title") or "")[:70]
            print(f"  {'':45}    [{date}] {title}")

        if not result.get("ok") or len(entries) == 0:
            problems.append((t["name"], t["url"], result.get("error", "0 items")))

    print()
    if problems:
        print(f"{RED}{BOLD}Problems ({len(problems)}):{RESET}")
        for name, url, err in problems:
            print(f"  {RED}{name}{RESET}: {err}  →  {url}")
    else:
        print(f"{GREEN}{BOLD}All targets returned results.{RESET}")


if __name__ == "__main__":
    main()
