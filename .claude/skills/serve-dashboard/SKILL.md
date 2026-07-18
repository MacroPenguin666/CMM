---
name: serve-dashboard
description: Use when asked to serve, run, start, or bring up the CMM dashboard, or to verify the dashboard's data is current — starts cmm-serve on port 5001 (auto-builds data/cmm.db from public sources if missing) and confirms every data group is fresh, refreshing any stale ones.
---

# Serve the CMM Dashboard

Bring the dashboard up **immediately** and confirm its data is on the latest
stage, downloading fresh data for anything stale. Serving and freshness are one
flow: `cmm-serve` auto-builds a missing `data/cmm.db` and starts a background
scheduler that re-fetches stale groups; this skill drives that and verifies it.

## Steps

### 1. Serve (reuse if already up)
The server binds `0.0.0.0:5001`; a second start on the same port fails, so
check first.

```bash
curl -s -o /dev/null -w "%{http_code}" http://localhost:5001/   # 200 = already serving
```

If not 200, start it in the **background** and poll until it answers:

```bash
cmm-serve --port 5001 > data/logs/serve.log 2>&1 &   # run detached / in background
for i in $(seq 1 8); do
  curl -s -o /dev/null -w "%{http_code}" http://localhost:5001/ | grep -q 200 && { echo UP; break; }
  sleep 1
done
```

Report the URL: **http://localhost:5001**

- If `data/cmm.db` is missing or under ~1 MB, `cmm-serve` builds it from public
  sources in the background (needs no API keys). The dashboard is usable at once;
  panels fill in as fetches land — news first, the full batch takes hours. For a
  foreground build instead, run `python -m backend.bootstrap_db`.
- Never point anyone at an external download for data — it is always built locally.

### 2. Verify freshness
```bash
curl -s http://localhost:5001/api/refresh/status | python3 -m json.tool
```

Each of the four groups reports `due` and `running`:

| Group | Refresh interval | Fetch command (foreground / blocking) |
|-------|------------------|----------------------------------------|
| `news` | 4 h | `python -m backend.cli news` |
| `policies` | 12 h | `python -m backend.cli policies` |
| `batch` | 24 h | `python -m backend.cli batch` |
| `commodities` | 7 d | `python -m backend.runners.fetch_commodities --no-trade` |

### 3. Download new where stale
The scheduler (started with the server, alive when `scheduler_alive: true`)
already runs every `due` group on startup, most-stale first, one at a time —
so `due: true` with `running: true` (or another group running ahead of it)
means the download is **already handled**; just report it.

Force an **immediate, blocking** refresh only when the user wants the data
current *now* rather than on the scheduler's cadence — run the group's command
from the table above. Duplicate runs are safe (fetchers dedupe on insert).

## Report
State the URL, `scheduler_alive`, and per-group status: fresh, refreshing now,
or forced. Progress log: `data/logs/auto_refresh.log`.

## Common mistakes
- **Starting a second server** on 5001 — check for a 200 first; reuse it.
- **Running `cmm-serve` in the foreground** — it blocks the shell; always detach.
- **Forcing `batch` needlessly** — it is long-running; let the scheduler do it
  unless the user explicitly wants to block until fresh.
- **`--no-refresh`** disables the scheduler entirely — never use it here; it
  leaves data stale and panels empty.
