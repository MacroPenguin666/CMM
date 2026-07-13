---
name: project-big-restructure
description: Major repo restructure plan — backend/frontend split, single cmm.db, local Flask only; no Supabase/GitHub Pages (revised 2026-05-27)
metadata:
  type: project
---

Plan saved in repo as `big_restructuring_plan.md`. 4 phases:

1. **Folder restructure** — `backend/` (fetchers, scrapers, sources, runners, api.py, customs_scraper) + `frontend/` (index.html + geo/). Single `run_fetch.py` replaces 8 `run_fetch_*.py` scripts.
2. **DB consolidation** — merge 5 SQLite DBs (feeds.db, unctad_trade.db, trade_stats.db, ccp_elites.db, regulations.db) into `data/cmm.db`. Single `storage.get_conn()` everywhere — no module keeps its own DB path constant. All table names are globally unique, no conflicts.
3. **Distribution setup** — `data/` gitignored. ~~`cmm.db` shared via Google Drive~~ (superseded 2026-07: no external distribution — `cmm-serve` auto-builds `cmm.db` from public sources on first run; `backend/bootstrap_db.py` is the foreground equivalent, no API keys needed). README has quick-start section.
4. **Cleanup** — old files to `.trash/`, update scheduler plists.

**Architecture decision (2026-05-27):** removed Supabase, GitHub Pages, GitHub Actions, and static export step. Dashboard is always local Flask (localhost:5001). Code repo is shareable without data; DB distributed as a separate file artifact.

**Why:** user wants one central DB that can be distributed separately from code, keeping everything local.

**How to apply:** reference `big_restructuring_plan.md` in repo root when starting implementation. Execute phases sequentially — each has a verification step.