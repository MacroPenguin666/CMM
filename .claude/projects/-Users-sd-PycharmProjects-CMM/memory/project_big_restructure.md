---
name: project-big-restructure
description: Major repo restructure plan — backend/frontend split, DB consolidation, private Supabase, GitHub Pages dashboard (2026-05-26)
metadata:
  type: project
---

Plan saved in repo as `big_restructuring_plan.md`. 6 phases:

1. **Folder restructure** — `backend/` (fetchers, scrapers, sources, runners, customs_scraper) + `frontend/` (index.html + data/). Single `run_fetch.py` replaces 8 `run_fetch_*.py` scripts.
2. **DB consolidation** — merge 5 SQLite DBs (feeds.db, unctad_trade.db, trade_stats.db, ccp_elites.db, regulations.db) into `data/cmm.db`. All table names are globally unique, no conflicts.
3. **Private Supabase** — PostgreSQL, service key only, RLS enabled with no anon policies. Dual-write: local SQLite + Supabase.
4. **Export pipeline** — `export.py` queries Supabase → writes ~30 JSON files to `frontend/data/`. GitHub Action runs every 4h (cron), uses service key from GitHub Secrets.
5. **Frontend rewiring** — replace `fetch('/api/...')` with `fetch('./data/....json')`. Remove voting, live flight/ship polling, AI advisor (POST endpoints can't work on static site).
6. **Cleanup** — old files to `.trash/`, update scheduler plists.

**Why:** line from [[project-pipeline-split]]. Architecture chosen: private Supabase (no public anon key) + GitHub Action export + GitHub Pages. See also [[feedback-data-privacy]].

**How to apply:** reference `big_restructuring_plan.md` in repo root when starting implementation. Execute phases sequentially — each has a verification step.