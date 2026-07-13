---
name: reference-supabase
description: CANCELLED — Supabase dropped from CMM restructure plan 2026-05-27; keeping everything local with SQLite cmm.db
metadata:
  type: reference
---

**CANCELLED 2026-05-27.** Supabase was considered (2026-05-26) but dropped from the restructuring plan. CMM stays fully local: single SQLite `data/cmm.db`, Flask server at localhost:5001, no cloud DB, no GitHub Actions, no GitHub Pages. DB is not distributed at all (superseded 2026-07): `cmm-serve` auto-builds `data/cmm.db` from public sources on first run.