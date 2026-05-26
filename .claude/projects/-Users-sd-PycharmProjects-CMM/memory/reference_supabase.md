---
name: reference-supabase
description: Supabase chosen as external DB host — setup details, free tier limits, security config
metadata:
  type: reference
---

Supabase selected as external DB for CMM (2026-05-26). Not yet created — pending implementation.

- Free tier: 500MB DB, unlimited API requests, 50k MAU
- Current data footprint: ~137MB across 5 SQLite DBs (~63 tables, ~1M rows)
- Largest tables: ship_history (262K rows), unctad_trade (149K rows), bis_policy_rates (108K rows)
- SQLite → PostgreSQL migration needed: `INSERT OR IGNORE` → `ON CONFLICT DO NOTHING`, `AUTOINCREMENT` → `SERIAL`, `datetime('now')` → `NOW()`, ~15 instances
- Security: RLS enabled on all tables, NO anon policies (default-deny), service-role key only
- Credentials go in `.env` (local) and GitHub Secrets (CI)
- Python SDK: `pip install supabase` (`supabase-py`)