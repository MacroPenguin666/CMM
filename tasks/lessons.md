# Lessons

- **Verify payloads, not status codes (2026-07-10).** The auto-refresh smoke test
  checked that endpoints returned HTTP 200 and missed that nearly every table in
  the local `cmm.db` was empty (endpoints return `200 []` on empty tables). When
  verifying "the dashboard works," assert on row counts / payload contents, and
  spot-check the DB itself (`SELECT COUNT(*)`) — a stub DB looks identical to a
  healthy one through status codes.

- **`data/cmm.db` on this machine was a stub until 2026-07-10.** The real DB
  (446 MB, 62 tables, ~2.9 M rows) lived offloaded in iCloud at
  `~/Documents/pycharm_archive/PycharmProjects/CMM/data/cmm.db` (there is NO
  Google Drive copy — earlier README claims were wrong). `api.py` reads the
  *legacy* table names
  (`items`, `financial_series`, `bruegel_series`, `ccp_cc_members`, old
  `fetch_log` with `item_count`) — a DB bootstrapped from `backend/schema.sql`
  (new 40-table layout) does NOT satisfy the API. Don't "fix" empty dashboards
  by re-fetching; check which DB file is actually in `data/` first.

- **Never document an external resource as a setup dependency without verifying
  it exists (2026-07-14).** I wrote README setup steps telling agents to download
  `cmm.db` from a Google Drive folder that doesn't exist (the link came from an
  old CLAUDE.md note). Setup instructions must only reference things the repo
  itself provides — prefer the self-contained path (fetchers rebuild all data
  from public sources) over any external artifact, and if an external link is
  ever unavoidable, fetch it first to prove it resolves.
