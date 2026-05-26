# CMM — Tasks

## Active

- [ ] Merge `data/regulations.db` tables into `data/feeds.db` (one DB to rule them all)
- [ ] Rename `data/feeds.db` → `data/cmm.db` and update all references in `scripts/`
- [ ] Update `scheduler/*.plist` paths to point to new script locations in `scripts/runners/`
- [ ] Restore missing modules: `policy_monitor.db`, `policy_monitor.advisor`, `policy_monitor.polity`, `policy_monitor.eurostat` (were lost in rollback; dashboard imports them but files don't exist)
- [ ] Verify `pip install -e .` works after pyproject.toml change (where = ["scripts"])
- [ ] Test that `python live/run.py` starts the dashboard and all API endpoints return data

## Backlog

- [ ] China Customs scraper: site requires proxy — bootstrap.py implemented but site unreachable without China network
- [ ] MOFCOM scraper: same issue — only populate from VPN/China network
- [ ] Add automated test for each scraper to check DB row counts after a run
- [ ] energy-monitor.md idea (from ideas/) — investigate feasibility
- [ ] Dashboard: consider adding Eurostat tab once eurostat.py is restored
