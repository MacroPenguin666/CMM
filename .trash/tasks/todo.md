# Split CMM into Batch + Realtime Pipelines

## Implementation
- [x] Enable WAL mode + add `batch_runs` table to `storage.py`
- [x] Create `runners/fetch_batch.py` and `run_fetch_batch.py`
- [x] Add RSS news thread to `fetch_realtime.py`
- [x] Deprecate old runners (`fetch_all`, `fetch_macro`, `fetch_news`)
- [x] Add `/api/pipeline/status` endpoint to `dashboard.py`
- [x] Create `launchd/com.chinapolicymonitor.batch.plist`

## Verification
- [x] All files compile without errors
- [x] WAL mode enabled on feeds.db
- [x] `batch_runs` table created
- [x] `/api/pipeline/status` endpoint registered

## Post-deploy (manual)
- [ ] Unload old launchd plists: `launchctl unload ~/Library/LaunchAgents/com.chinapolicymonitor.macro.plist`
- [ ] Unload old launchd plists: `launchctl unload ~/Library/LaunchAgents/com.chinapolicymonitor.news.plist`
- [ ] Load new batch plist: `launchctl load ~/Library/LaunchAgents/com.chinapolicymonitor.batch.plist`
- [ ] Verify realtime daemon still runs (it should — same script path, just has news thread now)
- [ ] Run `python run_fetch_batch.py --sources financial` to test a single-source batch run
- [ ] Hit `http://localhost:5001/api/pipeline/status` to confirm endpoint works end-to-end
