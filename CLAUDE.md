# CMM

<!-- General workflow, task-management, and core-principle rules live in the
     user's global ~/.claude/CLAUDE.md and are always loaded — not duplicated here. -->

## Project-Specific Information
- Data is NOT distributed externally. `data/cmm.db` is built locally from public
  sources: `cmm-serve` bootstraps it automatically in the background when missing
  (or `python -m backend.bootstrap_db` in the foreground). Never point users or
  agents to an external download for data.
