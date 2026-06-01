# 4-Week MVP Workplan (20 Working Days)

## Scope Baseline (Frozen for Sprint)
- Products: 1 product family (defined HS/CN code set).
- Countries: 3-5 EU countries.
- Frequency: monthly.
- Data sources: China monthly CSV input + Eurostat programmatic pull.
- Outputs: price comparison, relevance score, trend/ranking dashboard.

## Week 1 - Architecture, Access, and Data Contracts

### Day 1: Project kickoff and scope lock
**Tickets**
- Confirm product family, countries, and KPI definitions.
- Define MVP vs non-MVP boundaries.

**Acceptance criteria**
- Signed scope document with explicit in/out boundaries.
- KPI glossary approved (price gap, relevance score, trend metric).

### Day 2: Source access and legal/operational constraints
**Tickets**
- Document China Customs access mode (API/file/manual ingest entry).
- Validate Eurostat API datasets and endpoint parameters.

**Acceptance criteria**
- Source access matrix completed (auth, rate limits, refresh cadence, fallback path).
- Compliance note for usage/redistribution constraints.

### Day 3: Canonical data model v1
**Tickets**
- Define entities: Product, Country, Month, Price, SectorWeight, JobRun.
- Define required metadata fields (unit, currency, source, confidence).

**Acceptance criteria**
- ERD and data dictionary published.
- Example records for each entity validated by team.

### Day 4: Repo and service skeleton
**Tickets**
- Create modules: `ingestion/`, `transform/`, `analytics/`, `api/`, `web/`, `docs/`.
- Establish coding standards, lint/test skeleton, env management.

**Acceptance criteria**
- Services boot locally.
- CI pipeline runs basic checks on PR.

### Day 5: Test data and end-to-end dry run
**Tickets**
- Load sample China CSV and small Eurostat extract.
- Execute raw -> transformed -> query dry run.

**Acceptance criteria**
- Demo query returns one product-country-month comparison.
- Data quality report template created (missing/duplicate/outlier fields).

## Week 2 - Ingestion and Harmonization

### Day 6: China CSV ingestion pipeline
**Tickets**
- Build schema validator and parser.
- Add bad-record quarantine and ingestion logs.

**Acceptance criteria**
- Valid file ingests successfully.
- Invalid rows are quarantined with reason codes.

### Day 7: Eurostat connector
**Tickets**
- Build API client with dataset parameterization.
- Retrieve local price and sector/GDP structure data.

**Acceptance criteria**
- Scheduled/manual pull works for scoped countries/months.
- Raw payload snapshots stored with run metadata.

### Day 8: Product concordance mapping v1
**Tickets**
- Create mapping table (China codes to Eurostat/internal product IDs).
- Add mapping confidence levels.

**Acceptance criteria**
- >=80% mapping coverage for scoped product family.
- Unmapped codes listed in a review report.

### Day 9: Unit and currency normalization
**Tickets**
- Normalize measurement units and currency basis.
- Add conversion provenance fields.

**Acceptance criteria**
- All scoped records end in canonical unit/currency.
- Conversion errors flagged and excluded from KPIs.

### Day 10: Data quality gates
**Tickets**
- Implement checks: missing fields, duplicates, outlier thresholds, stale data.
- Define fail/warn thresholds.

**Acceptance criteria**
- Pipeline blocks on fail-level issues.
- QA report produced automatically per run.

## Week 3 - Analytics Engine and API

### Day 11: Core KPI calculations
**Tickets**
- Implement absolute and percentage price gap.
- Add MoM change and 3-month moving average.

**Acceptance criteria**
- KPIs computed for all scoped country-product-month rows.
- Unit tests pass for edge cases (zero/negative/NaN handling).

### Day 12: Relevance score model v1
**Tickets**
- Implement sector-weighted GDP contribution proxy.
- Add configurable weighting parameters.

**Acceptance criteria**
- Score generated per country-product-month.
- Methodology note includes formula and assumptions.

### Day 13: Ranking and indicator logic
**Tickets**
- Create combined indicator (price pressure x relevance).
- Add confidence score from mapping/completeness.

**Acceptance criteria**
- Ranking endpoint returns sorted countries/products.
- Confidence score visible in response payload.

### Day 14: API endpoints v1
**Tickets**
- Build endpoints: time series, product-country snapshot, relevance breakdown, ranking.
- Add filtering and pagination.

**Acceptance criteria**
- OpenAPI spec published.
- P95 response time within MVP target for scoped dataset.

### Day 15: Performance and reliability pass
**Tickets**
- Add materialized views/caching for heavy queries.
- Improve retry/backoff and idempotency for jobs.

**Acceptance criteria**
- Endpoints stable under expected load.
- Re-running same month does not duplicate data.

## Week 4 - Web MVP, Monitoring, and Handover

### Day 16: Dashboard shell and navigation
**Tickets**
- Build core app layout and routes.
- Add global filters (month/product/country).

**Acceptance criteria**
- Dashboard loads and filters sync across views.
- Mobile and desktop layouts functional.

### Day 17: Price comparison and trend views
**Tickets**
- Implement China vs EU price chart and gap table.
- Add trend visualization (MoM + moving average).

**Acceptance criteria**
- Charts render for all scoped countries/products.
- Tooltips and legends show normalized units/currency.

### Day 18: Relevance and ranking views
**Tickets**
- Build country relevance composition panel.
- Add ranking table with confidence badges.

**Acceptance criteria**
- User can drill down from rank to country detail.
- Methodology panel explains score interpretation clearly.

### Day 19: Ops and observability
**Tickets**
- Add job monitoring, stale-data alerts, and run history.
- Add admin run status page.

**Acceptance criteria**
- Failed/stale runs trigger alerts.
- Operator can identify failure reason in under 5 minutes.

### Day 20: UAT, fixes, and release readiness
**Tickets**
- Execute UAT checklist and bug triage.
- Finalize docs: runbook, data dictionary, KPI methodology.

**Acceptance criteria**
- Critical/high bugs closed or mitigated.
- Monthly refresh and dashboard demo succeeds end-to-end.

## Definition of Done (MVP)
- Monthly data ingestion works for both sources (or approved fallback).
- Harmonized dataset exists with QA gating and run logs.
- KPI engine produces price gap, relevance, and ranking indicators.
- API and web dashboard deliver scoped views reliably.
- Documentation and runbook enable repeat operation without developer hand-holding.

## Critical Risks and Mitigations
- Source instability/access gaps: keep manual CSV fallback and cached pulls.
- Product-code mismatch quality: confidence scoring plus unmapped review queue.
- Unit/currency inconsistency: enforce canonical schema at transform gate.
- Over-scope risk: freeze to 1 product family and 3-5 countries for MVP.
