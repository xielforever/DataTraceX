# E2E Validation

Validated on 2026-06-09.

## Backend

```text
PostgreSQL: ok
Neo4j: ok
schema versions: 001_schema, 002_lineage_core_upgrade, 003_ai_review
tests: 14 passed
```

## DataArts Graph

The UI loaded a real DataArts job graph:

```text
root: dataarts://.../job/job_sdi_fm_1d_0540
nodes: 293
links: 460
```

This verifies that the frontend is not rendering only a summary. It can display
the full returned graph for a real DataArts job at depth 2.

## AI + Manual Review

The mock AI flow created a lineage candidate, persisted it, and accepted it via
the review workflow.

Additional browser validation created a pending candidate and accepted it from
the web UI:

```text
pending before: 1
pending after: 0
manual_review edges: 2
```

After Neo4j projection, the UI loaded the accepted manual lineage:

```text
root: dws://ui-e2e/dw/public/fact_orders
nodes: 2
links: 1
relationship: DERIVES_FROM 0.73
```

## Frontend

Validated surfaces:

- Stats header
- Search by job/table/URN text
- Full graph rendering in SVG
- Node detail panel
- Review Queue list
- Accept action from UI
- Neo4j-backed graph refresh after projection

The page is served by:

```powershell
python scripts/start_web.py --host 127.0.0.1 --port 8787
```
