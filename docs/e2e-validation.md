# E2E Validation

Validated on 2026-06-09.

## Backend

```text
PostgreSQL: ok
Neo4j: ok
schema versions: 001_schema, 002_lineage_core_upgrade, 003_ai_review
tests: 20 passed
```

Current fact-store snapshot:

```text
entities: 9348
edges: 21606
evidence: 3712
raw_payload: 1297
script_detail payloads: 778
CodeArtifact entities: 777
uses_code edges: 1280
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

Additional validation created an AI candidate from a real DataArts Python script
payload using the mock provider. The reviewer accepted it from the Web UI:

```text
pending before: 1
pending after: 0
manual_review accepted candidates: 3
Neo4j projection after acceptance: 9348 entities, 21606 relationships
```

Real AI execution is wired through the OpenAI-compatible provider. It is not run
until `DATATRACEX_AI_API_KEY`, `DATATRACEX_AI_BASE_URL`, and
`DATATRACEX_AI_MODEL` are provided.

## Frontend

Validated surfaces:

- Stats header
- Search by job/table/URN text
- Full graph rendering in SVG
- Directed upstream/downstream data-flow layout
- Arrow rendering for `READS`, `WRITES`, and `DERIVES_FROM`
- Dense graph rendering without label overlap
- Node detail panel
- Review Queue list
- Accept action from UI
- Neo4j-backed graph refresh after projection

Latest browser validation loaded:

```text
root: dws://dws_dev/th_ai/sdi_th/sdi_th_etl_program_log
nodes: 418
links: 600
data-flow arrow links: 146
visible node labels in dense mode: 1
pending candidates after accept: 0
```

The page is served by:

```powershell
python scripts/start_web.py --host 127.0.0.1 --port 8787
```
