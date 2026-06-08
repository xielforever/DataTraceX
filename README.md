# DataTraceX

DataTraceX is a run-centric evidence lineage system based on the analysis in
`deep-research-report.md`. The formal storage direction is PostgreSQL for
lineage facts and Neo4j for graph projection.

The dependency foundation is:

- PostgreSQL: authoritative entity, edge, run, evidence, raw payload, checkpoint store
- Neo4j: multi-hop lineage and impact-analysis projection
- OBS or filesystem: large raw payload and log retention
- Huawei Cloud AK/SK: loaded only from private environment variables

## Local Dependencies

```powershell
docker compose up -d postgres neo4j
```

```powershell
python -m pip install -e ".[dev]"
python scripts/check_dependencies.py
python scripts/init_neo4j_schema.py
```

Run tests:

```powershell
$env:PYTHONPATH="src"
python -m pytest -q
```

## Current Shape

```text
src/datatracex/
  cli.py              local server entrypoint
  http_api.py         standard-library HTTP API
  models.py           entity, run, evidence, and edge models
  store.py            in-memory repository
  urn.py              URI/URN normalization
  demo.py             seeded demo graph
  collectors/base.py  collector contract
  parsers/base.py     parser contract
```

## Design Principles

- Built-in DataArts lineage is treated as a baseline, not the source of truth.
- Every edge should carry run context, evidence, and confidence.
- Raw evidence must be retained separately from normalized graph facts.
- OBS paths, `s3a://` paths, DWS tables, Hive tables, and DataArts runs must
  resolve to comparable URNs before graph merging.

See `docs/technical-design.md` for the current architecture notes.
See `docs/dependencies.md` for PostgreSQL and Neo4j setup.
See `docs/dataarts-ingestion.md` for the real DataArts job ingestion status.
See `GOAL.md` and `docs/unmanned-vibe-coding-goal-plan.md` for the autonomous development backlog.
See `docs/e2e-validation.md` for the latest end-to-end validation record.
