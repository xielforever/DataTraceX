# DataTraceX Goal

Build DataTraceX into a production-grade, run-centric lineage system for the
current Huawei Cloud DataArts/CDM/MRS/DWS/OBS environment.

This repository is intended for unattended coding-agent development. Before each
development pass, read:

- `docs/data-model.md`
- `docs/dataarts-ingestion.md`
- `docs/unmanned-vibe-coding-goal-plan.md`

## Current Baseline

- PostgreSQL and Neo4j are connected and verified.
- 512 DataArts jobs are ingested.
- 4509 DataArts nodes are materialized.
- PostgreSQL contains 6020 entities and 13031 lineage/design relationships.
- Neo4j contains the current graph projection.
- AI-assisted candidate persistence, manual review materialization, and the
  lineage Web UI now have an end-to-end validation path.

## Non-Negotiable Rules

- Do not write secrets, AK/SK, database passwords, SQL with sensitive literals,
  or `.env` contents into committed files.
- PostgreSQL is the source of truth. Neo4j is only a projection.
- Every inferred lineage edge must have evidence and confidence.
- Do not invent lineage for long Python, shell, Flink, or complex dynamic SQL.
  Use AI-assisted parsing and/or manual review workflows.
- Prefer idempotent scripts with `--missing-only` or checkpoint behavior.
- Respect Huawei API rate limits; use retries and conservative concurrency.
- After every coding pass, run tests and dependency checks.

## Immediate Next Goal

Implement the AI-assisted and human-review lineage workflow while continuing
deterministic collection:

1. Ingest DataArts script contents for `SCRIPT` mode SQL/Python/Spark/Flink nodes.
2. Create `CodeArtifact` entities and script evidence records.
3. Add an AI lineage analysis module for long Python and complex scripts.
4. Add a manual lineage review queue for uncertain or AI-suggested edges.
5. Persist accepted edges into PostgreSQL and project them to Neo4j.

Detailed tasks are in `docs/unmanned-vibe-coding-goal-plan.md`.
