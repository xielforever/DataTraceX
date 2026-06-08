# DataTraceX Data Model

This document defines the pre-harvest lineage model. It is the contract used by
DataArts, CDM, MRS, DWS, OBS, parser, and graph-projection code.

## URN Contract

URNs are stable, normalized, lowercase identifiers. Path and name parts are URL
encoded so Chinese job names, spaces, and special characters are safe inside
PostgreSQL and Neo4j.

Examples:

```text
obs://bucket/path/dt=2026-06-08
dws://cluster/database/schema/table
hive://cluster/database/table
dataarts://workspace/job/job-name/node/node-name
dataarts://workspace/job/job-name/instance/instance-id
cdm://cluster-id/job/job-name
cdm://cluster-id/job/job-name/run/external-id
connection://dataarts/connection-id
code://sha256/content-hash
```

## PostgreSQL Fact Store

PostgreSQL is the authoritative store.

Core tables:

- `entity`: datasets, OBS paths, jobs, nodes, clusters, connections, columns
- `run`: DataArts instances, CDM submissions, MRS jobs, DWS queries, OBS scans
- `raw_payload`: original API payloads with payload hash and request context
- `evidence`: evidence records derived from payloads, logs, SQL, code, or plans
- `lineage_edge`: normalized graph facts
- `edge_evidence`: many-to-many relation between edges and evidence
- `checkpoint_state`: incremental collection cursors
- `graph_projection_state`: Neo4j projection cursors

Important edge fields:

- `edge_id`: stable hash of source, target, kind, scope, and run
- `edge_scope`: `design`, `run`, or `inferred`
- `confidence`: `0.000` to `1.000`
- `effective_from/effective_to`: business-time validity
- `first_seen_at/last_seen_at`: observation-time validity

## Neo4j Projection

Neo4j is not the source of truth. It receives a projection of:

- `(:Entity {urn, kind, source_system, external_id})`
- `(:Run {run_id, kind, source_system, external_id})`
- `(:Evidence {evidence_id, kind})`
- lineage relationships such as `READS`, `WRITES`, `DERIVES_FROM`

Projection state is tracked in PostgreSQL by `graph_projection_state`.

## Current Readiness

Prepared:

- URN generation helpers
- PostgreSQL schema and migration
- Neo4j constraints and indexes
- `PostgresFactStore` write interface
- remote PostgreSQL/Neo4j connectivity
- DataArts job/node definition ingestion
- DataArts first-pass design lineage derivation
- DataArts script content ingestion and CodeArtifact materialization
- deterministic SQL script lineage derivation
- AI-assisted candidate persistence
- manual review inspect/edit/accept/reject workflow
- PostgreSQL to Neo4j graph projection

Next:

- DataArts instance run materialization
- CDM job/link/submission ingestion when CDM cluster discovery is implemented
