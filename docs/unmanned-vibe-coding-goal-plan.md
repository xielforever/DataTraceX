# Plan: DataTraceX Unmanned Vibe Coding Goal

**Generated**: 2026-06-09  
**Estimated Complexity**: High

## Overview

DataTraceX already has the first real DataArts definition graph in PostgreSQL
and Neo4j. The next goal is to move from deterministic job/node structure to
actionable lineage extraction.

## Current Pass Status

Validated on 2026-06-09:

- DataArts script API probing is implemented in `scripts/probe_dataarts_scripts.py`.
- DataArts script content ingestion is implemented in
  `scripts/ingest_dataarts_scripts.py`.
- 778 real script detail payloads were collected.
- 777 CodeArtifact entities were created from content SHA256 hashes.
- 1280 DataArts node to CodeArtifact `uses_code` relationships were persisted.
- SQL script lineage derivation is implemented in
  `scripts/derive_dataarts_script_lineage.py`.
- 955 SQL script references produced parser evidence; 929 had deterministic
  table-level edges.
- Long/complex script AI candidate ingestion is implemented in
  `scripts/analyze_dataarts_scripts_with_ai.py`.
- The Web UI now renders directed upstream/downstream data flow and supports
  inspecting, editing, accepting, and rejecting review candidates.
- PostgreSQL and Neo4j currently contain 9350 entities and 21607 relationships.
- `src/datatracex/review/api.py` provides the review queue service layer.

Open-source parsing is only reliable for clear SQL statements, explicit OBS
paths, and simple static references. Many real Python, shell, Flink, and dynamic
SQL scripts are too long or too contextual for SQLGlot, regex, or Python AST
alone. The system must therefore support three lanes:

- deterministic parser lane
- AI-assisted parser lane
- manual review and correction lane

All three lanes must write evidence-backed, confidence-scored edges into
PostgreSQL before Neo4j projection.

## Prerequisites

- `.env` contains valid Huawei, PostgreSQL, and Neo4j settings.
- PostgreSQL schema versions include `001_schema` and `002_lineage_core_upgrade`.
- Neo4j schema has been applied.
- DataArts job definitions have been ingested.
- Tests pass with `python -m pytest -q`.

## Sprint 1: Script Content Ingestion

**Goal**: Retrieve the actual script body for DataArts `SCRIPT` mode nodes so
SQL/Python/Flink lineage can be analyzed from code, not from script names.

**Demo/Validation**:

- `raw_payload` contains script payloads.
- Script nodes have `CodeArtifact` entities.
- Script content is linked to DataArts nodes by evidence.

### Task 1.1: Discover DataArts Script API Shape

- **Location**: `src/datatracex/huawei/`, `scripts/`
- **Description**: Add a small probe script that calls the DataArts script/list
  and script/detail APIs using existing AK/SK signing.
- **Dependencies**: Existing `HuaweiClient`.
- **Acceptance Criteria**:
  - Probe can fetch metadata for at least one `scriptName/scriptVersion` pair.
  - Does not print script content by default.
- **Validation**:
  - Run probe on 3 known nodes from `HiveSQL`, `Python`, and `MRSFlinkJob`.

### Task 1.2: Add Script Client

- **Location**: `src/datatracex/huawei/dataarts.py`
- **Description**: Add script list/detail methods once the endpoint and
  parameter shape are confirmed.
- **Dependencies**: Task 1.1.
- **Acceptance Criteria**:
  - Methods return raw JSON payloads.
  - Request paths and endpoint are captured for `raw_payload`.
- **Validation**:
  - Unit-test URL/path construction where possible.

### Task 1.3: Ingest Script Raw Payloads

- **Location**: `src/datatracex/ingest/dataarts_scripts.py`,
  `scripts/ingest_dataarts_scripts.py`
- **Description**: Read DataArts job details from PostgreSQL, find nodes with
  `statementOrScript=SCRIPT`, fetch script content, and write to `raw_payload`.
- **Dependencies**: Task 1.2.
- **Acceptance Criteria**:
  - Supports `--missing-only`.
  - Handles Huawei API throttling.
  - Stores payload hash and source key.
- **Validation**:
  - Script raw count matches distinct script name/version pairs.

### Task 1.4: Materialize CodeArtifact Entities

- **Location**: `src/datatracex/ingest/dataarts_scripts.py`,
  `src/datatracex/urn.py`
- **Description**: Create `code://sha256/...` entities and link DataArts nodes
  to code artifacts.
- **Dependencies**: Task 1.3.
- **Acceptance Criteria**:
  - Each script body has a stable content hash.
  - DataArts node has an evidence-backed edge to its script artifact.
- **Validation**:
  - Query PostgreSQL for CodeArtifact count and node-code edges.

## Sprint 2: Deterministic Parser Expansion

**Goal**: Extract reliable lineage from script content where deterministic
parsing is strong enough.

**Demo/Validation**:

- SQL script lineage produces table-level `READS` and `WRITES`.
- Python parser extracts explicit SQL strings and URI references.
- Parser never emits high-confidence edges from weak string guesses.

### Task 2.1: SQL Script Parser

- **Location**: `src/datatracex/parsers/sql_script.py`
- **Description**: Parse HiveSQL, SparkSQL, and DWSSQL script bodies with
  SQLGlot, including multi-statement scripts.
- **Dependencies**: Sprint 1.
- **Acceptance Criteria**:
  - Handles `INSERT`, `CREATE TABLE AS`, `ALTER TABLE ADD PARTITION`, and CTEs.
  - Emits table-level edges with parser evidence.
- **Validation**:
  - Golden tests with 10 real script samples.

### Task 2.2: Python Static Extractor

- **Location**: `src/datatracex/parsers/python_static.py`
- **Description**: Use Python AST/tokenization to extract imports, function
  calls, string literals, f-strings, explicit SQL strings, and object-storage
  URIs.
- **Dependencies**: Sprint 1.
- **Acceptance Criteria**:
  - Emits evidence facts, not final lineage, for dynamic or partial SQL.
  - Flags long or dynamic scripts for AI review.
- **Validation**:
  - Golden tests with short Python samples and long real script excerpts.

### Task 2.3: URI and OBS Path Parser

- **Location**: `src/datatracex/parsers/uri_parser.py`
- **Description**: Normalize OBS, S3A, HDFS, wildcard, and partition-style paths.
- **Dependencies**: Existing `urn.py`.
- **Acceptance Criteria**:
  - Distinguishes path-prefix entities from exact objects when possible.
  - Emits evidence-backed low/medium-confidence edges.
- **Validation**:
  - Tests for `obs://`, `s3a://`, and partition paths.

## Sprint 3: AI-Assisted Lineage Analysis

**Goal**: Add an AI lane for long Python, shell, Flink, and dynamic SQL scripts
where open-source parsers are insufficient.

**Demo/Validation**:

- Long scripts can be chunked, redacted, sent to an AI provider, and converted
  into structured lineage candidates.
- AI output is never accepted blindly.
- All AI suggestions enter a reviewable state with evidence, confidence, and
  rationale.

### Task 3.1: AI Provider Abstraction

- **Location**: `src/datatracex/ai/provider.py`,
  `src/datatracex/settings.py`
- **Description**: Add provider-agnostic AI client interface with an
  OpenAI-compatible adapter shape. Read keys only from environment variables.
- **Dependencies**: None.
- **Acceptance Criteria**:
  - No AI key is committed.
  - Adapter supports request timeout, model name, max tokens, and retries.
- **Validation**:
  - Mock-provider unit tests.

### Task 3.2: Sensitive Text Redaction

- **Location**: `src/datatracex/ai/redaction.py`
- **Description**: Redact AK/SK, passwords, tokens, JDBC URLs with passwords,
  and suspicious credential-like literals before AI submission.
- **Dependencies**: Task 3.1.
- **Acceptance Criteria**:
  - Redaction runs before every AI request.
  - Raw original content remains only in PostgreSQL evidence storage with access
    controls, not in logs.
- **Validation**:
  - Unit tests for common credential patterns.

### Task 3.3: Long Script Chunking

- **Location**: `src/datatracex/ai/chunker.py`
- **Description**: Split long Python/shell/Flink scripts into semantically useful
  chunks while preserving line numbers and surrounding context.
- **Dependencies**: Task 3.2.
- **Acceptance Criteria**:
  - Chunk metadata includes original line ranges.
  - Overlap is configurable.
- **Validation**:
  - Tests with long scripts and function/class boundaries.

### Task 3.4: Structured AI Lineage Prompt

- **Location**: `src/datatracex/ai/prompts.py`,
  `src/datatracex/ai/lineage_analyzer.py`
- **Description**: Ask AI to return strict JSON candidates:
  source assets, target assets, operation, confidence, rationale, evidence line
  ranges, and unresolved assumptions.
- **Dependencies**: Tasks 3.1-3.3.
- **Acceptance Criteria**:
  - Invalid JSON is rejected and retried.
  - AI output is stored as evidence, not direct truth.
  - Low-confidence suggestions go to manual review.
- **Validation**:
  - Mock AI response tests.

### Task 3.5: AI Candidate Persistence

- **Location**: `infra/postgres/migrations/003_ai_review.sql`,
  `src/datatracex/ai/repository.py`
- **Description**: Add tables for AI lineage candidates and review state.
- **Dependencies**: Task 3.4.
- **Acceptance Criteria**:
  - Candidate states: `pending`, `accepted`, `rejected`, `needs_more_context`.
  - Accepted candidates can generate PostgreSQL lineage edges.
- **Validation**:
  - Migration applies idempotently.

## Sprint 4: Manual Lineage Review

**Goal**: Let humans confirm, correct, or reject uncertain lineage.

**Demo/Validation**:

- Reviewer can inspect script evidence, AI rationale, candidate edges, and
  accept/reject/edit them.
- Accepted manual edges are persisted with manual evidence.

### Task 4.1: Review Queue API

- **Location**: `src/datatracex/review/api.py`
- **Description**: Add APIs to list pending candidates, inspect evidence, accept,
  reject, and edit candidate edges.
- **Dependencies**: Sprint 3.
- **Acceptance Criteria**:
  - All review actions are auditable.
  - Manual edits produce new evidence records.
- **Validation**:
  - API tests with fixture candidates.

### Task 4.2: Minimal Review UI

- **Location**: `src/datatracex/web/`
- **Description**: Build a dense operational UI for candidate review, not a
  marketing page. Show code evidence, line ranges, proposed edges, confidence,
  and rationale.
- **Dependencies**: Task 4.1.
- **Acceptance Criteria**:
  - Reviewer can accept/reject/edit without leaving the page.
  - No sensitive values are displayed unless explicitly enabled for admin.
- **Validation**:
  - Browser screenshot and workflow test.

### Task 4.3: Manual Edge Materialization

- **Location**: `src/datatracex/review/materialize.py`
- **Description**: Convert accepted review candidates into `lineage_edge`,
  `edge_evidence`, and Neo4j projection updates.
- **Dependencies**: Task 4.1.
- **Acceptance Criteria**:
  - Accepted edges have `source_system=manual_review` or equivalent marker.
  - Rejected candidates never create lineage edges.
- **Validation**:
  - PostgreSQL and Neo4j count checks.

## Sprint 5: CDM and Runtime Evidence

**Goal**: Resolve CDMJob anchors into actual CDM job configs and submissions.

**Demo/Validation**:

- CDM jobs linked from DataArts are expanded to source/sink configs.
- CDM submissions become run records.

### Task 5.1: CDM Cluster Discovery

- **Location**: `src/datatracex/huawei/cdm.py`
- **Description**: Discover CDM clusters or use configured cluster IDs when
  available.
- **Dependencies**: Existing Huawei signing client.
- **Acceptance Criteria**:
  - Does not require user-provided CDM cluster ID if API can discover it.
- **Validation**:
  - Discovery returns the known cluster referenced by DataArts CDMJob nodes.

### Task 5.2: CDM Job Detail Ingestion

- **Location**: `src/datatracex/ingest/cdm_jobs.py`
- **Description**: Fetch CDM job/link/status payloads for referenced CDM jobs.
- **Dependencies**: Task 5.1.
- **Acceptance Criteria**:
  - Raw payloads stored in PostgreSQL.
  - Source/sink assets parsed when explicit.
- **Validation**:
  - Sample 20 CDM jobs and compare source/sink counts.

### Task 5.3: CDM Submission Runs

- **Location**: `src/datatracex/ingest/cdm_runs.py`
- **Description**: Fetch CDM submissions and create run-scoped evidence and
  edges.
- **Dependencies**: Task 5.2.
- **Acceptance Criteria**:
  - `run` table contains CDM submission runs.
  - Runtime edges use `edge_scope=run`.
- **Validation**:
  - Query run detail by external ID.

## Sprint 6: Runtime Instances and DWS/MRS Expansion

**Goal**: Add run-level evidence from DataArts instances, DWS queries, and MRS
jobs.

### Task 6.1: DataArts Instance Ingestion

- **Location**: `src/datatracex/ingest/dataarts_instances.py`
- **Description**: Fetch recent DataArts job instances and node instance details.
- **Dependencies**: DataArts jobs ingestion.
- **Acceptance Criteria**:
  - `run` table contains DataArts instance IDs.
  - Node logs and statuses are evidence.
- **Validation**:
  - 7-day window ingestion completes with checkpoint.

### Task 6.2: MRS Runtime Evidence

- **Location**: `src/datatracex/ingest/mrs_jobs.py`
- **Description**: Resolve MRS Spark/Flink job arguments, properties, app IDs,
  queues, and script/JAR references.
- **Dependencies**: DataArts instance ingestion.
- **Acceptance Criteria**:
  - MRS runs linked to DataArts nodes.
  - Arguments produce URI and code artifact evidence.
- **Validation**:
  - Sample MRSFlinkJob and MRSSparkPython nodes.

### Task 6.3: DWS Query Evidence

- **Location**: `src/datatracex/ingest/dws_queries.py`
- **Description**: Pull DWS query monitoring records and match DataArts job
  context where possible.
- **Dependencies**: DWS read-only database/API credentials.
- **Acceptance Criteria**:
  - DWS query IDs become run records.
  - Query SQL becomes evidence.
- **Validation**:
  - Query by known DataArts job name or application name.

## Testing Strategy

- Unit tests for URN normalization, parsers, AI output validation, redaction, and
  repository writes.
- Golden tests using selected real payloads with sensitive values redacted.
- Integration tests:
  - PostgreSQL dependency check
  - Neo4j projection count check
  - DataArts `--missing-only` idempotency
  - AI candidate lifecycle from pending to accepted/rejected

## Potential Risks & Gotchas

- Huawei APIG throttles DataArts calls. Use conservative workers and retry
  `APIGW.0308`.
- DataArts `offset` behaves like page number for jobs, not row offset.
- `SCRIPT` mode nodes require separate script APIs; do not infer lineage from
  script names alone.
- Long Python and shell scripts can exceed model context. Use chunking and line
  ranges.
- AI can hallucinate assets. AI suggestions must be candidates, not committed
  lineage, until deterministic validation or human review accepts them.
- Sensitive values may appear in scripts or job properties. Redact before AI and
  before logs.
- Neo4j projection must remain disposable. PostgreSQL is the source of truth.

## Rollback Plan

- Parser-derived edges can be identified by `source_api=DataArtsNodeParser`,
  `source_system=dataarts`, or AI/manual source markers.
- Rejecting AI/manual candidates should not delete raw evidence.
- If a parser release emits bad edges, mark affected candidates rejected or
  delete edges by evidence/source marker and re-project Neo4j from PostgreSQL.

## Autonomous Coding Loop

For each unattended coding pass:

1. Read `GOAL.md` and this plan.
2. Pick the next incomplete atomic task.
3. Implement only that task and its tests.
4. Run `python -m pytest -q`.
5. Run dependency checks if storage or projection changed.
6. Update this document or the relevant status document with results.
7. Do not ask for input unless blocked by missing credentials, missing endpoint
   shape, or destructive production action.
