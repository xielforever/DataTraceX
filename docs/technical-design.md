# DataTraceX Technical Design

## Positioning

DataTraceX is an observation-first lineage plane. DataArts/DGC built-in lineage is
used as a baseline and optional write-back target, while DataTraceX owns the
run-level evidence graph.

## MVP Scope

The MVP covers:

- normalized URNs for datasets, object paths, DataArts nodes, and runs
- in-memory entity, run, evidence, and edge storage
- local APIs for asset lineage, run evidence, and URI search
- collector and parser contracts for future Huawei Cloud integration

The MVP does not yet include:

- Huawei Cloud IAM authentication
- DataArts, CDM, MRS, DWS, or OBS API clients
- SQLGlot/Python AST/Flink Explain parsing
- durable database storage
- graph database projection

## Core Model

Every lineage edge should answer five questions:

- source entity
- target entity
- run context
- evidence source
- confidence score

The core objects are:

- `Entity`: dataset, OBS path, job node, connection, cluster, column, or code artifact
- `Run`: DataArts instance, CDM submission, MRS job, DWS query, or OBS scan
- `Evidence`: API payload, SQL AST, explain plan, runtime log, static code, URI match, or manual assertion
- `LineageEdge`: read, write, derive, contain, depend, use connection, or execute on

See `docs/data-model.md` for the current database and URN contract.

## URN Rules

Storage paths:

```text
s3a://bucket/path -> obs://bucket/path
obs://bucket/path -> obs://bucket/path
hdfs://nameservice/path -> hdfs://nameservice/path
```

Datasets:

```text
dws://cluster/database/schema/table
hive://cluster/database/table
```

DataArts:

```text
dataarts://workspace/job/{job_name}/node/{node_name}
dataarts://workspace/job/{job_name}/instance/{instance_id}
```

## API

```http
GET /health
GET /lineage/nodes/{urn}?direction=in|out|both
GET /lineage/runs/{run_id}
GET /search?uri={normalized_uri}
```

## Collector Roadmap

Collector output should be normalized facts, not UI-specific graph nodes.

1. `DataArtsCollector`
   Pull jobs, job definitions, job instances, and node instance details.

2. `CDMCollector`
   Pull CDM links, job definitions, status, submissions, counters, and external IDs.

3. `MRSCollector`
   Pull job executions, arguments, properties, app IDs, queues, and launcher IDs.

4. `DWSCollector`
   Pull cluster metadata, query monitoring records, query plans, and table/column catalog data.

5. `OBSCollector`
   Pull bucket metadata, object prefix listings, head metadata, and checkpoint state.

## Parser Roadmap

1. SQL parser
   Use SQLGlot for dialect-aware table and column lineage.

2. Python parser
   Use Python `ast` for imports, function calls, SQL strings, file reads/writes,
   f-strings, and object storage SDK calls.

3. Flink parser
   Split Flink SQL and Flink JAR handling. Store `EXPLAIN` output when available.

4. URI parser
   Extract and normalize `obs://`, `s3a://`, and `hdfs://` paths from payloads,
   logs, scripts, and job arguments.

## Confidence

Suggested buckets:

- `0.95-1.00`: explicit API lineage or deterministic engine plan
- `0.80-0.94`: SQL AST or strong runtime evidence
- `0.60-0.79`: parsed job arguments, config, or logs
- `<0.60`: weak URI match or manual low-confidence inference

Only high-confidence results should be written back to DataArts Catalog.
