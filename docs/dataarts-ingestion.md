# DataArts Job Ingestion

DataTraceX currently ingests DataArts job definitions into PostgreSQL.

## Command

```powershell
python scripts/ingest_dataarts_jobs.py --page-size 100 --workers 6 --max-retries 5 --missing-only
```

Options:

- `--page-size`: DataArts job list page size.
- `--workers`: concurrent `ShowJob` calls. Keep this modest because Huawei APIG
  currently throttles this user at about 30 requests per second.
- `--max-retries`: retry count for HTTP 429 / `APIGW.0308`.
- `--missing-only`: skip jobs that already have a `raw_payload` record in
  `dataarts/job_detail`.

## Current Production Snapshot

The first real DataArts definition ingestion completed with:

```text
job_detail_raw_keys: 512
job_entities: 512
node_entities: 4509
dataarts_edges: 10021
edge_evidence_links: 10021
```

Node type distribution:

```text
CDMJob: 1108
DataQualityMonitor: 972
HiveSQL: 855
Dummy: 556
DWSSQL: 406
Python: 323
ForEachJob: 112
RESTAPI: 71
OBSManager: 41
SMN: 32
DLFSubJob: 21
Shell: 4
DataMigration: 2
MRSFlinkJob: 2
MRSSparkPython: 2
SparkSQL: 2
```

## Notes

The job list endpoint reports `total = 512`, but its `offset` behaves like a
page number, not a row offset. For example, `offset=1` returns the second page,
while `offset=100` returns no records.

The first implementation was intentionally simple and too slow because it opened
a PostgreSQL connection for each entity, evidence, and edge. The current path
uses a single PostgreSQL session per ingestion stage and concurrent `ShowJob`
fetching with rate-limit retries.

## Derive Design-Time Lineage

```powershell
python scripts/derive_dataarts_lineage.py
```

Current derived facts:

```text
job_payloads: 512
nodes_seen: 4509
nodes_with_facts: 2741
entities_upserted: 3006
evidence_added: 1457
edges_upserted: 3048
```

Current relationship distribution in PostgreSQL:

```text
CONTAINS: 5018
DEPENDS_ON: 5003
USES_CONNECTION: 1590
EXECUTES_ON: 1108
WRITES: 262
READS: 50
```

Parser scope:

- `DWSSQL` / `HiveSQL` / `SparkSQL`: parse direct `sql` statements with SQLGlot.
- `OBSManager`: parse source and target OBS directories.
- `CDMJob`: anchor DataArts nodes to CDM job URNs.
- Generic node properties: extract `obs://`, `s3a://`, `s3://`, and `hdfs://` URI references.

Most `HiveSQL` and `SparkSQL` nodes are currently `SCRIPT` mode and only expose
`scriptName/scriptVersion` in `ShowJob`; they require the DataArts script API
before reliable table lineage can be derived.

## Project to Neo4j

```powershell
python scripts/project_to_neo4j.py
```

Current graph projection:

```text
Entity nodes: 9350
Relationships: 21607
```

## Script Content Ingestion

```powershell
python scripts/probe_dataarts_scripts.py --sample-size 3
python scripts/ingest_dataarts_scripts.py --workers 6 --missing-only
```

The script API shape was validated with DataArts V1:

```text
GET /v1/{project_id}/scripts
GET /v1/{project_id}/scripts/{script_name}?version={scriptVersion}
```

The detail payload contains `content`, `name`, `type`, `version`, directory and
connection metadata. The probe script reports keys and content length only; it
does not print script content.

Current script ingestion snapshot:

```text
script references: 1280
distinct script name/version pairs: 778
script_detail raw payloads: 778
CodeArtifact entities: 777
node uses_code edges: 1280
script evidence records: 778
```

One fewer CodeArtifact than script payloads is expected because CodeArtifact URNs
are based on content SHA256, and two script versions currently share identical
content.

## Script SQL Lineage

```powershell
python scripts/derive_dataarts_script_lineage.py
```

Current deterministic script parser snapshot:

```text
SQL script references: 955
references with parsed edges: 929
parsed statements: 4800
failed statements: 598
script SQL evidence records: 955
script-derived entities upserted: 7292
script-derived edges upserted: 7292
```

The parser stores evidence and confidence for every derived edge. Unsupported or
partial Hive/DWS syntax is counted, not logged as script text, and does not
cause invented lineage.
