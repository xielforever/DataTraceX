# DataTraceX Dependencies

DataTraceX uses PostgreSQL as the fact store and Neo4j as the graph projection.

## Local Infrastructure

```powershell
docker compose up -d postgres neo4j
```

PostgreSQL:

```text
postgresql://datatracex:datatracex@127.0.0.1:5432/datatracex
```

Neo4j:

```text
Browser: http://127.0.0.1:7474
Bolt:    bolt://127.0.0.1:7687
User:    neo4j
Pass:    datatracex_graph
```

## Environment

Copy `.env.example` to a private local `.env` and fill in real values.

Do not commit `.env`, AK/SK, database passwords, or harvested payloads.

For real servers, override these values:

```text
DATATRACEX_POSTGRES_DSN=postgresql://user:password@pg-host:5432/datatracex
DATATRACEX_NEO4J_URI=bolt://neo4j-host:7687
DATATRACEX_NEO4J_USER=neo4j
DATATRACEX_NEO4J_PASSWORD=your-password
```

Then run:

```powershell
python scripts/check_dependencies.py
python scripts/init_neo4j_schema.py
```

If PostgreSQL already exists, apply `infra/postgres/init/001_schema.sql` with
your normal database migration process or `psql`.

## Production Preference

- PostgreSQL or GaussDB: authoritative lineage fact store
- Neo4j: graph projection for multi-hop lineage and impact analysis
- OBS or equivalent object storage: large raw payload and log retention
- DWS: optional reporting and audit analytics

## Current Verification

The dependency checker intentionally fails fast when services are unavailable or
credentials are wrong. This is useful before starting real Huawei Cloud
collection because it prevents harvesting payloads without a durable fact store.
