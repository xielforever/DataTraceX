CREATE CONSTRAINT datatracex_entity_urn IF NOT EXISTS
FOR (n:Entity)
REQUIRE n.urn IS UNIQUE;

CREATE CONSTRAINT datatracex_run_id IF NOT EXISTS
FOR (n:Run)
REQUIRE n.run_id IS UNIQUE;

CREATE CONSTRAINT datatracex_evidence_id IF NOT EXISTS
FOR (n:Evidence)
REQUIRE n.evidence_id IS UNIQUE;

CREATE INDEX datatracex_entity_kind IF NOT EXISTS
FOR (n:Entity)
ON (n.kind);

CREATE INDEX datatracex_run_kind IF NOT EXISTS
FOR (n:Run)
ON (n.kind);

CREATE INDEX datatracex_entity_source IF NOT EXISTS
FOR (n:Entity)
ON (n.source_system, n.external_id);

CREATE INDEX datatracex_run_source IF NOT EXISTS
FOR (n:Run)
ON (n.source_system, n.external_id);

CREATE INDEX datatracex_evidence_kind IF NOT EXISTS
FOR (n:Evidence)
ON (n.kind);
