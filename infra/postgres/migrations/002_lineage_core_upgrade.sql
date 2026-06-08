CREATE TABLE IF NOT EXISTS schema_version (
  version TEXT PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO schema_version(version) VALUES ('001_schema') ON CONFLICT DO NOTHING;

ALTER TABLE entity ADD COLUMN IF NOT EXISTS source_system TEXT NOT NULL DEFAULT 'unknown';
ALTER TABLE entity ADD COLUMN IF NOT EXISTS external_id TEXT;
ALTER TABLE entity ADD COLUMN IF NOT EXISTS qualified_name TEXT;

ALTER TABLE run ADD COLUMN IF NOT EXISTS source_system TEXT NOT NULL DEFAULT 'unknown';
ALTER TABLE run ADD COLUMN IF NOT EXISTS external_id TEXT;

ALTER TABLE raw_payload ADD COLUMN IF NOT EXISTS endpoint TEXT;
ALTER TABLE raw_payload ADD COLUMN IF NOT EXISTS request_path TEXT;
ALTER TABLE raw_payload ADD COLUMN IF NOT EXISTS project_id TEXT;
ALTER TABLE raw_payload ADD COLUMN IF NOT EXISTS workspace_id TEXT;
ALTER TABLE raw_payload ADD COLUMN IF NOT EXISTS payload_hash TEXT;
UPDATE raw_payload
SET payload_hash = md5(payload::text)
WHERE payload_hash IS NULL;
ALTER TABLE raw_payload ALTER COLUMN payload_hash SET NOT NULL;

ALTER TABLE evidence ADD COLUMN IF NOT EXISTS source_system TEXT NOT NULL DEFAULT 'unknown';
ALTER TABLE evidence ADD COLUMN IF NOT EXISTS source_api TEXT;
ALTER TABLE evidence ADD COLUMN IF NOT EXISTS raw_id TEXT REFERENCES raw_payload(raw_id) ON DELETE SET NULL;
ALTER TABLE evidence ADD COLUMN IF NOT EXISTS confidence NUMERIC(4, 3) CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1));

ALTER TABLE lineage_edge ADD COLUMN IF NOT EXISTS edge_id TEXT;
ALTER TABLE lineage_edge ADD COLUMN IF NOT EXISTS edge_scope TEXT NOT NULL DEFAULT 'design';
ALTER TABLE lineage_edge ADD COLUMN IF NOT EXISTS source_system TEXT NOT NULL DEFAULT 'unknown';
ALTER TABLE lineage_edge ALTER COLUMN run_id DROP NOT NULL;

UPDATE lineage_edge
SET edge_id = md5(src_urn || '|' || dst_urn || '|' || kind || '|' || edge_scope || '|' || COALESCE(run_id, ''))
WHERE edge_id IS NULL;

ALTER TABLE lineage_edge ALTER COLUMN edge_id SET NOT NULL;

DO $$
DECLARE
  constraint_name TEXT;
  is_edge_id_pk BOOLEAN;
BEGIN
  SELECT c.conname,
         EXISTS (
           SELECT 1
           FROM unnest(c.conkey) AS key(attnum)
           JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = key.attnum
           WHERE a.attname = 'edge_id'
         )
  INTO constraint_name, is_edge_id_pk
  FROM pg_constraint
  c
  WHERE c.conrelid = 'lineage_edge'::regclass
    AND c.contype = 'p'
  LIMIT 1;

  IF constraint_name IS NOT NULL AND NOT is_edge_id_pk THEN
    EXECUTE format('ALTER TABLE lineage_edge DROP CONSTRAINT %I', constraint_name);
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint c
    JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
    WHERE c.conrelid = 'lineage_edge'::regclass
      AND c.contype = 'p'
      AND a.attname = 'edge_id'
  ) THEN
    ALTER TABLE lineage_edge ADD PRIMARY KEY (edge_id);
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS edge_evidence (
  edge_id TEXT NOT NULL REFERENCES lineage_edge(edge_id) ON DELETE CASCADE,
  evidence_id TEXT NOT NULL REFERENCES evidence(evidence_id) ON DELETE CASCADE,
  evidence_weight NUMERIC(4, 3) NOT NULL DEFAULT 1.0 CHECK (evidence_weight >= 0 AND evidence_weight <= 1),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (edge_id, evidence_id)
);

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_name = 'lineage_edge'
      AND column_name = 'evidence_ids'
  ) THEN
    EXECUTE '
      INSERT INTO edge_evidence(edge_id, evidence_id)
      SELECT le.edge_id, unnest(le.evidence_ids)
      FROM lineage_edge le
      ON CONFLICT DO NOTHING
    ';
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS graph_projection_state (
  projection_name TEXT PRIMARY KEY,
  last_projected_at TIMESTAMPTZ,
  last_edge_seen_at TIMESTAMPTZ,
  last_entity_seen_at TIMESTAMPTZ,
  attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_lineage_edge_natural
  ON lineage_edge (src_urn, dst_urn, kind, edge_scope, COALESCE(run_id, ''));
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_payload_natural
  ON raw_payload (service, category, source_key, payload_hash);
CREATE INDEX IF NOT EXISTS idx_entity_source ON entity(source_system, external_id);
CREATE INDEX IF NOT EXISTS idx_run_source ON run(source_system, external_id);
CREATE INDEX IF NOT EXISTS idx_evidence_raw ON evidence(raw_id);
CREATE INDEX IF NOT EXISTS idx_edge_scope ON lineage_edge(edge_scope, kind);

INSERT INTO schema_version(version) VALUES ('002_lineage_core_upgrade') ON CONFLICT DO NOTHING;
