CREATE TABLE IF NOT EXISTS schema_version (
  version TEXT PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS entity (
  urn TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  source_system TEXT NOT NULL DEFAULT 'unknown',
  external_id TEXT,
  qualified_name TEXT,
  name TEXT NOT NULL,
  attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (urn <> ''),
  CHECK (kind <> '')
);

CREATE TABLE IF NOT EXISTS run (
  run_id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  source_system TEXT NOT NULL DEFAULT 'unknown',
  external_id TEXT,
  status TEXT,
  plan_time TIMESTAMPTZ,
  start_time TIMESTAMPTZ,
  end_time TIMESTAMPTZ,
  attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
  observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (run_id <> ''),
  CHECK (kind <> '')
);

CREATE TABLE IF NOT EXISTS raw_payload (
  raw_id TEXT PRIMARY KEY,
  service TEXT NOT NULL,
  category TEXT NOT NULL,
  source_key TEXT NOT NULL,
  endpoint TEXT,
  request_path TEXT,
  project_id TEXT,
  workspace_id TEXT,
  payload_hash TEXT NOT NULL,
  payload JSONB NOT NULL,
  captured_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (raw_id <> ''),
  CHECK (service <> ''),
  CHECK (category <> ''),
  CHECK (source_key <> ''),
  UNIQUE (service, category, source_key, payload_hash)
);

CREATE TABLE IF NOT EXISTS evidence (
  evidence_id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  source_system TEXT NOT NULL DEFAULT 'unknown',
  source TEXT NOT NULL,
  source_api TEXT,
  summary TEXT NOT NULL,
  raw_id TEXT REFERENCES raw_payload(raw_id) ON DELETE SET NULL,
  raw_ref TEXT,
  run_id TEXT REFERENCES run(run_id) ON DELETE SET NULL,
  confidence NUMERIC(4, 3) CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
  attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
  observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (evidence_id <> ''),
  CHECK (kind <> '')
);

CREATE TABLE IF NOT EXISTS lineage_edge (
  edge_id TEXT PRIMARY KEY,
  src_urn TEXT NOT NULL REFERENCES entity(urn) ON DELETE CASCADE,
  dst_urn TEXT NOT NULL REFERENCES entity(urn) ON DELETE CASCADE,
  kind TEXT NOT NULL,
  edge_scope TEXT NOT NULL DEFAULT 'design',
  run_id TEXT REFERENCES run(run_id) ON DELETE SET NULL,
  source_system TEXT NOT NULL DEFAULT 'unknown',
  confidence NUMERIC(4, 3) NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
  attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
  effective_from TIMESTAMPTZ,
  effective_to TIMESTAMPTZ,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (src_urn <> dst_urn),
  CHECK (kind <> ''),
  CHECK (edge_scope IN ('design', 'run', 'inferred'))
);

CREATE TABLE IF NOT EXISTS edge_evidence (
  edge_id TEXT NOT NULL REFERENCES lineage_edge(edge_id) ON DELETE CASCADE,
  evidence_id TEXT NOT NULL REFERENCES evidence(evidence_id) ON DELETE CASCADE,
  evidence_weight NUMERIC(4, 3) NOT NULL DEFAULT 1.0 CHECK (evidence_weight >= 0 AND evidence_weight <= 1),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (edge_id, evidence_id)
);

CREATE TABLE IF NOT EXISTS checkpoint_state (
  checkpoint_key TEXT PRIMARY KEY,
  service TEXT NOT NULL,
  cursor_value TEXT,
  attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

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
CREATE INDEX IF NOT EXISTS idx_entity_kind ON entity(kind);
CREATE INDEX IF NOT EXISTS idx_entity_source ON entity(source_system, external_id);
CREATE INDEX IF NOT EXISTS idx_entity_attrs ON entity USING GIN(attrs);
CREATE INDEX IF NOT EXISTS idx_run_kind_status ON run(kind, status);
CREATE INDEX IF NOT EXISTS idx_run_source ON run(source_system, external_id);
CREATE INDEX IF NOT EXISTS idx_evidence_run ON evidence(run_id);
CREATE INDEX IF NOT EXISTS idx_evidence_raw ON evidence(raw_id);
CREATE INDEX IF NOT EXISTS idx_edge_src ON lineage_edge(src_urn);
CREATE INDEX IF NOT EXISTS idx_edge_dst ON lineage_edge(dst_urn);
CREATE INDEX IF NOT EXISTS idx_edge_run ON lineage_edge(run_id);
CREATE INDEX IF NOT EXISTS idx_edge_scope ON lineage_edge(edge_scope, kind);
CREATE INDEX IF NOT EXISTS idx_edge_attrs ON lineage_edge USING GIN(attrs);
CREATE INDEX IF NOT EXISTS idx_raw_payload_service_category ON raw_payload(service, category, captured_at DESC);

INSERT INTO schema_version(version) VALUES ('001_schema') ON CONFLICT DO NOTHING;
