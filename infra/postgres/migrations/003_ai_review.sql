CREATE TABLE IF NOT EXISTS lineage_candidate (
  candidate_id TEXT PRIMARY KEY,
  source_system TEXT NOT NULL DEFAULT 'ai',
  provider TEXT,
  model TEXT,
  status TEXT NOT NULL DEFAULT 'pending',
  node_urn TEXT,
  code_urn TEXT,
  source_evidence_id TEXT REFERENCES evidence(evidence_id) ON DELETE SET NULL,
  proposed_src_urn TEXT NOT NULL,
  proposed_dst_urn TEXT NOT NULL,
  proposed_kind TEXT NOT NULL,
  proposed_edge_scope TEXT NOT NULL DEFAULT 'inferred',
  proposed_confidence NUMERIC(4, 3) NOT NULL CHECK (proposed_confidence >= 0 AND proposed_confidence <= 1),
  rationale TEXT NOT NULL,
  line_start INTEGER,
  line_end INTEGER,
  candidate_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  prompt_hash TEXT,
  response_hash TEXT,
  response_raw JSONB,
  reviewer TEXT,
  review_comment TEXT,
  reviewed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (status IN ('pending', 'accepted', 'rejected', 'needs_more_context')),
  CHECK (proposed_kind IN ('reads', 'writes', 'derives_from', 'uses_connection', 'executes_on', 'depends_on', 'contains')),
  CHECK (line_start IS NULL OR line_start > 0),
  CHECK (line_end IS NULL OR line_end >= line_start)
);

CREATE TABLE IF NOT EXISTS review_event (
  review_event_id TEXT PRIMARY KEY,
  candidate_id TEXT NOT NULL REFERENCES lineage_candidate(candidate_id) ON DELETE CASCADE,
  action TEXT NOT NULL,
  reviewer TEXT,
  comment TEXT,
  before_status TEXT,
  after_status TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (action IN ('accept', 'reject', 'needs_more_context', 'edit', 'materialize'))
);

CREATE INDEX IF NOT EXISTS idx_lineage_candidate_status ON lineage_candidate(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_lineage_candidate_node ON lineage_candidate(node_urn);
CREATE INDEX IF NOT EXISTS idx_lineage_candidate_code ON lineage_candidate(code_urn);
CREATE INDEX IF NOT EXISTS idx_lineage_candidate_evidence ON lineage_candidate(source_evidence_id);
CREATE INDEX IF NOT EXISTS idx_lineage_candidate_json ON lineage_candidate USING GIN(candidate_json);
CREATE INDEX IF NOT EXISTS idx_review_event_candidate ON review_event(candidate_id, created_at DESC);

INSERT INTO schema_version(version) VALUES ('003_ai_review') ON CONFLICT DO NOTHING;
