from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg.rows import dict_row

from .lineage_analyzer import LineageCandidate


@dataclass(frozen=True, slots=True)
class StoredCandidate:
    candidate_id: str
    status: str


class LineageCandidateRepository:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    def add_candidate(
        self,
        candidate: LineageCandidate,
        source_evidence_id: str | None,
        node_urn: str | None,
        code_urn: str | None,
        provider: str,
        model: str,
        prompt_hash: str | None = None,
        response_hash: str | None = None,
        response_raw: dict[str, Any] | None = None,
    ) -> StoredCandidate:
        candidate_id = candidate_id_for(candidate, node_urn, code_urn)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO lineage_candidate
                  (candidate_id, source_system, provider, model, status, node_urn,
                   code_urn, source_evidence_id, proposed_src_urn, proposed_dst_urn,
                   proposed_kind, proposed_edge_scope, proposed_confidence, rationale,
                   line_start, line_end, candidate_json, prompt_hash, response_hash, response_raw)
                VALUES
                  (%s, 'ai', %s, %s, 'pending', %s, %s, %s, %s, %s, %s,
                   'inferred', %s, %s, %s, %s, %s::jsonb, %s, %s, %s::jsonb)
                ON CONFLICT (candidate_id) DO UPDATE SET
                  provider = EXCLUDED.provider,
                  model = EXCLUDED.model,
                  source_evidence_id = COALESCE(EXCLUDED.source_evidence_id, lineage_candidate.source_evidence_id),
                  proposed_confidence = EXCLUDED.proposed_confidence,
                  rationale = EXCLUDED.rationale,
                  candidate_json = EXCLUDED.candidate_json,
                  prompt_hash = COALESCE(EXCLUDED.prompt_hash, lineage_candidate.prompt_hash),
                  response_hash = COALESCE(EXCLUDED.response_hash, lineage_candidate.response_hash),
                  response_raw = COALESCE(EXCLUDED.response_raw, lineage_candidate.response_raw),
                  updated_at = now()
                """,
                (
                    candidate_id,
                    provider,
                    model,
                    node_urn,
                    code_urn,
                    source_evidence_id,
                    candidate.source_urn,
                    candidate.target_urn,
                    candidate.edge_kind,
                    candidate.confidence,
                    candidate.rationale,
                    candidate.line_start,
                    candidate.line_end,
                    _json(candidate.raw or {}),
                    prompt_hash,
                    response_hash,
                    _json(response_raw or {}),
                ),
            )
        return StoredCandidate(candidate_id, "pending")

    def list_candidates(self, status: str = "pending", limit: int = 100) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM lineage_candidate
                WHERE status = %s
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (status, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def update_status(self, candidate_id: str, status: str, reviewer: str, comment: str | None = None) -> None:
        if status not in {"accepted", "rejected", "needs_more_context"}:
            raise ValueError(f"unsupported review status: {status}")
        event_id = _stable_id("review", candidate_id, status, reviewer, comment or "")
        with self._connect() as conn:
            current = conn.execute(
                "SELECT status FROM lineage_candidate WHERE candidate_id = %s",
                (candidate_id,),
            ).fetchone()
            if not current:
                raise KeyError(candidate_id)
            before = current["status"]
            conn.execute(
                """
                UPDATE lineage_candidate
                SET status = %s,
                    reviewer = %s,
                    review_comment = %s,
                    reviewed_at = now(),
                    updated_at = now()
                WHERE candidate_id = %s
                """,
                (status, reviewer, comment, candidate_id),
            )
            conn.execute(
                """
                INSERT INTO review_event
                  (review_event_id, candidate_id, action, reviewer, comment, before_status, after_status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (event_id, candidate_id, _action(status), reviewer, comment, before, status),
            )

    def get_candidate(self, candidate_id: str) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM lineage_candidate WHERE candidate_id = %s", (candidate_id,)).fetchone()
        if not row:
            raise KeyError(candidate_id)
        return dict(row)

    def _connect(self) -> psycopg.Connection:
        return psycopg.connect(self.dsn, row_factory=dict_row)


def candidate_id_for(candidate: LineageCandidate, node_urn: str | None, code_urn: str | None) -> str:
    return _stable_id(
        "cand",
        node_urn or "",
        code_urn or "",
        candidate.source_urn,
        candidate.target_urn,
        candidate.edge_kind,
        str(candidate.line_start or ""),
        str(candidate.line_end or ""),
    )


def _action(status: str) -> str:
    return "accept" if status == "accepted" else "reject" if status == "rejected" else "needs_more_context"


def _stable_id(prefix: str, *parts: str) -> str:
    return f"{prefix}_{hashlib.sha256('|'.join(parts).encode('utf-8')).hexdigest()}"


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)
