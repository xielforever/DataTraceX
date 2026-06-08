from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

import psycopg
from psycopg.rows import dict_row

from .lineage_analyzer import LineageCandidate


EDITABLE_FIELDS = {
    "proposed_src_urn",
    "proposed_dst_urn",
    "proposed_kind",
    "proposed_edge_scope",
    "proposed_confidence",
    "rationale",
    "line_start",
    "line_end",
}
ALLOWED_REVIEW_KINDS = {"reads", "writes", "derives_from", "uses_connection", "executes_on", "depends_on", "contains"}
ALLOWED_REVIEW_SCOPES = {"design", "run", "inferred"}


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

    def edit_candidate(
        self,
        candidate_id: str,
        updates: dict[str, Any],
        reviewer: str,
        comment: str | None = None,
    ) -> dict[str, Any]:
        sanitized = validate_candidate_updates(updates)
        if not sanitized:
            raise ValueError("no editable fields provided")
        event_id = _stable_id(
            "review",
            candidate_id,
            "edit",
            reviewer,
            json.dumps(sanitized, ensure_ascii=False, sort_keys=True),
            comment or "",
        )
        with self._connect() as conn:
            current = conn.execute(
                "SELECT * FROM lineage_candidate WHERE candidate_id = %s",
                (candidate_id,),
            ).fetchone()
            if not current:
                raise KeyError(candidate_id)
            before = dict(current)
            if before["status"] not in {"pending", "needs_more_context"}:
                raise ValueError("only pending or needs_more_context candidates can be edited")

            set_clauses = [f"{field} = %s" for field in sanitized]
            params = list(sanitized.values())
            params.extend([reviewer, comment, candidate_id])
            row = conn.execute(
                f"""
                UPDATE lineage_candidate
                SET {", ".join(set_clauses)},
                    reviewer = %s,
                    review_comment = %s,
                    updated_at = now()
                WHERE candidate_id = %s
                RETURNING *
                """,
                tuple(params),
            ).fetchone()
            after = dict(row)
            conn.execute(
                """
                INSERT INTO review_event
                  (review_event_id, candidate_id, action, reviewer, comment,
                   before_status, after_status, payload)
                VALUES (%s, %s, 'edit', %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT DO NOTHING
                """,
                (
                    event_id,
                    candidate_id,
                    reviewer,
                    comment,
                    before["status"],
                    after["status"],
                    _json({"before": _event_fields(before, sanitized), "after": _event_fields(after, sanitized)}),
                ),
            )
        return after

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


def validate_candidate_updates(updates: dict[str, Any]) -> dict[str, Any]:
    sanitized: dict[str, Any] = {}
    for field, value in updates.items():
        if field not in EDITABLE_FIELDS:
            continue
        if field in {"proposed_src_urn", "proposed_dst_urn", "rationale"}:
            text = _required_text(value, field)
            sanitized[field] = text
        elif field == "proposed_kind":
            text = _required_text(value, field)
            if text not in ALLOWED_REVIEW_KINDS:
                raise ValueError(f"unsupported proposed_kind: {text}")
            sanitized[field] = text
        elif field == "proposed_edge_scope":
            text = _required_text(value, field)
            if text not in ALLOWED_REVIEW_SCOPES:
                raise ValueError(f"unsupported proposed_edge_scope: {text}")
            sanitized[field] = text
        elif field == "proposed_confidence":
            confidence = float(value)
            if confidence < 0 or confidence > 1:
                raise ValueError("proposed_confidence must be between 0 and 1")
            sanitized[field] = confidence
        elif field in {"line_start", "line_end"}:
            sanitized[field] = _optional_positive_int(value, field)
    start = sanitized.get("line_start")
    end = sanitized.get("line_end")
    if start is not None and end is not None and end < start:
        raise ValueError("line_end must be >= line_start")
    return sanitized


def _required_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} is required")
    return value.strip()


def _optional_positive_int(value: Any, field: str) -> int | None:
    if value in {None, ""}:
        return None
    result = int(value)
    if result <= 0:
        raise ValueError(f"{field} must be positive")
    return result


def _event_fields(row: dict[str, Any], fields: dict[str, Any]) -> dict[str, Any]:
    return {field: row.get(field) for field in fields}


def _action(status: str) -> str:
    return "accept" if status == "accepted" else "reject" if status == "rejected" else "needs_more_context"


def _stable_id(prefix: str, *parts: str) -> str:
    return f"{prefix}_{hashlib.sha256('|'.join(parts).encode('utf-8')).hexdigest()}"


def _json(value: Any) -> str:
    return json.dumps(_jsonable(value), ensure_ascii=False, sort_keys=True)


def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    return value
