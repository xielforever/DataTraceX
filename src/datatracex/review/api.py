from __future__ import annotations

from typing import Any

import psycopg
from psycopg.rows import dict_row

from datatracex.ai.redaction import redact_sensitive_text
from datatracex.ai.repository import LineageCandidateRepository
from datatracex.ingest.dataarts_scripts import extract_script_content
from datatracex.postgres_store import PostgresFactStore

from .materialize import CandidateMaterializer


class ReviewQueueService:
    def __init__(self, postgres_dsn: str) -> None:
        self.postgres_dsn = postgres_dsn
        self.repository = LineageCandidateRepository(postgres_dsn)

    def list_candidates(self, status: str = "pending", limit: int = 50) -> list[dict[str, Any]]:
        return self.repository.list_candidates(status=status, limit=limit)

    def candidate_detail(self, candidate_id: str) -> dict[str, Any]:
        candidate = self.repository.get_candidate(candidate_id)
        evidence, snippet = self._candidate_evidence(candidate)
        return {"candidate": candidate, "evidence": evidence, "snippet": snippet}

    def edit_candidate(
        self,
        candidate_id: str,
        payload: dict[str, Any],
        reviewer: str = "web",
    ) -> dict[str, Any]:
        updates = payload.get("updates") if isinstance(payload.get("updates"), dict) else payload
        comment = payload.get("comment") if isinstance(payload.get("comment"), str) else "edited from review API"
        candidate = self.repository.edit_candidate(candidate_id, updates, reviewer, comment)
        return {"candidate_id": candidate_id, "candidate": candidate}

    def accept_candidate(self, candidate_id: str, reviewer: str = "web", comment: str | None = None) -> dict[str, Any]:
        materializer = CandidateMaterializer(PostgresFactStore(self.postgres_dsn), self.repository)
        edge_id = materializer.accept_and_materialize(candidate_id, reviewer, comment or "accepted from review API")
        return {"candidate_id": candidate_id, "edge_id": edge_id}

    def reject_candidate(self, candidate_id: str, reviewer: str = "web", comment: str | None = None) -> dict[str, Any]:
        self.repository.update_status(candidate_id, "rejected", reviewer, comment or "rejected from review API")
        return {"candidate_id": candidate_id, "status": "rejected"}

    def _candidate_evidence(self, candidate: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        evidence_id = candidate.get("source_evidence_id")
        if not evidence_id:
            return None, None
        with psycopg.connect(self.postgres_dsn, row_factory=dict_row) as conn:
            row = conn.execute(
                """
                SELECT ev.evidence_id, ev.kind, ev.source_system, ev.source, ev.source_api,
                       ev.summary, ev.raw_id, ev.raw_ref, ev.confidence, ev.attrs,
                       rp.source_key, rp.category, rp.payload_hash, rp.payload
                FROM evidence ev
                LEFT JOIN raw_payload rp ON rp.raw_id = ev.raw_id
                WHERE ev.evidence_id = %s
                """,
                (evidence_id,),
            ).fetchone()
        if not row:
            return None, None
        evidence = dict(row)
        payload = evidence.pop("payload", None)
        snippet = redacted_script_snippet(
            payload,
            candidate.get("line_start"),
            candidate.get("line_end"),
        )
        return evidence, snippet


def redacted_script_snippet(payload: Any, line_start: Any, line_end: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    content = extract_script_content(payload)
    if content is None:
        return None
    lines = content.splitlines()
    if not lines:
        return {"start_line": 1, "end_line": 1, "text": "", "redactions": 0}
    start = _line_number(line_start) or 1
    end = _line_number(line_end) or min(len(lines), start + 24)
    start = max(1, min(start, len(lines)))
    end = max(start, min(end, len(lines)))
    window_start = max(1, start - 3)
    window_end = min(len(lines), end + 3)
    selected = "\n".join(
        f"{line_no}: {lines[line_no - 1]}"
        for line_no in range(window_start, window_end + 1)
    )
    redacted = redact_sensitive_text(selected)
    return {
        "start_line": window_start,
        "end_line": window_end,
        "text": redacted.text[:12000],
        "redactions": redacted.replacements,
        "truncated": len(redacted.text) > 12000,
    }


def _line_number(value: Any) -> int | None:
    if value in {None, ""}:
        return None
    try:
        result = int(value)
    except (TypeError, ValueError):
        return None
    return result if result > 0 else None
