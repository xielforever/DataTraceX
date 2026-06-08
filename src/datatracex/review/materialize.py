from __future__ import annotations

import hashlib
from typing import Any

from datatracex.ai.repository import LineageCandidateRepository
from datatracex.models import EdgeKind, Entity, EntityKind, Evidence, EvidenceKind, LineageEdge
from datatracex.postgres_store import PostgresFactStore


class CandidateMaterializer:
    def __init__(self, store: PostgresFactStore, repository: LineageCandidateRepository) -> None:
        self.store = store
        self.repository = repository

    def accept_and_materialize(self, candidate_id: str, reviewer: str, comment: str | None = None) -> str:
        self.repository.update_status(candidate_id, "accepted", reviewer, comment)
        candidate = self.repository.get_candidate(candidate_id)
        return self.materialize(candidate, reviewer=reviewer, comment=comment)

    def materialize(self, candidate: dict[str, Any], reviewer: str, comment: str | None = None) -> str:
        if candidate["status"] != "accepted":
            raise ValueError("only accepted candidates can be materialized")
        src_urn = candidate["proposed_src_urn"]
        dst_urn = candidate["proposed_dst_urn"]
        kind = EdgeKind(candidate["proposed_kind"])
        evidence_id = _manual_evidence_id(candidate["candidate_id"])
        edge = LineageEdge(
            src_urn=src_urn,
            dst_urn=dst_urn,
            kind=kind,
            confidence=float(candidate["proposed_confidence"]),
            edge_scope=candidate["proposed_edge_scope"],
            source_system="manual_review",
            evidence_ids=[evidence_id],
            attrs={
                "candidate_id": candidate["candidate_id"],
                "reviewer": reviewer,
                "ai_provider": candidate.get("provider"),
                "ai_model": candidate.get("model"),
                "rationale": candidate.get("rationale"),
            },
        )
        with self.store.session() as writer:
            writer.upsert_entity(_placeholder_entity(src_urn))
            writer.upsert_entity(_placeholder_entity(dst_urn))
            writer.add_evidence(
                Evidence(
                    evidence_id=evidence_id,
                    kind=EvidenceKind.MANUAL_ASSERTION,
                    source="Manual lineage review",
                    summary=f"Accepted lineage candidate {candidate['candidate_id']}",
                    source_system="manual_review",
                    source_api="ReviewMaterializer",
                    raw_ref=candidate["candidate_id"],
                    confidence=float(candidate["proposed_confidence"]),
                    attrs={
                        "reviewer": reviewer,
                        "comment": comment,
                        "candidate": {
                            "source_evidence_id": candidate.get("source_evidence_id"),
                            "line_start": candidate.get("line_start"),
                            "line_end": candidate.get("line_end"),
                        },
                    },
                )
            )
            edge_id = writer.upsert_edge(edge)
        return edge_id


def _placeholder_entity(urn: str) -> Entity:
    kind = EntityKind.PATH_PREFIX if urn.startswith(("obs://", "hdfs://")) else EntityKind.DATASET
    source_system = urn.split("://", 1)[0] if "://" in urn else "manual"
    return Entity(
        urn=urn,
        kind=kind,
        name=urn.rsplit("/", 1)[-1],
        source_system=source_system,
        qualified_name=urn,
        attrs={"created_by": "review_materializer"},
    )


def _manual_evidence_id(candidate_id: str) -> str:
    return "ev_manual_" + hashlib.sha256(candidate_id.encode("utf-8")).hexdigest()
