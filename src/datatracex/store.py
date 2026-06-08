from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime
from typing import Any

from .models import EdgeKind, Entity, Evidence, LineageEdge, Run, utc_now


class LineageStore:
    def __init__(self) -> None:
        self.entities: dict[str, Entity] = {}
        self.runs: dict[str, Run] = {}
        self.evidence: dict[str, Evidence] = {}
        self.edges: dict[tuple[str, str, EdgeKind, str | None], LineageEdge] = {}

    def upsert_entity(self, entity: Entity) -> Entity:
        existing = self.entities.get(entity.urn)
        if existing:
            existing.attrs.update(entity.attrs)
            existing.last_seen_at = utc_now()
            return existing
        self.entities[entity.urn] = entity
        return entity

    def upsert_run(self, run: Run) -> Run:
        existing = self.runs.get(run.run_id)
        if existing:
            existing.status = run.status or existing.status
            existing.attrs.update(run.attrs)
            existing.observed_at = utc_now()
            return existing
        self.runs[run.run_id] = run
        return run

    def add_evidence(self, evidence: Evidence) -> Evidence:
        self.evidence[evidence.evidence_id] = evidence
        return evidence

    def upsert_edge(self, edge: LineageEdge) -> LineageEdge:
        existing = self.edges.get(edge.key)
        if existing:
            existing.evidence_ids = sorted(set(existing.evidence_ids + edge.evidence_ids))
            existing.confidence = max(existing.confidence, edge.confidence)
            existing.attrs.update(edge.attrs)
            existing.last_seen_at = utc_now()
            return existing
        self.edges[edge.key] = edge
        return edge

    def lineage_for_node(self, urn: str, direction: str = "both") -> dict[str, Any]:
        matched = []
        for edge in self.edges.values():
            if direction in {"both", "out"} and edge.src_urn == urn:
                matched.append(edge)
            elif direction in {"both", "in"} and edge.dst_urn == urn:
                matched.append(edge)

        entity_urns = {urn}
        for edge in matched:
            entity_urns.add(edge.src_urn)
            entity_urns.add(edge.dst_urn)

        return {
            "root": urn,
            "entities": [self._to_json(self.entities[item]) for item in sorted(entity_urns) if item in self.entities],
            "edges": [self._to_json(edge) for edge in matched],
        }

    def run_detail(self, run_id: str) -> dict[str, Any]:
        run = self.runs.get(run_id)
        run_edges = [edge for edge in self.edges.values() if edge.run_id == run_id]
        evidence_ids = {item for edge in run_edges for item in edge.evidence_ids}
        evidence_ids.update(item.evidence_id for item in self.evidence.values() if item.run_id == run_id)
        return {
            "run": self._to_json(run) if run else None,
            "edges": [self._to_json(edge) for edge in run_edges],
            "evidence": [self._to_json(self.evidence[item]) for item in sorted(evidence_ids) if item in self.evidence],
        }

    def search_uri(self, uri: str) -> dict[str, Any]:
        matched_entities = [entity for entity in self.entities.values() if entity.urn.startswith(uri)]
        matched_edges = [
            edge
            for edge in self.edges.values()
            if edge.src_urn.startswith(uri) or edge.dst_urn.startswith(uri)
        ]
        return {
            "query": uri,
            "entities": [self._to_json(entity) for entity in matched_entities],
            "edges": [self._to_json(edge) for edge in matched_edges],
        }

    def stats(self) -> dict[str, int]:
        return {
            "entities": len(self.entities),
            "runs": len(self.runs),
            "evidence": len(self.evidence),
            "edges": len(self.edges),
        }

    def _to_json(self, value: Any) -> Any:
        if value is None:
            return None
        if is_dataclass(value):
            return self._to_json(asdict(value))
        if isinstance(value, dict):
            return {key: self._to_json(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._to_json(item) for item in value]
        if isinstance(value, datetime):
            return value.isoformat()
        return value
