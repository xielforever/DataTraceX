from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from dataclasses import asdict, is_dataclass
from datetime import datetime
from collections.abc import Iterator
from typing import Any

import psycopg
from psycopg.rows import dict_row

from .models import Entity, Evidence, LineageEdge, Run


class PostgresFactStore:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    def upsert_entity(self, entity: Entity) -> Entity:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO entity
                  (urn, kind, source_system, external_id, qualified_name, name, attrs, first_seen_at, last_seen_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
                ON CONFLICT (urn) DO UPDATE SET
                  kind = EXCLUDED.kind,
                  source_system = EXCLUDED.source_system,
                  external_id = COALESCE(EXCLUDED.external_id, entity.external_id),
                  qualified_name = COALESCE(EXCLUDED.qualified_name, entity.qualified_name),
                  name = EXCLUDED.name,
                  attrs = entity.attrs || EXCLUDED.attrs,
                  last_seen_at = now()
                """,
                (
                    entity.urn,
                    entity.kind.value,
                    entity.source_system,
                    entity.external_id,
                    entity.qualified_name,
                    entity.name,
                    _json(entity.attrs),
                    entity.first_seen_at,
                    entity.last_seen_at,
                ),
            )
        return entity

    def upsert_run(self, run: Run) -> Run:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO run
                  (run_id, kind, source_system, external_id, status, plan_time, start_time, end_time, attrs, observed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                ON CONFLICT (run_id) DO UPDATE SET
                  kind = EXCLUDED.kind,
                  source_system = EXCLUDED.source_system,
                  external_id = COALESCE(EXCLUDED.external_id, run.external_id),
                  status = COALESCE(EXCLUDED.status, run.status),
                  plan_time = COALESCE(EXCLUDED.plan_time, run.plan_time),
                  start_time = COALESCE(EXCLUDED.start_time, run.start_time),
                  end_time = COALESCE(EXCLUDED.end_time, run.end_time),
                  attrs = run.attrs || EXCLUDED.attrs,
                  observed_at = now()
                """,
                (
                    run.run_id,
                    run.kind.value,
                    run.source_system,
                    run.external_id,
                    run.status,
                    run.plan_time,
                    run.start_time,
                    run.end_time,
                    _json(run.attrs),
                    run.observed_at,
                ),
            )
        return run

    def add_raw_payload(
        self,
        service: str,
        category: str,
        source_key: str,
        payload: Any,
        endpoint: str | None = None,
        request_path: str | None = None,
        project_id: str | None = None,
        workspace_id: str | None = None,
    ) -> str:
        payload_json = _json(payload)
        payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        raw_id = _stable_id("raw", service, category, source_key, payload_hash)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO raw_payload
                  (raw_id, service, category, source_key, endpoint, request_path,
                   project_id, workspace_id, payload_hash, payload)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (raw_id) DO NOTHING
                """,
                (
                    raw_id,
                    service,
                    category,
                    source_key,
                    endpoint,
                    request_path,
                    project_id,
                    workspace_id,
                    payload_hash,
                    payload_json,
                ),
            )
        return raw_id

    def add_evidence(self, evidence: Evidence) -> Evidence:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO evidence
                  (evidence_id, kind, source_system, source, source_api, summary,
                   raw_id, raw_ref, run_id, confidence, attrs, observed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                ON CONFLICT (evidence_id) DO UPDATE SET
                  source_system = EXCLUDED.source_system,
                  source = EXCLUDED.source,
                  source_api = COALESCE(EXCLUDED.source_api, evidence.source_api),
                  summary = EXCLUDED.summary,
                  raw_id = COALESCE(EXCLUDED.raw_id, evidence.raw_id),
                  raw_ref = COALESCE(EXCLUDED.raw_ref, evidence.raw_ref),
                  run_id = COALESCE(EXCLUDED.run_id, evidence.run_id),
                  confidence = COALESCE(EXCLUDED.confidence, evidence.confidence),
                  attrs = evidence.attrs || EXCLUDED.attrs,
                  observed_at = now()
                """,
                (
                    evidence.evidence_id,
                    evidence.kind.value,
                    evidence.source_system,
                    evidence.source,
                    evidence.source_api,
                    evidence.summary,
                    evidence.raw_id,
                    evidence.raw_ref,
                    evidence.run_id,
                    evidence.confidence,
                    _json(evidence.attrs),
                    evidence.observed_at,
                ),
            )
        return evidence

    def upsert_edge(self, edge: LineageEdge) -> str:
        edge_id = edge_id_for(edge)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO lineage_edge
                  (edge_id, src_urn, dst_urn, kind, edge_scope, run_id, source_system,
                   confidence, attrs, effective_from, effective_to, first_seen_at, last_seen_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s)
                ON CONFLICT (edge_id) DO UPDATE SET
                  confidence = GREATEST(lineage_edge.confidence, EXCLUDED.confidence),
                  attrs = lineage_edge.attrs || EXCLUDED.attrs,
                  effective_from = COALESCE(EXCLUDED.effective_from, lineage_edge.effective_from),
                  effective_to = COALESCE(EXCLUDED.effective_to, lineage_edge.effective_to),
                  last_seen_at = now()
                """,
                (
                    edge_id,
                    edge.src_urn,
                    edge.dst_urn,
                    edge.kind.value,
                    edge.edge_scope,
                    edge.run_id,
                    edge.source_system,
                    edge.confidence,
                    _json(edge.attrs),
                    edge.effective_from,
                    edge.effective_to,
                    edge.first_seen_at,
                    edge.last_seen_at,
                ),
            )
            for evidence_id in edge.evidence_ids:
                conn.execute(
                    """
                    INSERT INTO edge_evidence(edge_id, evidence_id)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (edge_id, evidence_id),
                )
        return edge_id

    def stats(self) -> dict[str, int]:
        with self._connect() as conn:
            return {
                "entities": conn.execute("SELECT COUNT(*) AS n FROM entity").fetchone()["n"],
                "runs": conn.execute("SELECT COUNT(*) AS n FROM run").fetchone()["n"],
                "evidence": conn.execute("SELECT COUNT(*) AS n FROM evidence").fetchone()["n"],
                "edges": conn.execute("SELECT COUNT(*) AS n FROM lineage_edge").fetchone()["n"],
                "raw_payload": conn.execute("SELECT COUNT(*) AS n FROM raw_payload").fetchone()["n"],
            }

    def raw_source_keys(self, service: str, category: str) -> set[str]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT DISTINCT source_key FROM raw_payload WHERE service = %s AND category = %s",
                (service, category),
            ).fetchall()
        return {row["source_key"] for row in rows}

    @contextmanager
    def session(self) -> Iterator[PostgresFactSession]:
        with self._connect() as conn:
            yield PostgresFactSession(conn)

    def _connect(self) -> psycopg.Connection:
        return psycopg.connect(self.dsn, row_factory=dict_row)


class PostgresFactSession:
    def __init__(self, conn: psycopg.Connection) -> None:
        self.conn = conn

    def upsert_entity(self, entity: Entity) -> Entity:
        self.conn.execute(
            """
            INSERT INTO entity
              (urn, kind, source_system, external_id, qualified_name, name, attrs, first_seen_at, last_seen_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
            ON CONFLICT (urn) DO UPDATE SET
              kind = EXCLUDED.kind,
              source_system = EXCLUDED.source_system,
              external_id = COALESCE(EXCLUDED.external_id, entity.external_id),
              qualified_name = COALESCE(EXCLUDED.qualified_name, entity.qualified_name),
              name = EXCLUDED.name,
              attrs = entity.attrs || EXCLUDED.attrs,
              last_seen_at = now()
            """,
            (
                entity.urn,
                entity.kind.value,
                entity.source_system,
                entity.external_id,
                entity.qualified_name,
                entity.name,
                _json(entity.attrs),
                entity.first_seen_at,
                entity.last_seen_at,
            ),
        )
        return entity

    def upsert_run(self, run: Run) -> Run:
        self.conn.execute(
            """
            INSERT INTO run
              (run_id, kind, source_system, external_id, status, plan_time, start_time, end_time, attrs, observed_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
            ON CONFLICT (run_id) DO UPDATE SET
              kind = EXCLUDED.kind,
              source_system = EXCLUDED.source_system,
              external_id = COALESCE(EXCLUDED.external_id, run.external_id),
              status = COALESCE(EXCLUDED.status, run.status),
              plan_time = COALESCE(EXCLUDED.plan_time, run.plan_time),
              start_time = COALESCE(EXCLUDED.start_time, run.start_time),
              end_time = COALESCE(EXCLUDED.end_time, run.end_time),
              attrs = run.attrs || EXCLUDED.attrs,
              observed_at = now()
            """,
            (
                run.run_id,
                run.kind.value,
                run.source_system,
                run.external_id,
                run.status,
                run.plan_time,
                run.start_time,
                run.end_time,
                _json(run.attrs),
                run.observed_at,
            ),
        )
        return run

    def add_raw_payload(
        self,
        service: str,
        category: str,
        source_key: str,
        payload: Any,
        endpoint: str | None = None,
        request_path: str | None = None,
        project_id: str | None = None,
        workspace_id: str | None = None,
    ) -> str:
        payload_json = _json(payload)
        payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        raw_id = _stable_id("raw", service, category, source_key, payload_hash)
        self.conn.execute(
            """
            INSERT INTO raw_payload
              (raw_id, service, category, source_key, endpoint, request_path,
               project_id, workspace_id, payload_hash, payload)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (raw_id) DO NOTHING
            """,
            (
                raw_id,
                service,
                category,
                source_key,
                endpoint,
                request_path,
                project_id,
                workspace_id,
                payload_hash,
                payload_json,
            ),
        )
        return raw_id

    def add_evidence(self, evidence: Evidence) -> Evidence:
        self.conn.execute(
            """
            INSERT INTO evidence
              (evidence_id, kind, source_system, source, source_api, summary,
               raw_id, raw_ref, run_id, confidence, attrs, observed_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
            ON CONFLICT (evidence_id) DO UPDATE SET
              source_system = EXCLUDED.source_system,
              source = EXCLUDED.source,
              source_api = COALESCE(EXCLUDED.source_api, evidence.source_api),
              summary = EXCLUDED.summary,
              raw_id = COALESCE(EXCLUDED.raw_id, evidence.raw_id),
              raw_ref = COALESCE(EXCLUDED.raw_ref, evidence.raw_ref),
              run_id = COALESCE(EXCLUDED.run_id, evidence.run_id),
              confidence = COALESCE(EXCLUDED.confidence, evidence.confidence),
              attrs = evidence.attrs || EXCLUDED.attrs,
              observed_at = now()
            """,
            (
                evidence.evidence_id,
                evidence.kind.value,
                evidence.source_system,
                evidence.source,
                evidence.source_api,
                evidence.summary,
                evidence.raw_id,
                evidence.raw_ref,
                evidence.run_id,
                evidence.confidence,
                _json(evidence.attrs),
                evidence.observed_at,
            ),
        )
        return evidence

    def upsert_edge(self, edge: LineageEdge) -> str:
        edge_id = edge_id_for(edge)
        self.conn.execute(
            """
            INSERT INTO lineage_edge
              (edge_id, src_urn, dst_urn, kind, edge_scope, run_id, source_system,
               confidence, attrs, effective_from, effective_to, first_seen_at, last_seen_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s)
            ON CONFLICT (edge_id) DO UPDATE SET
              confidence = GREATEST(lineage_edge.confidence, EXCLUDED.confidence),
              attrs = lineage_edge.attrs || EXCLUDED.attrs,
              effective_from = COALESCE(EXCLUDED.effective_from, lineage_edge.effective_from),
              effective_to = COALESCE(EXCLUDED.effective_to, lineage_edge.effective_to),
              last_seen_at = now()
            """,
            (
                edge_id,
                edge.src_urn,
                edge.dst_urn,
                edge.kind.value,
                edge.edge_scope,
                edge.run_id,
                edge.source_system,
                edge.confidence,
                _json(edge.attrs),
                edge.effective_from,
                edge.effective_to,
                edge.first_seen_at,
                edge.last_seen_at,
            ),
        )
        for evidence_id in edge.evidence_ids:
            self.conn.execute(
                """
                INSERT INTO edge_evidence(edge_id, evidence_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                """,
                (edge_id, evidence_id),
            )
        return edge_id


def edge_id_for(edge: LineageEdge) -> str:
    return _stable_id(
        "edge",
        edge.src_urn,
        edge.dst_urn,
        edge.kind.value,
        edge.edge_scope,
        edge.run_id or "",
    )


def _stable_id(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
    return f"{prefix}_{digest}"


def _json(value: Any) -> str:
    return json.dumps(_to_json(value), ensure_ascii=False, sort_keys=True)


def _to_json(value: Any) -> Any:
    if value is None:
        return None
    if is_dataclass(value):
        return _to_json(asdict(value))
    if isinstance(value, dict):
        return {key: _to_json(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_to_json(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    return value
