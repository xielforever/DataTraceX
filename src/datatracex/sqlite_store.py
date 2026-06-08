from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, is_dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .models import EdgeKind, Entity, Evidence, LineageEdge, Run, utc_now


class SQLiteLineageStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS entity (
              urn TEXT PRIMARY KEY,
              kind TEXT NOT NULL,
              name TEXT NOT NULL,
              attrs_json TEXT NOT NULL,
              first_seen_at TEXT NOT NULL,
              last_seen_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS run (
              run_id TEXT PRIMARY KEY,
              kind TEXT NOT NULL,
              status TEXT,
              plan_time TEXT,
              start_time TEXT,
              end_time TEXT,
              attrs_json TEXT NOT NULL,
              observed_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS evidence (
              evidence_id TEXT PRIMARY KEY,
              kind TEXT NOT NULL,
              source TEXT NOT NULL,
              summary TEXT NOT NULL,
              raw_ref TEXT,
              run_id TEXT,
              attrs_json TEXT NOT NULL,
              observed_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS lineage_edge (
              src_urn TEXT NOT NULL,
              dst_urn TEXT NOT NULL,
              kind TEXT NOT NULL,
              run_id TEXT,
              confidence REAL NOT NULL,
              evidence_ids_json TEXT NOT NULL,
              attrs_json TEXT NOT NULL,
              effective_from TEXT,
              effective_to TEXT,
              first_seen_at TEXT NOT NULL,
              last_seen_at TEXT NOT NULL,
              PRIMARY KEY (src_urn, dst_urn, kind, run_id)
            );

            CREATE INDEX IF NOT EXISTS idx_edge_src ON lineage_edge(src_urn);
            CREATE INDEX IF NOT EXISTS idx_edge_dst ON lineage_edge(dst_urn);
            CREATE INDEX IF NOT EXISTS idx_edge_run ON lineage_edge(run_id);
            CREATE INDEX IF NOT EXISTS idx_evidence_run ON evidence(run_id);
            """
        )
        self.conn.commit()

    def upsert_entity(self, entity: Entity) -> Entity:
        existing = self.conn.execute("SELECT attrs_json FROM entity WHERE urn = ?", (entity.urn,)).fetchone()
        now = _dt(utc_now())
        attrs = entity.attrs
        if existing:
            attrs = {**json.loads(existing["attrs_json"]), **entity.attrs}
            self.conn.execute(
                "UPDATE entity SET kind = ?, name = ?, attrs_json = ?, last_seen_at = ? WHERE urn = ?",
                (entity.kind.value, entity.name, _json(attrs), now, entity.urn),
            )
        else:
            self.conn.execute(
                """
                INSERT INTO entity (urn, kind, name, attrs_json, first_seen_at, last_seen_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (entity.urn, entity.kind.value, entity.name, _json(attrs), _dt(entity.first_seen_at), _dt(entity.last_seen_at)),
            )
        self.conn.commit()
        return entity

    def upsert_run(self, run: Run) -> Run:
        existing = self.conn.execute("SELECT attrs_json FROM run WHERE run_id = ?", (run.run_id,)).fetchone()
        now = _dt(utc_now())
        attrs = run.attrs
        if existing:
            attrs = {**json.loads(existing["attrs_json"]), **run.attrs}
            self.conn.execute(
                """
                UPDATE run
                SET kind = ?, status = COALESCE(?, status), plan_time = COALESCE(?, plan_time),
                    start_time = COALESCE(?, start_time), end_time = COALESCE(?, end_time),
                    attrs_json = ?, observed_at = ?
                WHERE run_id = ?
                """,
                (
                    run.kind.value,
                    run.status,
                    _dt(run.plan_time),
                    _dt(run.start_time),
                    _dt(run.end_time),
                    _json(attrs),
                    now,
                    run.run_id,
                ),
            )
        else:
            self.conn.execute(
                """
                INSERT INTO run (run_id, kind, status, plan_time, start_time, end_time, attrs_json, observed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run.run_id,
                    run.kind.value,
                    run.status,
                    _dt(run.plan_time),
                    _dt(run.start_time),
                    _dt(run.end_time),
                    _json(attrs),
                    _dt(run.observed_at),
                ),
            )
        self.conn.commit()
        return run

    def add_evidence(self, evidence: Evidence) -> Evidence:
        self.conn.execute(
            """
            INSERT OR REPLACE INTO evidence
              (evidence_id, kind, source, summary, raw_ref, run_id, attrs_json, observed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                evidence.evidence_id,
                evidence.kind.value,
                evidence.source,
                evidence.summary,
                evidence.raw_ref,
                evidence.run_id,
                _json(evidence.attrs),
                _dt(evidence.observed_at),
            ),
        )
        self.conn.commit()
        return evidence

    def upsert_edge(self, edge: LineageEdge) -> LineageEdge:
        existing = self.conn.execute(
            """
            SELECT confidence, evidence_ids_json, attrs_json
            FROM lineage_edge
            WHERE src_urn = ? AND dst_urn = ? AND kind = ? AND run_id IS ?
            """,
            (edge.src_urn, edge.dst_urn, edge.kind.value, edge.run_id),
        ).fetchone()
        now = _dt(utc_now())
        if existing:
            evidence_ids = sorted(set(json.loads(existing["evidence_ids_json"]) + edge.evidence_ids))
            attrs = {**json.loads(existing["attrs_json"]), **edge.attrs}
            self.conn.execute(
                """
                UPDATE lineage_edge
                SET confidence = ?, evidence_ids_json = ?, attrs_json = ?, last_seen_at = ?
                WHERE src_urn = ? AND dst_urn = ? AND kind = ? AND run_id IS ?
                """,
                (
                    max(float(existing["confidence"]), edge.confidence),
                    _json(evidence_ids),
                    _json(attrs),
                    now,
                    edge.src_urn,
                    edge.dst_urn,
                    edge.kind.value,
                    edge.run_id,
                ),
            )
        else:
            self.conn.execute(
                """
                INSERT INTO lineage_edge
                  (src_urn, dst_urn, kind, run_id, confidence, evidence_ids_json, attrs_json,
                   effective_from, effective_to, first_seen_at, last_seen_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    edge.src_urn,
                    edge.dst_urn,
                    edge.kind.value,
                    edge.run_id,
                    edge.confidence,
                    _json(edge.evidence_ids),
                    _json(edge.attrs),
                    _dt(edge.effective_from),
                    _dt(edge.effective_to),
                    _dt(edge.first_seen_at),
                    _dt(edge.last_seen_at),
                ),
            )
        self.conn.commit()
        return edge

    def lineage_for_node(self, urn: str, direction: str = "both") -> dict[str, Any]:
        clauses = []
        params: list[Any] = []
        if direction in {"both", "out"}:
            clauses.append("src_urn = ?")
            params.append(urn)
        if direction in {"both", "in"}:
            clauses.append("dst_urn = ?")
            params.append(urn)
        where = " OR ".join(clauses) or "src_urn = ? OR dst_urn = ?"
        if not clauses:
            params = [urn, urn]

        edges = [self._edge_row(row) for row in self.conn.execute(f"SELECT * FROM lineage_edge WHERE {where}", params)]
        entity_urns = {urn}
        for edge in edges:
            entity_urns.add(edge["src_urn"])
            entity_urns.add(edge["dst_urn"])
        entities = self._entities(entity_urns)
        return {"root": urn, "entities": entities, "edges": edges}

    def run_detail(self, run_id: str) -> dict[str, Any]:
        run_row = self.conn.execute("SELECT * FROM run WHERE run_id = ?", (run_id,)).fetchone()
        edges = [self._edge_row(row) for row in self.conn.execute("SELECT * FROM lineage_edge WHERE run_id = ?", (run_id,))]
        evidence_rows = self.conn.execute("SELECT * FROM evidence WHERE run_id = ?", (run_id,)).fetchall()
        evidence_ids = {item for edge in edges for item in edge["evidence_ids"]}
        for row in evidence_rows:
            evidence_ids.add(row["evidence_id"])
        evidence = self._evidence(evidence_ids)
        return {"run": self._run_row(run_row) if run_row else None, "edges": edges, "evidence": evidence}

    def search_uri(self, uri: str) -> dict[str, Any]:
        like = f"{uri}%"
        entities = [self._entity_row(row) for row in self.conn.execute("SELECT * FROM entity WHERE urn LIKE ?", (like,))]
        edges = [
            self._edge_row(row)
            for row in self.conn.execute(
                "SELECT * FROM lineage_edge WHERE src_urn LIKE ? OR dst_urn LIKE ?",
                (like, like),
            )
        ]
        return {"query": uri, "entities": entities, "edges": edges}

    def recent_runs(self, limit: int = 50) -> list[dict[str, Any]]:
        return [
            self._run_row(row)
            for row in self.conn.execute("SELECT * FROM run ORDER BY observed_at DESC LIMIT ?", (limit,))
        ]

    def recent_evidence(self, limit: int = 50) -> list[dict[str, Any]]:
        return [
            self._evidence_row(row)
            for row in self.conn.execute("SELECT * FROM evidence ORDER BY observed_at DESC LIMIT ?", (limit,))
        ]

    def entities_page(self, limit: int = 100) -> list[dict[str, Any]]:
        return [
            self._entity_row(row)
            for row in self.conn.execute("SELECT * FROM entity ORDER BY last_seen_at DESC LIMIT ?", (limit,))
        ]

    def stats(self) -> dict[str, int]:
        return {
            "entities": self.conn.execute("SELECT COUNT(*) FROM entity").fetchone()[0],
            "runs": self.conn.execute("SELECT COUNT(*) FROM run").fetchone()[0],
            "evidence": self.conn.execute("SELECT COUNT(*) FROM evidence").fetchone()[0],
            "edges": self.conn.execute("SELECT COUNT(*) FROM lineage_edge").fetchone()[0],
        }

    def _entities(self, urns: set[str]) -> list[dict[str, Any]]:
        if not urns:
            return []
        placeholders = ",".join("?" for _ in urns)
        return [
            self._entity_row(row)
            for row in self.conn.execute(f"SELECT * FROM entity WHERE urn IN ({placeholders})", sorted(urns))
        ]

    def _evidence(self, evidence_ids: set[str]) -> list[dict[str, Any]]:
        if not evidence_ids:
            return []
        placeholders = ",".join("?" for _ in evidence_ids)
        return [
            self._evidence_row(row)
            for row in self.conn.execute(f"SELECT * FROM evidence WHERE evidence_id IN ({placeholders})", sorted(evidence_ids))
        ]

    def _entity_row(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "urn": row["urn"],
            "kind": row["kind"],
            "name": row["name"],
            "attrs": json.loads(row["attrs_json"]),
            "first_seen_at": row["first_seen_at"],
            "last_seen_at": row["last_seen_at"],
        }

    def _run_row(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "run_id": row["run_id"],
            "kind": row["kind"],
            "status": row["status"],
            "plan_time": row["plan_time"],
            "start_time": row["start_time"],
            "end_time": row["end_time"],
            "attrs": json.loads(row["attrs_json"]),
            "observed_at": row["observed_at"],
        }

    def _evidence_row(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "evidence_id": row["evidence_id"],
            "kind": row["kind"],
            "source": row["source"],
            "summary": row["summary"],
            "raw_ref": row["raw_ref"],
            "run_id": row["run_id"],
            "attrs": json.loads(row["attrs_json"]),
            "observed_at": row["observed_at"],
        }

    def _edge_row(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "src_urn": row["src_urn"],
            "dst_urn": row["dst_urn"],
            "kind": row["kind"],
            "confidence": row["confidence"],
            "evidence_ids": json.loads(row["evidence_ids_json"]),
            "run_id": row["run_id"],
            "attrs": json.loads(row["attrs_json"]),
            "effective_from": row["effective_from"],
            "effective_to": row["effective_to"],
            "first_seen_at": row["first_seen_at"],
            "last_seen_at": row["last_seen_at"],
        }


def _dt(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


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
