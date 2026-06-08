from __future__ import annotations

import re
import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import psycopg
from neo4j import GraphDatabase
from psycopg.rows import dict_row


REL_TYPE_RE = re.compile(r"[^A-Z0-9_]")


@dataclass(slots=True)
class ProjectionSummary:
    entities_projected: int = 0
    edges_projected: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "entities_projected": self.entities_projected,
            "edges_projected": self.edges_projected,
        }


class Neo4jProjector:
    def __init__(self, postgres_dsn: str, neo4j_uri: str, neo4j_user: str, neo4j_password: str) -> None:
        self.postgres_dsn = postgres_dsn
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

    def close(self) -> None:
        self.driver.close()

    def project_all(self, batch_size: int = 500) -> ProjectionSummary:
        summary = ProjectionSummary()
        with psycopg.connect(self.postgres_dsn, row_factory=dict_row) as pg:
            with self.driver.session() as graph:
                offset = 0
                while True:
                    entities = pg.execute(
                        """
                        SELECT urn, kind, source_system, external_id, qualified_name, name, attrs
                        FROM entity
                        ORDER BY urn
                        LIMIT %s OFFSET %s
                        """,
                        (batch_size, offset),
                    ).fetchall()
                    if not entities:
                        break
                    graph.execute_write(_merge_entities, [_entity_record(row) for row in entities])
                    summary.entities_projected += len(entities)
                    offset += len(entities)

                offset = 0
                while True:
                    edges = pg.execute(
                        """
                        SELECT edge_id, src_urn, dst_urn, kind, edge_scope, run_id,
                               source_system, confidence, attrs, first_seen_at, last_seen_at
                        FROM lineage_edge
                        ORDER BY edge_id
                        LIMIT %s OFFSET %s
                        """,
                        (batch_size, offset),
                    ).fetchall()
                    if not edges:
                        break
                    grouped: dict[str, list[dict[str, Any]]] = {}
                    for row in edges:
                        rel_type = _rel_type(row["kind"])
                        item = _edge_record(row)
                        item["first_seen_at"] = str(item["first_seen_at"]) if item["first_seen_at"] else None
                        item["last_seen_at"] = str(item["last_seen_at"]) if item["last_seen_at"] else None
                        grouped.setdefault(rel_type, []).append(item)
                    for rel_type, rel_edges in grouped.items():
                        graph.execute_write(_merge_edges, rel_type, rel_edges)
                        summary.edges_projected += len(rel_edges)
                    offset += len(edges)

                graph.execute_write(_write_projection_marker)
        return summary


def _merge_entities(tx, entities: list[dict[str, Any]]) -> None:
    tx.run(
        """
        UNWIND $entities AS entity
        MERGE (n:Entity {urn: entity.urn})
        SET n.kind = entity.kind,
            n.source_system = entity.source_system,
            n.external_id = entity.external_id,
            n.qualified_name = entity.qualified_name,
            n.name = entity.name,
            n.attrs_json = entity.attrs_json
        """,
        entities=entities,
    )


def _merge_edges(tx, rel_type: str, edges: list[dict[str, Any]]) -> None:
    query = f"""
        UNWIND $edges AS edge
        MATCH (src:Entity {{urn: edge.src_urn}})
        MATCH (dst:Entity {{urn: edge.dst_urn}})
        MERGE (src)-[r:{rel_type} {{edge_id: edge.edge_id}}]->(dst)
        SET r.kind = edge.kind,
            r.edge_scope = edge.edge_scope,
            r.run_id = edge.run_id,
            r.source_system = edge.source_system,
            r.confidence = edge.confidence,
            r.attrs_json = edge.attrs_json,
            r.first_seen_at = edge.first_seen_at,
            r.last_seen_at = edge.last_seen_at
    """
    tx.run(query, edges=edges)


def _write_projection_marker(tx) -> None:
    tx.run(
        """
        MERGE (n:Projection {name: 'default'})
        SET n.last_projected_at = datetime()
        """
    )


def _rel_type(kind: str) -> str:
    rel = REL_TYPE_RE.sub("_", kind.upper())
    return rel or "RELATED_TO"


def _entity_record(row: dict[str, Any]) -> dict[str, Any]:
    item = dict(row)
    item["attrs_json"] = json.dumps(item.pop("attrs") or {}, ensure_ascii=False, sort_keys=True)
    return item


def _edge_record(row: dict[str, Any]) -> dict[str, Any]:
    item = dict(row)
    item["confidence"] = float(item["confidence"]) if isinstance(item["confidence"], Decimal) else item["confidence"]
    item["attrs_json"] = json.dumps(item.pop("attrs") or {}, ensure_ascii=False, sort_keys=True)
    return item
