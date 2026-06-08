from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from typing import Any

import sqlglot
from sqlglot import exp

from datatracex.models import EdgeKind, Entity, EntityKind, Evidence, EvidenceKind, LineageEdge
from datatracex.urn import cdm_job_urn, connection_urn, dataset_urn, normalize_storage_uri


URI_RE = re.compile(r"\b(?:obs|s3a|s3n|s3|hdfs)://[^\s'\"),;]+", re.IGNORECASE)


@dataclass(slots=True)
class NodeLineageFacts:
    entities: list[Entity] = field(default_factory=list)
    edges: list[LineageEdge] = field(default_factory=list)
    evidence: list[Evidence] = field(default_factory=list)


def props_map(node: dict[str, Any]) -> dict[str, Any]:
    props = node.get("properties")
    if isinstance(props, dict):
        return {str(key): value for key, value in props.items()}
    if isinstance(props, list):
        result: dict[str, Any] = {}
        for item in props:
            if not isinstance(item, dict):
                continue
            key = item.get("name") or item.get("key")
            if key is not None:
                result[str(key)] = item.get("value")
        return result
    return {}


def parse_node_lineage(
    node_urn: str,
    node: dict[str, Any],
    job_name: str,
    evidence_parent_id: str,
) -> NodeLineageFacts:
    props = props_map(node)
    node_type = str(node.get("type") or node.get("nodeType") or "unknown")
    facts = NodeLineageFacts()

    connection_entities = _connection_entities(props, node_type)
    facts.entities.extend(connection_entities)
    for connection in connection_entities:
        facts.edges.append(
            LineageEdge(
                src_urn=node_urn,
                dst_urn=connection.urn,
                kind=EdgeKind.USES_CONNECTION,
                confidence=0.93,
                edge_scope="design",
                source_system="dataarts",
                evidence_ids=[evidence_parent_id],
                attrs={"node_type": node_type},
            )
        )

    if node_type in {"DWSSQL", "HiveSQL", "SparkSQL"}:
        sql_text = _sql_text(props)
        if sql_text:
            facts.evidence.append(_evidence("sql", evidence_parent_id, node_urn, sql_text, node_type))
            facts.edges.extend(_sql_edges(node_urn, node_type, props, sql_text, facts.entities, facts.evidence[-1].evidence_id))

    if node_type == "OBSManager":
        facts.evidence.append(_evidence("uri", evidence_parent_id, node_urn, str(props), node_type))
        facts.edges.extend(_obs_manager_edges(node_urn, props, facts.entities, facts.evidence[-1].evidence_id))

    if node_type == "CDMJob":
        facts.evidence.append(_evidence("cdm", evidence_parent_id, node_urn, str(props), node_type))
        facts.edges.extend(_cdm_edges(node_urn, props, facts.entities, facts.evidence[-1].evidence_id))

    uri_text = "\n".join(str(value) for value in props.values() if value is not None)
    uri_edges = _uri_edges(node_urn, uri_text, facts.entities, evidence_parent_id)
    facts.edges.extend(uri_edges)
    return facts


def _connection_entities(props: dict[str, Any], node_type: str) -> list[Entity]:
    connection_id = str(props.get("connectionId") or "").strip()
    connection_name = str(props.get("connectionName") or "").strip()
    if not connection_id and not connection_name:
        return []
    urn = connection_urn("dataarts", connection_id or connection_name)
    return [
        Entity(
            urn=urn,
            kind=EntityKind.CONNECTION,
            name=connection_name or connection_id,
            source_system="dataarts",
            external_id=connection_id or None,
            qualified_name=connection_name or connection_id,
            attrs={"node_type": node_type, "connection_name": connection_name},
        )
    ]


def _sql_text(props: dict[str, Any]) -> str | None:
    for key in ("sql", "statement", "scriptContent", "content"):
        value = props.get(key)
        if value and str(value).strip():
            return str(value)
    return None


def _sql_edges(
    node_urn: str,
    node_type: str,
    props: dict[str, Any],
    sql_text: str,
    entities: list[Entity],
    evidence_id: str,
) -> list[LineageEdge]:
    dialect = _dialect(node_type)
    try:
        expressions = sqlglot.parse(sql_text, read=dialect)
    except Exception:
        return []

    read_urns: set[str] = set()
    write_urns: set[str] = set()
    for expression in expressions:
        if expression is None:
            continue
        write_urns.update(_write_tables(expression, node_type, props, entities))
        for table in expression.find_all(exp.Table):
            urn = _table_urn(table, node_type, props)
            if urn:
                read_urns.add(urn)

    read_urns -= write_urns
    edges: list[LineageEdge] = []
    for urn in sorted(read_urns):
        entities.append(_dataset_entity_from_urn(urn, node_type, props))
        edges.append(
            LineageEdge(
                src_urn=node_urn,
                dst_urn=urn,
                kind=EdgeKind.READS,
                confidence=0.86,
                edge_scope="design",
                source_system="dataarts",
                evidence_ids=[evidence_id],
                attrs={"parser": "sqlglot", "node_type": node_type},
            )
        )
    for urn in sorted(write_urns):
        edges.append(
            LineageEdge(
                src_urn=node_urn,
                dst_urn=urn,
                kind=EdgeKind.WRITES,
                confidence=0.9,
                edge_scope="design",
                source_system="dataarts",
                evidence_ids=[evidence_id],
                attrs={"parser": "sqlglot", "node_type": node_type},
            )
        )
    return edges


def _write_tables(expression: exp.Expression, node_type: str, props: dict[str, Any], entities: list[Entity]) -> set[str]:
    urns: set[str] = set()
    for insert in expression.find_all(exp.Insert):
        table = insert.this
        if isinstance(table, exp.Table):
            urn = _table_urn(table, node_type, props)
            if urn:
                urns.add(urn)
                entities.append(_dataset_entity(urn, table, node_type, props))
    for create in expression.find_all(exp.Create):
        table = create.this
        if isinstance(table, exp.Table):
            urn = _table_urn(table, node_type, props)
            if urn:
                urns.add(urn)
                entities.append(_dataset_entity(urn, table, node_type, props))
    for urn in list(urns):
        if not any(entity.urn == urn for entity in entities):
            entities.append(_dataset_entity_from_urn(urn, node_type, props))
    return urns


def _table_urn(table: exp.Table, node_type: str, props: dict[str, Any]) -> str | None:
    name = table.name
    if not name:
        return None
    db = table.db
    catalog = table.catalog
    default_db = str(props.get("database") or "default").strip() or "default"
    connection = str(props.get("connectionName") or props.get("connectionId") or "unknown").strip() or "unknown"

    if node_type == "DWSSQL":
        if catalog and db:
            return dataset_urn("dws", connection, catalog, name, schema=db)
        if db:
            return dataset_urn("dws", connection, default_db, name, schema=db)
        return dataset_urn("dws", connection, default_db, name)

    service = "spark" if node_type == "SparkSQL" else "hive"
    if catalog and db:
        return dataset_urn(service, connection, catalog, name, schema=db)
    if db:
        return dataset_urn(service, connection, db, name)
    return dataset_urn(service, connection, default_db, name)


def _dataset_entity(urn: str, table: exp.Table, node_type: str, props: dict[str, Any]) -> Entity:
    return _dataset_entity_from_urn(urn, node_type, props, table_name=table.name)


def _dataset_entity_from_urn(urn: str, node_type: str, props: dict[str, Any], table_name: str | None = None) -> Entity:
    return Entity(
        urn=urn,
        kind=EntityKind.DATASET,
        name=table_name or urn.rsplit("/", 1)[-1],
        source_system=_service_from_node_type(node_type),
        qualified_name=urn,
        attrs={
            "node_type": node_type,
            "connection_name": props.get("connectionName"),
            "connection_id": props.get("connectionId"),
            "database": props.get("database"),
        },
    )


def _obs_manager_edges(
    node_urn: str,
    props: dict[str, Any],
    entities: list[Entity],
    evidence_id: str,
) -> list[LineageEdge]:
    edges: list[LineageEdge] = []
    action = str(props.get("action") or "").upper()
    source = str(props.get("sourceDirectory") or "").strip()
    target = str(props.get("targetDirectory") or "").strip()
    if source:
        source_urn = normalize_storage_uri(source)
        entities.append(_path_entity(source_urn))
        edges.append(_edge(node_urn, source_urn, EdgeKind.READS, evidence_id, 0.82, {"action": action}))
    if target:
        target_urn = normalize_storage_uri(target)
        entities.append(_path_entity(target_urn))
        edges.append(_edge(node_urn, target_urn, EdgeKind.WRITES, evidence_id, 0.82, {"action": action}))
    return edges


def _cdm_edges(
    node_urn: str,
    props: dict[str, Any],
    entities: list[Entity],
    evidence_id: str,
) -> list[LineageEdge]:
    cluster_id = str(props.get("clusterId") or props.get("clusterName") or "").strip()
    job_name = str(props.get("jobName") or "").strip()
    if not cluster_id or not job_name:
        return []
    job_urn = cdm_job_urn(cluster_id, job_name)
    entities.append(
        Entity(
            urn=job_urn,
            kind=EntityKind.JOB,
            name=job_name,
            source_system="cdm",
            external_id=job_name,
            qualified_name=job_urn,
            attrs={"cluster_id": cluster_id, "cluster_name": props.get("clusterName")},
        )
    )
    return [
        LineageEdge(
            src_urn=node_urn,
            dst_urn=job_urn,
            kind=EdgeKind.EXECUTES_ON,
            confidence=0.92,
            edge_scope="design",
            source_system="dataarts",
            evidence_ids=[evidence_id],
            attrs={"relation": "dataarts_node_references_cdm_job"},
        )
    ]


def _uri_edges(node_urn: str, text: str, entities: list[Entity], evidence_id: str) -> list[LineageEdge]:
    edges: list[LineageEdge] = []
    for raw in sorted(set(URI_RE.findall(text))):
        try:
            uri = normalize_storage_uri(raw)
        except ValueError:
            continue
        entities.append(_path_entity(uri))
        edges.append(_edge(node_urn, uri, EdgeKind.READS, evidence_id, 0.55, {"parser": "uri_regex"}))
    return edges


def _path_entity(urn: str) -> Entity:
    return Entity(
        urn=urn,
        kind=EntityKind.PATH_PREFIX,
        name=urn,
        source_system="obs" if urn.startswith("obs://") else "hdfs",
        qualified_name=urn,
        attrs={},
    )


def _edge(src: str, dst: str, kind: EdgeKind, evidence_id: str, confidence: float, attrs: dict[str, Any]) -> LineageEdge:
    return LineageEdge(
        src_urn=src,
        dst_urn=dst,
        kind=kind,
        confidence=confidence,
        edge_scope="design",
        source_system="dataarts",
        evidence_ids=[evidence_id],
        attrs=attrs,
    )


def _evidence(kind: str, parent_id: str, node_urn: str, content: str, node_type: str) -> Evidence:
    evidence_kind = EvidenceKind.SQL_AST if kind == "sql" else EvidenceKind.URI_MATCH
    return Evidence(
        evidence_id="ev_" + _hash(parent_id, node_urn, kind, content),
        kind=evidence_kind,
        source="DataArts node parser",
        summary=f"{node_type} {kind} evidence for {node_urn}",
        source_system="dataarts",
        source_api="DataArtsNodeParser",
        raw_id=None,
        raw_ref=parent_id,
        confidence=0.85 if kind == "sql" else 0.65,
        attrs={"node_urn": node_urn, "node_type": node_type, "content_hash": _hash(content)},
    )


def _dialect(node_type: str) -> str:
    if node_type == "DWSSQL":
        return "postgres"
    if node_type == "SparkSQL":
        return "spark"
    return "hive"


def _service_from_node_type(node_type: str) -> str:
    if node_type == "DWSSQL":
        return "dws"
    if node_type == "SparkSQL":
        return "spark"
    return "hive"


def _hash(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
