from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from typing import Any

import sqlglot
from sqlglot import exp
from sqlglot.errors import ErrorLevel

from datatracex.models import EdgeKind, Entity, EntityKind, Evidence, EvidenceKind, LineageEdge
from datatracex.urn import dataset_urn


SQLGLOT_LOGGER = logging.getLogger("sqlglot")


@dataclass(slots=True)
class SqlScriptLineageFacts:
    entities: list[Entity] = field(default_factory=list)
    evidence: list[Evidence] = field(default_factory=list)
    edges: list[LineageEdge] = field(default_factory=list)
    parsed_statements: int = 0
    failed_statements: int = 0


def parse_sql_script_lineage(
    node_urn: str,
    node_type: str,
    props: dict[str, Any],
    sql_text: str,
    raw_id: str,
) -> SqlScriptLineageFacts:
    facts = SqlScriptLineageFacts()
    evidence = Evidence(
        evidence_id=_evidence_id("script-sql", raw_id, node_urn, _hash(sql_text)),
        kind=EvidenceKind.SQL_AST,
        source="DataArts script SQL parser",
        summary=f"{node_type} script SQL evidence for {node_urn}",
        source_system="dataarts",
        source_api="DataArtsScriptSqlParser",
        raw_id=raw_id,
        confidence=0.82,
        attrs={
            "node_urn": node_urn,
            "node_type": node_type,
            "content_hash": _hash(sql_text),
        },
    )
    facts.evidence.append(evidence)

    expressions, failed = _parse_expressions(sql_text, _dialect(node_type))
    facts.parsed_statements = len(expressions)
    facts.failed_statements = failed
    if not expressions:
        return facts

    read_urns: set[str] = set()
    write_urns: set[str] = set()
    for expression in expressions:
        statement_writes = {
            urn
            for table in _write_tables(expression)
            if (urn := _table_urn(table, node_type, props))
        }
        write_urns.update(statement_writes)
        for table in expression.find_all(exp.Table):
            urn = _table_urn(table, node_type, props)
            if urn and urn not in statement_writes:
                read_urns.add(urn)

    read_urns -= write_urns
    for urn in sorted(read_urns | write_urns):
        facts.entities.append(_dataset_entity_from_urn(urn, node_type, props))
    for urn in sorted(read_urns):
        facts.edges.append(
            _edge(
                node_urn,
                urn,
                EdgeKind.READS,
                evidence.evidence_id,
                0.82 if facts.failed_statements == 0 else 0.68,
                {
                    "parser": "sqlglot",
                    "node_type": node_type,
                    "parsed_statements": facts.parsed_statements,
                    "failed_statements": facts.failed_statements,
                },
            )
        )
    for urn in sorted(write_urns):
        facts.edges.append(
            _edge(
                node_urn,
                urn,
                EdgeKind.WRITES,
                evidence.evidence_id,
                0.88 if facts.failed_statements == 0 else 0.72,
                {
                    "parser": "sqlglot",
                    "node_type": node_type,
                    "parsed_statements": facts.parsed_statements,
                    "failed_statements": facts.failed_statements,
                },
            )
        )
    return facts


def _parse_expressions(sql_text: str, dialect: str) -> tuple[list[exp.Expression], int]:
    previous_level = SQLGLOT_LOGGER.level
    SQLGLOT_LOGGER.setLevel(logging.ERROR)
    try:
        try:
            expressions = sqlglot.parse(sql_text, read=dialect, error_level=ErrorLevel.IGNORE)
            parsed = [expression for expression in expressions if expression is not None]
            if parsed:
                return parsed, max(0, len(_split_statements(sql_text)) - len(parsed))
        except Exception:
            pass

        parsed: list[exp.Expression] = []
        failed = 0
        for statement in _split_statements(sql_text):
            try:
                parsed.append(sqlglot.parse_one(statement, read=dialect, error_level=ErrorLevel.IGNORE))
            except Exception:
                failed += 1
        return parsed, failed
    finally:
        SQLGLOT_LOGGER.setLevel(previous_level)


def _split_statements(sql_text: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    quote: str | None = None
    escape = False
    for char in sql_text:
        current.append(char)
        if escape:
            escape = False
            continue
        if char == "\\" and quote:
            escape = True
            continue
        if char in {"'", '"', "`"}:
            if quote == char:
                quote = None
            elif quote is None:
                quote = char
            continue
        if char == ";" and quote is None:
            statement = "".join(current).strip().rstrip(";").strip()
            if statement:
                statements.append(statement)
            current = []
    tail = "".join(current).strip()
    if tail:
        statements.append(tail)
    return statements


def _write_tables(expression: exp.Expression) -> list[exp.Table]:
    tables: list[exp.Table] = []
    for insert in expression.find_all(exp.Insert):
        tables.extend(_table_exprs(insert.this))
    for create in expression.find_all(exp.Create):
        tables.extend(_table_exprs(create.this))
    for alter in expression.find_all(exp.Alter):
        tables.extend(_table_exprs(alter.this))
    for update in expression.find_all(exp.Update):
        tables.extend(_table_exprs(update.this))
    for delete in expression.find_all(exp.Delete):
        tables.extend(_table_exprs(delete.this))
    if type(expression).__name__ == "TruncateTable":
        for item in expression.args.get("expressions") or []:
            tables.extend(_table_exprs(item))
    return tables


def _table_exprs(value: Any) -> list[exp.Table]:
    if isinstance(value, exp.Table):
        return [value]
    if isinstance(value, exp.Schema):
        return _table_exprs(value.this)
    return []


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


def _dataset_entity_from_urn(urn: str, node_type: str, props: dict[str, Any]) -> Entity:
    return Entity(
        urn=urn,
        kind=EntityKind.DATASET,
        name=urn.rsplit("/", 1)[-1],
        source_system=_service_from_node_type(node_type),
        qualified_name=urn,
        attrs={
            "node_type": node_type,
            "connection_name": props.get("connectionName"),
            "connection_id": props.get("connectionId"),
            "database": props.get("database"),
        },
    )


def _edge(
    src: str,
    dst: str,
    kind: EdgeKind,
    evidence_id: str,
    confidence: float,
    attrs: dict[str, Any],
) -> LineageEdge:
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


def _evidence_id(*parts: str) -> str:
    return "ev_" + _hash("|".join(parts))


def _hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
