from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from datatracex.ingest.dataarts_scripts import (
    extract_script_content,
    extract_script_references,
)
from datatracex.parsers.sql_script import parse_sql_script_lineage
from datatracex.postgres_store import PostgresFactStore


SQL_NODE_TYPES = {"HiveSQL", "DWSSQL", "SparkSQL"}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-references", type=int)
    args = parser.parse_args()

    _load_env_file(Path(".env"))
    dsn = os.environ["DATATRACEX_POSTGRES_DSN"]
    store = PostgresFactStore(dsn)
    references = _load_references(dsn)
    if args.max_references is not None:
        references = references[: args.max_references]
    script_payloads = _load_script_payloads(dsn)

    summary = {
        "references_seen": len(references),
        "sql_references_seen": 0,
        "references_with_payload": 0,
        "references_with_content": 0,
        "references_with_edges": 0,
        "entities_upserted": 0,
        "evidence_added": 0,
        "edges_upserted": 0,
        "parsed_statements": 0,
        "failed_statements": 0,
        "node_types": Counter(),
    }

    with store.session() as writer:
        for ref in references:
            summary["node_types"][ref.node_type] += 1
            if ref.node_type not in SQL_NODE_TYPES:
                continue
            summary["sql_references_seen"] += 1
            raw = script_payloads.get(ref.script_source_key)
            if not raw:
                continue
            raw_id, payload = raw
            summary["references_with_payload"] += 1
            content = extract_script_content(payload)
            if content is None:
                continue
            summary["references_with_content"] += 1
            props = {
                "connectionName": ref.connection_name,
                "connectionId": ref.connection_id,
                "database": ref.database,
            }
            facts = parse_sql_script_lineage(ref.node_urn, ref.node_type, props, content, raw_id)
            summary["parsed_statements"] += facts.parsed_statements
            summary["failed_statements"] += facts.failed_statements
            if facts.edges:
                summary["references_with_edges"] += 1
            for entity in _unique_by_urn(facts.entities):
                writer.upsert_entity(entity)
                summary["entities_upserted"] += 1
            for evidence in facts.evidence:
                writer.add_evidence(evidence)
                summary["evidence_added"] += 1
            for edge in facts.edges:
                writer.upsert_edge(edge)
                summary["edges_upserted"] += 1

    serializable = dict(summary)
    serializable["node_types"] = dict(summary["node_types"])
    print(json.dumps(serializable, ensure_ascii=True, indent=2))
    print(json.dumps({"store_stats": store.stats()}, ensure_ascii=True, indent=2))
    return 0


def _load_references(dsn: str):
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        rows = conn.execute(
            """
            SELECT source_key, payload
            FROM raw_payload
            WHERE service = 'dataarts'
              AND category = 'job_detail'
            ORDER BY source_key
            """
        ).fetchall()
    refs = []
    for row in rows:
        refs.extend(extract_script_references(row["source_key"], row["payload"]))
    return refs


def _load_script_payloads(dsn: str) -> dict[str, tuple[str, dict[str, Any]]]:
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT ON (source_key) source_key, raw_id, payload
            FROM raw_payload
            WHERE service = 'dataarts'
              AND category = 'script_detail'
            ORDER BY source_key, captured_at DESC
            """
        ).fetchall()
    return {
        row["source_key"]: (row["raw_id"], row["payload"])
        for row in rows
        if isinstance(row["payload"], dict)
    }


def _unique_by_urn(entities):
    seen = set()
    for entity in entities:
        if entity.urn in seen:
            continue
        seen.add(entity.urn)
        yield entity


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


if __name__ == "__main__":
    raise SystemExit(main())
