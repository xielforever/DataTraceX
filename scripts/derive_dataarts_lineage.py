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

from datatracex.parsers.dataarts_node import parse_node_lineage
from datatracex.postgres_store import PostgresFactStore
from datatracex.urn import dataarts_node_urn


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()

    env_file = Path(".env")
    if env_file.exists():
        _load_env_file(env_file)

    dsn = os.environ["DATATRACEX_POSTGRES_DSN"]
    store = PostgresFactStore(dsn)
    rows = _load_job_payloads(dsn, limit=args.limit)

    summary = {
        "job_payloads": len(rows),
        "nodes_seen": 0,
        "nodes_with_facts": 0,
        "entities_upserted": 0,
        "evidence_added": 0,
        "edges_upserted": 0,
        "node_types": Counter(),
    }

    with store.session() as writer:
        for row in rows:
            raw_id = row["raw_id"]
            source_key = row["source_key"]
            payload = row["payload"]
            workspace_id, job_name = _split_source_key(source_key)
            parent_evidence_id = row["evidence_id"] or raw_id
            for idx, node in enumerate(payload.get("nodes") or []):
                if not isinstance(node, dict):
                    continue
                summary["nodes_seen"] += 1
                node_type = str(node.get("type") or node.get("nodeType") or "unknown")
                summary["node_types"][node_type] += 1
                node_name = str(node.get("name") or node.get("nodeName") or f"node_{idx + 1}")
                node_urn = dataarts_node_urn(workspace_id, job_name, node_name)
                facts = parse_node_lineage(node_urn, node, job_name, parent_evidence_id)
                if facts.entities or facts.edges or facts.evidence:
                    summary["nodes_with_facts"] += 1
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


def _load_job_payloads(dsn: str, limit: int | None) -> list[dict[str, Any]]:
    sql = """
        SELECT rp.raw_id,
               rp.source_key,
               rp.payload,
               ev.evidence_id
        FROM raw_payload rp
        LEFT JOIN evidence ev
          ON ev.raw_id = rp.raw_id
         AND ev.source_api = 'ShowJob'
        WHERE rp.service = 'dataarts'
          AND rp.category = 'job_detail'
        ORDER BY rp.source_key
    """
    if limit:
        sql += " LIMIT %s"
        params: tuple[Any, ...] = (limit,)
    else:
        params = ()
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        return list(conn.execute(sql, params).fetchall())


def _split_source_key(source_key: str) -> tuple[str, str]:
    if ":" not in source_key:
        return "", source_key
    workspace_id, job_name = source_key.split(":", 1)
    return workspace_id, job_name


def _unique_by_urn(entities):
    seen = set()
    for entity in entities:
        if entity.urn in seen:
            continue
        seen.add(entity.urn)
        yield entity


def _load_env_file(path: Path) -> None:
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


if __name__ == "__main__":
    raise SystemExit(main())
