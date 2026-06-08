from __future__ import annotations

import hashlib
import random
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any

import psycopg
from psycopg.rows import dict_row

from datatracex.huawei.dataarts import DataArtsClient
from datatracex.models import EdgeKind, Entity, EntityKind, Evidence, EvidenceKind, LineageEdge
from datatracex.parsers.dataarts_node import props_map
from datatracex.postgres_store import PostgresFactStore
from datatracex.urn import code_artifact_urn, dataarts_node_urn


SCRIPT_NODE_TYPES = {
    "DWSSQL",
    "HiveSQL",
    "SparkSQL",
    "Python",
    "Shell",
    "MRSFlinkJob",
    "MRSSparkPython",
}


@dataclass(frozen=True, slots=True)
class ScriptReference:
    source_key: str
    workspace_id: str
    job_name: str
    node_name: str
    node_urn: str
    node_type: str
    script_name: str
    script_version: str | None
    connection_name: str | None = None
    connection_id: str | None = None
    database: str | None = None

    @property
    def script_source_key(self) -> str:
        return script_source_key(self.workspace_id, self.script_name, self.script_version)


@dataclass(slots=True)
class DataArtsScriptIngestSummary:
    references_seen: int = 0
    distinct_scripts: int = 0
    scripts_skipped: int = 0
    scripts_fetched: int = 0
    scripts_failed: int = 0
    raw_payloads_added: int = 0
    code_entities_upserted: int = 0
    evidence_added: int = 0
    node_code_edges_upserted: int = 0
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "references_seen": self.references_seen,
            "distinct_scripts": self.distinct_scripts,
            "scripts_skipped": self.scripts_skipped,
            "scripts_fetched": self.scripts_fetched,
            "scripts_failed": self.scripts_failed,
            "raw_payloads_added": self.raw_payloads_added,
            "code_entities_upserted": self.code_entities_upserted,
            "evidence_added": self.evidence_added,
            "node_code_edges_upserted": self.node_code_edges_upserted,
            "errors": self.errors,
        }


class DataArtsScriptIngestor:
    def __init__(
        self,
        client: DataArtsClient,
        store: PostgresFactStore,
        endpoint: str,
        project_id: str,
        workspace_id: str,
    ) -> None:
        self.client = client
        self.store = store
        self.endpoint = endpoint.rstrip("/")
        self.project_id = project_id
        self.workspace_id = workspace_id

    def ingest(
        self,
        max_scripts: int | None = None,
        max_workers: int = 4,
        missing_only: bool = False,
        max_retries: int = 5,
    ) -> DataArtsScriptIngestSummary:
        summary = DataArtsScriptIngestSummary()
        references = self.load_references()
        grouped = _group_references(references)
        if max_scripts is not None:
            grouped = dict(list(grouped.items())[:max_scripts])

        summary.references_seen = sum(len(items) for items in grouped.values())
        summary.distinct_scripts = len(grouped)
        if not grouped:
            return summary

        existing_payloads = self._load_existing_script_payloads()
        payloads_by_key: dict[str, tuple[str, dict[str, Any]]] = {}
        to_fetch: list[ScriptReference] = []
        for key, refs in grouped.items():
            if missing_only and key in existing_payloads:
                payloads_by_key[key] = existing_payloads[key]
                summary.scripts_skipped += 1
            else:
                to_fetch.append(refs[0])

        fetched = self._fetch_script_details(
            to_fetch,
            max_workers=max_workers,
            max_retries=max_retries,
        )
        with self.store.session() as writer:
            for ref, payload, error in fetched:
                if error:
                    summary.scripts_failed += 1
                    summary.errors.append(f"{ref.script_name}@{ref.script_version or 'latest'}: {error}")
                    continue
                if not isinstance(payload, dict):
                    summary.scripts_failed += 1
                    summary.errors.append(f"{ref.script_name}@{ref.script_version or 'latest'}: empty payload")
                    continue
                request_path = self.client.script_detail_path(ref.script_name)
                if ref.script_version is not None:
                    request_path = f"{request_path}?version={ref.script_version}"
                raw_id = writer.add_raw_payload(
                    service="dataarts",
                    category="script_detail",
                    source_key=ref.script_source_key,
                    payload=payload,
                    endpoint=self.endpoint,
                    request_path=request_path,
                    project_id=self.project_id,
                    workspace_id=self.workspace_id,
                )
                payloads_by_key[ref.script_source_key] = (raw_id, payload)
                summary.raw_payloads_added += 1
                summary.scripts_fetched += 1

            for key, refs in grouped.items():
                raw = payloads_by_key.get(key)
                if not raw:
                    continue
                raw_id, payload = raw
                content = extract_script_content(payload)
                if content is None:
                    summary.scripts_failed += 1
                    summary.errors.append(f"{key}: script payload does not contain content")
                    continue
                content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
                code_urn = code_artifact_urn(content_hash)
                first_ref = refs[0]
                writer.upsert_entity(
                    Entity(
                        urn=code_urn,
                        kind=EntityKind.CODE_ARTIFACT,
                        name=f"{first_ref.script_name}@{first_ref.script_version or 'latest'}",
                        source_system="dataarts",
                        external_id=str(payload.get("id") or first_ref.script_name),
                        qualified_name=code_urn,
                        attrs={
                            "script_name": first_ref.script_name,
                            "script_version": str(payload.get("version") or first_ref.script_version or ""),
                            "script_type": payload.get("type") or first_ref.node_type,
                            "directory": payload.get("directory"),
                            "database": payload.get("database") or first_ref.database,
                            "content_hash": content_hash,
                            "content_length": len(content),
                            "raw_id": raw_id,
                        },
                    )
                )
                summary.code_entities_upserted += 1

                evidence_id = _evidence_id("dataarts-script", raw_id, code_urn)
                writer.add_evidence(
                    Evidence(
                        evidence_id=evidence_id,
                        kind=EvidenceKind.STATIC_CODE,
                        source="DataArts ShowScript",
                        summary=f"DataArts script content: {first_ref.script_name}@{first_ref.script_version or 'latest'}",
                        source_system="dataarts",
                        source_api="ShowScript",
                        raw_id=raw_id,
                        confidence=1.0,
                        attrs={
                            "code_urn": code_urn,
                            "script_name": first_ref.script_name,
                            "script_version": str(payload.get("version") or first_ref.script_version or ""),
                            "script_type": payload.get("type") or first_ref.node_type,
                            "content_hash": content_hash,
                            "content_length": len(content),
                        },
                    )
                )
                summary.evidence_added += 1

                for ref in refs:
                    writer.upsert_edge(
                        LineageEdge(
                            src_urn=ref.node_urn,
                            dst_urn=code_urn,
                            kind=EdgeKind.USES_CODE,
                            confidence=1.0,
                            edge_scope="design",
                            source_system="dataarts",
                            evidence_ids=[evidence_id],
                            attrs={
                                "relation": "dataarts_node_uses_script_code",
                                "script_name": ref.script_name,
                                "script_version": ref.script_version,
                                "node_type": ref.node_type,
                                "job_name": ref.job_name,
                            },
                        )
                    )
                    summary.node_code_edges_upserted += 1

        return summary

    def load_references(self) -> list[ScriptReference]:
        with psycopg.connect(self.store.dsn, row_factory=dict_row) as conn:
            rows = conn.execute(
                """
                SELECT source_key, payload
                FROM raw_payload
                WHERE service = 'dataarts'
                  AND category = 'job_detail'
                ORDER BY source_key
                """
            ).fetchall()
        references: list[ScriptReference] = []
        for row in rows:
            references.extend(extract_script_references(row["source_key"], row["payload"]))
        return references

    def _fetch_script_details(
        self,
        refs: list[ScriptReference],
        max_workers: int,
        max_retries: int,
    ) -> list[tuple[ScriptReference, dict[str, Any] | None, str | None]]:
        if not refs:
            return []
        results: list[tuple[ScriptReference, dict[str, Any] | None, str | None]] = []
        with ThreadPoolExecutor(max_workers=max(1, max_workers)) as executor:
            futures = {
                executor.submit(self._show_script_with_retry, ref, max_retries): ref
                for ref in refs
            }
            for future in as_completed(futures):
                ref = futures[future]
                try:
                    results.append((ref, future.result(), None))
                except Exception as exc:
                    results.append((ref, None, str(exc)))
        return results

    def _show_script_with_retry(self, ref: ScriptReference, max_retries: int) -> Any:
        for attempt in range(max_retries + 1):
            try:
                return self.client.show_script(ref.script_name, version=ref.script_version)
            except Exception as exc:
                message = str(exc)
                is_rate_limited = "HTTP 429" in message or "APIGW.0308" in message
                if not is_rate_limited or attempt >= max_retries:
                    raise
                delay = min(12.0, (0.8 * (2**attempt)) + random.uniform(0.0, 0.5))
                time.sleep(delay)

    def _load_existing_script_payloads(self) -> dict[str, tuple[str, dict[str, Any]]]:
        with psycopg.connect(self.store.dsn, row_factory=dict_row) as conn:
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


def extract_script_references(source_key: str, payload: dict[str, Any]) -> list[ScriptReference]:
    workspace_id, job_name = _split_source_key(source_key)
    nodes = payload.get("nodes")
    if not isinstance(nodes, list):
        job = payload.get("job")
        nodes = job.get("nodes") if isinstance(job, dict) else []
    references: list[ScriptReference] = []
    for idx, node in enumerate(nodes or []):
        if not isinstance(node, dict):
            continue
        node_type = str(node.get("type") or node.get("nodeType") or "unknown")
        if node_type not in SCRIPT_NODE_TYPES:
            continue
        props = props_map(node)
        script_name = str(props.get("scriptName") or props.get("script_name") or "").strip()
        if not script_name:
            continue
        statement_mode = str(props.get("statementOrScript") or props.get("statement_or_script") or "").strip()
        has_script_mode = statement_mode.upper() == "SCRIPT"
        if statement_mode and not has_script_mode:
            continue
        node_name = str(node.get("name") or node.get("nodeName") or f"node_{idx + 1}")
        version = props.get("scriptVersion") or props.get("script_version")
        references.append(
            ScriptReference(
                source_key=source_key,
                workspace_id=workspace_id,
                job_name=job_name,
                node_name=node_name,
                node_urn=dataarts_node_urn(workspace_id, job_name, node_name),
                node_type=node_type,
                script_name=script_name,
                script_version=str(version).strip() if version is not None and str(version).strip() else None,
                connection_name=_optional_str(props.get("connectionName")),
                connection_id=_optional_str(props.get("connectionId")),
                database=_optional_str(props.get("database")),
            )
        )
    return references


def extract_script_content(payload: dict[str, Any]) -> str | None:
    for key in ("content", "scriptContent", "script_content", "body"):
        value = payload.get(key)
        if isinstance(value, str):
            return value
    for key in ("script", "data"):
        value = payload.get(key)
        if isinstance(value, dict):
            nested = extract_script_content(value)
            if nested is not None:
                return nested
    return None


def script_source_key(workspace_id: str, script_name: str, script_version: str | None) -> str:
    return f"{workspace_id}:{script_name}:{script_version or 'latest'}"


def _group_references(references: list[ScriptReference]) -> dict[str, list[ScriptReference]]:
    grouped: dict[str, list[ScriptReference]] = defaultdict(list)
    for ref in references:
        grouped[ref.script_source_key].append(ref)
    return dict(grouped)


def _split_source_key(source_key: str) -> tuple[str, str]:
    if ":" not in source_key:
        return "", source_key
    return source_key.split(":", 1)


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _evidence_id(*parts: str) -> str:
    return "ev_" + hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
