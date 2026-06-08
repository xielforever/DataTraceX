from __future__ import annotations

import hashlib
import random
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any

from datatracex.huawei.dataarts import DataArtsClient
from datatracex.models import EdgeKind, Entity, EntityKind, Evidence, EvidenceKind, LineageEdge
from datatracex.postgres_store import PostgresFactStore
from datatracex.urn import dataarts_job_urn, dataarts_node_urn, workspace_urn


@dataclass(slots=True)
class DataArtsJobIngestSummary:
    jobs_seen: int = 0
    jobs_skipped: int = 0
    jobs_ingested: int = 0
    job_details: int = 0
    entities_upserted: int = 0
    edges_upserted: int = 0
    evidence_added: int = 0
    raw_payloads_added: int = 0
    node_types: Counter[str] = field(default_factory=Counter)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "jobs_seen": self.jobs_seen,
            "jobs_skipped": self.jobs_skipped,
            "jobs_ingested": self.jobs_ingested,
            "job_details": self.job_details,
            "entities_upserted": self.entities_upserted,
            "edges_upserted": self.edges_upserted,
            "evidence_added": self.evidence_added,
            "raw_payloads_added": self.raw_payloads_added,
            "node_types": dict(self.node_types),
            "errors": self.errors,
        }


class DataArtsJobIngestor:
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
        max_jobs: int | None = None,
        page_size: int = 100,
        max_workers: int = 6,
        missing_only: bool = False,
        max_retries: int = 5,
    ) -> DataArtsJobIngestSummary:
        summary = DataArtsJobIngestSummary()

        workspace = Entity(
            urn=workspace_urn(self.workspace_id),
            kind=EntityKind.WORKSPACE,
            name=self.workspace_id,
            source_system="dataarts",
            external_id=self.workspace_id,
            qualified_name=self.workspace_id,
            attrs={"project_id": self.project_id},
        )
        with self.store.session() as writer:
            writer.upsert_entity(workspace)
            summary.entities_upserted += 1

        jobs = self._list_all_jobs(page_size=page_size, max_jobs=max_jobs)
        summary.jobs_seen = len(jobs)
        if missing_only:
            existing = self.store.raw_source_keys("dataarts", "job_detail")
            before = len(jobs)
            jobs = [
                job
                for job in jobs
                if _job_name(job) and f"{self.workspace_id}:{_job_name(job)}" not in existing
            ]
            summary.jobs_skipped = before - len(jobs)

        with self.store.session() as writer:
            list_raw_id = writer.add_raw_payload(
                service="dataarts",
                category="job_list",
                source_key=f"{self.workspace_id}:jobs",
                payload={"jobs": jobs, "count": len(jobs)},
                endpoint=self.endpoint,
                request_path=f"/v1/{self.project_id}/jobs",
                project_id=self.project_id,
                workspace_id=self.workspace_id,
            )
            summary.raw_payloads_added += 1
            writer.add_evidence(
                Evidence(
                    evidence_id=_evidence_id("dataarts-job-list", list_raw_id),
                    kind=EvidenceKind.API_PAYLOAD,
                    source="DataArts ListJobs",
                    summary=f"DataArts job list for workspace {self.workspace_id}: {len(jobs)} jobs",
                    source_system="dataarts",
                    source_api="ListJobs",
                    raw_id=list_raw_id,
                    confidence=1.0,
                )
            )
            summary.evidence_added += 1

        fetched = self._fetch_job_details(jobs, max_workers=max_workers, max_retries=max_retries)
        with self.store.session() as writer:
            for job, job_name, detail, error in fetched:
                if error:
                    summary.errors.append(f"{job_name or '<unknown>'}: {error}")
                    continue
                if not detail:
                    summary.errors.append(f"{job_name or '<unknown>'}: empty detail")
                    continue
                summary.job_details += 1
                summary.jobs_ingested += 1

                raw_id = writer.add_raw_payload(
                    service="dataarts",
                    category="job_detail",
                    source_key=f"{self.workspace_id}:{job_name}",
                    payload=detail,
                    endpoint=self.endpoint,
                    request_path=f"/v1/{self.project_id}/jobs/{job_name}",
                    project_id=self.project_id,
                    workspace_id=self.workspace_id,
                )
                summary.raw_payloads_added += 1

                evidence = Evidence(
                    evidence_id=_evidence_id("dataarts-job-detail", self.workspace_id, job_name, raw_id),
                    kind=EvidenceKind.API_PAYLOAD,
                    source="DataArts ShowJob",
                    summary=f"DataArts job detail: {job_name}",
                    source_system="dataarts",
                    source_api="ShowJob",
                    raw_id=raw_id,
                    confidence=1.0,
                    attrs={
                        "job_name": job_name,
                        "job_id": str(detail.get("id") or job.get("id") or ""),
                    },
                )
                writer.add_evidence(evidence)
                summary.evidence_added += 1

                entities, edges = self._materialize_job(job, detail, evidence.evidence_id)
                for entity in entities:
                    writer.upsert_entity(entity)
                    summary.entities_upserted += 1
                for edge in edges:
                    writer.upsert_edge(edge)
                    summary.edges_upserted += 1
                for node in _extract_nodes(detail):
                    summary.node_types[_node_type(node)] += 1

        return summary

    def _list_all_jobs(self, page_size: int, max_jobs: int | None) -> list[dict[str, Any]]:
        jobs: list[dict[str, Any]] = []
        offset = 0
        while True:
            payload = self.client.list_jobs(limit=page_size, offset=offset)
            page = _extract_items(payload)
            if not page:
                break
            jobs.extend(page)
            if max_jobs is not None and len(jobs) >= max_jobs:
                return jobs[:max_jobs]
            total = payload.get("total") if isinstance(payload, dict) else None
            offset += 1
            if total is not None and len(jobs) >= int(total):
                break
            if len(page) < page_size:
                break
        return jobs

    def _fetch_job_details(
        self,
        jobs: list[dict[str, Any]],
        max_workers: int,
        max_retries: int,
    ) -> list[tuple[dict[str, Any], str | None, dict[str, Any] | None, str | None]]:
        results: list[tuple[dict[str, Any], str | None, dict[str, Any] | None, str | None]] = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for job in jobs:
                job_name = _job_name(job)
                if not job_name:
                    results.append((job, None, None, f"job without name: {job}"))
                    continue
                futures[executor.submit(self._show_job_with_retry, job_name, max_retries)] = (job, job_name)

            for future in as_completed(futures):
                job, job_name = futures[future]
                try:
                    detail = future.result()
                    results.append((job, job_name, detail, None))
                except Exception as exc:
                    results.append((job, job_name, None, str(exc)))
        return results

    def _show_job_with_retry(self, job_name: str, max_retries: int) -> Any:
        for attempt in range(max_retries + 1):
            try:
                return self.client.show_job(job_name)
            except Exception as exc:
                message = str(exc)
                is_rate_limited = "HTTP 429" in message or "APIGW.0308" in message
                if not is_rate_limited or attempt >= max_retries:
                    raise
                delay = min(12.0, (0.8 * (2**attempt)) + random.uniform(0.0, 0.5))
                time.sleep(delay)

    def _materialize_job(
        self,
        job: dict[str, Any],
        detail: dict[str, Any],
        evidence_id: str,
    ) -> tuple[list[Entity], list[LineageEdge]]:
        job_name = _job_name(detail) or _job_name(job) or "unknown"
        job_id = str(detail.get("id") or job.get("id") or job_name)
        job_urn = dataarts_job_urn(self.workspace_id, job_name)
        workspace = workspace_urn(self.workspace_id)

        entities = [
            Entity(
                urn=job_urn,
                kind=EntityKind.JOB,
                name=job_name,
                source_system="dataarts",
                external_id=job_id,
                qualified_name=f"{self.workspace_id}.{job_name}",
                attrs={
                    "project_id": self.project_id,
                    "workspace_id": self.workspace_id,
                    "directory": detail.get("directory"),
                    "process_type": detail.get("processType"),
                    "version": detail.get("version"),
                    "create_time": detail.get("createTime"),
                    "last_update_user": detail.get("lastUpdateUser"),
                    "single_node_job_flag": detail.get("singleNodeJobFlag"),
                },
            )
        ]
        edges = [
            LineageEdge(
                src_urn=workspace,
                dst_urn=job_urn,
                kind=EdgeKind.CONTAINS,
                confidence=1.0,
                edge_scope="design",
                source_system="dataarts",
                evidence_ids=[evidence_id],
                attrs={"relation": "workspace_contains_job"},
            )
        ]

        node_urn_by_name: dict[str, str] = {}
        for idx, node in enumerate(_extract_nodes(detail)):
            node_name = _node_name(node, idx)
            node_type = _node_type(node)
            node_urn = dataarts_node_urn(self.workspace_id, job_name, node_name)
            node_urn_by_name[node_name] = node_urn
            entities.append(
                Entity(
                    urn=node_urn,
                    kind=EntityKind.NODE,
                    name=node_name,
                    source_system="dataarts",
                    external_id=str(node.get("id") or node.get("nodeId") or node_name),
                    qualified_name=f"{self.workspace_id}.{job_name}.{node_name}",
                    attrs={
                        "project_id": self.project_id,
                        "workspace_id": self.workspace_id,
                        "job_name": job_name,
                        "node_type": node_type,
                        "pre_node_name": node.get("preNodeName"),
                        "conditions": node.get("conditions"),
                        "property_hash": _stable_hash(node.get("properties")),
                    },
                )
            )
            edges.append(
                LineageEdge(
                    src_urn=job_urn,
                    dst_urn=node_urn,
                    kind=EdgeKind.CONTAINS,
                    confidence=1.0,
                    edge_scope="design",
                    source_system="dataarts",
                    evidence_ids=[evidence_id],
                    attrs={"relation": "job_contains_node", "node_type": node_type},
                )
            )

        for idx, node in enumerate(_extract_nodes(detail)):
            node_name = _node_name(node, idx)
            node_urn = node_urn_by_name.get(node_name)
            if not node_urn:
                continue
            for pre_name in _pre_node_names(node):
                pre_urn = node_urn_by_name.get(pre_name)
                if pre_urn:
                    edges.append(
                        LineageEdge(
                            src_urn=node_urn,
                            dst_urn=pre_urn,
                            kind=EdgeKind.DEPENDS_ON,
                            confidence=0.95,
                            edge_scope="design",
                            source_system="dataarts",
                            evidence_ids=[evidence_id],
                            attrs={"relation": "node_depends_on_pre_node"},
                        )
                    )

        return entities, edges


def _extract_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("jobs", "instances", "jobInstances", "records", "items", "data"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def _extract_nodes(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    nodes = payload.get("nodes")
    if isinstance(nodes, list):
        return [item for item in nodes if isinstance(item, dict)]
    job = payload.get("job")
    if isinstance(job, dict) and isinstance(job.get("nodes"), list):
        return [item for item in job["nodes"] if isinstance(item, dict)]
    return []


def _job_name(job: dict[str, Any]) -> str | None:
    value = job.get("name") or job.get("jobName") or job.get("job_name")
    return str(value) if value else None


def _node_name(node: dict[str, Any], idx: int) -> str:
    value = node.get("name") or node.get("nodeName") or node.get("node_name")
    return str(value) if value else f"node_{idx + 1}"


def _node_type(node: dict[str, Any]) -> str:
    return str(node.get("type") or node.get("nodeType") or node.get("node_type") or "unknown")


def _pre_node_names(node: dict[str, Any]) -> list[str]:
    raw = node.get("preNodeName") or node.get("pre_node_name") or node.get("preNodes")
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(item) for item in raw if str(item)]
    return [item.strip() for item in str(raw).split(",") if item.strip()]


def _evidence_id(*parts: str) -> str:
    return "ev_" + _stable_hash("|".join(parts))


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()
