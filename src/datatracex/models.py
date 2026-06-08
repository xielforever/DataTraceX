from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class EntityKind(StrEnum):
    WORKSPACE = "workspace"
    CONNECTION = "connection"
    CLUSTER = "cluster"
    BUCKET = "bucket"
    PATH_PREFIX = "path_prefix"
    DATASET = "dataset"
    COLUMN = "column"
    PARTITION = "partition"
    JOB = "job"
    NODE = "node"
    CODE_ARTIFACT = "code_artifact"


class RunKind(StrEnum):
    DATAARTS_INSTANCE = "dataarts_instance"
    CDM_SUBMISSION = "cdm_submission"
    MRS_JOB = "mrs_job"
    DWS_QUERY = "dws_query"
    OBS_SCAN = "obs_scan"


class EdgeKind(StrEnum):
    CONTAINS = "contains"
    DEPENDS_ON = "depends_on"
    USES_CONNECTION = "uses_connection"
    EXECUTES_ON = "executes_on"
    READS = "reads"
    WRITES = "writes"
    DERIVES_FROM = "derives_from"


class EvidenceKind(StrEnum):
    API_PAYLOAD = "api_payload"
    SQL_AST = "sql_ast"
    EXPLAIN_PLAN = "explain_plan"
    RUNTIME_LOG = "runtime_log"
    STATIC_CODE = "static_code"
    URI_MATCH = "uri_match"
    MANUAL_ASSERTION = "manual_assertion"


@dataclass(slots=True)
class Entity:
    urn: str
    kind: EntityKind
    name: str
    source_system: str = "unknown"
    external_id: str | None = None
    qualified_name: str | None = None
    attrs: dict[str, Any] = field(default_factory=dict)
    first_seen_at: datetime = field(default_factory=utc_now)
    last_seen_at: datetime = field(default_factory=utc_now)


@dataclass(slots=True)
class Run:
    run_id: str
    kind: RunKind
    source_system: str = "unknown"
    external_id: str | None = None
    status: str | None = None
    plan_time: datetime | None = None
    start_time: datetime | None = None
    end_time: datetime | None = None
    attrs: dict[str, Any] = field(default_factory=dict)
    observed_at: datetime = field(default_factory=utc_now)


@dataclass(slots=True)
class Evidence:
    evidence_id: str
    kind: EvidenceKind
    source: str
    summary: str
    source_system: str = "unknown"
    source_api: str | None = None
    raw_id: str | None = None
    raw_ref: str | None = None
    run_id: str | None = None
    confidence: float | None = None
    attrs: dict[str, Any] = field(default_factory=dict)
    observed_at: datetime = field(default_factory=utc_now)


@dataclass(slots=True)
class LineageEdge:
    src_urn: str
    dst_urn: str
    kind: EdgeKind
    confidence: float
    edge_scope: str = "design"
    source_system: str = "unknown"
    evidence_ids: list[str] = field(default_factory=list)
    run_id: str | None = None
    attrs: dict[str, Any] = field(default_factory=dict)
    effective_from: datetime | None = None
    effective_to: datetime | None = None
    first_seen_at: datetime = field(default_factory=utc_now)
    last_seen_at: datetime = field(default_factory=utc_now)

    @property
    def key(self) -> tuple[str, str, EdgeKind, str, str | None]:
        return self.src_urn, self.dst_urn, self.kind, self.edge_scope, self.run_id
