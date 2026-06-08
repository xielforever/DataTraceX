from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class HuaweiSettings:
    ak: str
    sk: str
    region: str
    project_id: str
    workspace_id: str | None
    dataarts_factory_endpoint: str
    dataarts_endpoint: str
    cdm_endpoint: str
    cdm_cluster_ids: tuple[str, ...]
    raw_dir: Path
    job_instance_days: int


@dataclass(frozen=True, slots=True)
class PostgresSettings:
    dsn: str


@dataclass(frozen=True, slots=True)
class Neo4jSettings:
    uri: str
    user: str
    password: str


@dataclass(frozen=True, slots=True)
class AppSettings:
    postgres: PostgresSettings
    neo4j: Neo4jSettings
    raw_dir: Path


def load_huawei_settings() -> HuaweiSettings:
    region = _required_env("DATATRACEX_REGION")
    factory_endpoint = os.getenv("DATATRACEX_DATAARTS_FACTORY_ENDPOINT") or (
        f"https://dayu-dlf.{region}.myhuaweicloud.com"
    )
    dataarts_endpoint = os.getenv("DATATRACEX_DATAARTS_ENDPOINT") or (
        f"https://dayu.{region}.myhuaweicloud.com"
    )
    cdm_endpoint = os.getenv("DATATRACEX_CDM_ENDPOINT") or (
        f"https://cdm.{region}.myhuaweicloud.com"
    )
    return HuaweiSettings(
        ak=_required_env("DATATRACEX_HUAWEI_AK"),
        sk=_required_env("DATATRACEX_HUAWEI_SK"),
        region=region,
        project_id=_required_env("DATATRACEX_PROJECT_ID"),
        workspace_id=os.getenv("DATATRACEX_WORKSPACE_ID") or None,
        dataarts_factory_endpoint=factory_endpoint.rstrip("/"),
        dataarts_endpoint=dataarts_endpoint.rstrip("/"),
        cdm_endpoint=cdm_endpoint.rstrip("/"),
        cdm_cluster_ids=_csv_env("DATATRACEX_CDM_CLUSTER_IDS"),
        raw_dir=Path(os.getenv("DATATRACEX_RAW_DIR", "data/raw")),
        job_instance_days=int(os.getenv("DATATRACEX_JOB_INSTANCE_DAYS", "1")),
    )


def load_app_settings() -> AppSettings:
    return AppSettings(
        postgres=PostgresSettings(
            dsn=os.getenv(
                "DATATRACEX_POSTGRES_DSN",
                "postgresql://datatracex:datatracex@127.0.0.1:5432/datatracex",
            )
        ),
        neo4j=Neo4jSettings(
            uri=os.getenv("DATATRACEX_NEO4J_URI", "bolt://127.0.0.1:7687"),
            user=os.getenv("DATATRACEX_NEO4J_USER", "neo4j"),
            password=os.getenv("DATATRACEX_NEO4J_PASSWORD", "datatracex_graph"),
        ),
        raw_dir=Path(os.getenv("DATATRACEX_RAW_DIR", "data/raw")),
    )


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"missing required environment variable: {name}")
    return value


def _csv_env(name: str) -> tuple[str, ...]:
    value = os.getenv(name, "")
    return tuple(item.strip() for item in value.split(",") if item.strip())
