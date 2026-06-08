from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from datatracex.huawei.dataarts import DataArtsClient
from datatracex.raw import RawLake, utc_stamp


@dataclass(slots=True)
class DataArtsHarvestSummary:
    jobs_seen: int = 0
    jobs_harvested: int = 0
    job_details: int = 0
    job_instances: int = 0
    job_instance_details: int = 0
    node_types: Counter[str] = field(default_factory=Counter)
    statuses: Counter[str] = field(default_factory=Counter)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "jobs_seen": self.jobs_seen,
            "jobs_harvested": self.jobs_harvested,
            "job_details": self.job_details,
            "job_instances": self.job_instances,
            "job_instance_details": self.job_instance_details,
            "node_types": dict(self.node_types),
            "statuses": dict(self.statuses),
            "errors": self.errors,
        }


class DataArtsHarvester:
    def __init__(self, client: DataArtsClient, raw_lake: RawLake) -> None:
        self.client = client
        self.raw_lake = raw_lake

    def harvest(self, days: int, max_jobs: int | None = None, include_instance_details: bool = False) -> DataArtsHarvestSummary:
        summary = DataArtsHarvestSummary()
        stamp = utc_stamp()

        jobs_payload = self.client.list_jobs()
        self.raw_lake.write_json(f"dataarts/{stamp}/jobs", "list", jobs_payload)

        jobs = _extract_items(jobs_payload)
        summary.jobs_seen = len(jobs)
        if max_jobs is not None:
            jobs = jobs[:max_jobs]

        min_plan_time, max_plan_time = _window(days)

        for job in jobs:
            job_name = _job_name(job)
            if not job_name:
                summary.errors.append(f"job without name: {job}")
                continue

            summary.jobs_harvested += 1
            try:
                detail = self.client.show_job(job_name)
                self.raw_lake.write_json(f"dataarts/{stamp}/job-details", job_name, detail)
                summary.job_details += 1
                for node in _extract_nodes(detail):
                    node_type = str(node.get("type") or node.get("nodeType") or "unknown")
                    summary.node_types[node_type] += 1
            except Exception as exc:
                summary.errors.append(f"show_job {job_name}: {exc}")
                continue

            try:
                inst_payload = self.client.list_job_instances(
                    job_name,
                    min_plan_time=min_plan_time,
                    max_plan_time=max_plan_time,
                )
                self.raw_lake.write_json(f"dataarts/{stamp}/job-instances", job_name, inst_payload)
                instances = _extract_items(inst_payload)
                summary.job_instances += len(instances)
                for inst in instances:
                    status = str(inst.get("status") or "unknown")
                    summary.statuses[status] += 1
                    instance_id = str(inst.get("instanceId") or inst.get("instance_id") or "")
                    if include_instance_details and instance_id:
                        inst_detail = self.client.show_job_instance(job_name, instance_id)
                        self.raw_lake.write_json(
                            f"dataarts/{stamp}/job-instance-details",
                            f"{job_name}_{instance_id}",
                            inst_detail,
                        )
                        summary.job_instance_details += 1
            except Exception as exc:
                summary.errors.append(f"instances {job_name}: {exc}")

        self.raw_lake.write_json(f"dataarts/{stamp}", "summary", summary.to_dict())
        return summary


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


def _window(days: int) -> tuple[str, str]:
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    return _format_time(start), _format_time(end)


def _format_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
