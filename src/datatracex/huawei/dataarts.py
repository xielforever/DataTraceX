from __future__ import annotations

from typing import Any
from urllib.parse import quote

from .client import HuaweiClient


class DataArtsClient:
    def __init__(self, client: HuaweiClient, project_id: str) -> None:
        self.client = client
        self.project_id = project_id

    def list_jobs(self, limit: int = 1000, offset: int = 0) -> Any:
        return self.client.get(
            f"/v1/{self.project_id}/jobs",
            query={"limit": limit, "offset": offset},
        )

    def show_job(self, job_name: str) -> Any:
        return self.client.get(f"/v1/{self.project_id}/jobs/{quote(job_name, safe='')}")

    def list_job_instances(
        self,
        job_name: str,
        min_plan_time: str | None = None,
        max_plan_time: str | None = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> Any:
        return self.client.get(
            f"/v1/{self.project_id}/jobs/instances/detail",
            query={
                "jobName": job_name,
                "minPlanTime": min_plan_time,
                "maxPlanTime": max_plan_time,
                "limit": limit,
                "offset": offset,
            },
        )

    def show_job_instance(self, job_name: str, instance_id: str) -> Any:
        return self.client.get(
            f"/v1/{self.project_id}/jobs/{quote(job_name, safe='')}/instances/{quote(instance_id, safe='')}"
        )
