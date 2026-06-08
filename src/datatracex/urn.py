from __future__ import annotations

from urllib.parse import quote, urlparse


def normalize_storage_uri(value: str) -> str:
    raw = value.strip()
    parsed = urlparse(raw)
    scheme = parsed.scheme.lower()

    if scheme in {"s3a", "s3n", "s3", "obs"}:
        bucket = parsed.netloc.lower()
        path = parsed.path.lstrip("/")
        return _join_uri("obs", bucket, path)

    if scheme == "hdfs":
        host = parsed.netloc.lower()
        path = parsed.path.lstrip("/")
        return _join_uri("hdfs", host, path)

    raise ValueError(f"unsupported storage URI: {value}")


def dataset_urn(service: str, cluster_or_connection: str, database: str, table: str, schema: str | None = None) -> str:
    parts = [
        service.strip().lower(),
        _part(cluster_or_connection),
        _part(database),
    ]
    if schema:
        parts.append(_part(schema))
    parts.append(_part(table))
    return f"{parts[0]}://" + "/".join(parts[1:])


def column_urn(dataset: str, column: str) -> str:
    return f"{dataset}#{_part(column)}"


def workspace_urn(workspace_id: str) -> str:
    return f"dataarts://{_part(workspace_id)}"


def dataarts_job_urn(workspace_id: str, job_name: str) -> str:
    return f"{workspace_urn(workspace_id)}/job/{_part(job_name)}"


def dataarts_run_urn(workspace_id: str, job_name: str, instance_id: str) -> str:
    return f"{dataarts_job_urn(workspace_id, job_name)}/instance/{_part(instance_id)}"


def dataarts_node_urn(workspace_id: str, job_name: str, node_name: str) -> str:
    return f"{dataarts_job_urn(workspace_id, job_name)}/node/{_part(node_name)}"


def dataarts_node_key_urn(workspace_id: str, job_name: str, node_key: str) -> str:
    return f"{dataarts_job_urn(workspace_id, job_name)}/node-key/{_part(node_key)}"


def connection_urn(service: str, connection_id: str) -> str:
    return f"connection://{_part(service)}/{_part(connection_id)}"


def cluster_urn(service: str, cluster_id: str) -> str:
    return f"{_part(service)}://cluster/{_part(cluster_id)}"


def cdm_job_urn(cluster_id: str, job_name: str) -> str:
    return f"cdm://{_part(cluster_id)}/job/{_part(job_name)}"


def cdm_run_urn(cluster_id: str, job_name: str, external_id: str) -> str:
    return f"{cdm_job_urn(cluster_id, job_name)}/run/{_part(external_id)}"


def dws_query_run_urn(cluster_id: str, database: str, query_id: str) -> str:
    return f"dws://{_part(cluster_id)}/{_part(database)}/query/{_part(query_id)}"


def code_artifact_urn(content_hash: str) -> str:
    return f"code://sha256/{_part(content_hash)}"


def _join_uri(scheme: str, authority: str, path: str) -> str:
    normalized_path = "/".join(_part(part) for part in path.split("/") if part)
    if normalized_path:
        return f"{scheme}://{authority}/{normalized_path}"
    return f"{scheme}://{authority}"


def _part(value: str) -> str:
    return quote(value.strip().lower(), safe="-_.=:@")
