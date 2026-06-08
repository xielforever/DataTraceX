from __future__ import annotations

from .models import EdgeKind, Entity, EntityKind, Evidence, EvidenceKind, LineageEdge, Run, RunKind
from .store import LineageStore
from .urn import dataarts_node_urn, dataarts_run_urn, dataset_urn, normalize_storage_uri


def build_demo_store() -> LineageStore:
    store = LineageStore()

    workspace_id = "workspace-001"
    job_name = "daily_order"
    node_name = "load_fact_order"

    obs_orders = normalize_storage_uri("s3a://raw-bucket/order/dt=2026-06-08/")
    fact_order = dataset_urn("dws", "dws-prod", "dw", "fact_order", schema="public")
    node_urn = dataarts_node_urn(workspace_id, job_name, node_name)
    run_id = dataarts_run_urn(workspace_id, job_name, "202606080001")

    store.upsert_entity(Entity(obs_orders, EntityKind.PATH_PREFIX, "raw order partition", attrs={
        "service": "OBS",
        "bucket": "raw-bucket",
        "path": "order/dt=2026-06-08/",
    }))
    store.upsert_entity(Entity(fact_order, EntityKind.DATASET, "public.fact_order", attrs={
        "service": "DWS",
        "cluster": "dws-prod",
        "database": "dw",
        "schema": "public",
        "table": "fact_order",
    }))
    store.upsert_entity(Entity(node_urn, EntityKind.NODE, node_name, attrs={
        "workspace_id": workspace_id,
        "job_name": job_name,
        "node_type": "DWSSQL",
    }))

    store.upsert_run(Run(run_id, RunKind.DATAARTS_INSTANCE, status="success", attrs={
        "workspace_id": workspace_id,
        "job_name": job_name,
        "instance_id": "202606080001",
        "instance_type": "schedule",
    }))

    sql_evidence = Evidence(
        evidence_id="ev-demo-dws-query-001",
        kind=EvidenceKind.SQL_AST,
        source="demo:DWSSQL",
        summary="INSERT INTO public.fact_order SELECT ... FROM obs://raw-bucket/order/dt=2026-06-08/",
        run_id=run_id,
        attrs={
            "query_id": "demo-query-001",
            "parser": "seeded-demo",
        },
    )
    store.add_evidence(sql_evidence)

    store.upsert_edge(LineageEdge(
        src_urn=node_urn,
        dst_urn=obs_orders,
        kind=EdgeKind.READS,
        confidence=0.85,
        edge_scope="run",
        source_system="dataarts",
        evidence_ids=[sql_evidence.evidence_id],
        run_id=run_id,
        attrs={"mode": "batch", "source": "sql"},
    ))
    store.upsert_edge(LineageEdge(
        src_urn=node_urn,
        dst_urn=fact_order,
        kind=EdgeKind.WRITES,
        confidence=0.9,
        edge_scope="run",
        source_system="dataarts",
        evidence_ids=[sql_evidence.evidence_id],
        run_id=run_id,
        attrs={"mode": "insert", "source": "sql"},
    ))
    store.upsert_edge(LineageEdge(
        src_urn=obs_orders,
        dst_urn=fact_order,
        kind=EdgeKind.DERIVES_FROM,
        confidence=0.82,
        edge_scope="run",
        source_system="dataarts",
        evidence_ids=[sql_evidence.evidence_id],
        run_id=run_id,
        attrs={"via_node": node_urn},
    ))

    return store
