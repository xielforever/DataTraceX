from datatracex.urn import cdm_job_urn, dataarts_node_urn, dataarts_run_urn, dataset_urn, normalize_storage_uri


def test_normalize_s3a_to_obs() -> None:
    assert normalize_storage_uri("s3a://Raw-Bucket/order/dt=2026-06-08/") == "obs://raw-bucket/order/dt=2026-06-08"


def test_dataset_urn_with_schema() -> None:
    assert dataset_urn("DWS", "DWS-PROD", "DW", "Fact_Order", schema="Public") == "dws://dws-prod/dw/public/fact_order"


def test_dataarts_run_urn() -> None:
    assert dataarts_run_urn("workspace-001", "daily_order", "202606080001") == (
        "dataarts://workspace-001/job/daily_order/instance/202606080001"
    )


def test_dataarts_node_urn_encodes_non_ascii() -> None:
    assert dataarts_node_urn("workspace-001", "补数 Job", "节点 A") == (
        "dataarts://workspace-001/job/%E8%A1%A5%E6%95%B0%20job/node/%E8%8A%82%E7%82%B9%20a"
    )


def test_cdm_job_urn() -> None:
    assert cdm_job_urn("cluster-1", "sync_order") == "cdm://cluster-1/job/sync_order"
