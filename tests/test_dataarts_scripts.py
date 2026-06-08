from datatracex.huawei.dataarts import DataArtsClient
from datatracex.ingest.dataarts_scripts import (
    extract_script_content,
    extract_script_references,
    script_source_key,
)
from datatracex.urn import code_artifact_urn


class FakeHuaweiClient:
    def __init__(self) -> None:
        self.calls = []

    def get(self, path, query=None):
        self.calls.append((path, query))
        return {"path": path, "query": query}


def test_dataarts_client_builds_script_detail_request() -> None:
    fake = FakeHuaweiClient()
    client = DataArtsClient(fake, "project-1")

    payload = client.show_script("etl_order", version="3")

    assert payload["path"] == "/v1/project-1/scripts/etl_order"
    assert payload["query"] == {"version": "3"}


def test_extract_script_references_from_list_properties() -> None:
    payload = {
        "nodes": [
            {
                "name": "load_orders",
                "type": "HiveSQL",
                "properties": [
                    {"name": "scriptName", "value": "etl_orders"},
                    {"name": "scriptVersion", "value": "7"},
                    {"name": "statementOrScript", "value": "SCRIPT"},
                    {"name": "connectionName", "value": "mrs_hive"},
                    {"name": "database", "value": "dw"},
                ],
            },
            {
                "name": "inline_py",
                "type": "Python",
                "properties": [
                    {"name": "scriptName", "value": "ignored"},
                    {"name": "statementOrScript", "value": "STATEMENT"},
                ],
            },
        ]
    }

    refs = extract_script_references("workspace-1:daily_job", payload)

    assert len(refs) == 1
    assert refs[0].script_source_key == "workspace-1:etl_orders:7"
    assert refs[0].node_urn == "dataarts://workspace-1/job/daily_job/node/load_orders"


def test_extract_script_content_supports_nested_payload() -> None:
    assert extract_script_content({"data": {"content": "select 1"}}) == "select 1"


def test_script_source_and_code_urn_are_stable() -> None:
    assert script_source_key("ws", "script", None) == "ws:script:latest"
    assert code_artifact_urn("a" * 64) == f"code://sha256/{'a' * 64}"
