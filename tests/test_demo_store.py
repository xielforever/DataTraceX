from datatracex.demo import build_demo_store


def test_demo_store_has_run_centric_evidence() -> None:
    store = build_demo_store()

    assert store.stats() == {
        "entities": 3,
        "runs": 1,
        "evidence": 1,
        "edges": 3,
    }

    run = store.run_detail("dataarts://workspace-001/job/daily_order/instance/202606080001")

    assert run["run"]["status"] == "success"
    assert len(run["edges"]) == 3
    assert run["evidence"][0]["kind"] == "sql_ast"


def test_lineage_for_dws_table() -> None:
    store = build_demo_store()
    result = store.lineage_for_node("dws://dws-prod/dw/public/fact_order", direction="in")

    assert len(result["edges"]) == 2
    assert {edge["kind"] for edge in result["edges"]} == {"writes", "derives_from"}
