from datatracex.models import EdgeKind, LineageEdge
from datatracex.postgres_store import edge_id_for


def test_edge_id_is_stable_for_natural_key() -> None:
    edge = LineageEdge(
        src_urn="obs://bucket/path",
        dst_urn="dws://cluster/db/public/table",
        kind=EdgeKind.DERIVES_FROM,
        confidence=0.8,
        edge_scope="run",
        run_id="dataarts://workspace/job/job-a/instance/1",
    )

    assert edge_id_for(edge) == edge_id_for(edge)
    assert edge_id_for(edge).startswith("edge_")
