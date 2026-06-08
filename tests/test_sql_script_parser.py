from datatracex.models import EdgeKind
from datatracex.parsers.sql_script import parse_sql_script_lineage


def test_parse_sql_script_lineage_reads_and_writes_tables() -> None:
    facts = parse_sql_script_lineage(
        "dataarts://ws/job/j/node/n",
        "HiveSQL",
        {"connectionName": "mrs_hive", "database": "dw"},
        """
        insert overwrite table mart.fact_order
        select * from ods.order_detail;
        alter table mart.fact_order add partition (dt='2026-06-09');
        """,
        "raw_1",
    )

    edges = {(edge.kind, edge.dst_urn) for edge in facts.edges}

    assert (EdgeKind.READS, "hive://mrs_hive/ods/order_detail") in edges
    assert (EdgeKind.WRITES, "hive://mrs_hive/mart/fact_order") in edges
    assert facts.parsed_statements == 2


def test_parse_sql_script_lineage_handles_dws_schema() -> None:
    facts = parse_sql_script_lineage(
        "dataarts://ws/job/j/node/n",
        "DWSSQL",
        {"connectionName": "dws-prod", "database": "dw"},
        "create table public.fact_order as select * from staging.orders",
        "raw_2",
    )

    edges = {(edge.kind, edge.dst_urn) for edge in facts.edges}

    assert (EdgeKind.READS, "dws://dws-prod/dw/staging/orders") in edges
    assert (EdgeKind.WRITES, "dws://dws-prod/dw/public/fact_order") in edges
