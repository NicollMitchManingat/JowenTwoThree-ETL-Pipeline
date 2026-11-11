from pathlib import Path

from pipelines.transformation.schema import (
    FACT_KEY_COLUMNS,
    TABLE_COLUMNS,
)

SCHEMA_PATH = Path(__file__).resolve().parents[1] / "warehouse" / "schema.sql"


def test_schema_contract_lists_all_transformation_tables() -> None:
    assert set(TABLE_COLUMNS) == {
        "dim_date",
        "fact_sales_summary",
        "fact_inventory_summary",
        "fact_traffic_summary",
        "transformation_quarantine",
        "transformation_run_log",
    }


def test_fact_key_columns_are_unique_per_fact_table() -> None:
    assert FACT_KEY_COLUMNS == {
        "fact_sales_summary": "sales_summary_key",
        "fact_inventory_summary": "inventory_summary_key",
        "fact_traffic_summary": "traffic_summary_key",
    }


def test_warehouse_schema_contains_all_tables_and_keys() -> None:
    sql = SCHEMA_PATH.read_text(encoding="utf-8")

    for table, columns in TABLE_COLUMNS.items():
        assert f"CREATE TABLE IF NOT EXISTS {table}" in sql
        for column in columns:
            assert column in sql


def test_warehouse_schema_enforces_referential_integrity() -> None:
    sql = SCHEMA_PATH.read_text(encoding="utf-8")

    assert sql.count("REFERENCES dim_date (date_key)") == 3
    assert "CHECK (total_sales >= 0)" in sql
    assert "UNIQUE (run_id, source_table, rule_id, source_record_id)" in sql


def test_daily_grain_and_net_revenue_constraints():
    sql = SCHEMA_PATH.read_text(encoding="utf-8")
    assert sql.count("date_key INTEGER NOT NULL UNIQUE REFERENCES") == 3
    assert "CREATE SCHEMA IF NOT EXISTS dw_analytics" in sql
    assert "CHECK (net_sales = total_sales - refund_amount)" in sql
    assert "NUMERIC(18, 2)" in sql
