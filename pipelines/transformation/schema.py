"""Column contracts for Stage 2 transformed tables and warehouse targets."""

from __future__ import annotations

DIM_DATE_COLUMNS = [
    "date_key",
    "full_date",
    "day_of_month",
    "month",
    "month_name",
    "quarter",
    "year",
    "day_of_week",
    "day_name",
    "is_weekend",
]

FACT_SALES_SUMMARY_COLUMNS = [
    "sales_summary_key",
    "date_key",
    "total_sales",
    "total_orders",
    "loaded_at",
]

FACT_INVENTORY_SUMMARY_COLUMNS = [
    "inventory_summary_key",
    "date_key",
    "total_items",
    "low_stock_items",
    "out_of_stock_items",
    "low_stock_threshold",
    "loaded_at",
]

FACT_TRAFFIC_SUMMARY_COLUMNS = [
    "traffic_summary_key",
    "date_key",
    "total_customers",
    "peak_hour",
    "loaded_at",
]

QUARANTINE_COLUMNS = [
    "run_id",
    "parent_run_id",
    "source_table",
    "source_record_id",
    "rule_id",
    "error_code",
    "reason",
    "payload",
    "rejected_at",
]

TRANSFORM_LOG_COLUMNS = [
    "run_id",
    "parent_run_id",
    "table_name",
    "batch_window",
    "records_read",
    "records_output",
    "records_rejected",
    "status",
    "started_at",
    "finished_at",
    "duration_seconds",
    "error_message",
]

TABLE_COLUMNS: dict[str, list[str]] = {
    "dim_date": DIM_DATE_COLUMNS,
    "fact_sales_summary": FACT_SALES_SUMMARY_COLUMNS,
    "fact_inventory_summary": FACT_INVENTORY_SUMMARY_COLUMNS,
    "fact_traffic_summary": FACT_TRAFFIC_SUMMARY_COLUMNS,
    "transformation_quarantine": QUARANTINE_COLUMNS,
    "transformation_run_log": TRANSFORM_LOG_COLUMNS,
}

FACT_KEY_COLUMNS: dict[str, str] = {
    "fact_sales_summary": "sales_summary_key",
    "fact_inventory_summary": "inventory_summary_key",
    "fact_traffic_summary": "traffic_summary_key",
}
