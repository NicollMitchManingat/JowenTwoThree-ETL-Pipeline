-- Stage 2 warehouse schema contract.
-- These tables are defined here for the future load stage; Stage 2 itself
-- writes Parquet handover files and does not connect to this database.

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day_of_month SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    month_name TEXT NOT NULL,
    quarter SMALLINT NOT NULL,
    year SMALLINT NOT NULL,
    day_of_week SMALLINT NOT NULL,
    day_name TEXT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_sales_summary (
    sales_summary_key TEXT PRIMARY KEY,
    date_key INTEGER NOT NULL REFERENCES dim_date (date_key),
    total_sales NUMERIC(10, 2) NOT NULL CHECK (total_sales >= 0),
    total_orders INTEGER NOT NULL CHECK (total_orders >= 0),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fact_inventory_summary (
    inventory_summary_key TEXT PRIMARY KEY,
    date_key INTEGER NOT NULL REFERENCES dim_date (date_key),
    total_items INTEGER NOT NULL CHECK (total_items >= 0),
    low_stock_items INTEGER NOT NULL CHECK (low_stock_items >= 0),
    out_of_stock_items INTEGER NOT NULL CHECK (out_of_stock_items >= 0),
    low_stock_threshold INTEGER NOT NULL CHECK (low_stock_threshold >= 0),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fact_traffic_summary (
    traffic_summary_key TEXT PRIMARY KEY,
    date_key INTEGER NOT NULL REFERENCES dim_date (date_key),
    total_customers INTEGER NOT NULL CHECK (total_customers >= 0),
    peak_hour TIME,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS transformation_quarantine (
    quarantine_id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    parent_run_id TEXT,
    source_table TEXT NOT NULL,
    source_record_id TEXT,
    rule_id TEXT NOT NULL,
    error_code TEXT NOT NULL,
    reason TEXT NOT NULL,
    payload JSONB NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_id, source_table, rule_id, source_record_id)
);

CREATE TABLE IF NOT EXISTS transformation_run_log (
    run_id TEXT NOT NULL,
    parent_run_id TEXT,
    table_name TEXT NOT NULL,
    batch_window TEXT,
    records_read INTEGER NOT NULL DEFAULT 0,
    records_output INTEGER NOT NULL DEFAULT 0,
    records_rejected INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    duration_seconds NUMERIC(10, 3),
    error_message TEXT,
    PRIMARY KEY (run_id, table_name)
);

CREATE INDEX IF NOT EXISTS idx_fact_sales_summary_date_key
    ON fact_sales_summary (date_key);

CREATE INDEX IF NOT EXISTS idx_fact_inventory_summary_date_key
    ON fact_inventory_summary (date_key);

CREATE INDEX IF NOT EXISTS idx_fact_traffic_summary_date_key
    ON fact_traffic_summary (date_key);

CREATE INDEX IF NOT EXISTS idx_quarantine_run_id
    ON transformation_quarantine (run_id);
