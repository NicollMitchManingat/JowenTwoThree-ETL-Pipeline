-- Execute only in the local warehouse, as its owner.
SET search_path TO dw_analytics, pg_catalog;

CREATE TABLE IF NOT EXISTS load_log (
    load_run_id TEXT NOT NULL,
    transformation_run_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    batch_window TEXT NOT NULL,
    table_name TEXT NOT NULL,
    load_type TEXT NOT NULL DEFAULT 'INCREMENTAL' CHECK (load_type = 'INCREMENTAL'),
    load_start_ts TIMESTAMPTZ NOT NULL,
    load_end_ts TIMESTAMPTZ NOT NULL,
    load_duration_sec NUMERIC(12,3) NOT NULL CHECK (load_duration_sec >= 0),
    records_received BIGINT NOT NULL CHECK (records_received >= 0),
    records_inserted BIGINT NOT NULL CHECK (records_inserted >= 0),
    records_updated BIGINT NOT NULL CHECK (records_updated >= 0),
    records_loaded BIGINT NOT NULL CHECK (records_loaded = records_inserted + records_updated),
    records_rejected BIGINT NOT NULL CHECK (records_rejected >= 0),
    pre_load_result JSONB NOT NULL,
    post_load_result JSONB NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('SUCCESS','FAILED','MANUAL_REVIEW','SKIPPED')),
    error_code TEXT,
    error_message TEXT,
    load_code_version TEXT NOT NULL,
    details JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (load_run_id, table_name),
    CHECK (status = 'SKIPPED' OR records_received = records_loaded + records_rejected)
);

CREATE INDEX IF NOT EXISTS load_log_batch_idx ON load_log (batch_id, status);
CREATE TABLE IF NOT EXISTS load_quarantine (
    quarantine_id BIGSERIAL PRIMARY KEY,
    load_run_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    reason TEXT NOT NULL,
    payload JSONB NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
GRANT SELECT, DELETE ON load_log, load_quarantine TO etl_warehouse_maintainer;
