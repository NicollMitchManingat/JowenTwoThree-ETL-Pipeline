-- Execute as warehouse owner, only in the separate local reporting database.
DO $roles$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'etl_warehouse_writer') THEN
        CREATE ROLE etl_warehouse_writer NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'etl_reporting_reader') THEN
        CREATE ROLE etl_reporting_reader NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'etl_warehouse_maintainer') THEN
        CREATE ROLE etl_warehouse_maintainer NOLOGIN;
    END IF;
END
$roles$;

REVOKE ALL ON SCHEMA dw_analytics FROM PUBLIC;
GRANT USAGE ON SCHEMA dw_analytics TO etl_warehouse_writer, etl_reporting_reader, etl_warehouse_maintainer;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA dw_analytics TO etl_warehouse_writer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA dw_analytics TO etl_warehouse_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA dw_analytics
    GRANT SELECT, INSERT, UPDATE ON TABLES TO etl_warehouse_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA dw_analytics
    GRANT USAGE, SELECT ON SEQUENCES TO etl_warehouse_writer;
GRANT SELECT ON dw_analytics.dim_date, dw_analytics.fact_sales_summary,
    dw_analytics.fact_inventory_summary, dw_analytics.fact_traffic_summary TO etl_reporting_reader;
-- Maintenance is separate from routine loading; no fact DELETE is granted.
GRANT SELECT, DELETE ON dw_analytics.transformation_run_log,
    dw_analytics.transformation_quarantine TO etl_warehouse_maintainer;
