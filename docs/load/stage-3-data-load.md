# Stage 3: Data Load Documentation

## 1. Transformation Input and Processing Specification

**Load Input**

- Name and Location of Transformed Output: A sealed contract-version 2 manifest under data/silver/Transform_<timestamp>_<id>/manifest.json and the run-specific Parquet files alongside it: dim_date, the three fact summaries, and transformation_quarantine. Stage 3 receives the explicit manifest path and never searches mutable date partitions.

- Transformation Metadata Linkage: Linked directly to the Stage 2 transformation execution output via Transformation Run ID TRN_20261013_0200 (derived from extraction Run_20261013_001), processing the previous business day window (2026-10-12 00:00:00 to 2026-10-12 23:59:59).

**Target System**

- Target System Name & Purpose: Jowen Two Three Analytics Data Warehouse, dedicated to storing historical summaries of sales, inventory, and customer traffic for daily, weekly, and monthly business performance reporting without impacting operational performance.

- Database Management System (DBMS): Separate local PostgreSQL 17 database jowen_analytics on loopback port 55432. Sample data uses the independent jowen_analytics_demo database.

- Target Schema: dw_analytics (data warehouse reporting schema).

**Load Method**

- Tooling / Technology: Python (psycopg2/ SQLAlchemy batch loading scripts) executing SQL batch insertion and upsert statements.

- Batch Frequency & Schedule: Daily batch schedule executed at 04:00 AM, immediately following the completion and verification of the Stage 2 data transformation.

- Write Logic: Idempotent transactional batch insert and upsert logic. All dimensions and facts use INSERT ... ON CONFLICT (date_key) DO UPDATE. Each daily fact enforces UNIQUE(date_key) in addition to its deterministic text summary primary key. Only affected dates are replaced; no append or additive update inflates totals.

**Load Scope**

- Load Order: Dimension tables (dim_date) are loaded/synchronized first, followed by child fact tables (fact_sales_summary, fact_inventory_summary, fact_traffic_summary) to guarantee referential integrity.

- Expected Volume & Execution Window: Approximately 1 to 3 daily summary records per batch run across fact tables. The total load execution window is scheduled to complete within 3 minutes.

**Limitations and Assumptions**

- Write Access & Role: The load process operates under a restricted database role (etl_warehouse_writer) with USAGE on dw_analytics, SELECT/INSERT/UPDATE on warehouse tables, and sequence USAGE/SELECT. It has no schema CREATE or fact DELETE. Administrative privileges (e.g., DROP TABLE, schema alterations) are strictly prohibited.

- Operational Boundary: Loading occurs off-peak (04:00 AM) to eliminate target warehouse table locking during operational hours. Data loaded is strictly limited to the dataset handed over from Stage 2 without re-extracting or re-transforming.

## 2. Target Tables and Column Specification

All targets are in the separate local database schema `dw_analytics`. The following specification is verified against the local PostgreSQL catalog. The SQL files in `warehouse/` are the setup contract.

Stage 2 handover columns match their mandatory target names and types. Stage 3 adds source_run_id and source_created_at for lineage on facts. Batch identity lives in load_batches/load_log, linked through the transformation run; it is not a fact-grain key. Calendar attributes do not need SCD Type 2 history. Daily facts use replacement/upsert semantics, while immutable run packages retain prior versions.

### dim_date

Calendar dimension, matched by date_key; full_date is unique.

| Column | PostgreSQL type | Required | Default |
| --- | --- | --- | --- |
| `date_key` | `integer` | Yes | `?` |
| `full_date` | `date` | Yes | `?` |
| `day_of_month` | `smallint` | Yes | `?` |
| `month` | `smallint` | Yes | `?` |
| `month_name` | `text` | Yes | `?` |
| `quarter` | `smallint` | Yes | `?` |
| `year` | `smallint` | Yes | `?` |
| `day_of_week` | `smallint` | Yes | `?` |
| `day_name` | `text` | Yes | `?` |
| `is_weekend` | `boolean` | Yes | `?` |

**Keys and constraints:**

- `PRIMARY KEY (date_key)`
- `UNIQUE (full_date)`

**Indexes:**

- `CREATE UNIQUE INDEX dim_date_full_date_key ON dw_analytics.dim_date USING btree (full_date)`
- `CREATE UNIQUE INDEX dim_date_pkey ON dw_analytics.dim_date USING btree (date_key)`

### fact_sales_summary

One paid-sales/refund/net summary per Manila business date.

| Column | PostgreSQL type | Required | Default |
| --- | --- | --- | --- |
| `sales_summary_key` | `text` | Yes | `?` |
| `date_key` | `integer` | Yes | `?` |
| `total_sales` | `numeric(18,2)` | Yes | `?` |
| `refund_amount` | `numeric(18,2)` | Yes | `?` |
| `net_sales` | `numeric(18,2)` | Yes | `?` |
| `total_orders` | `bigint` | Yes | `?` |
| `source_run_id` | `text` | No | `?` |
| `source_created_at` | `timestamp with time zone` | Yes | `'-infinity'::timestamp with time zone` |
| `loaded_at` | `timestamp with time zone` | Yes | `now()` |

**Keys and constraints:**

- `CHECK ((net_sales = (total_sales - refund_amount)))`
- `CHECK ((refund_amount >= (0)::numeric))`
- `CHECK ((total_orders >= 0))`
- `CHECK ((total_sales >= (0)::numeric))`
- `FOREIGN KEY (date_key) REFERENCES dw_analytics.dim_date(date_key)`
- `PRIMARY KEY (sales_summary_key)`
- `UNIQUE (date_key)`

**Indexes:**

- `CREATE UNIQUE INDEX fact_sales_summary_date_key_key ON dw_analytics.fact_sales_summary USING btree (date_key)`
- `CREATE UNIQUE INDEX fact_sales_summary_pkey ON dw_analytics.fact_sales_summary USING btree (sales_summary_key)`
- `CREATE INDEX idx_fact_sales_summary_date_key ON dw_analytics.fact_sales_summary USING btree (date_key)`

### fact_inventory_summary

One active-inventory observation per actual capture date; no historical reconstruction.

| Column | PostgreSQL type | Required | Default |
| --- | --- | --- | --- |
| `inventory_summary_key` | `text` | Yes | `?` |
| `date_key` | `integer` | Yes | `?` |
| `total_items` | `bigint` | Yes | `?` |
| `low_stock_items` | `bigint` | Yes | `?` |
| `out_of_stock_items` | `bigint` | Yes | `?` |
| `threshold_policy` | `text` | Yes | `?` |
| `snapshot_at` | `timestamp with time zone` | Yes | `?` |
| `source_run_id` | `text` | No | `?` |
| `source_created_at` | `timestamp with time zone` | Yes | `'-infinity'::timestamp with time zone` |
| `loaded_at` | `timestamp with time zone` | Yes | `now()` |

**Keys and constraints:**

- `CHECK (((low_stock_items + out_of_stock_items) <= total_items))`
- `CHECK ((low_stock_items >= 0))`
- `CHECK ((out_of_stock_items >= 0))`
- `CHECK ((threshold_policy = 'PER_ITEM'::text))`
- `CHECK ((total_items >= 0))`
- `FOREIGN KEY (date_key) REFERENCES dw_analytics.dim_date(date_key)`
- `PRIMARY KEY (inventory_summary_key)`
- `UNIQUE (date_key)`

**Indexes:**

- `CREATE UNIQUE INDEX fact_inventory_summary_date_key_key ON dw_analytics.fact_inventory_summary USING btree (date_key)`
- `CREATE UNIQUE INDEX fact_inventory_summary_pkey ON dw_analytics.fact_inventory_summary USING btree (inventory_summary_key)`
- `CREATE INDEX idx_fact_inventory_summary_date_key ON dw_analytics.fact_inventory_summary USING btree (date_key)`

### fact_traffic_summary

One customer-traffic total and peak hour per business date.

| Column | PostgreSQL type | Required | Default |
| --- | --- | --- | --- |
| `traffic_summary_key` | `text` | Yes | `?` |
| `date_key` | `integer` | Yes | `?` |
| `total_customers` | `bigint` | Yes | `?` |
| `peak_hour` | `time without time zone` | No | `?` |
| `source_run_id` | `text` | No | `?` |
| `source_created_at` | `timestamp with time zone` | Yes | `'-infinity'::timestamp with time zone` |
| `loaded_at` | `timestamp with time zone` | Yes | `now()` |

**Keys and constraints:**

- `CHECK ((total_customers >= 0))`
- `FOREIGN KEY (date_key) REFERENCES dw_analytics.dim_date(date_key)`
- `PRIMARY KEY (traffic_summary_key)`
- `UNIQUE (date_key)`

**Indexes:**

- `CREATE UNIQUE INDEX fact_traffic_summary_date_key_key ON dw_analytics.fact_traffic_summary USING btree (date_key)`
- `CREATE UNIQUE INDEX fact_traffic_summary_pkey ON dw_analytics.fact_traffic_summary USING btree (traffic_summary_key)`
- `CREATE INDEX idx_fact_traffic_summary_date_key ON dw_analytics.fact_traffic_summary USING btree (date_key)`

### load_batches

Success-only identity ledger; a batch and transformation run may succeed only once.

| Column | PostgreSQL type | Required | Default |
| --- | --- | --- | --- |
| `batch_id` | `text` | Yes | `?` |
| `transformation_run_id` | `text` | Yes | `?` |
| `manifest_sha256` | `text` | Yes | `?` |
| `load_run_id` | `text` | Yes | `?` |
| `source_created_at` | `timestamp with time zone` | Yes | `?` |
| `business_dates` | `date[]` | Yes | `?` |
| `loaded_at` | `timestamp with time zone` | Yes | `now()` |

**Keys and constraints:**

- `PRIMARY KEY (batch_id)`
- `UNIQUE (load_run_id)`
- `UNIQUE (transformation_run_id)`

**Indexes:**

- `CREATE UNIQUE INDEX load_batches_load_run_id_key ON dw_analytics.load_batches USING btree (load_run_id)`
- `CREATE UNIQUE INDEX load_batches_pkey ON dw_analytics.load_batches USING btree (batch_id)`
- `CREATE UNIQUE INDEX load_batches_transformation_run_id_key ON dw_analytics.load_batches USING btree (transformation_run_id)`

### load_log

Per-table load attempt audit and exact committed-row accounting.

| Column | PostgreSQL type | Required | Default |
| --- | --- | --- | --- |
| `load_run_id` | `text` | Yes | `?` |
| `transformation_run_id` | `text` | Yes | `?` |
| `batch_id` | `text` | Yes | `?` |
| `batch_window` | `text` | Yes | `?` |
| `table_name` | `text` | Yes | `?` |
| `load_type` | `text` | Yes | `'INCREMENTAL'::text` |
| `load_start_ts` | `timestamp with time zone` | Yes | `?` |
| `load_end_ts` | `timestamp with time zone` | Yes | `?` |
| `load_duration_sec` | `numeric(12,3)` | Yes | `?` |
| `records_received` | `bigint` | Yes | `?` |
| `records_inserted` | `bigint` | Yes | `?` |
| `records_updated` | `bigint` | Yes | `?` |
| `records_loaded` | `bigint` | Yes | `?` |
| `records_rejected` | `bigint` | Yes | `?` |
| `pre_load_result` | `jsonb` | Yes | `?` |
| `post_load_result` | `jsonb` | Yes | `?` |
| `status` | `text` | Yes | `?` |
| `error_code` | `text` | No | `?` |
| `error_message` | `text` | No | `?` |
| `load_code_version` | `text` | Yes | `?` |
| `details` | `jsonb` | Yes | `'{}'::jsonb` |
| `created_at` | `timestamp with time zone` | Yes | `now()` |

**Keys and constraints:**

- `CHECK ((records_loaded = (records_inserted + records_updated)))`
- `CHECK (((status = 'SKIPPED'::text) OR (records_received = (records_loaded + records_rejected))))`
- `CHECK ((load_duration_sec >= (0)::numeric))`
- `CHECK ((load_type = 'INCREMENTAL'::text))`
- `CHECK ((records_inserted >= 0))`
- `CHECK ((records_received >= 0))`
- `CHECK ((records_rejected >= 0))`
- `CHECK ((records_updated >= 0))`
- `CHECK ((status = ANY (ARRAY['SUCCESS'::text, 'FAILED'::text, 'MANUAL_REVIEW'::text, 'SKIPPED'::text])))`
- `PRIMARY KEY (load_run_id, table_name)`

**Indexes:**

- `CREATE INDEX load_log_batch_idx ON dw_analytics.load_log USING btree (batch_id, status)`
- `CREATE UNIQUE INDEX load_log_pkey ON dw_analytics.load_log USING btree (load_run_id, table_name)`

### load_quarantine

Rejected target payloads retained independently of rolled-back publication.

| Column | PostgreSQL type | Required | Default |
| --- | --- | --- | --- |
| `quarantine_id` | `bigint` | Yes | `nextval('dw_analytics.load_quarantine_quarantine_id_seq'::regclass)` |
| `load_run_id` | `text` | Yes | `?` |
| `table_name` | `text` | Yes | `?` |
| `reason` | `text` | Yes | `?` |
| `payload` | `jsonb` | Yes | `?` |
| `rejected_at` | `timestamp with time zone` | Yes | `now()` |

**Keys and constraints:**

- `PRIMARY KEY (quarantine_id)`

**Indexes:**

- `CREATE UNIQUE INDEX load_quarantine_pkey ON dw_analytics.load_quarantine USING btree (quarantine_id)`

### transformation_quarantine

Upstream rejections copied for audit, excluded from loader received-row counts.

| Column | PostgreSQL type | Required | Default |
| --- | --- | --- | --- |
| `quarantine_id` | `bigint` | Yes | `nextval('dw_analytics.transformation_quarantine_quarantine_id_seq'::regclass)` |
| `run_id` | `text` | Yes | `?` |
| `parent_run_id` | `text` | No | `?` |
| `source_table` | `text` | Yes | `?` |
| `source_record_id` | `text` | No | `?` |
| `rule_id` | `text` | Yes | `?` |
| `error_code` | `text` | Yes | `?` |
| `reason` | `text` | Yes | `?` |
| `payload` | `jsonb` | Yes | `?` |
| `rejected_at` | `timestamp with time zone` | Yes | `now()` |

**Keys and constraints:**

- `PRIMARY KEY (quarantine_id)`
- `UNIQUE (run_id, source_table, rule_id, source_record_id)`

**Indexes:**

- `CREATE INDEX idx_quarantine_run_id ON dw_analytics.transformation_quarantine USING btree (run_id)`
- `CREATE UNIQUE INDEX transformation_quarantine_pkey ON dw_analytics.transformation_quarantine USING btree (quarantine_id)`
- `CREATE UNIQUE INDEX transformation_quarantine_run_id_source_table_rule_id_sourc_key ON dw_analytics.transformation_quarantine USING btree (run_id, source_table, rule_id, source_record_id)`

The earlier transformation_run_log table is retained for legacy compatibility; version-2 transformation lineage and validation are stored in sealed manifests. The current loader writes load_log and the success ledger.

## 3. Load Strategy and Rules

| Table | Load Type | Write Logic | Match Key | On Failure |
| --- | --- | --- | --- | --- |
| dim_date | Incremental | Upsert (ON CONFLICT (date_key) DO UPDATE) | date_key | Roll back batch transaction, stop load execution. |
| fact_sales_summary | Incremental | Upsert (ON CONFLICT (date_key) DO UPDATE) | date_key | Roll back transaction, preserve prior warehouse state. |
| fact_inventory_summary | Incremental | Upsert (ON CONFLICT (date_key) DO UPDATE) | date_key | Roll back transaction, preserve prior warehouse state. |
| fact_traffic_summary | Incremental | Upsert (ON CONFLICT (date_key) DO UPDATE) | date_key | Roll back transaction, preserve prior warehouse state. |

## 4. Standardization and Data Type Rules

| Check Name | Target | Purpose | Validation Criteria | Severity |
| --- | --- | --- | --- | --- |
| Target Connection | PostgreSQL Data Warehouse | Verify target availability and write permission | Connection established within 10-second timeout; write test query succeeds | Critical |
| Handover Status | Transformation Stage Metadata | Ensure only approved datasets are loaded | manifest.decision = APPROVED, contract_version = 2, and stage = transformation | Critical |
| Schema Compatibility | dw_analytics, Target Tables | Prevent column or type mismatch failures | Target table names, column names, and data types match Stage 2 contract strictly | Critical |
| Duplicate Batch Check | load_log.batch_id | Prevent duplicate processing of an already completed batch | An identical successful manifest is SKIPPED without replay. A new approved correction may upsert the same dates; older extraction snapshots are rejected. | Critical |
| Storage Capacity Check | Warehouse Storage Volume | Confirm adequate disk space for batch insertion | Available storage volume > 1 GB threshold | Critical |

## 5. Post-Load Validation and Reconciliation

| Check Name | Target | Purpose | Validation Criteria | Severity |
| --- | --- | --- | --- | --- |
| Record Reconciliation | All target fact tables | Verify zero unhandled record loss during loading | records_received = records_loaded + records_rejected | Critical |
| Primary Key Uniqueness | fact_sales_summary, fact_inventory_summary, fact_traffic_summary | Guarantee primary key integrity | Count of duplicate primary keys in target tables = 0 | Critical |
| Referential Integrity | Fact tables (date_key) | Enforce valid foreign key references | 100% of loaded date_key values exist in dim_date.date_key | Critical |
| Financial Metric Reconciliation | fact_sales_summary.total_sales, refund_amount, net_sales | Prevent lost or changed financial values | Each handed-over row and each exact Decimal column sum equals the target for the affected date keys; tolerance 0.00 PHP | Critical |
| Null Constraint Check | Mandatory target columns | Prevent invalid rows reaching publication | Null count in NOT NULL constrained columns = 0 | Critical |

## 6. Transformed Data Contract and Naming Convention

| Field | Purpose | Example |
| --- | --- | --- |
| load_run_id | Unique primary key identifying the load process run | LOD_20261013_0400 |
| transformation_run_id | Foreign key linking to the Stage 2 transformation execution | TRN_20261013_0200 |
| batch_id | Identified target batch identifier | BATCH_20261012 |
| batch_window | Business date range processed | 2026-10-12 00:00:00 to 2026-10-12 23:59:59 |
| table_name | Target warehouse table receiving data | fact_sales_summary |
| load_type | Method of write operation (FULL or INCREMENTAL) | INCREMENTAL |
| load_start_ts | Execution start timestamp | 2026-10-13 04:00:02 |
| load_end_ts | Execution completion timestamp | 2026-10-13 04:00:18 |
| load_duration_sec | Total load duration in seconds | 16 |
| records_received | Total rows handed over from transformation | 1 |
| records_inserted | New rows written to target table | 1 |
| records_updated | Existing rows updated via idempotent overwrite | 0 |
| records_loaded | records_inserted + records_updated, committed rows only | 1 |
| records_rejected | Rows failing target write constraints | 0 |
| pre_load_result | Outcome of pre-load checks (PASS / FAIL) | PASS |
| post_load_result | Outcome of post-load reconciliation checks (PASS / FAIL) | PASS |
| status | Execution status (SUCCESS / FAILED / WARNING) | SUCCESS |
| error_code | Categorized error code if failed | NULL |
| error_message | Diagnostic reason without credentials | NULL |
| load_code_version | Version control tag of the loading script | v1.2.0 |

## 7. Log Retention, Rejected Data, and Access

**Retention Policies & Storage Locations**

- Operational Logs: Retained in dw_analytics.load_log for 90 days to support daily monitoring and troubleshooting.

- Audit Summaries: High-level batch run summaries are retained for 1 year to fulfill institutional requirements.

- Quarantine Records: Data rows rejected during target loading due to schema or constraint checks are written to dw_analytics.load_quarantine and retained for 30 days for manual inspection.

**Access Permissions & Governance**

- ETL Service Role (etl_warehouse_writer): USAGE plus SELECT, INSERT and UPDATE for upserts, validation and load logs; no DDL or fact DELETE.

- ETL Pipeline Administrator: Full access to manage tables, rerun batches, inspect quarantine tables, and manage log cleanup.

- Reporting / Analytics Users: Read-only access (SELECT) granted strictly to dw_analytics fact and dimension tables. Access to log and quarantine tables is restricted.

Deletion and Backup Protocols

- Automatic purge routines delete logs and quarantine records only after their designated retention period has expired.

- Daily automated snapshot backups of the PostgreSQL data warehouse are performed prior to initiating the scheduled 04:00 AM batch load.

## 8. Load Acceptance, Rollback, and Completion Rules

| Overall Condition | Acceptance Decision | Pipeline Action |
| --- | --- | --- |
| All pre-load and post-load critical checks PASS; record reconciliation matches 100% | APPROVED | Commit transaction, mark batch complete, publish warehouse data to reporting views |
| All critical checks PASS, but non-critical warnings are flagged (e.g., minor null rate in optional fields) | MANUAL REVIEW | Hold publication; retain the immutable package and notify the ETL operator through the failure/completion record |
| Any critical check FAILS (e.g., target connection failure, broken foreign key, amount mismatch) | REJECTED | Issue ROLLBACK, halt pipeline, send critical alert notification |

| Validation Summary | Result |
| --- | --- |
| dim_date Synchronization | PASS |
| fact_sales_summary Load | PASS |
| fact_inventory_summary Load | PASS |
| fact_traffic_summary Load | PASS |
| Post-Load Reconciliation & Financial Check | PASS |
| Overall Load Status | APPROVED |

## 9. Common Load Problems and Mitigation

| Problem | Possible Cause | Impact | Mitigation |
| --- | --- | --- | --- |
| Duplicate Rows on Rerun | Pipeline batch rerun without clearing previous target entries | Inflated aggregate sales totals, distorted inventory metrics | Implement explicit delete-by-date_key or upsert write logic prior to loading batch records. |
| Foreign Key Constraint Violation | Fact table records attempting to load before dim_date contains the required date_key | Load process aborts with transactional rollback | Load dimensions first and reject facts whose real YYYYMMDD date_key is missing or invalid; never substitute an unknown date. |
| Partial Data Load after Crash | Database crash or network disconnection midway through batch execution | Target warehouse left in an inconsistent state | Enclose all load statements for the batch inside a single atomic database transaction (BEGIN ... COMMIT). |
### Duplicate, correction, and backfill rules (issue #31)

The batch identity is the SHA-256 of the sealed Stage 2 manifest. A success-only load_batches ledger is committed atomically with facts and SUCCESS logs. Identical replays are no-ops; changing a published transformation identity is rejected. New approved runs replace matching date_key rows. The source extraction snapshot timestamp prevents an older extraction, even one transformed later, from overwriting newer data. Overlapping loads serialize under a transaction advisory lock. Backfills create fresh Stage 1 packages for the requested dates; no force flag bypasses critical checks.

### Sealed handover validation (issue #27)

Each manifest includes its pipeline_run_id, parent extraction run, complete business-date list, source snapshot timestamp, source mode, source reconciliation counts and validation results. Every file has a relative path contained in the run package, exact final row count, column list and SHA-256. Stage 3 verifies all of these before writing. Transformation rejections are upstream audit evidence and are not counted as rows received by loading. A package is immutable; manual review creates a new package linked to the held manifest. Old v1 manifests are rejected and must be regenerated.

### Atomic publication and durable failure evidence (issue #28)

Dimensions, facts, source lineage, the success ledger and SUCCESS load logs commit in one transaction after all post-load checks pass. No reporting rows are published on MANUAL_REVIEW or REJECTED outcomes. Constraint, schema, financial and referential failures cannot be overridden. A failed transaction rolls back all attempted rows, reports records_loaded=0, and treats its received rows as rejected for that attempt. Failure JSON is persisted outside the transaction even when the database is unavailable; FAILED database logs and exact-value quarantine payloads use a new transaction when the target is reachable. A later retry gets a new load_run_id. An ambiguous connection failure after COMMIT is resolved by the successful-batch ledger on replay.

### Role boundary (issue #29)

The POS connection is always read-only. Warehouse credentials are independent WAREHOUSE_DB_* settings, stored locally in gitignored .env.warehouse.local. The setup owner creates schema and roles. The routine local login inherits etl_warehouse_writer. etl_reporting_reader has SELECT only on dim_date and the three facts, not logs/quarantine. etl_warehouse_maintainer has SELECT/DELETE on log and quarantine tables for approved retention work, not business fact deletion. Public schema access is revoked; runtime never applies DDL.

### Load accounting contract (issue #30)

Counts refer only to final dimension/fact rows handed to Stage 3, never source rows or transformation rejections. For processed attempts, received = inserted + updated + rejected and loaded = inserted + updated. A rolled-back attempt has loaded/inserted/updated zero and rejected=received. An identical successful replay has status SKIPPED and zero processed counts, linked to the original successful batch. Per-table load_log rows include pre/post validation details, exact financial audit totals, UTC timestamps, duration, code version, error code and message. The separate load_batches ledger contains only committed successful identities. Local failure JSON and retained run packages remain available if the database cannot record its log.

### Calendar contract (issue #32)

The date dimension includes date_key (YYYYMMDD), full_date, day_of_month, month, month_name, quarter, year, day_of_week (Monday=0), day_name and is_weekend. full_date is unique and required. Stage 2 supplies a contiguous range covering the sales/traffic window and actual inventory capture date. Stage 3 validates date_key against full_date and requires every fact date to exist in the handed-over dimension before writing. Invalid or orphan dates are critical failures, with no default/unknown member.
