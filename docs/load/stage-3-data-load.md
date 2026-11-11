# Stage 3: Data Load Documentation

Load only approved transformed output into the separate local reporting warehouse. The implementation is scheduled batch ETL; streaming and real-time loading are out of scope. This document describes implemented behavior and its approval criteria; example identifiers are illustrative.

## 1. Load Source and Target Specification

**Load Input**

- Name and Location of Transformed Output: A sealed contract-version 2 manifest under data/silver/Transform_<timestamp>_<id>/manifest.json and the run-specific Parquet files alongside it: dim_date, the three fact summaries, and transformation_quarantine. Stage 3 receives the explicit manifest path and never searches mutable date partitions.

- Transformation Metadata Linkage: Linked directly to the Stage 2 transformation execution output via Transformation Run ID Transform_20261110_180001_a1b2c3d4e5f6 (derived from extraction Run_20261013_001), processing the previous business day window (2026-11-10 to 2026-11-10).

**Target System**

- Target System Name & Purpose: Jowen Two Three Analytics Data Warehouse, dedicated to storing historical summaries of sales, inventory, and customer traffic for daily, weekly, and monthly business performance reporting without impacting operational performance.

- Database Management System (DBMS): Separate local PostgreSQL 17 database jowen_analytics on loopback port 55432. Sample data uses the independent jowen_analytics_demo database.

- Target Schema: dw_analytics (data warehouse reporting schema).

**Load Method**

- Tooling / Technology: Python 3.11 with psycopg 3 executing SQL batch insertion and upsert statements.

- Batch Frequency & Schedule: The complete daily pipeline starts at 02:00 Asia/Manila. Loading starts only after the same pipeline produces an approved Stage 2 package; it does not run independently at a fixed later hour.

- Write Logic: Idempotent transactional batch insert and upsert logic. All dimensions and facts use INSERT ... ON CONFLICT (date_key) DO UPDATE. Each daily fact enforces UNIQUE(date_key) in addition to its deterministic text summary primary key. Only affected dates are replaced; no append or additive update inflates totals.

**Load Scope**

- Load Order: Dimension tables (dim_date) are loaded/synchronized first, followed by child fact tables (fact_sales_summary, fact_inventory_summary, fact_traffic_summary) to guarantee referential integrity.

- Expected Volume & Execution Window: For N refreshed sales/traffic dates, expect 2N+1 fact rows plus the supplied calendar rows. Default N=3 yields seven fact rows. Exact counts come from the manifest. The operational target is completion within three minutes; each SQL statement has a 180-second timeout, with 10-second connection and lock timeouts, and duration is logged.

**Limitations and Assumptions**

- Write Access & Role: The load process operates under a restricted database role (etl_warehouse_writer) with USAGE on dw_analytics, SELECT/INSERT/UPDATE on warehouse tables, and sequence USAGE/SELECT. It has no schema CREATE or fact DELETE. Administrative privileges (e.g., DROP TABLE, schema alterations) are strictly prohibited.

- Operational Boundary: Loading is scheduled off-peak. Transactional row locks still exist; the separate target, overlap lock and bounded SQL timeouts limit contention. Data loaded is strictly limited to the dataset handed over from Stage 2 without re-extracting or re-transforming.

### Sealed handover validation

Each manifest includes its pipeline_run_id, parent extraction run, complete business-date list, source snapshot timestamp, source mode, source reconciliation counts and validation results. Every file has a relative path contained in the run package, exact final row count, column list and SHA-256. Stage 3 verifies all of these before writing. Transformation rejections are upstream audit evidence and are not counted as rows received by loading. A package is immutable; manual review creates a new package linked to the held manifest. Old v1 manifests are rejected and must be regenerated.

## 2. Target Tables and Column Specification

All targets are in the separate local database schema `dw_analytics`. The following specification is verified against the local PostgreSQL catalog. The SQL files in `warehouse/` are the setup contract.

Stage 2 handover columns match their mandatory target names and types. Stage 3 adds source_run_id and source_created_at for lineage on facts. Batch identity lives in load_batches/load_log, linked through the transformation run; it is not a fact-grain key. Calendar attributes do not need SCD Type 2 history. Daily facts use replacement/upsert semantics, while immutable run packages retain prior versions.

### dim_date

Calendar dimension, matched by date_key; full_date is unique.

| Column | PostgreSQL type | Required | Default |
| --- | --- | --- | --- |
| `date_key` | `integer` | Yes | `none` |
| `full_date` | `date` | Yes | `none` |
| `day_of_month` | `smallint` | Yes | `none` |
| `month` | `smallint` | Yes | `none` |
| `month_name` | `text` | Yes | `none` |
| `quarter` | `smallint` | Yes | `none` |
| `year` | `smallint` | Yes | `none` |
| `day_of_week` | `smallint` | Yes | `none` |
| `day_name` | `text` | Yes | `none` |
| `is_weekend` | `boolean` | Yes | `none` |

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
| `sales_summary_key` | `text` | Yes | `none` |
| `date_key` | `integer` | Yes | `none` |
| `total_sales` | `numeric(18,2)` | Yes | `none` |
| `refund_amount` | `numeric(18,2)` | Yes | `none` |
| `net_sales` | `numeric(18,2)` | Yes | `none` |
| `total_orders` | `bigint` | Yes | `none` |
| `source_run_id` | `text` | No | `none` |
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
| `inventory_summary_key` | `text` | Yes | `none` |
| `date_key` | `integer` | Yes | `none` |
| `total_items` | `bigint` | Yes | `none` |
| `low_stock_items` | `bigint` | Yes | `none` |
| `out_of_stock_items` | `bigint` | Yes | `none` |
| `threshold_policy` | `text` | Yes | `none` |
| `snapshot_at` | `timestamp with time zone` | Yes | `none` |
| `source_run_id` | `text` | No | `none` |
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
| `traffic_summary_key` | `text` | Yes | `none` |
| `date_key` | `integer` | Yes | `none` |
| `total_customers` | `bigint` | Yes | `none` |
| `peak_hour` | `time without time zone` | No | `none` |
| `source_run_id` | `text` | No | `none` |
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
| `batch_id` | `text` | Yes | `none` |
| `transformation_run_id` | `text` | Yes | `none` |
| `manifest_sha256` | `text` | Yes | `none` |
| `load_run_id` | `text` | Yes | `none` |
| `source_created_at` | `timestamp with time zone` | Yes | `none` |
| `business_dates` | `date[]` | Yes | `none` |
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
| `load_run_id` | `text` | Yes | `none` |
| `transformation_run_id` | `text` | Yes | `none` |
| `batch_id` | `text` | Yes | `none` |
| `batch_window` | `text` | Yes | `none` |
| `table_name` | `text` | Yes | `none` |
| `load_type` | `text` | Yes | `'INCREMENTAL'::text` |
| `load_start_ts` | `timestamp with time zone` | Yes | `none` |
| `load_end_ts` | `timestamp with time zone` | Yes | `none` |
| `load_duration_sec` | `numeric(12,3)` | Yes | `none` |
| `records_received` | `bigint` | Yes | `none` |
| `records_inserted` | `bigint` | Yes | `none` |
| `records_updated` | `bigint` | Yes | `none` |
| `records_loaded` | `bigint` | Yes | `none` |
| `records_rejected` | `bigint` | Yes | `none` |
| `pre_load_result` | `jsonb` | Yes | `none` |
| `post_load_result` | `jsonb` | Yes | `none` |
| `status` | `text` | Yes | `none` |
| `error_code` | `text` | No | `none` |
| `error_message` | `text` | No | `none` |
| `load_code_version` | `text` | Yes | `none` |
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
| `load_run_id` | `text` | Yes | `none` |
| `table_name` | `text` | Yes | `none` |
| `reason` | `text` | Yes | `none` |
| `payload` | `jsonb` | Yes | `none` |
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
| `run_id` | `text` | Yes | `none` |
| `parent_run_id` | `text` | No | `none` |
| `source_table` | `text` | Yes | `none` |
| `source_record_id` | `text` | No | `none` |
| `rule_id` | `text` | Yes | `none` |
| `error_code` | `text` | Yes | `none` |
| `reason` | `text` | Yes | `none` |
| `payload` | `jsonb` | Yes | `none` |
| `rejected_at` | `timestamp with time zone` | Yes | `now()` |

**Keys and constraints:**

- `PRIMARY KEY (quarantine_id)`
- `UNIQUE (run_id, source_table, rule_id, source_record_id)`

**Indexes:**

- `CREATE INDEX idx_quarantine_run_id ON dw_analytics.transformation_quarantine USING btree (run_id)`
- `CREATE UNIQUE INDEX transformation_quarantine_pkey ON dw_analytics.transformation_quarantine USING btree (quarantine_id)`
- `CREATE UNIQUE INDEX transformation_quarantine_run_id_source_table_rule_id_sourc_key ON dw_analytics.transformation_quarantine USING btree (run_id, source_table, rule_id, source_record_id)`

The earlier transformation_run_log table is retained for legacy compatibility; version-2 transformation lineage and validation are stored in sealed manifests. The current loader writes load_log and the success ledger.

### Calendar contract

The date dimension includes date_key (YYYYMMDD), full_date, day_of_month, month, month_name, quarter, year, day_of_week (Monday=0), day_name and is_weekend. full_date is unique and required. Stage 2 supplies a contiguous range covering the sales/traffic window and actual inventory capture date. Stage 3 validates date_key against full_date and requires every fact date to exist in the handed-over dimension before writing. Invalid or orphan dates are critical failures, with no default/unknown member.

## 3. Load Strategy and Rules

| Table | Load Type | Write Logic | Match Key | On Failure |
| --- | --- | --- | --- | --- |
| dim_date | Incremental | Upsert (ON CONFLICT (date_key) DO UPDATE) | date_key | Roll back batch transaction, stop load execution. |
| fact_sales_summary | Incremental | Upsert (ON CONFLICT (date_key) DO UPDATE) | date_key | Roll back transaction, preserve prior warehouse state. |
| fact_inventory_summary | Incremental | Upsert (ON CONFLICT (date_key) DO UPDATE) | date_key | Roll back transaction, preserve prior warehouse state. |
| fact_traffic_summary | Incremental | Upsert (ON CONFLICT (date_key) DO UPDATE) | date_key | Roll back transaction, preserve prior warehouse state. |

### Duplicate, correction, and backfill rules

The batch identity is the SHA-256 of the sealed Stage 2 manifest. A success-only load_batches ledger is committed atomically with facts and SUCCESS logs. Identical replays are no-ops; changing a published transformation identity is rejected. New approved runs replace matching date_key rows. The source extraction snapshot timestamp prevents an older extraction, even one transformed later, from overwriting newer data. Overlapping loads serialize under a transaction advisory lock. Backfills create fresh Stage 1 packages for the requested dates; no force flag bypasses critical checks.

## 4. Pre-Load Validation Checks

| Check Name | Target | Purpose | Validation Criteria | Severity |
| --- | --- | --- | --- | --- |
| Target Connection | PostgreSQL Data Warehouse | Verify target availability and write permission | Connection established within 10-second timeout; write test query succeeds | Critical |
| Handover Status | Transformation Stage Metadata | Ensure only approved datasets are loaded | manifest.decision = APPROVED, contract_version = 2, and stage = transformation | Critical |
| Schema Compatibility | dw_analytics, Target Tables | Prevent column or type mismatch failures | Target table names, column names, and data types match Stage 2 contract strictly | Critical |
| Duplicate Batch Check | load_batches.batch_id | Prevent duplicate processing of an already completed batch | An identical successful manifest is SKIPPED without replay. A new approved correction may upsert the same dates; older extraction snapshots are rejected. | Critical |
| Storage Capacity Check | Warehouse Storage Volume | Confirm adequate disk space for batch insertion | Configured local warehouse volume has at least 1 GiB free (configurable) | Critical |

Additional critical checks: all files stay inside the immutable package; their SHA-256, columns and actual row counts match the manifest; required values and real date keys are present; date coverage is exact; monetary values are exact Decimal; mock packages target only an explicitly enabled _demo/_test database; and source snapshots cannot replace newer target observations. No cleaning or business transformations occur in these checks.

## 5. Post-Load Validation and Reconciliation

| Check Name | Target | Purpose | Validation Criteria | Severity |
| --- | --- | --- | --- | --- |
| Record Reconciliation | All target fact tables | Verify zero unhandled record loss during loading | records_received = records_loaded + records_rejected | Critical |
| Primary Key Uniqueness | fact_sales_summary, fact_inventory_summary, fact_traffic_summary | Guarantee primary key integrity | Count of duplicate primary keys in target tables = 0 | Critical |
| Referential Integrity | Fact tables (date_key) | Enforce valid foreign key references | 100% of loaded date_key values exist in dim_date.date_key | Critical |
| Financial Metric Reconciliation | fact_sales_summary.total_sales, refund_amount, net_sales | Prevent lost or changed financial values | Each handed-over row and each exact Decimal column sum equals the target for the affected date keys; tolerance 0.00 PHP | Critical |
| Null Constraint Check | Mandatory target columns | Prevent invalid rows reaching publication | Null count in NOT NULL constrained columns = 0 | Critical |

## 6. Load Metadata and Log Specification

| Field | Purpose | Example |
| --- | --- | --- |
| load_run_id | Unique primary key identifying the load process run | Load_20261110_180002_a1b2c3d4e5f6 |
| transformation_run_id | Foreign key linking to the Stage 2 transformation execution | Transform_20261110_180001_a1b2c3d4e5f6 |
| batch_id | Identified target batch identifier | SHA-256 of the approved manifest |
| batch_window | Business date range processed | 2026-11-10 to 2026-11-10 |
| table_name | Target warehouse table receiving data | fact_sales_summary |
| load_type | Method of write operation (FULL or INCREMENTAL) | INCREMENTAL |
| load_start_ts | Execution start timestamp | 2026-11-11T02:00:02+08:00 |
| load_end_ts | Execution completion timestamp | 2026-11-11T02:00:18+08:00 |
| load_duration_sec | Total load duration in seconds | 16 |
| records_received | Total rows handed over from transformation | 1 |
| records_inserted | New rows written to target table | 1 |
| records_updated | Existing rows updated via idempotent overwrite | 0 |
| records_loaded | records_inserted + records_updated, committed rows only | 1 |
| records_rejected | Rows failing target write constraints | 0 |
| pre_load_result | JSON outcomes of individual pre-load checks | {schema: PASS, counts: PASS} |
| post_load_result | JSON row/amount reconciliation and exact received/loaded sums | {fact_sales_summary: PASS} |
| status | Execution status (SUCCESS / FAILED / SKIPPED; MANUAL_REVIEW is held before loading) | SUCCESS |
| error_code | Categorized error code if failed | NULL |
| error_message | Diagnostic reason without credentials | NULL |
| load_code_version | Version control tag of the loading script | 2.0.0 |

### Load accounting contract

Counts refer only to final dimension/fact rows handed to Stage 3, never source rows or transformation rejections. For processed attempts, received = inserted + updated + rejected and loaded = inserted + updated. A rolled-back attempt has loaded/inserted/updated zero and rejected=received. An identical successful replay has status SKIPPED and zero processed counts, linked to the original successful batch. Per-table load_log rows include pre/post validation details, exact financial audit totals, UTC timestamps, duration, code version, error code and message. The separate load_batches ledger contains only committed successful identities. Local failure JSON and retained run packages remain available if the database cannot record its log.

## 7. Log Retention, Rejected Data, and Access

**Retention Policies & Storage Locations**

- Operational Logs: Retained in dw_analytics.load_log for at least 90 days to support daily monitoring and troubleshooting.

- Audit Summaries: High-level batch run summaries are retained for 1 year to fulfill institutional requirements.

- Quarantine Records: Data rows rejected during target loading due to schema or constraint checks are written to dw_analytics.load_quarantine and retained for at least 30 days and longer while needed for manual inspection or recovery.

**Access Permissions & Governance**

- ETL Service Role (etl_warehouse_writer): USAGE plus SELECT, INSERT and UPDATE for upserts, validation and load logs; no DDL or fact DELETE.

- ETL Pipeline Administrator: Full access to manage tables, rerun batches, inspect quarantine tables, and manage log cleanup.

- Reporting / Analytics Users: Read-only access (SELECT) granted strictly to dw_analytics fact and dimension tables. Access to log and quarantine tables is restricted.

Deletion and Backup Protocols

- No automatic deletion of run packages or quarantine occurs. An ETL administrator authorizes archival/deletion only after the minimum retention and all troubleshooting/recovery holds have ended.

- Before major schema changes or backfills, an operator may run tools/local_warehouse.py --backup to create local pg_dump archives. Routine loads rely on atomic rollback and retained input packages; there is no claimed automatic daily backup job.

### Role boundary

The POS connection is always read-only. Warehouse credentials are independent WAREHOUSE_DB_* settings, stored locally in gitignored .env.warehouse.local. The setup owner creates schema and roles. The routine local login inherits etl_warehouse_writer. etl_reporting_reader has SELECT only on dim_date and the three facts, not logs/quarantine. etl_warehouse_maintainer has SELECT/DELETE on log and quarantine tables for approved retention work, not business fact deletion. Public schema access is revoked; runtime never applies DDL.

The completion/audit JSON under data/handover is retained at least one year. Retain referenced input/output packages at least one year, and retained inventory observations as required for historical reporting. The restricted writer cannot delete fact rows or perform retention maintenance.

## 8. Load Acceptance, Rollback, and Completion Rules

| Overall Condition | Acceptance Decision | Pipeline Action |
| --- | --- | --- |
| All pre-load and post-load critical checks PASS; record reconciliation matches 100% | APPROVED | Commit transaction, mark batch complete, make committed warehouse tables available to the reporting role |
| All critical checks PASS, but non-critical warnings are flagged (e.g., minor null rate in optional fields) | MANUAL REVIEW | Hold publication; retain the immutable package and notify the ETL operator through the failure/completion record |
| Any critical check FAILS (e.g., target connection failure, broken foreign key, amount mismatch) | REJECTED | Issue ROLLBACK, halt pipeline, write the failed completion/log record and return a nonzero exit code |

| Validation Summary | Required Result for Approval |
| --- | --- |
| dim_date Synchronization | PASS |
| fact_sales_summary Load | PASS |
| fact_inventory_summary Load | PASS |
| fact_traffic_summary Load | PASS |
| Post-Load Reconciliation & Financial Check | PASS |
| Overall Load Status | APPROVED |

### Atomic publication and durable failure evidence

Dimensions, facts, source lineage, the success ledger and SUCCESS load logs commit in one transaction after all post-load checks pass. No reporting rows are published on MANUAL_REVIEW or REJECTED outcomes. Constraint, schema, financial and referential failures cannot be overridden. A failed transaction rolls back all attempted rows, reports records_loaded=0, and treats its received rows as rejected for that attempt. Failure JSON is persisted outside the transaction even when the database is unavailable; FAILED database logs and exact-value quarantine payloads use a new transaction when the target is reachable. A later retry gets a new load_run_id. An ambiguous connection failure after COMMIT is resolved by the successful-batch ledger on replay.

The ETL operator is responsible for review. Use the explicit transformation --review-reason command to approve noncritical warnings; it records the operator, reason and original manifest hash without altering held files. Critical failures require correction and a new run. Transient connection/conflict failures receive up to three attempts with 2- and 4-second waits by default; persistent/data failures stop. Console output, JSON logs and completion manifests notify operators/downstream automation. SUCCESS or SKIPPED exits 0; held/failed execution exits 1. External email/chat notifications are not configured.

## 9. Common Load Problems and Mitigation

| Problem | Possible Cause | Impact | Mitigation |
| --- | --- | --- | --- |
| Duplicate Rows on Rerun | Pipeline batch rerun without clearing previous target entries | Inflated aggregate sales totals, distorted inventory metrics | Check the success ledger and upsert by unique date_key; identical replays are skipped. |
| Foreign Key Constraint Violation | Fact table records attempting to load before dim_date contains the required date_key | Load process aborts with transactional rollback | Load dimensions first and reject facts whose real YYYYMMDD date_key is missing or invalid; never substitute an unknown date. |
| Partial Data Load after Crash | Database crash or network disconnection midway through batch execution | Target warehouse left in an inconsistent state | Enclose all load statements for the batch inside a single atomic database transaction (BEGIN ... COMMIT). |

| Additional problem | Cause | Impact | Handling |
| --- | --- | --- | --- |
| Altered handover | A file changes after approval | Invalid source of truth | Reject on checksum/count/column mismatch before writing |
| Older snapshot replay | An older extraction is transformed after a newer load | Historical values or inventory could regress | Reject the whole stale batch; re-extract the bounded dates |
| Warehouse unavailable | Local cluster stopped or connection lost | Load cannot complete or commit acknowledgement is ambiguous | Preserve failure JSON; restart the local cluster; retry the sealed package and consult the success ledger |
| Disk/storage threshold | Local warehouse volume below configured minimum | Unsafe or failing writes | Reject before loading; operator restores capacity without deleting held evidence |
