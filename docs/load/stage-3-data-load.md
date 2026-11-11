# Stage 3: Data Load Documentation

## 1. Transformation Input and Processing Specification

**Load Input**

- Name and Location of Transformed Output: Staged cleaned data and transformed aggregate datasets produced in Stage 2 Data Transformation, held in memory and temporary staging tables (staged_dim_date, staged_fact_sales_summary, staged_fact_inventory_summary, staged_fact_traffic_summary) within PostgreSQL.

- Transformation Metadata Linkage: Linked directly to the Stage 2 transformation execution output via Transformation Run ID TRN_20261013_0200 (derived from extraction Run_20261013_001), processing the previous business day window (2026-10-12 00:00:00 to 2026-10-12 23:59:59).

**Target System**

- Target System Name & Purpose: Jowen Two Three Analytics Data Warehouse, dedicated to storing historical summaries of sales, inventory, and customer traffic for daily, weekly, and monthly business performance reporting without impacting operational performance.

- Database Management System (DBMS): PostgreSQL DBMS (hosted separately from the operational Supabase PostgreSQL DB).

- Target Schema: dw_analytics (data warehouse reporting schema).

**Load Method**

- Tooling / Technology: Python (psycopg2/ SQLAlchemy batch loading scripts) executing SQL batch insertion and upsert statements.

- Batch Frequency & Schedule: Daily batch schedule executed at 04:00 AM, immediately following the completion and verification of the Stage 2 data transformation.

- Write Logic: Idempotent transactional batch insert and upsert logic. Existing aggregate summary rows for the specific date key are deleted or updated via primary key matching before inserting new batch records.

**Load Scope**

- Load Order: Dimension tables (dim_date) are loaded/synchronized first, followed by child fact tables (fact_sales_summary, fact_inventory_summary, fact_traffic_summary) to guarantee referential integrity.

- Expected Volume & Execution Window: Approximately 1 to 3 daily summary records per batch run across fact tables. The total load execution window is scheduled to complete within 3 minutes.

**Limitations and Assumptions**

- Write Access & Role: The load process operates under a restricted database role (etl_warehouse_writer) with SELECT, INSERT, UPDATE and DELETE privileges on the dw_analytics schema. Administrative privileges (e.g., DROP TABLE, schema alterations) are strictly prohibited.

- Operational Boundary: Loading occurs off-peak (04:00 AM) to eliminate target warehouse table locking during operational hours. Data loaded is strictly limited to the dataset handed over from Stage 2 without re-extracting or re-transforming.

## 2. Target Tables and Column Specification

| Table | Type | Primary Key | Foreign Keys | Key Constraints | Purpose |
| --- | --- | --- | --- | --- | --- |
| dim_date | Dimension | date_key | None | full_date NOT NULL, UNIQUE | Calendar date dimension for time-series aggregation. |
| fact_sales_summary | Fact | sales_summary_key | date_key→dim_date.date_key | total_orders≥0; total_sales≥0.00 | Daily aggregated sales metrics and transaction totals. |
| fact_inventory_summary | Fact | inventory_summary_key | date_key→dim_date.date_key | total_items≥0; low_stock_items≥0; out_of_stock_items≥0 | Daily stock level summary and replenishment alerts. |
| fact_traffic_summary | Fact | traffic_summary_key | date_key→dim_date.date_key | total_customers≥0 | Daily total customer count and peak traffic hour. |

## 3. Load Strategy and Rules

| Table | Load Type | Write Logic | Match Key | On Failure |
| --- | --- | --- | --- | --- |
| dim_date | Incremental | Upsert (ON CONFLICT (full_date) DO NOTHING) | full_date | Roll back batch transaction, stop load execution. |
| fact_sales_summary | Incremental | Delete-and-Insert by date_key | full_date | Roll back transaction, preserve prior warehouse state. |
| fact_inventory_summary | Incremental | Delete-and-Insert by date_key | date_key | Roll back transaction, preserve prior warehouse state. |
| fact_traffic_summary | Incremental | Delete-and-Insert by date_key | date_key | Roll back transaction, preserve prior warehouse state. |

## 4. Standardization and Data Type Rules

| Check Name | Target | Purpose | Validation Criteria | Severity |
| --- | --- | --- | --- | --- |
| Target Connection | PostgreSQL Data Warehouse | Verify target availability and write permission | Connection established within 10-second timeout; write test query succeeds | Critical |
| Handover Status | Transformation Stage Metadata | Ensure only approved datasets are loaded | Stage 2 execution status = APPROVED | Critical |
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
| Null Constraint Check | Mandatory target columns | Ensure no unexpected null values exist in required fields | Null count in NOT NULL constrained columns = 0 | Non-critical |

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
| records_rejected | Rows failing target write constraints | 0 |
| pre_load_result | Outcome of pre-load checks (PASS / FAIL) | PASS |
| post_load_result | Outcome of post-load reconciliation checks (PASS / FAIL) | PASS |
| status | Execution status (SUCCESS / FAILED / WARNING) | SUCCESS |
| error_code | Categorized error code if failed | NULL |
| load_code_version | Version control tag of the loading script | v1.2.0 |

## 7. Log Retention, Rejected Data, and Access

**Retention Policies & Storage Locations**

- Operational Logs: Retained in dw_analytics.load_log for 90 days to support daily monitoring and troubleshooting.

- Audit Summaries: High-level batch run summaries are retained for 1 year to fulfill institutional requirements.

- Quarantine Records: Data rows rejected during target loading due to schema or constraint checks are written to dw_analytics.load_quarantine and retained for 30 days for manual inspection.

**Access Permissions & Governance**

- ETL Service Role (etl_warehouse_writer): Write-only access to write data rows and append logs.

- ETL Pipeline Administrator: Full access to manage tables, rerun batches, inspect quarantine tables, and manage log cleanup.

- Reporting / Analytics Users: Read-only access (SELECT) granted strictly to dw_analytics fact and dimension tables. Access to log and quarantine tables is restricted.

Deletion and Backup Protocols

- Automatic purge routines delete logs and quarantine records only after their designated retention period has expired.

- Daily automated snapshot backups of the PostgreSQL data warehouse are performed prior to initiating the scheduled 04:00 AM batch load.

## 8. Load Acceptance, Rollback, and Completion Rules

| Overall Condition | Acceptance Decision | Pipeline Action |
| --- | --- | --- |
| All pre-load and post-load critical checks PASS; record reconciliation matches 100% | APPROVED | Commit transaction, mark batch complete, publish warehouse data to reporting views |
| All critical checks PASS, but non-critical warnings are flagged (e.g., minor null rate in optional fields) | MANUAL REVIEW | Commit data but flag batch for inspection; notify ETL administrator |
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
| Foreign Key Constraint Violation | Fact table records attempting to load before dim_date contains the required date_key | Load process aborts with transactional rollback | Strictly enforce load sequence (dimensions before facts); include default "Unknown Date" surrogate key fallback. |
| Partial Data Load after Crash | Database crash or network disconnection midway through batch execution | Target warehouse left in an inconsistent state | Enclose all load statements for the batch inside a single atomic database transaction (BEGIN ... COMMIT). |
### Duplicate, correction, and backfill rules (issue #31)

The batch identity is the SHA-256 of the sealed Stage 2 manifest. A success-only load_batches ledger is committed atomically with facts and SUCCESS logs. Identical replays are no-ops; changing a published transformation identity is rejected. New approved runs replace matching date_key rows. The source extraction snapshot timestamp prevents an older extraction, even one transformed later, from overwriting newer data. Overlapping loads serialize under a transaction advisory lock. Backfills create fresh Stage 1 packages for the requested dates; no force flag bypasses critical checks.
