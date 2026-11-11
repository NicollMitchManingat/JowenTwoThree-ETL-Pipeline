# Stage 2: Data Transformation Documentation

## 1. Transformation Input and Processing Specification

### Input Source

- Name of Extraction Output / Staging Area: Staged records from Stage 1 extraction run (transactions, inventory, and customer_traffic).
- Format and Location: Structured rows queried from the Supabase PostgreSQL operational database, handed over in-memory/staging context to the transformation script.
- Extraction Run ID & Batch Window: Linked via extraction run ID Run_20261013_001 covering the previous business day window (2026-10-12 00:00:00 to 23:59:59).

### Batch Processing Method

- Transformation Tool/Technology: SQL for data transformation and aggregation, orchestrated via Python scripts.
- Batch Frequency & Schedule: Daily batch processing executed right after business operations conclude (e.g., daily at 02:00).
- Batch Size / Window: All operational records extracted from the previous 24-hour business day window.
- Failure Handling & Idempotency: Reruns are fully idempotent. Using batch date keys and upsert/delete-and-insert logic by batch ID ensures that running the same transformation batch multiple times yields identical results without creating duplicate rows. Failed batches trigger a pipeline hold, allowing operators to fix issues and safely retry the execution.

### Transformation Scope

- Tables Transformed: Staged records corresponding to transactions, inventory, and customer_traffic.
- Fields Processed: Cleaned, standardized, derived, and aggregated across fields such as created_at, transaction_number, total, name, stock_quantity, and number_of_customer.

### Dependencies and Assumptions

- Processing Order: Dimension tables (dim_date) are processed and populated before fact tables (fact_sales_summary, fact_inventory_summary, fact_traffic_summary) to guarantee referential integrity.
- Assumptions: Late-arriving data streams are processed within a designated lookback retry window; lookup values and source columns adhere to the documented schema contract.

## 2. Data Profiling and Issue Identification

| Table / Source | Column Affected | Issue Type | Example | Affected Records (%) | Planned Treatment |
| --- | --- | --- | --- | --- | --- |
| transactions | transaction_number | Leading/trailing spaces | TRX-1002 | 1.5% | Trim whitespace (Rule C-01) |
| inventory | name | Mixed upper/lower case | Iced COFFEE | 4.0% | Convert to standardized title case (Rule C-02) |
| inventory | stock_quantity | Negative values | -5.00 | 0.2% | Reject to quarantine table (Rule C-04) |
| customer_traffic | number_of_customer | Missing / Null values | NULL | 0.5% | Set default value to 0 or flag/reject (Rule C-03) |

## 3. Data Cleaning Rules

| Rule ID | Target | Condition | Action | Justification |
| --- | --- | --- | --- | --- |
| C-01 (Whitespace Trimming) | transactions.<br>transaction_number | Presence of leading or trailing whitespace characters. | Trim leading and trailing spaces. | Prevents false mismatches during joins and aggregation lookups. |
| C-02 (Case Normalization) | inventory.name | Inconsistent capitalization across item descriptions. | Convert string values to standardized title case. | Ensures consistent grouping and presentation in inventory summaries. |
| C-03 (Null Handling for Traffic) | customer_traffic.<br>number_of_customer | Missing or null customer entry counts. | Replace null counts with a default value of 0 or quarantine if critical. | Protects downstream aggregate calculations from returning NULL results. |
| C-04 (Negative Stock Rejection) | inventory.stock_quantity | Stock quantity value less than 0. | Reject record and route to the transformation quarantine table. | Negative physical inventory quantities represent invalid operational data. |

## 4. Standardization and Data Type Rules

### 4.1 Data Types

The extracted raw fields from the source tables (transactions, inventory, and customer_traffic) are cast into standardized PostgreSQL target data types for storage in dimension and fact tables:

- Date and Time Fields: Timestamps extracted as timestamptz strings (e.g., created_at in transactions and customer_traffic) are explicitly converted into target DATE, INTEGER (for date keys/components), or TIME types.
- Monetary Values: Transaction amounts and unit prices recorded as general numeric/text values are explicitly cast to NUMERIC(10,2) to prevent floating-point rounding errors during financial reporting.
- Counts and Quantities: Stock counts and customer entry totals are cast to non-negative INTEGER types.
- Text Descriptions: String fields (such as item names and transaction numbers) are cast to standardized text types with leading/trailing whitespaces stripped.

### 4.2 Formats

- Date & Time Formats:
  - All calendar dates are formatted strictly using the ISO 8601 standard (YYYY-MM-DD).
  - All timestamps are converted to UTC or local business timezone before date extraction.
  - Peak hours are standardized into 24-hour time format (HH:MM:SS).
- Numeric Precision and Rounding:
  - Monetary values (e.g., total) are rounded to 2 decimal places using standard half-up rounding (NUMERIC(10,2)).
  - Counts (total_orders, total_items, total_customers) are represented strictly as whole integers.
- Text Casing Rules:
  - Item names (inventory.name) are converted to Title Case (e.g., "iced coffee" → "Iced Coffee").
  - Transaction identifier codes are standardized to UPPERCASE (e.g., "trx-101" → "TRX-101").

### 4.3 Reference Values and Codes

- Currency Code: All financial figures are standardized in Philippine Peso (PHP / ₱). Currency symbols included in raw input strings are stripped during transformation, leaving purely numeric decimal values.
- Unit of Measure: Inventory stock quantities are measured in single units (pcs or count).
- Status / Category Normalization: Operational status codes or inventory alerts are mapped to standard uppercase descriptors (e.g., LOW_STOCK, OUT_OF_STOCK, NORMAL).

### 4.4 Handling of Conversion Failures

When a field fails to convert into its expected standard data type or format:

1. Critical Fields (Keys & Financial Measures): If a date key, transaction total, or primary key cannot be parsed or converted, the row is rejected, logged with an error code (e.g., ERR_TYPE_CONVERSION), and stored in the quarantine table. The row is not passed to the data warehouse.
2. Non-Critical Descriptive Fields: If a non-essential text field or status fails conversion, it is replaced with a standard fallback value such as 'UNKNOWN' or 'UNSPECIFIED'.

| Rule ID | Column | Source Format | Standard Format | On Failure |
| --- | --- | --- | --- | --- |
| S-01 | transactions.created_at | TIMESTAMPTZ string / MM/DD/YYYY | YYYY-MM-DD (DATE / dim_date.full_date) | Reject record and log |
| S-02 | transactions.total | String or general NUMERIC with symbol | NUMERIC(10,2) | Reject record and log |
| S-03 | inventory.name | Inconsistent upper/lower text | Title Case string | Set to 'UNNAMED ITEM' |
| S-04 | inventory.stock_quantity | String / Float representation | INTEGER | Reject record to quarantine |
| S-05 | customer_traffic.created_at | TIMESTAMPTZ string | HH:MM:SS (TIME / peak_hour) | Set peak hour to NULL and log warning |

## 5. Business Transformation Rules

| Rule ID | Input Columns | Logic | Output Column | Business Reason |
| --- | --- | --- | --- | --- |
| B-01 | total | SUM of transaction totals per day | total_sales | Computes daily revenue performance |
| B-02 | id (transactions) | COUNT of order records per day | total_orders | Tracks daily order volume |
| B-03 | stock_quantity | Count items where stock <= low_stock_threshold | low_stock_items | Identifies inventory replenishment needs |
| B-04 | number_of_customer, created_at | MAX / GROUP BY hour evaluation | peak_hour | Determines peak customer traffic windows |

## 6. Transformed Data Contract and Naming Convention

| Source Column | Target Column | Source Type | Target Type | Transformation Applied |
| --- | --- | --- | --- | --- |
| transaction_number | transaction_number | TEXT | TEXT | Trim whitespace (C-01) |
| created_at | full_date (dim_date) | TIMESTAMPTZ | DATE | Extracted and converted to UTC Date (S-01) |
| total | total_sales | NUMERIC | DECIMAL(10,2) | Aggregated daily sum (B-01) |

## 7. Transformation Validation and Data Quality Checks

| Check Name | Target | Purpose | Validation Criteria | Severity |
| --- | --- | --- | --- | --- |
| Record Reconciliation | All output fact tables | Detect lost or unaccounted records | input_count = output_count + rejected_count | Critical |
| Key Uniqueness | fact_sales_summary.sales_summary_key | Prevent primary key duplication | Duplicate count = 0 | Critical |
| Referential Integrity | Fact tables (date_key) | Prevent orphaned fact records | 100% match in dim_date.date_key | Critical |
| Business Rule Check | fact_sales_summary.total_sales | Ensure financial validity | total_sales >= 0.00 | Critical |

## 8. Transformation Metadata and Log Specification

| Field Name | Purpose | Example Value / Format |
| --- | --- | --- |
| run_id | Unique identifier of the current pipeline execution | RUN_20261013_0200 |
| parent_run_id | Linked extraction run ID from Stage 1 | Run_20261013_001 |
| batch_window | Date range or period processed by the batch | 2026-10-12 to 2026-10-13 |
| table_name | Name of the table undergoing transformation | fact_sales_summary |
| records_read | Total rows received from extraction staging | 1,250 |
| records_output | Total rows successfully transformed and loaded | 1,245 |
| records_rejected | Rows filtered out or routed to quarantine | 5 |
| status | Execution result state | SUCCESS / WARNING / FAILED |

## 9. Log Retention, Rejected Data, and Access

- Log Retention Period: Retained for a minimum of 90 days for daily operational monitoring, with audit summaries kept for 1 year.
- Storage Location: Centralized ETL log tables within the PostgreSQL data warehouse environment.
- Quarantine Management: Rejected rows are held in a dedicated transformation quarantine table complete with batch IDs and rule violation tags.
- Access Permissions: Strictly restricted to authorized ETL database administrators and maintainers to preserve log integrity.

## 10. Transformation Acceptance and Handover Rules

### Rejection-Rate Threshold and Justification

- Threshold Value: 1.0% of total records processed within the daily batch window.
- Justification: A 1.0% threshold balances data quality with operational continuity for Jowen’s Kitchen & Cafe. It accommodates minor input irregularities (such as occasional formatting anomalies or whitespace errors) without stalling daily analytics generation, while safeguarding data warehouse integrity against widespread corruption or systemic extraction faults.

### Required Decision

- Automatic Approval
  - Validation Conditions: All critical transformation checks (record reconciliation, key uniqueness, and referential integrity) pass completely, and the total rejection rate remains at or below the 1.0% threshold.
  - Pipeline Action: Automatically package and pass the transformed datasets to the Load Stage.
  - Decision Type: Automatic.
- Manual Review
  - Validation Conditions: Non-critical warnings are detected (e.g., minor non-fatal formatting alerts) or the rejection rate breaches the 1.0% threshold but stays under 5.0%.
  - Pipeline Action: Hold the batch in a staging queue and flag it for manual evaluation.
  - Decision Type: Manual.
  - Responsible Person / Role: ETL Pipeline Administrator / Lead Data Analyst.
- Rejection or Failure
  - Validation Conditions: A critical validation check fails (e.g., broken referential integrity with dim_date, unhandled schema drift, or record reconciliation mismatch) or rejection rates exceed 5.0%.
  - Pipeline Action: Stop the pipeline execution immediately, route affected/invalid records to the transformation quarantine table, and halt loading. Temporary connection or runtime issues may trigger an automatic retry, while persistent logic or data errors require manual intervention.
  - Decision Type: Automatic / Manual.
  - Responsible Person / Role: ETL Pipeline Administrator.
- Handover
  - Handover Package: Consists of cleaned fact table rows (fact_sales_summary, fact_inventory_summary, fact_traffic_summary), linked dimension keys (dim_date), execution metadata (run_id, parent_run_id), and comprehensive validation logs.

### Decision Rules

| Overall Condition | Acceptance Decision | Pipeline Action |
| --- | --- | --- |
| All tables pass critical checks and rejection rate ≤1.0% | APPROVED | Proceed to Load Stage |
| One or more non-critical warnings or rejection rate between 1.0% and 5.0% | MANUAL REVIEW | Hold batch and flag for inspection |
| Critical validation failure or rejection rate >5.0% | REJECTED | Stop pipeline, quarantine records, alert admin |

### Validation Summary

| Validation Summary | Result |
| --- | --- |
| transactions transformation (fact_sales_summary) | PASS |
| inventory transformation (fact_inventory_summary) | WARNING |
| customer_traffic transformation (fact_traffic_summary) | PASS |
| dim_date synchronization & integrity | PASS |
| Overall Transformation Status | MANUAL REVIEW |

## 11. Common Transformation Problems and Mitigation

| Problem | Possible Cause | Impact | Mitigation |
| --- | --- | --- | --- |
| Duplicate Records on Rerun | Pipeline rerun executed without idempotent configuration | Inflated sales totals and metric distortion | Implement upsert logic and date-based partition clearing by batch ID |
| Late-Arriving Transactions | Point-of-sale transactions posted after batch window closure | Missing revenue metrics for specific dates | Utilize a rolling 3-day lookback window during daily transformations |
| Unexpected Schema Drift | Source database column renamed without notice | Transformation query failure | Execute schema validation checks prior to running transformation scripts |
