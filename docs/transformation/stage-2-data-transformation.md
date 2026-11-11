# Stage 2: Data Transformation

## Input and implementation

Consume only an approved version-2 extraction package. Verify file containment, SHA-256, declared columns and exact row counts before processing. Transform with Python/pandas and Decimal arithmetic. Stage 2 does not reconnect to the POS; refreshing late data is Stage 1's responsibility.

The source date list, capture timestamp, source mode and extraction run ID remain in the output lineage. A package contains the complete bounded window from that extraction; the default orchestrator refreshes three dates before transformation.

## Cleaning and standardization rules

| Rule | Behavior |
| --- | --- |
| Transaction references | Strip surrounding whitespace and normalize to uppercase |
| Inventory names | Strip/title-case text; missing names use UNNAMED ITEM and trigger review |
| Traffic null counts | Replace permitted null number_of_customer values with zero |
| Negative/invalid stock | Quarantine rather than silently repairing it |
| Timestamps | Convert timezone-aware source timestamps to Asia/Manila business dates/hours |
| Money | Parse exact Decimal, reject nonfinite/negative source amounts, round half-up to two decimals, validate NUMERIC(18,2) bounds |
| Stock and thresholds | Retain exact Decimal quantities in each item's recorded unit; never round to integers |
| Invalid traffic timestamps | Keep the row's count under the declared processing date, leave the peak-hour contribution missing, and require review |

Any invalid financial record rejects the batch, regardless of the general rejection-rate threshold. Source types and counts are verified in extraction; value and conversion failures are visible in transformation quarantine.

## Business summaries

| Output | Grain | Calculation |
| --- | --- | --- |
| fact_sales_summary | One row per requested business date | total_sales = paid transaction totals; refund_amount = refunds recorded that date; net_sales = total_sales - refund_amount; total_orders = distinct paid transaction IDs |
| fact_inventory_summary | One row for the actual inventory capture date | Active item count; low stock where 0 < quantity <= that item's threshold; out of stock where quantity = 0 |
| fact_traffic_summary | One row per requested business date | Sum recorded customer_traffic counts; peak hour has the largest hourly count, with the earliest hour breaking ties |
| dim_date | One row per calendar date in the needed range | YYYYMMDD date key and full calendar attributes |

Sales totals are already after POS discounts and before refunds. Fully/partially refunded paid sales stay in transaction totals; refund movements affect their recorded date. Refund-only dates may have negative net_sales. The loader must not recalculate these measures.

Inventory stores threshold_policy=PER_ITEM and snapshot_at. Historical backfills refresh historical sales/refunds/traffic while capturing only a new current inventory observation. Retained prior snapshots are the historical inventory evidence; today's quantity is never assigned to an older date.

## Output columns

- dim_date: date_key, full_date, day_of_month, month, month_name, quarter, year, day_of_week (Monday=0), day_name, is_weekend.
- Sales: sales_summary_key, date_key, total_sales, refund_amount, net_sales, total_orders, loaded_at.
- Inventory: inventory_summary_key, date_key, total_items, low_stock_items, out_of_stock_items, threshold_policy, snapshot_at, loaded_at.
- Traffic: traffic_summary_key, date_key, total_customers, peak_hour, loaded_at.
- Quarantine: source_table, source_record_id, rule_id, error_code, reason, payload.

Summary keys are deterministic text values SALES_YYYYMMDD, INV_YYYYMMDD and TRAFFIC_YYYYMMDD. loaded_at in handed-over summaries is the transformation/snapshot timestamp; actual warehouse publication time is recorded in the load ledger and logs.

## Reconciliation and approval

Before aggregation, account for each source table as read = accepted output + intentionally excluded + rejected. Inactive inventory and unpaid transaction rows are intentional exclusions. After aggregation, fact counts are daily rows, not source record counts.

- Critical conversion, date coverage, financial or reconciliation failure: REJECTED.
- Rejection rate over 5%: REJECTED.
- Noncritical warnings or rejection rate over 1% through 5%: MANUAL_REVIEW.
- Otherwise: APPROVED.

Held and rejected outputs are retained for inspection but are not loadable. Explicit operator review can create a new approved copy of a noncritical held package with a reason and original-manifest hash. It cannot override critical failures or change the transformed values.

## Output, traceability and recovery

Each result is sealed in data/silver/Transform_<timestamp>_<id>/, with Parquet files and a version-2 manifest published last. The manifest includes source lineage, source reconciliation counts, decisions, warnings, dates, snapshot_at and file hashes/counts. A rerun creates a new package rather than changing the files referenced by an earlier approval.

Stage 3 receives the explicit approved manifest path. The separate [load specification](../load/stage-3-data-load.md) defines target columns, validation, transactional loading, accounting and publication. [Scheduling](../extraction/scheduling.md) defines retries, review commands and retention. Logs and quarantine are retained for the documented minimums and are never automatically deleted while needed for recovery.
