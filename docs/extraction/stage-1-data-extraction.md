# Stage 1: Data Extraction

## Source and boundaries

The Jowen Two Three POS operational database is Supabase PostgreSQL. Stage 1 performs only SELECT and session/transaction configuration; it never changes POS schema, permissions or business records. Source read-only mode is mandatory even if a legacy environment flag says otherwise.

The supported entry points are the full batch command and the version-2 extraction CLI described in [Scheduling](scheduling.md). Version-1 in-memory runner helpers are retained for legacy tests/data inspection and cannot feed the current loader.

## Source tables and columns

| Table | Source column | Extracted column | PostgreSQL source type |
| --- | --- | --- | --- |
| transactions | id | transaction_id | uuid |
| transactions | created_at | created_at | timestamptz |
| transactions | transaction_number | transaction_number | text |
| transactions | total | total | numeric |
| transactions | payment_status | payment_status | text |
| refunds | id | refund_id | uuid |
| refunds | transaction_id | transaction_id | uuid |
| refunds | created_at | created_at | timestamptz |
| refunds | refund_amount | refund_amount | numeric |
| inventory | id | item_id | uuid |
| inventory | name | name | text |
| inventory | stock_quantity | stock_quantity | unconstrained numeric |
| inventory | low_stock_threshold | low_stock_threshold | unconstrained numeric |
| inventory | unit | unit | text |
| inventory | is_active | is_active | boolean |
| customer_traffic | id | entry_id | bigint |
| customer_traffic | created_at | created_at | timestamptz |
| customer_traffic | number_of_customer | number_of_customer | bigint, nullable |

Customer traffic counts are the recorded entries from customer_traffic. The pipeline does not add transaction headcounts, which would change the selected traffic measure. Unpaid saved_orders are not extracted as sales. Refund records are extracted by their own timestamps, even when the associated sale is older.

## Window and consistency

- Business timezone: Asia/Manila.
- Completed dates use inclusive midnight start and exclusive next-midnight end.
- Default orchestration freshly reads the last three completed dates. An explicit date range performs a bounded backfill.
- Sales, refunds and traffic share the requested date window.
- Inventory is one current observation, captured once per extraction, not reconstructed for each historical sales date.
- A repeatable-read, read-only transaction covers schema checks, source counts and all fetched rows. The source transaction timestamp identifies the inventory observation.

## Validation

Verify table names and exact PostgreSQL underlying types before extraction. For every table, compare the extracted row count with COUNT(*) using the same window and transaction snapshot. Require non-null, unique extracted identifiers. Empty business dates are valid. Required-column/value cleaning and business classification belong to Stage 2.

## Immutable output and handover

Each run creates data/bronze/Run_<timestamp>_<id>/ with one Parquet file per source table and a manifest.json published last. The manifest records contract_version=2, stage=extraction, decision=APPROVED, source_mode, dates, snapshot_at, counts, column lists and per-file SHA-256 hashes.

Bronze numeric amounts, stock quantities and thresholds use lossless decimal text. This preserves PostgreSQL NUMERIC values beyond Arrow's decimal precision/scale limits. It is a transport representation; Stage 2 parses exact Decimal values. No float conversion or stock rounding occurs.

A new extraction never overwrites an earlier run. Failed captures have no approved manifest. Dry runs perform reads and validation without publishing files.

## Failures, logs and acceptance

Only completed, validated packages receive APPROVED handovers. The orchestration command records the failing stage and final error, including failures opening the source connection. Transient connection/transaction failures receive bounded retries; deterministic schema/data failures stop. See scheduling documentation for retry, retention and recovery rules.
