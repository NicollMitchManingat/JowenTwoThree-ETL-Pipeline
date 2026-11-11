# JowenTwoThree ETL Pipeline

Daily batch reporting for the Jowen Two Three POS, inventory and customer-traffic system.

**Read-only Supabase PostgreSQL → immutable bronze packages → validated transformation → approved silver packages → separate local PostgreSQL warehouse.**

## Setup

Use Python 3.11. All local runtime files, database data and credentials stay in gitignored locations inside this ETL project.

~~~powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
Copy-Item .env.example .env
.\.venv\Scripts\python.exe -B tools/local_warehouse.py --download
~~~

The portable Windows setup listens only on 127.0.0.1:55432, initializes jowen_analytics and jowen_analytics_demo, and creates independent credentials in .env.warehouse.local. It does not install a system service. For live reads, set ETL_MOCK_MODE=false and configure the source SUPABASE_DB_* values in .env. Never reuse POS credentials as warehouse credentials.

The warehouse is local for both live reporting and demos. Its default profile is .env.warehouse.local; .env.warehouse and .env.warehouse.admin are ignored by the pipeline. Supabase remains the read-only POS source. Explicit WAREHOUSE_DB_* environment variables override the local profile, but remote warehouse hosts are rejected.

## Run

~~~powershell
# Sample data goes only to the separate demo database.
.\.venv\Scripts\python.exe -B -m pipelines.run --demo --date 2026-11-10

# Live source, read-only; default date is yesterday and lookback is three days.
.\.venv\Scripts\python.exe -B -m pipelines.run

# Explicit inclusive historical date range.
.\.venv\Scripts\python.exe -B -m pipelines.run --start-date 2026-07-15 --end-date 2026-11-10

# Read/validate without writing artifacts or warehouse rows.
.\.venv\Scripts\python.exe -B -m pipelines.run --demo --date 2026-11-10 --dry-run
~~~

Inventory records its actual capture date, even when sales/traffic are backfilled. It preserves exact quantities and per-item thresholds. Reports keep transaction totals, refunds and net revenue separate.

For a complete historical backfill, use the earliest source business date through yesterday in Asia/Manila. The example above covers the source history observed on November 11, 2026. This warehouse stores daily sales, refund, traffic and inventory reporting summaries; retained bronze packages contain the source rows used to build them. Current-day sales/traffic wait until the business day is complete. Start an existing local database with .\.venv\Scripts\python.exe -B tools/local_warehouse.py before running; the daily wrapper starts it automatically.

## Documentation

- [Stage 1: Extraction](docs/extraction/stage-1-data-extraction.md)
- [Stage 2: Transformation](docs/transformation/stage-2-data-transformation.md)
- [Stage 3: Load specification](docs/load/stage-3-data-load.md)
- [Verified local historical backfill](docs/load/local-backfill-2026-11-11.md)
- [Scheduling, review, retries, retention and recovery](docs/extraction/scheduling.md)

The optional daily task registration script is tools/install_daily_task.ps1; registration requires a separate explicit system action. The implemented pipeline is batch only. POS dashboards and real-time synchronization are outside this repository.

## Tests

~~~powershell
.\.venv\Scripts\python.exe -B -m pytest -q -p no:cacheprovider
.\.venv\Scripts\python.exe -B -m ruff check .

# Enable native PostgreSQL fault/replay checks against the prepared demo database.
$env:WAREHOUSE_TEST_DATABASE = 'jowen_analytics_demo'
.\.venv\Scripts\python.exe -B -m pytest -q -p no:cacheprovider
~~~

Native database tests are explicitly skipped when WAREHOUSE_TEST_DATABASE is not configured. They refuse database names without a _demo or _test suffix. Tests cover immutable packages, exact stock/money, review gates, dry runs, rollback after writes, duplicate replay, stale snapshots and one-cent financial corruption.

## Operational boundaries

Only approved version-2 manifests are loadable. Each load publishes dimensions, facts, lineage and its SUCCESS audit in one transaction after reconciliation. Failed batches retain diagnostics; manual-review batches remain unpublished. Identical successful replays are skipped. Logs and retained run packages support recovery.

SQL in warehouse/ is for a separate local reporting database, never the operational POS. Older v1 runner helpers remain for legacy tests/data inspection and cannot feed the current loader. No source database migrations or POS application changes are required.
