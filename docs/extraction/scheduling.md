# Daily batch operation

All business windows use Asia/Manila. Run the full pipeline daily at 02:00 after the previous business day closes. Stages are dependency-driven: extraction completes before transformation, and only approved transformation packages reach loading. Stage 3 never queries the POS.

## Setup and manual commands

Use Python 3.11 and the repository requirements. For the Windows-local target:

~~~powershell
.\.venv\Scripts\python.exe -B tools/local_warehouse.py --download
.\.venv\Scripts\python.exe -B -m pipelines.run --demo --date 2026-11-10
.\.venv\Scripts\python.exe -B -m pipelines.run --date 2026-11-10
~~~

The portable cluster stays inside ignored data/warehouse, listens only on 127.0.0.1:55432, and uses independent generated warehouse credentials. Demo data goes only to jowen_analytics_demo; live reporting uses jowen_analytics. No system service is installed. Run tools/local_warehouse.py again to start an existing cluster; --stop stops only that cluster. --backup writes local pg_dump archives before major backfills or schema work.

## Windows schedule

tools/run_daily.ps1 starts the local warehouse and runs the complete pipeline. After verifying it, explicitly run tools/install_daily_task.ps1 to register the optional 02:00 Windows task. Registration is a separate system action, not part of editing this repository. The host must use UTC+08:00 Philippine time. This interactive task needs the user logged in and the machine available; missed runs start when available. Multiple scheduled instances are ignored, and the pipeline also holds an OS file lock. Do not treat a powered-off workstation as an always-on server.

## Bounded refresh, retry, and backfill

Default processing freshly extracts the previous three completed business dates, not old bronze files. --date selects the last date; --lookback-days changes the number of refreshed dates. An explicit inclusive --start-date/--end-date pair selects a backfill without adding lookback days. Future/current incomplete dates and unbounded --full extraction are not supported.

~~~powershell
.\.venv\Scripts\python.exe -B -m pipelines.run --start-date 2026-11-01 --end-date 2026-11-10
.\.venv\Scripts\python.exe -B -m pipelines.run --demo --date 2026-11-10 --dry-run
~~~

Transient source/target connection failures and retryable transaction conflicts get at most three attempts with 2- and 4-second waits by default. Invalid data, schemas, permissions, checksums and stale snapshots stop immediately. Source failure attempts do not write partial approved packages. Every final pipeline outcome has an operational JSON log and a separate completion/audit JSON. Exit codes are 0 for success/skip/dry-run, 1 for held/failed execution, and 2 for invalid command configuration. Console output and completion manifests are the notification interface; no external messages are sent automatically.

## Stage commands and explicit review

~~~powershell
.\.venv\Scripts\python.exe -B -m pipelines.extraction.run --date 2026-11-10
.\.venv\Scripts\python.exe -B -m pipelines.transformation.run --manifest <bronze-run-manifest>
.\.venv\Scripts\python.exe -B -m pipelines.load.run --manifest <approved-silver-manifest>
.\.venv\Scripts\python.exe -B -m pipelines.transformation.run --manifest <held-silver-manifest> --review-reason 'Verified noncritical inventory naming warning'
~~~

Review creates a new approved package containing the same transformed values and an operator/reason audit trail. It cannot approve REJECTED batches or bypass critical checks. Rerun loading with the newly approved manifest. Only the version-2 commands above are supported for production; legacy in-memory runner helpers and version-1 manifests are retained for older test/data inspection and are not accepted by the loader.

## Retention and recovery

Retain operational logs at least 90 days, completion/audit records and input/output packages at least one year, and quarantine at least 30 days. Keep held runs and anything needed for troubleshooting longer. No automatic package or quarantine deletion occurs. An ETL administrator reviews release/retention before archive or deletion; the maintenance role has log/quarantine DELETE privileges and the routine writer has none. Preserve retained inventory observations because a fresh historical extraction cannot recreate past inventory balances.
