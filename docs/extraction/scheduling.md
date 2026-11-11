# Scheduling the Daily Extraction Batch

The extraction batch runs once per day after business operations have closed.
It extracts the previous business day's records, validates them, writes Parquet
partitions under `data/bronze/`, and records run metadata under `data/logs/`.

## Manual run

```powershell
.\.venv\Scripts\python.exe -m pipelines.extraction.run
```

Useful options:

| Option | Purpose |
|--------|---------|
| `--date YYYY-MM-DD` | Override the business date instead of using the previous day |
| `--demo` | Force mock mode with deterministic sample data |
| `--full` | Extract full tables instead of the previous-day window |
| `--dry-run` | Validate without writing Parquet or log files |

## Linux/macOS cron

Run at 02:00 local time every day:

```cron
0 2 * * * cd /path/to/JowenTwoThree-ETL-Pipeline && .venv/bin/python -m pipelines.extraction.run >> data/logs/cron.log 2>&1
```

## Windows Task Scheduler

Create a daily task that runs:

```powershell
C:\path\to\JowenTwoThree-ETL-Pipeline\.venv\Scripts\python.exe -m pipelines.extraction.run
```

Set **Start in** to the repository root so relative paths resolve correctly.

## Retry and safe reruns

- Connection failures are retried with exponential backoff. Configure the
  number of attempts with `ETL_RETRY_ATTEMPTS` and the base delay with
  `ETL_RETRY_BACKOFF_SECONDS`.
- Each Parquet partition is written to a temporary file and atomically renamed,
  so rerunning the same business date replaces the partition instead of
  appending duplicates.
- Every run writes a separate `Run_*.json` log. Failed or partial runs retain
  their error message and error code for troubleshooting.
- Logs older than `ETL_LOG_RETENTION_DAYS` are purged automatically after a
  successful write.
