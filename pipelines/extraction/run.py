"""Daily extraction runner with retry and safe reruns."""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import psycopg
from dotenv import load_dotenv

from pipelines.config import AppConfig, ConfigError, load_config
from pipelines.db import get_source_connection, with_retry
from pipelines.logging_setup import configure_logging

from .extract import extract_inventory, extract_sales, extract_traffic
from .metadata import (
    TableRunMetadata,
    new_run_id,
    purge_old_logs,
    write_run_log,
)
from .parquet_io import write_parquet
from .validate import validate_extraction
from .windows import business_day_window, previous_business_day


@dataclass
class RunResult:
    run_id: str
    business_date: date
    status: str
    validation_passed: bool
    parquet_paths: dict[str, Path]
    log_path: Path | None


def resolve_business_date(
    value: str | None,
    config: AppConfig,
    now: datetime | None = None,
) -> date:
    """Resolve an explicit date or fall back to the previous business day."""

    if value:
        return date.fromisoformat(value)
    reference = now or datetime.now(UTC)
    return previous_business_day(reference, config.etl.timezone)


def _window_label(business_date: date, config: AppConfig, full: bool) -> str:
    if full:
        return "full extraction"
    start, end = business_day_window(business_date, config.etl.timezone)
    return f"{start.isoformat()} to {end.isoformat()}"


def _open_connection(config: AppConfig) -> psycopg.Connection:
    return with_retry(
        lambda: get_source_connection(config),
        attempts=config.etl.retry_attempts,
        backoff_seconds=config.etl.retry_backoff_seconds,
    )


def run_extraction(
    business_date: date,
    config: AppConfig,
    full: bool = False,
    dry_run: bool = False,
) -> RunResult:
    """Run the full extraction batch for a business date."""

    logger = configure_logging(config.etl.log_level)
    run_id = new_run_id()
    frames: dict[str, pd.DataFrame] = {}
    parquet_paths: dict[str, Path] = {}
    records: dict[str, dict[str, object]] = {}
    errors: list[str] = []

    connection: psycopg.Connection | None = None
    owns_connection = False
    if not config.etl.mock_mode:
        connection = _open_connection(config)
        owns_connection = True

    try:
        extractors = [
            (
                "transactions",
                lambda: extract_sales(
                    business_date, config, connection, apply_window=not full
                ),
            ),
            ("inventory", lambda: extract_inventory(config, connection)),
            (
                "customer_traffic",
                lambda: extract_traffic(
                    business_date, config, connection, apply_window=not full
                ),
            ),
        ]

        for table, extractor in extractors:
            started = datetime.now(UTC)
            status = "SUCCESS"
            frame = pd.DataFrame()
            try:
                frame = extractor()
                if not dry_run:
                    parquet_paths[table] = write_parquet(
                        frame, table, business_date, config.etl.staging_dir
                    )
                logger.info("Extracted %s records from %s", len(frame), table)
            except Exception as exc:  # noqa: BLE001 - recorded as run metadata
                status = "FAILED"
                errors.append(f"{table}: {exc}")
                logger.error("Extraction failed for %s: %s", table, exc)
            finished = datetime.now(UTC)
            frames[table] = frame
            records[table] = {
                "started": started,
                "finished": finished,
                "status": status,
            }

        report = validate_extraction(
            frames,
            business_date,
            config,
            connection=connection,
            errors=errors,
        )
    finally:
        if owns_connection and connection is not None:
            connection.close()

    validation_label = "PASS" if report.passed else "FAIL"
    entries = [
        _build_metadata(
            run_id,
            table,
            records[table],
            len(frames[table]),
            config,
            business_date,
            full,
            validation_label,
            errors,
        )
        for table in records
    ]

    failed = [table for table, record in records.items() if record["status"] == "FAILED"]
    succeeded = [table for table, record in records.items() if record["status"] == "SUCCESS"]
    if failed and succeeded:
        status = "PARTIAL"
    elif failed or not report.passed:
        status = "FAILED"
    else:
        status = "SUCCESS"

    log_path: Path | None = None
    if not dry_run:
        log_path = write_run_log(entries, config.etl.log_dir)
        purge_old_logs(config.etl.log_dir, config.etl.log_retention_days)

    return RunResult(
        run_id=run_id,
        business_date=business_date,
        status=status,
        validation_passed=report.passed,
        parquet_paths=parquet_paths,
        log_path=log_path,
    )


def _build_metadata(
    run_id: str,
    table: str,
    record: dict[str, object],
    record_count: int,
    config: AppConfig,
    business_date: date,
    full: bool,
    validation_label: str,
    errors: list[str],
) -> TableRunMetadata:
    started = record["started"]
    finished = record["finished"]
    status = str(record["status"])
    error_message = None
    error_code = None
    if status == "FAILED":
        matching = [error for error in errors if error.startswith(f"{table}: ")]
        error_message = matching[0] if matching else "extraction failed"
        error_code = "EXTRACTION_FAILED"

    return TableRunMetadata(
        pipeline_run_id=run_id,
        table_name=table,
        extraction_start_timestamp=started.isoformat(),
        extraction_end_timestamp=finished.isoformat(),
        extraction_status=status,
        records_read=record_count,
        records_extracted=record_count,
        records_rejected=0,
        extraction_window=_window_label(business_date, config, full),
        validation_results=validation_label,
        error_message=error_message,
        error_code=error_code,
        extraction_duration_seconds=round((finished - started).total_seconds(), 3),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the Jowen Two Three extraction batch.")
    parser.add_argument("--date", help="business date in YYYY-MM-DD format")
    parser.add_argument("--demo", action="store_true", help="force mock mode")
    parser.add_argument("--full", action="store_true", help="extract full tables")
    parser.add_argument("--dry-run", action="store_true", help="skip Parquet and log writes")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    load_dotenv()

    env = dict(os.environ)
    if args.demo:
        env["ETL_MOCK_MODE"] = "true"

    try:
        config = load_config(env)
    except ConfigError as exc:
        print(f"Configuration error: {exc}")
        return 2

    business_date = resolve_business_date(args.date, config)
    result = run_extraction(
        business_date,
        config,
        full=args.full,
        dry_run=args.dry_run,
    )

    print(
        f"{result.run_id} {result.business_date.isoformat()} "
        f"status={result.status} validation={'PASS' if result.validation_passed else 'FAIL'}"
    )
    for table, path in result.parquet_paths.items():
        print(f" - {table}: {path}")
    if result.log_path is not None:
        print(f" log: {result.log_path}")

    return 0 if result.status == "SUCCESS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
