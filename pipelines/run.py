"""Bounded batch orchestration. Stage 3 never queries the POS."""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg
from dotenv import dotenv_values

from pipelines.artifacts import new_id, write_json
from pipelines.config import ConfigError, load_config
from pipelines.db import SourceConnectionError
from pipelines.extraction.batch import (
    dates_between,
    extract_batch,
    stage_extraction,
)
from pipelines.load.runner import load_batch
from pipelines.load.target import warehouse_config
from pipelines.transformation.batch import transform_batch

TRANSIENT_CODES = {"OperationalError", "SerializationFailure", "DeadlockDetected", "LockNotAvailable"}


def configuration(demo: bool = False):
    env = {**dotenv_values(".env"), **os.environ}
    if demo:
        env["ETL_MOCK_MODE"] = "true"
    return load_config(env)


def add_window_arguments(parser):
    parser.add_argument("--date", help="last completed business date; defaults to yesterday in Manila")
    parser.add_argument("--start-date", help="inclusive start for explicit backfill")
    parser.add_argument("--end-date", help="inclusive end for explicit backfill")
    parser.add_argument("--lookback-days", type=int, help="fresh source dates to extract; default from config")
    parser.add_argument("--demo", action="store_true")
    parser.add_argument("--dry-run", action="store_true", help="validate without writing files or warehouse data")


def window(args, config):
    today = datetime.now(ZoneInfo(config.etl.timezone)).date()
    if args.start_date or args.end_date:
        if not args.start_date or not args.end_date or args.date or args.lookback_days is not None:
            raise ValueError("use both --start-date and --end-date without --date or --lookback-days")
        start, end = date.fromisoformat(args.start_date), date.fromisoformat(args.end_date)
    else:
        end = date.fromisoformat(args.date) if args.date else today - timedelta(days=1)
        days = args.lookback_days if args.lookback_days is not None else config.etl.lookback_days
        if days < 1:
            raise ValueError("lookback-days must be positive")
        start = end - timedelta(days=days - 1)
    dates_between(start, end)
    if end >= today:
        raise ValueError("only completed business dates may be extracted")
    return start, end


def retry(operation, attempts: int, delay: float):
    for attempt in range(attempts):
        try:
            return operation()
        except (SourceConnectionError, psycopg.OperationalError,
                psycopg.errors.SerializationFailure, psycopg.errors.DeadlockDetected):
            if attempt + 1 == attempts:
                raise
            time.sleep(delay * 2 ** attempt)
    raise RuntimeError("no retry attempts configured")


def _run_pipeline(start: date, end: date, config, target=None, dry_run=False):
    report = {"pipeline_run_id": new_id("Pipeline"), "started_at": datetime.now(UTC).isoformat(),
              "dates": [day.isoformat() for day in dates_between(start, end)], "status": "FAILED",
              "stage": "extraction"}
    try:
        if dry_run:
            frames, captured = retry(lambda: extract_batch(start, end, config),
                                      config.etl.retry_attempts, config.etl.retry_backoff_seconds)
            metadata = {"dates": report["dates"], "snapshot_at": captured.isoformat(),
                        "source_mode": "mock" if config.etl.mock_mode else "live",
                        "pipeline_run_id": "DRY_RUN"}
            report["extraction_counts"] = {k: len(v) for k, v in frames.items()}
            report["stage"] = "transformation"
            preview = transform_batch(Path("."), config, dry_run=True, source_package=(metadata, frames))
            report["transformation"] = preview
            report["status"] = "DRY_RUN" if preview["decision"] == "APPROVED" else preview["decision"]
            # Dry-run transformation frames never get written just to satisfy the loader.
            # A supplied warehouse target still receives schema/permission/storage checks below.
            if target:
                import shutil

                from pipelines.load.runner import validate_target
                with target.connect() as connection:
                    validate_target(connection, preview.pop("_frames"))
                    if shutil.disk_usage(target.storage_path).free < target.minimum_free_bytes:
                        raise ValueError("insufficient local warehouse space")
                report["warehouse_preflight"] = "PASS"
            else:
                preview.pop("_frames")
                report["warehouse_preflight"] = "NOT_CONFIGURED"
            return report
        raw = retry(lambda: stage_extraction(start, end, config),
                    config.etl.retry_attempts, config.etl.retry_backoff_seconds)
        report["extraction_manifest"] = str(raw)
        report["stage"] = "transformation"
        transformed = transform_batch(raw, config)
        report["transformation_manifest"] = str(transformed)
        metadata = json.loads(transformed.read_text(encoding="utf-8"))
        if metadata["decision"] != "APPROVED":
            report["status"] = metadata["decision"]
            return report
        report["stage"] = "load"
        if target is None:
            target = warehouse_config()
        if config.etl.mock_mode:
            target = replace(target, dbname=target.dbname if target.dbname.endswith("_demo")
                             else target.dbname + "_demo", allow_demo=True)
        for attempt in range(config.etl.retry_attempts):
            result = load_batch(transformed, target, config.etl.log_dir / "loading")
            if result["status"] in {"SUCCESS", "SKIPPED"} or result.get("error_code") not in TRANSIENT_CODES:
                break
            if attempt + 1 < config.etl.retry_attempts:
                time.sleep(config.etl.retry_backoff_seconds * 2 ** attempt)
        report["load"] = result
        report["status"] = result["status"]
        return report
    except (ValueError, TypeError, KeyError, OSError, ArithmeticError, psycopg.Error, SourceConnectionError) as exc:
        report.update(error_code=type(exc).__name__, error_message=str(exc)[:2000])
        return report
    finally:
        report["finished_at"] = datetime.now(UTC).isoformat()
        if not dry_run:
            write_json(config.etl.log_dir / "pipeline" / f"{report['pipeline_run_id']}.json", report)
            # Preserve a full-year audit copy independently of operational log cleanup.
            write_json(config.etl.handover_dir / report["pipeline_run_id"] / "completion.json", report)


def run_pipeline(start: date, end: date, config, target=None, dry_run=False):
    from pipelines.lock import pipeline_lock
    if dry_run:
        return _run_pipeline(start, end, config, target, dry_run=True)
    with pipeline_lock(config.etl.log_dir / "pipeline.lock"):
        return _run_pipeline(start, end, config, target)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    add_window_arguments(parser)
    args = parser.parse_args(argv)
    try:
        config = configuration(args.demo)
        start, end = window(args, config)
        target = None
        if args.dry_run:
            try:
                target = warehouse_config()
                if config.etl.mock_mode:
                    target = replace(target, dbname=target.dbname if target.dbname.endswith("_demo")
                                     else target.dbname + "_demo", allow_demo=True)
            except ValueError:
                pass
        result = run_pipeline(start, end, config, target, args.dry_run)
        print(json.dumps(result, default=str, indent=2))
        if result["status"] not in {"SUCCESS", "SKIPPED", "DRY_RUN"}:
            print(f"ETL stopped at {result['stage']}: {result['status']}", file=sys.stderr)
            return 1
        return 0
    except (ConfigError, ValueError, OSError) as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
