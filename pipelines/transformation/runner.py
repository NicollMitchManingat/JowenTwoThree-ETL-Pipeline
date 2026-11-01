"""Stage 2 transformation runner with lookback and safe reruns."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd

from pipelines.config import AppConfig
from pipelines.logging_setup import configure_logging

from .acceptance import (
    Decision,
    decide,
    write_handover_manifest,
)
from .cleaning import clean_inventory, clean_traffic, clean_transactions
from .dimensions import build_dim_date, ensure_dim_date
from .input import (
    SOURCE_TABLES,
    TransformationInputError,
    find_extraction_manifest,
    load_bronze,
)
from .inventory import build_inventory_summary
from .metadata import (
    TransformRunMetadata,
    new_transform_run_id,
    purge_old_transform_logs,
    write_transform_log,
)
from .parquet_io import read_silver, write_silver
from .quarantine import build_quarantine, write_quarantine
from .sales import build_sales_summary
from .schema import (
    FACT_INVENTORY_SUMMARY_COLUMNS,
    FACT_SALES_SUMMARY_COLUMNS,
    FACT_TRAFFIC_SUMMARY_COLUMNS,
)
from .standardization import (
    standardize_inventory,
    standardize_traffic,
    standardize_transactions,
)
from .traffic import build_traffic_summary
from .validate import validate_transformation


@dataclass
class TransformRunResult:
    run_id: str
    parent_run_id: str
    business_date: date
    status: str
    validation_passed: bool
    silver_paths: dict[str, Path]
    quarantine_paths: dict[str, Path]
    log_path: Path | None
    decision: str | None = None
    handover_path: Path | None = None


def _dates(business_date: date, lookback_days: int) -> list[date]:
    return [
        business_date - timedelta(days=offset)
        for offset in range(lookback_days - 1, -1, -1)
    ]


def _concat(
    frames: list[pd.DataFrame],
    columns: list[str],
) -> pd.DataFrame:
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame(columns=columns)
    return pd.concat(non_empty, ignore_index=True)


def run_transformation(
    business_date: date,
    config: AppConfig,
    lookback_days: int | None = None,
    dry_run: bool = False,
    parent_run_id: str | None = None,
) -> TransformRunResult:
    """Run Stage 2 for a business date and its lookback window."""

    logger = configure_logging(config.etl.log_level)
    run_id = new_transform_run_id()
    lookback = lookback_days if lookback_days is not None else config.etl.lookback_days
    if lookback < 1:
        raise ValueError("lookback_days must be at least 1")

    primary_path, primary_manifest = find_extraction_manifest(
        business_date, config.etl.handover_dir, parent_run_id
    )
    resolved_parent = str(primary_manifest.get("pipeline_run_id", ""))
    if not resolved_parent:
        raise TransformationInputError(f"manifest is missing pipeline_run_id: {primary_path}")

    started = datetime.now(UTC)
    counts: dict[str, dict[str, int]] = {
        table: {"read": 0, "output": 0, "rejected": 0} for table in SOURCE_TABLES
    }
    silver_paths: dict[str, Path] = {}
    quarantine_paths: dict[str, Path] = {}
    sales_frames: list[pd.DataFrame] = []
    inventory_frames: list[pd.DataFrame] = []
    traffic_frames: list[pd.DataFrame] = []
    warnings = []
    processed_dates: list[date] = []

    for day in _dates(business_date, lookback):
        if day == business_date:
            manifest = primary_manifest
        else:
            try:
                _, manifest = find_extraction_manifest(day, config.etl.handover_dir)
            except TransformationInputError:
                logger.warning(
                    "No approved extraction manifest for %s; skipping lookback date",
                    day,
                )
                continue

        bronze = load_bronze(manifest)
        processed_dates.append(day)

        clean_txn = clean_transactions(bronze["transactions"])
        std_txn = standardize_transactions(clean_txn.cleaned, config.etl.timezone)
        counts["transactions"]["read"] += len(bronze["transactions"])
        counts["transactions"]["output"] += len(std_txn.cleaned)
        counts["transactions"]["rejected"] += (
            len(clean_txn.rejected) + len(std_txn.rejected)
        )
        sales = build_sales_summary(
            std_txn.cleaned,
            business_date=day,
        )
        if not dry_run:
            silver_paths[f"fact_sales_summary/{day.isoformat()}"] = write_silver(
                sales, "fact_sales_summary", config.etl.silver_dir, day
            )
        sales_frames.append(sales)

        if day == business_date:
            clean_inv = clean_inventory(bronze["inventory"])
            std_inv = standardize_inventory(clean_inv.cleaned)
            counts["inventory"]["read"] += len(bronze["inventory"])
            counts["inventory"]["output"] += len(std_inv.cleaned)
            counts["inventory"]["rejected"] += (
                len(clean_inv.rejected) + len(std_inv.rejected)
            )
            warnings.extend(std_inv.warnings)
            inventory = build_inventory_summary(
                std_inv.cleaned,
                business_date,
                config.etl.low_stock_threshold,
            )
            if not dry_run:
                silver_paths["fact_inventory_summary"] = write_silver(
                    inventory,
                    "fact_inventory_summary",
                    config.etl.silver_dir,
                    business_date,
                )
            inventory_frames.append(inventory)

        clean_traffic_result = clean_traffic(bronze["customer_traffic"])
        std_traffic = standardize_traffic(
            clean_traffic_result.cleaned, day, config.etl.timezone
        )
        counts["customer_traffic"]["read"] += len(bronze["customer_traffic"])
        counts["customer_traffic"]["output"] += len(std_traffic.cleaned)
        counts["customer_traffic"]["rejected"] += (
            len(clean_traffic_result.rejected) + len(std_traffic.rejected)
        )
        warnings.extend(std_traffic.warnings)
        traffic = build_traffic_summary(std_traffic.cleaned, day)
        if not dry_run:
            silver_paths[f"fact_traffic_summary/{day.isoformat()}"] = write_silver(
                traffic, "fact_traffic_summary", config.etl.silver_dir, day
            )
        traffic_frames.append(traffic)

        rejected = [clean_txn.rejected, std_txn.rejected, std_traffic.rejected]
        if day == business_date:
            rejected.append(clean_inv.rejected)
            rejected.append(std_inv.rejected)
        quarantine = build_quarantine(
            rejected, run_id, resolved_parent
        )
        if not dry_run:
            written = write_quarantine(
                quarantine, day, config.etl.quarantine_dir
            )
            for table, path in written.items():
                quarantine_paths[f"{table}/{day.isoformat()}"] = path

    if not processed_dates:
        raise TransformationInputError("no bronze dates were processed")

    if dry_run:
        dim_date = build_dim_date(min(processed_dates), max(processed_dates))
    else:
        dim_path = ensure_dim_date(
            min(processed_dates), max(processed_dates), config.etl.silver_dir
        )
        silver_paths["dim_date"] = dim_path
        dim_date = read_silver("dim_date", config.etl.silver_dir)

    facts = {
        "fact_sales_summary": _concat(
            sales_frames, FACT_SALES_SUMMARY_COLUMNS
        ),
        "fact_inventory_summary": _concat(
            inventory_frames, FACT_INVENTORY_SUMMARY_COLUMNS
        ),
        "fact_traffic_summary": _concat(
            traffic_frames, FACT_TRAFFIC_SUMMARY_COLUMNS
        ),
    }
    report = validate_transformation(counts, facts, dim_date, warnings)
    if not report.passed:
        status = "FAILED"
    elif report.has_warnings:
        status = "WARNING"
    else:
        status = "SUCCESS"

    total_read = sum(count["read"] for count in counts.values())
    total_rejected = sum(count["rejected"] for count in counts.values())
    acceptance = decide(
        report,
        total_read,
        total_rejected,
        config.etl.rejection_warn_rate,
        config.etl.rejection_fail_rate,
    )

    finished = datetime.now(UTC)
    batch_window = f"{min(processed_dates)} to {max(processed_dates)}"
    entries = [
        TransformRunMetadata(
            run_id=run_id,
            parent_run_id=resolved_parent,
            table_name=table,
            batch_window=batch_window,
            records_read=counts[table]["read"],
            records_output=counts[table]["output"],
            records_rejected=counts[table]["rejected"],
            status=status,
            started_at=started.isoformat(),
            finished_at=finished.isoformat(),
            duration_seconds=round((finished - started).total_seconds(), 3),
        )
        for table in SOURCE_TABLES
    ]

    log_path: Path | None = None
    if not dry_run:
        log_path = write_transform_log(entries, config.etl.transform_log_dir)
        purge_old_transform_logs(
            config.etl.transform_log_dir, config.etl.log_retention_days
        )

    handover_path: Path | None = None
    if acceptance.decision is Decision.APPROVED and not dry_run:
        handover_path = write_handover_manifest(
            run_id,
            resolved_parent,
            business_date,
            acceptance,
            report,
            silver_paths,
            counts,
            config.etl.handover_dir,
            transform_status=status,
        )

    return TransformRunResult(
        run_id=run_id,
        parent_run_id=resolved_parent,
        business_date=business_date,
        status=status,
        validation_passed=report.passed,
        silver_paths=silver_paths,
        quarantine_paths=quarantine_paths,
        log_path=log_path,
        decision=acceptance.decision.value,
        handover_path=handover_path,
    )
