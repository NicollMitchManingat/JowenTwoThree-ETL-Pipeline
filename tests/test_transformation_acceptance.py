from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from pipelines.config import load_config
from pipelines.extraction.extract import (
    INVENTORY_COLUMNS,
    SALES_COLUMNS,
    TRAFFIC_COLUMNS,
)
from pipelines.extraction.parquet_io import write_parquet
from pipelines.extraction.run import run_extraction
from pipelines.transformation.acceptance import (
    Decision,
    HandoverBlockedError,
    decide,
    read_handover_manifest,
    write_handover_manifest,
)
from pipelines.transformation.runner import run_transformation
from pipelines.transformation.validate import PASS, WARN, CheckResult, ValidationReport

BUSINESS_DATE = date(2026, 10, 16)


def _report(*statuses: str, warning: bool = False) -> ValidationReport:
    results = [
        CheckResult("T1", "Record reconciliation", "transactions", status, "")
        for status in statuses[:1]
    ]
    for check_id in ("T2", "T3", "T4"):
        results.append(CheckResult(check_id, check_id, "fact", PASS, ""))
    results.append(
        CheckResult("W1", "Non-critical warnings", "transformation", WARN if warning else PASS, "")
    )
    return ValidationReport(results)


def _config(tmp_path: Path):
    return load_config(
        {
            "ETL_MOCK_MODE": "true",
            "ETL_STAGING_DIR": str(tmp_path / "bronze"),
            "ETL_LOG_DIR": str(tmp_path / "logs"),
            "ETL_HANDOVER_DIR": str(tmp_path / "handover"),
            "ETL_SILVER_DIR": str(tmp_path / "silver"),
            "ETL_QUARANTINE_DIR": str(tmp_path / "quarantine"),
            "ETL_TRANSFORM_LOG_DIR": str(tmp_path / "logs" / "transformation"),
            "ETL_LOOKBACK_DAYS": "1",
            "ETL_LOW_STOCK_THRESHOLD": "5",
        }
    )


def test_clean_batch_is_approved() -> None:
    result = decide(_report(PASS), 100, 0, 0.01, 0.05)

    assert result.decision is Decision.APPROVED
    assert result.rejection_rate == 0.0
    assert result.failed_checks == ()


def test_rate_above_warn_requires_manual_review() -> None:
    result = decide(_report(PASS), 100, 2, 0.01, 0.05)

    assert result.decision is Decision.MANUAL_REVIEW


def test_rate_above_fail_is_rejected() -> None:
    result = decide(_report(PASS), 100, 6, 0.01, 0.05)

    assert result.decision is Decision.REJECTED


def test_critical_validation_failure_is_rejected() -> None:
    result = decide(_report("FAIL"), 100, 0, 0.01, 0.05)

    assert result.decision is Decision.REJECTED
    assert "T1" in result.failed_checks


def test_warnings_require_manual_review() -> None:
    result = decide(_report(PASS, warning=True), 100, 0, 0.01, 0.05)

    assert result.decision is Decision.MANUAL_REVIEW


def test_handover_manifest_round_trip(tmp_path: Path) -> None:
    report = _report(PASS)
    acceptance = decide(report, 100, 0, 0.01, 0.05)
    silver = {"fact_sales_summary/2026-10-16": tmp_path / "sales.parquet"}
    counts = {"transactions": {"read": 100, "output": 100, "rejected": 0}}

    path = write_handover_manifest(
        "Transform_1",
        "Run_1",
        BUSINESS_DATE,
        acceptance,
        report,
        silver,
        counts,
        tmp_path / "handover",
    )

    payload = read_handover_manifest(path)
    assert payload["decision"] == "APPROVED"
    assert payload["processing_date"] == "2026-10-16"
    assert payload["parent_run_id"] == "Run_1"
    assert payload["tables"]["fact_sales_summary/2026-10-16"]["path"] == str(
        silver["fact_sales_summary/2026-10-16"]
    )
    assert payload["source_counts"]["transactions"]["read"] == 100


def test_handover_blocked_for_manual_review(tmp_path: Path) -> None:
    report = _report(PASS)
    acceptance = decide(report, 100, 2, 0.01, 0.05)

    with pytest.raises(HandoverBlockedError, match="MANUAL_REVIEW"):
        write_handover_manifest(
            "Transform_1",
            "Run_1",
            BUSINESS_DATE,
            acceptance,
            report,
            {},
            {},
            tmp_path / "handover",
        )


def test_runner_approves_mock_batch(tmp_path: Path) -> None:
    config = _config(tmp_path)
    assert run_extraction(BUSINESS_DATE, config).status == "SUCCESS"

    result = run_transformation(BUSINESS_DATE, config)

    assert result.status == "SUCCESS"
    assert result.decision == Decision.APPROVED.value
    assert result.handover_path is not None
    assert result.handover_path.exists()


def test_runner_rejects_batch_with_high_rejection_rate(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    assert run_extraction(BUSINESS_DATE, config).status == "SUCCESS"

    rejected_inventory = pd.DataFrame(
        [
            {
                "item_id": f"item-bad-{index}",
                "name": "Bad Item",
                "stock_quantity": -1,
            }
            for index in range(10)
        ],
        columns=INVENTORY_COLUMNS,
    )
    write_parquet(
        rejected_inventory,
        "inventory",
        BUSINESS_DATE,
        config.etl.staging_dir,
    )

    result = run_transformation(BUSINESS_DATE, config)

    assert result.status == "SUCCESS"
    assert result.decision == Decision.REJECTED.value
    assert result.handover_path is None
    assert not any((tmp_path / "handover").glob("Transform_*"))


def test_runner_writes_handover_for_approved_batch(tmp_path: Path) -> None:
    config = _config(tmp_path)
    assert run_extraction(BUSINESS_DATE, config).status == "SUCCESS"

    transactions = pd.DataFrame(
        [
            {
                "transaction_id": f"trx-{index:03d}",
                "created_at": pd.Timestamp("2026-10-16 10:00:00")
                + pd.Timedelta(minutes=index),
                "transaction_number": f"TRX-{index:03d}",
                "total": 100.00,
            }
            for index in range(120)
        ],
        columns=SALES_COLUMNS,
    )
    inventory = pd.DataFrame(
        [
            {"item_id": f"item-{index:02d}", "name": f"Item {index}", "stock_quantity": 10}
            for index in range(9)
        ]
        + [{"item_id": "item-bad", "name": "Bad", "stock_quantity": -1}],
        columns=INVENTORY_COLUMNS,
    )
    traffic = pd.DataFrame(
        [
            {
                "entry_id": index,
                "number_of_customer": 2,
                "created_at": pd.Timestamp("2026-10-16 12:00:00")
                + pd.Timedelta(minutes=index),
            }
            for index in range(20)
        ],
        columns=TRAFFIC_COLUMNS,
    )
    write_parquet(transactions, "transactions", BUSINESS_DATE, config.etl.staging_dir)
    write_parquet(inventory, "inventory", BUSINESS_DATE, config.etl.staging_dir)
    write_parquet(
        traffic, "customer_traffic", BUSINESS_DATE, config.etl.staging_dir
    )

    result = run_transformation(BUSINESS_DATE, config)

    assert result.decision == Decision.APPROVED.value
    assert result.handover_path is not None
    payload = read_handover_manifest(result.handover_path)
    assert payload["decision"] == "APPROVED"
    assert payload["transform_status"] == "SUCCESS"
    assert payload["source_counts"]["transactions"]["read"] == 120
    assert payload["source_counts"]["inventory"]["rejected"] == 1
    assert (tmp_path / "silver" / "fact_sales_summary").exists()
