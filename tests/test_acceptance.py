from datetime import date
from pathlib import Path

import pytest

from pipelines.extraction.acceptance import (
    Decision,
    HandoverBlockedError,
    decide,
    read_handover_manifest,
    write_handover_manifest,
)
from pipelines.extraction.validate import CheckResult, ValidationReport


def _report(*statuses: tuple[str, str]) -> ValidationReport:
    return ValidationReport(
        results=[
            CheckResult(check_id, f"check {check_id}", "target", status, "message")
            for check_id, status in statuses
        ]
    )


def test_all_checks_pass_is_approved() -> None:
    report = _report(("V1", "PASS"), ("V2", "PASS"), ("V5", "PASS"))

    result = decide(report)

    assert result.decision is Decision.APPROVED
    assert result.failed_checks == ()


def test_non_critical_failure_needs_manual_review() -> None:
    report = _report(("V1", "PASS"), ("V5", "FAIL"))

    result = decide(report)

    assert result.decision is Decision.MANUAL_REVIEW
    assert result.failed_checks == ("V5",)


def test_critical_failure_is_rejected() -> None:
    report = _report(("V1", "PASS"), ("V4", "FAIL"), ("V5", "FAIL"))

    result = decide(report)

    assert result.decision is Decision.REJECTED
    assert "V4" in result.failed_checks


def test_handover_manifest_written_for_approved_batch(tmp_path: Path) -> None:
    report = _report(("V1", "PASS"), ("V2", "PASS"))
    parquet_paths = {
        "transactions": tmp_path / "transactions.parquet",
        "inventory": tmp_path / "inventory.parquet",
    }

    path = write_handover_manifest(
        "Run_20261013_001",
        date(2026, 10, 12),
        report,
        parquet_paths,
        {"transactions": 5, "inventory": 6},
        tmp_path / "handover",
    )

    manifest = read_handover_manifest(path)

    assert path.name == "manifest.json"
    assert manifest["pipeline_run_id"] == "Run_20261013_001"
    assert manifest["processing_date"] == "2026-10-12"
    assert manifest["decision"] == "APPROVED"
    assert manifest["tables"]["transactions"]["records"] == 5


def test_handover_is_blocked_for_manual_review(tmp_path: Path) -> None:
    report = _report(("V1", "PASS"), ("V5", "FAIL"))

    with pytest.raises(HandoverBlockedError, match="MANUAL_REVIEW"):
        write_handover_manifest(
            "Run_20261013_001",
            date(2026, 10, 12),
            report,
            {},
            {},
            tmp_path / "handover",
        )


def test_handover_is_blocked_for_rejected_batch(tmp_path: Path) -> None:
    report = _report(("V1", "FAIL"))

    with pytest.raises(HandoverBlockedError, match="REJECTED"):
        write_handover_manifest(
            "Run_20261013_001",
            date(2026, 10, 12),
            report,
            {},
            {},
            tmp_path / "handover",
        )
