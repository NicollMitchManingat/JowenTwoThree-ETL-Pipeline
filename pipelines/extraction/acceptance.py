"""Extraction acceptance and handover rules (Stage 1 section 7)."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import Enum
from pathlib import Path

from .validate import PASS, ValidationReport

CRITICAL_CHECKS = frozenset({"V1", "V2", "V3", "V4", "V6", "V7"})


class Decision(str, Enum):
    APPROVED = "APPROVED"
    MANUAL_REVIEW = "MANUAL_REVIEW"
    REJECTED = "REJECTED"


class HandoverBlockedError(Exception):
    """Raised when a batch is not approved for handover to Stage 2."""


@dataclass(frozen=True)
class AcceptanceResult:
    decision: Decision
    reason: str
    failed_checks: tuple[str, ...] = ()


def decide(report: ValidationReport) -> AcceptanceResult:
    """Map validation results to an acceptance decision."""

    failed = tuple(result.check_id for result in report.results if result.status != PASS)
    critical = tuple(check for check in failed if check in CRITICAL_CHECKS)

    if critical:
        return AcceptanceResult(
            Decision.REJECTED,
            f"critical validation checks failed: {', '.join(critical)}",
            failed,
        )
    if failed:
        return AcceptanceResult(
            Decision.MANUAL_REVIEW,
            f"non-critical warnings: {', '.join(failed)}",
            failed,
        )
    return AcceptanceResult(Decision.APPROVED, "all validation checks passed")


def write_handover_manifest(
    run_id: str,
    business_date: date,
    report: ValidationReport,
    parquet_paths: Mapping[str, Path],
    record_counts: Mapping[str, int],
    handover_dir: Path,
    extraction_status: str = "SUCCESS",
) -> Path:
    """Write the Stage 2 handover manifest, or block a non-approved batch."""

    result = decide(report)
    if result.decision is not Decision.APPROVED:
        raise HandoverBlockedError(f"{result.decision.value}: {result.reason}")

    manifest = {
        "pipeline_run_id": run_id,
        "processing_date": business_date.isoformat(),
        "decision": result.decision.value,
        "extraction_status": extraction_status,
        "created_at": datetime.now(UTC).isoformat(),
        "validation_results": report.to_dict(),
        "tables": {
            table: {
                "path": str(path),
                "records": int(record_counts.get(table, 0)),
            }
            for table, path in parquet_paths.items()
        },
    }

    path = handover_dir / run_id / "manifest.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return path


def read_handover_manifest(path: Path) -> dict[str, object]:
    """Read a previously written handover manifest."""

    return json.loads(path.read_text(encoding="utf-8"))
