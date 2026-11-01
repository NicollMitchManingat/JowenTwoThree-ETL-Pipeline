"""Stage 2 transformation acceptance and handover rules."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import Enum
from pathlib import Path

from .validate import FAIL, ValidationReport

CRITICAL_CHECKS = frozenset({"T1", "T2", "T3", "T4"})


class Decision(str, Enum):
    APPROVED = "APPROVED"
    MANUAL_REVIEW = "MANUAL_REVIEW"
    REJECTED = "REJECTED"


class HandoverBlockedError(Exception):
    """Raised when a batch is not approved for the Load Stage."""


@dataclass(frozen=True)
class AcceptanceResult:
    decision: Decision
    reason: str
    rejection_rate: float
    failed_checks: tuple[str, ...] = ()


def decide(
    report: ValidationReport,
    records_read: int,
    records_rejected: int,
    warn_rate: float,
    fail_rate: float,
) -> AcceptanceResult:
    """Map validation results and rejection rate to an acceptance decision."""

    failed = tuple(result.check_id for result in report.results if result.status == FAIL)
    critical = tuple(check for check in failed if check in CRITICAL_CHECKS)
    rate = records_rejected / records_read if records_read else 0.0

    if critical:
        return AcceptanceResult(
            Decision.REJECTED,
            f"critical validation checks failed: {', '.join(critical)}",
            rate,
            failed,
        )
    if rate > fail_rate:
        return AcceptanceResult(
            Decision.REJECTED,
            f"rejection rate {rate:.4f} exceeds {fail_rate:.4f}",
            rate,
            failed,
        )
    if report.has_warnings or rate > warn_rate:
        return AcceptanceResult(
            Decision.MANUAL_REVIEW,
            f"warnings or rejection rate {rate:.4f} above {warn_rate:.4f}",
            rate,
            failed,
        )
    return AcceptanceResult(
        Decision.APPROVED,
        f"all critical checks passed; rejection rate {rate:.4f}",
        rate,
    )


def write_handover_manifest(
    run_id: str,
    parent_run_id: str,
    business_date: date,
    acceptance: AcceptanceResult,
    report: ValidationReport,
    silver_paths: Mapping[str, Path],
    source_counts: Mapping[str, Mapping[str, int]],
    handover_dir: Path,
    transform_status: str = "SUCCESS",
) -> Path:
    """Write the Stage 3 handover manifest for an approved batch."""

    if acceptance.decision is not Decision.APPROVED:
        raise HandoverBlockedError(
            f"{acceptance.decision.value}: {acceptance.reason}"
        )

    manifest = {
        "pipeline_run_id": run_id,
        "parent_run_id": parent_run_id,
        "processing_date": business_date.isoformat(),
        "decision": acceptance.decision.value,
        "transform_status": transform_status,
        "rejection_rate": acceptance.rejection_rate,
        "created_at": datetime.now(UTC).isoformat(),
        "validation_results": report.to_dict(),
        "tables": {
            name: {"path": str(path)} for name, path in silver_paths.items()
        },
        "source_counts": {
            table: dict(counts) for table, counts in source_counts.items()
        },
    }

    path = handover_dir / run_id / "manifest.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return path


def read_handover_manifest(path: Path) -> dict[str, object]:
    """Read a previously written transformation handover manifest."""

    return json.loads(path.read_text(encoding="utf-8"))
