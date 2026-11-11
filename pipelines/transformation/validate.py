"""Stage 2 transformation validation checks."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

import pandas as pd

from .schema import FACT_KEY_COLUMNS
from .standardization import WarningEvent

PASS = "PASS"
WARN = "WARN"
FAIL = "FAIL"

CRITICAL_CHECKS = frozenset({"T1", "T2", "T3", "T4"})


@dataclass(frozen=True)
class CheckResult:
    check_id: str
    name: str
    target: str
    status: str
    message: str


@dataclass
class ValidationReport:
    results: list[CheckResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not any(result.status == FAIL for result in self.results)

    @property
    def has_warnings(self) -> bool:
        return any(result.status == WARN for result in self.results)

    def to_dict(self) -> dict[str, object]:
        return {
            "passed": self.passed,
            "has_warnings": self.has_warnings,
            "checks": [
                {
                    "check_id": result.check_id,
                    "name": result.name,
                    "target": result.target,
                    "status": result.status,
                    "message": result.message,
                }
                for result in self.results
            ],
        }


def validate_transformation(
    table_counts: Mapping[str, Mapping[str, int]],
    facts: Mapping[str, pd.DataFrame],
    dim_date: pd.DataFrame,
    warnings: Sequence[WarningEvent] = (),
) -> ValidationReport:
    """Run reconciliation, key, referential, and business-rule checks."""

    report = ValidationReport()

    for table, counts in table_counts.items():
        read = int(counts.get("read", 0))
        output = int(counts.get("output", 0))
        rejected = int(counts.get("rejected", 0))
        status = PASS if read == output + rejected else FAIL
        report.results.append(
            CheckResult(
                "T1",
                "Record reconciliation",
                table,
                status,
                f"read={read} output={output} rejected={rejected}",
            )
        )
        if read == 0:
            report.results.append(
                CheckResult(
                    "T5",
                    "Empty source date",
                    table,
                    PASS,
                    "no source rows; zero-value fact written",
                )
            )

    for table, key_column in FACT_KEY_COLUMNS.items():
        frame = facts.get(table)
        if frame is None or frame.empty:
            report.results.append(
                CheckResult(
                    "T2", "Key uniqueness", table, PASS, "no fact rows"
                )
            )
            continue
        duplicated = int(frame[key_column].duplicated().sum())
        missing = int(frame[key_column].isna().sum())
        status = PASS if duplicated == 0 and missing == 0 else FAIL
        report.results.append(
            CheckResult(
                "T2",
                "Key uniqueness",
                table,
                status,
                f"duplicates={duplicated} missing={missing}",
            )
        )

    dim_keys = set(dim_date.get("date_key", pd.Series(dtype="int64")).tolist())
    for table, frame in facts.items():
        if frame is None or frame.empty:
            report.results.append(
                CheckResult(
                    "T3", "Referential integrity", table, PASS, "no fact rows"
                )
            )
            continue
        orphans = sorted(set(frame["date_key"].tolist()) - dim_keys)
        status = PASS if not orphans else FAIL
        report.results.append(
            CheckResult(
                "T3",
                "Referential integrity",
                table,
                status,
                f"orphan date keys={orphans}",
            )
        )

    sales = facts.get("fact_sales_summary")
    if sales is None or sales.empty:
        report.results.append(
            CheckResult(
                "T4", "Non-negative sales", "fact_sales_summary", PASS, "no fact rows"
            )
        )
    else:
        negative = int((sales["total_sales"] < 0).sum())
        status = PASS if negative == 0 else FAIL
        report.results.append(
            CheckResult(
                "T4",
                "Non-negative sales",
                "fact_sales_summary",
                status,
                f"negative rows={negative}",
            )
        )

    if warnings:
        message = "; ".join(
            f"{warning.rule_id} {warning.error_code} x{warning.count}"
            for warning in warnings
        )
        report.results.append(
            CheckResult("W1", "Non-critical warnings", "transformation", WARN, message)
        )
    else:
        report.results.append(
            CheckResult(
                "W1", "Non-critical warnings", "transformation", PASS, "none"
            )
        )

    return report
