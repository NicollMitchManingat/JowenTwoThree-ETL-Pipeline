"""Extraction validation checks (V1-V7) from Stage 1 section 3."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date

import pandas as pd
import psycopg

from pipelines.config import AppConfig

from .extract import INVENTORY_COLUMNS, SALES_COLUMNS, TRAFFIC_COLUMNS
from .windows import business_day_window

EXPECTED_COLUMNS: dict[str, list[str]] = {
    "transactions": SALES_COLUMNS,
    "inventory": INVENTORY_COLUMNS,
    "customer_traffic": TRAFFIC_COLUMNS,
}

IDENTIFIER_COLUMNS: dict[str, str] = {
    "transactions": "transaction_id",
    "inventory": "item_id",
    "customer_traffic": "entry_id",
}

PASS = "PASS"
FAIL = "FAIL"


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
        return all(result.status == PASS for result in self.results)

    def to_dict(self) -> dict[str, object]:
        return {
            "passed": self.passed,
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


def _check_connection(
    config: AppConfig,
    connection: psycopg.Connection | None,
) -> CheckResult:
    if config.etl.mock_mode:
        return CheckResult(
            "V1", "Source connection and accessibility", "source", PASS, "mock mode"
        )
    if connection is None:
        return CheckResult(
            "V1", "Source connection and accessibility", "source", FAIL,
            "no source connection supplied",
        )
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()
    except psycopg.Error as exc:
        return CheckResult(
            "V1", "Source connection and accessibility", "source", FAIL, str(exc)
        )
    return CheckResult(
        "V1", "Source connection and accessibility", "source", PASS, "connection ok"
    )


def _check_columns(frames: Mapping[str, pd.DataFrame]) -> CheckResult:
    problems: list[str] = []
    for table, expected in EXPECTED_COLUMNS.items():
        frame = frames.get(table)
        if frame is None:
            problems.append(f"{table}: missing frame")
            continue
        missing = [column for column in expected if column not in frame.columns]
        if missing:
            problems.append(f"{table}: missing {', '.join(missing)}")

    if problems:
        return CheckResult(
            "V2", "Required tables and columns", "extracted frames", FAIL,
            "; ".join(problems),
        )
    return CheckResult(
        "V2", "Required tables and columns", "extracted frames", PASS,
        "all required columns present",
    )


def _check_identifiers(frames: Mapping[str, pd.DataFrame]) -> CheckResult:
    problems: list[str] = []
    for table, identifier in IDENTIFIER_COLUMNS.items():
        frame = frames.get(table)
        if frame is None or identifier not in frame.columns:
            problems.append(f"{table}: identifier {identifier} unavailable")
            continue
        null_count = int(frame[identifier].isna().sum())
        if null_count:
            problems.append(f"{table}: {null_count} null {identifier}")

    if problems:
        return CheckResult(
            "V3", "Required identifiers present", "identifiers", FAIL,
            "; ".join(problems),
        )
    return CheckResult(
        "V3", "Required identifiers present", "identifiers", PASS,
        "all identifiers present",
    )


def _check_uniqueness(frames: Mapping[str, pd.DataFrame]) -> CheckResult:
    problems: list[str] = []
    for table, identifier in IDENTIFIER_COLUMNS.items():
        frame = frames.get(table)
        if frame is None or identifier not in frame.columns:
            continue
        duplicate_count = int(frame[identifier].duplicated().sum())
        if duplicate_count:
            problems.append(f"{table}: {duplicate_count} duplicate {identifier}")

    if problems:
        return CheckResult(
            "V4", "Primary key uniqueness", "identifiers", FAIL,
            "; ".join(problems),
        )
    return CheckResult(
        "V4", "Primary key uniqueness", "identifiers", PASS,
        "all identifiers unique",
    )


def _check_record_counts(
    frames: Mapping[str, pd.DataFrame],
    expected_counts: Mapping[str, int] | None,
) -> CheckResult:
    if expected_counts is None:
        return CheckResult(
            "V5", "Record count validation", "extracted frames", PASS,
            "expected counts not supplied",
        )

    problems: list[str] = []
    for table, expected in expected_counts.items():
        frame = frames.get(table)
        actual = 0 if frame is None else len(frame)
        if actual != expected:
            problems.append(f"{table}: expected {expected}, got {actual}")

    if problems:
        return CheckResult(
            "V5", "Record count validation", "extracted frames", FAIL,
            "; ".join(problems),
        )
    return CheckResult(
        "V5", "Record count validation", "extracted frames", PASS,
        "record counts match",
    )


def _check_completeness(
    frames: Mapping[str, pd.DataFrame],
    business_date: date,
    config: AppConfig,
) -> CheckResult:
    problems: list[str] = []
    window_start, window_end = business_day_window(business_date, config.etl.timezone)

    for table, frame in frames.items():
        if frame.empty:
            problems.append(f"{table}: no records extracted")
            continue
        if "created_at" not in frame.columns:
            continue

        timestamps = pd.to_datetime(frame["created_at"])
        if timestamps.dt.tz is None:
            outside = timestamps.dt.date.ne(business_date)
        else:
            outside = (timestamps < window_start) | (timestamps >= window_end)
        if outside.any():
            problems.append(f"{table}: {int(outside.sum())} records outside window")

    if problems:
        return CheckResult(
            "V6", "Extraction completeness", "extracted frames", FAIL,
            "; ".join(problems),
        )
    return CheckResult(
        "V6", "Extraction completeness", "extracted frames", PASS,
        "all records within the extraction window",
    )


def _check_errors(errors: Sequence[str] | None) -> CheckResult:
    if errors:
        return CheckResult(
            "V7", "Extraction errors and interruptions", "execution", FAIL,
            "; ".join(errors),
        )
    return CheckResult(
        "V7", "Extraction errors and interruptions", "execution", PASS,
        "no extraction errors",
    )


def validate_extraction(
    frames: Mapping[str, pd.DataFrame],
    business_date: date,
    config: AppConfig,
    connection: psycopg.Connection | None = None,
    expected_counts: Mapping[str, int] | None = None,
    errors: Sequence[str] | None = None,
) -> ValidationReport:
    """Run all extraction validation checks and return a report."""

    return ValidationReport(
        results=[
            _check_connection(config, connection),
            _check_columns(frames),
            _check_identifiers(frames),
            _check_uniqueness(frames),
            _check_record_counts(frames, expected_counts),
            _check_completeness(frames, business_date, config),
            _check_errors(errors),
        ]
    )
