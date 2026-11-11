from datetime import UTC, datetime

import pandas as pd

from pipelines.transformation.standardization import WarningEvent
from pipelines.transformation.validate import (
    FAIL,
    PASS,
    WARN,
    validate_transformation,
)

LOADED_AT = datetime(2026, 11, 1, 0, 0, tzinfo=UTC)


def _counts() -> dict[str, dict[str, int]]:
    return {
        "transactions": {"read": 10, "output": 9, "rejected": 1},
        "inventory": {"read": 6, "output": 5, "rejected": 1},
        "customer_traffic": {"read": 8, "output": 8, "rejected": 0},
    }


def _dim_date() -> pd.DataFrame:
    return pd.DataFrame({"date_key": [20261016]})


def _facts() -> dict[str, pd.DataFrame]:
    return {
        "fact_sales_summary": pd.DataFrame(
            {
                "sales_summary_key": ["SALES_20261016"],
                "date_key": [20261016],
                "total_sales": [100.0],
                "total_orders": [4],
                "loaded_at": [pd.Timestamp(LOADED_AT)],
            }
        ),
        "fact_inventory_summary": pd.DataFrame(
            {
                "inventory_summary_key": ["INV_20261016"],
                "date_key": [20261016],
                "total_items": [5],
                "low_stock_items": [2],
                "out_of_stock_items": [0],
                "low_stock_threshold": [5],
                "loaded_at": [pd.Timestamp(LOADED_AT)],
            }
        ),
        "fact_traffic_summary": pd.DataFrame(
            {
                "traffic_summary_key": ["TRAFFIC_20261016"],
                "date_key": [20261016],
                "total_customers": [20],
                "peak_hour": ["18:00:00"],
                "loaded_at": [pd.Timestamp(LOADED_AT)],
            }
        ),
    }


def test_all_checks_pass_for_consistent_batch() -> None:
    report = validate_transformation(_counts(), _facts(), _dim_date())

    assert report.passed is True
    assert report.has_warnings is False
    assert all(result.status == PASS for result in report.results)


def test_reconciliation_failure_is_critical() -> None:
    counts = _counts()
    counts["transactions"] = {"read": 10, "output": 8, "rejected": 1}

    report = validate_transformation(counts, _facts(), _dim_date())

    assert report.passed is False
    assert any(
        result.check_id == "T1" and result.status == FAIL
        for result in report.results
    )


def test_duplicate_fact_key_fails() -> None:
    facts = _facts()
    facts["fact_sales_summary"] = pd.concat(
        [facts["fact_sales_summary"], facts["fact_sales_summary"]],
        ignore_index=True,
    )

    report = validate_transformation(_counts(), facts, _dim_date())

    assert any(
        result.check_id == "T2" and result.status == FAIL
        for result in report.results
    )


def test_orphan_date_key_fails_referential_integrity() -> None:
    facts = _facts()
    facts["fact_traffic_summary"]["date_key"] = 20261015

    report = validate_transformation(_counts(), facts, _dim_date())

    assert any(
        result.check_id == "T3" and result.status == FAIL
        for result in report.results
    )


def test_negative_sales_fail_business_rule() -> None:
    facts = _facts()
    facts["fact_sales_summary"]["total_sales"] = -1.0

    report = validate_transformation(_counts(), facts, _dim_date())

    assert any(
        result.check_id == "T4" and result.status == FAIL
        for result in report.results
    )


def test_warnings_mark_report_as_warning_not_failed() -> None:
    warnings = [
        WarningEvent("S-05", "WARN_PEAK_HOUR", 1, "invalid peak hour")
    ]

    report = validate_transformation(_counts(), _facts(), _dim_date(), warnings)

    assert report.passed is True
    assert report.has_warnings is True
    assert any(
        result.check_id == "W1" and result.status == WARN
        for result in report.results
    )


def test_empty_source_is_not_a_failure() -> None:
    counts = {
        "transactions": {"read": 0, "output": 0, "rejected": 0},
        "inventory": {"read": 0, "output": 0, "rejected": 0},
        "customer_traffic": {"read": 0, "output": 0, "rejected": 0},
    }
    facts = {table: frame.iloc[0:0] for table, frame in _facts().items()}

    report = validate_transformation(counts, facts, _dim_date())

    assert report.passed is True
    assert sum(1 for result in report.results if result.check_id == "T5") == 3
