from datetime import date

import pandas as pd

from pipelines.config import load_config
from pipelines.extraction.extract import (
    extract_inventory,
    extract_sales,
    extract_traffic,
)
from pipelines.extraction.validate import validate_extraction

BUSINESS_DATE = date(2026, 10, 12)


def _config():
    return load_config({"ETL_MOCK_MODE": "true"})


def _frames(config):
    return {
        "transactions": extract_sales(BUSINESS_DATE, config),
        "inventory": extract_inventory(config),
        "customer_traffic": extract_traffic(BUSINESS_DATE, config),
    }


def _expected_counts(frames):
    return {table: len(frame) for table, frame in frames.items()}


def test_valid_extraction_passes_all_checks() -> None:
    config = _config()
    frames = _frames(config)

    report = validate_extraction(
        frames,
        BUSINESS_DATE,
        config,
        expected_counts=_expected_counts(frames),
    )

    assert report.passed is True
    assert [result.check_id for result in report.results] == [
        "V1",
        "V2",
        "V3",
        "V4",
        "V5",
        "V6",
        "V7",
    ]
    assert report.to_dict()["passed"] is True


def test_missing_column_fails_columns_check() -> None:
    config = _config()
    frames = _frames(config)
    frames["transactions"] = frames["transactions"].drop(columns=["total"])

    report = validate_extraction(frames, BUSINESS_DATE, config)

    v2 = next(result for result in report.results if result.check_id == "V2")
    assert v2.status == "FAIL"
    assert "total" in v2.message


def test_null_identifier_fails_identifier_check() -> None:
    config = _config()
    frames = _frames(config)
    frames["transactions"].loc[0, "transaction_id"] = None

    report = validate_extraction(frames, BUSINESS_DATE, config)

    v3 = next(result for result in report.results if result.check_id == "V3")
    assert v3.status == "FAIL"
    assert "null" in v3.message


def test_duplicate_identifier_fails_uniqueness_check() -> None:
    config = _config()
    frames = _frames(config)
    frames["transactions"].loc[1, "transaction_id"] = frames["transactions"].loc[
        0, "transaction_id"
    ]

    report = validate_extraction(frames, BUSINESS_DATE, config)

    v4 = next(result for result in report.results if result.check_id == "V4")
    assert v4.status == "FAIL"
    assert "duplicate" in v4.message


def test_record_count_mismatch_fails_count_check() -> None:
    config = _config()
    frames = _frames(config)

    report = validate_extraction(
        frames,
        BUSINESS_DATE,
        config,
        expected_counts={"transactions": 99, "inventory": 6, "customer_traffic": 5},
    )

    v5 = next(result for result in report.results if result.check_id == "V5")
    assert v5.status == "FAIL"
    assert "expected 99" in v5.message


def test_out_of_window_record_fails_completeness_check() -> None:
    config = _config()
    frames = _frames(config)
    frames["transactions"].loc[0, "created_at"] = pd.Timestamp(
        BUSINESS_DATE
    ) + pd.Timedelta(days=1)

    report = validate_extraction(frames, BUSINESS_DATE, config)

    v6 = next(result for result in report.results if result.check_id == "V6")
    assert v6.status == "FAIL"
    assert "outside window" in v6.message


def test_errors_fail_error_check() -> None:
    config = _config()
    frames = _frames(config)

    report = validate_extraction(
        frames,
        BUSINESS_DATE,
        config,
        errors=["Database connection timeout"],
    )

    v7 = next(result for result in report.results if result.check_id == "V7")
    assert v7.status == "FAIL"
    assert "timeout" in v7.message


def test_empty_frame_fails_completeness_check() -> None:
    config = _config()
    frames = _frames(config)
    frames["inventory"] = frames["inventory"].iloc[0:0]

    report = validate_extraction(frames, BUSINESS_DATE, config)

    v6 = next(result for result in report.results if result.check_id == "V6")
    assert v6.status == "FAIL"
    assert "no records" in v6.message
