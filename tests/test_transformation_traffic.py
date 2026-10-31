from datetime import UTC, date, datetime

import pandas as pd

from pipelines.transformation.schema import FACT_TRAFFIC_SUMMARY_COLUMNS
from pipelines.transformation.traffic import build_traffic_summary

BUSINESS_DATE = date(2026, 10, 16)
LOADED_AT = datetime(2026, 11, 1, 0, 0, tzinfo=UTC)


def _traffic() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "entry_id": ["entry-1", "entry-2", "entry-3"],
            "business_date": [BUSINESS_DATE, BUSINESS_DATE, BUSINESS_DATE],
            "number_of_customer": [4, 12, 7],
            "entry_hour": [8, 18, 18],
        }
    )


def test_b04_sums_customers_and_finds_peak_hour() -> None:
    result = build_traffic_summary(_traffic(), BUSINESS_DATE, loaded_at=LOADED_AT)

    row = result.iloc[0]
    assert row["total_customers"] == 23
    assert row["peak_hour"] == "18:00:00"
    assert row["traffic_summary_key"] == "TRAFFIC_20261016"
    assert row["date_key"] == 20261016


def test_peak_hour_tie_uses_earliest_hour() -> None:
    frame = pd.DataFrame(
        {
            "entry_id": ["entry-1", "entry-2"],
            "business_date": [BUSINESS_DATE, BUSINESS_DATE],
            "number_of_customer": [5, 5],
            "entry_hour": [18, 8],
        }
    )

    result = build_traffic_summary(frame, BUSINESS_DATE, loaded_at=LOADED_AT)

    assert result.iloc[0]["peak_hour"] == "08:00:00"


def test_invalid_entry_hours_are_ignored() -> None:
    frame = pd.DataFrame(
        {
            "entry_id": ["entry-1", "entry-2"],
            "business_date": [BUSINESS_DATE, BUSINESS_DATE],
            "number_of_customer": [5, 3],
            "entry_hour": [pd.NA, pd.NA],
        }
    )

    result = build_traffic_summary(frame, BUSINESS_DATE, loaded_at=LOADED_AT)

    assert result.iloc[0]["total_customers"] == 8
    assert result.iloc[0]["peak_hour"] is None


def test_empty_traffic_writes_zero_value_fact() -> None:
    result = build_traffic_summary(
        pd.DataFrame(), BUSINESS_DATE, loaded_at=LOADED_AT
    )

    assert list(result.columns) == FACT_TRAFFIC_SUMMARY_COLUMNS
    assert result.iloc[0]["total_customers"] == 0
    assert result.iloc[0]["peak_hour"] is None


def test_rows_for_other_dates_are_ignored() -> None:
    frame = _traffic()
    frame["business_date"] = [date(2026, 10, 15), BUSINESS_DATE, BUSINESS_DATE]

    result = build_traffic_summary(frame, BUSINESS_DATE, loaded_at=LOADED_AT)

    assert result.iloc[0]["total_customers"] == 19
