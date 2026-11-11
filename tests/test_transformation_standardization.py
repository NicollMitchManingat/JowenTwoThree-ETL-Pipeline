from datetime import date

import pandas as pd
import pytest

from pipelines.transformation.standardization import (
    standardize_inventory,
    standardize_traffic,
    standardize_transactions,
)

TIMEZONE = "Asia/Manila"


def _transactions() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "transaction_id": ["trx-1", "trx-2", "trx-3", "trx-4", "trx-5"],
            "created_at": [
                pd.Timestamp("2026-10-15 16:13:54", tz="UTC"),
                pd.Timestamp("2026-10-15 15:59:59", tz="UTC"),
                "not-a-date",
                pd.Timestamp("2026-10-15 20:00:00", tz="UTC"),
                pd.Timestamp("2026-10-15 21:00:00", tz="UTC"),
            ],
            "transaction_number": [" trx-1 ", "trx-2", "trx-3", "trx-4", "trx-5"],
            "total": ["₱1,234.565", 10.005, 5, "abc", 5],
        }
    )


def test_s01_uses_asia_manila_business_date() -> None:
    result = standardize_transactions(_transactions(), TIMEZONE)

    assert result.cleaned.loc[0, "business_date"] == date(2026, 10, 16)
    assert result.cleaned.loc[1, "business_date"] == date(2026, 10, 15)


def test_s01_rejects_invalid_created_at() -> None:
    result = standardize_transactions(_transactions(), TIMEZONE)

    assert "trx-3" in result.rejected["source_record_id"].tolist()
    assert "S-01" in result.rejected["rule_id"].tolist()


def test_s02_strips_currency_and_rounds_half_up() -> None:
    result = standardize_transactions(_transactions(), TIMEZONE)

    assert result.cleaned.loc[0, "total"] == pytest.approx(1234.57)
    assert result.cleaned.loc[1, "total"] == pytest.approx(10.01)


def test_s02_rejects_unparseable_total() -> None:
    result = standardize_transactions(_transactions(), TIMEZONE)

    assert "trx-4" in result.rejected["source_record_id"].tolist()
    assert "S-02" in result.rejected["rule_id"].tolist()


def test_transaction_numbers_are_uppercased() -> None:
    result = standardize_transactions(_transactions(), TIMEZONE)

    assert result.cleaned["transaction_number"].tolist() == [
        "TRX-1",
        "TRX-2",
        "TRX-5",
    ]


def test_s03_falls_back_for_missing_name() -> None:
    frame = pd.DataFrame(
        {
            "item_id": ["item-1", "item-2"],
            "name": [None, "Iced Coffee"],
            "stock_quantity": [3, 4],
        }
    )

    result = standardize_inventory(frame)

    assert result.cleaned.loc[0, "name"] == "UNNAMED ITEM"
    assert result.warnings[0].error_code == "WARN_UNNAMED_ITEM"


def test_s04_casts_stock_to_integer_and_rejects_invalid() -> None:
    frame = pd.DataFrame(
        {
            "item_id": ["item-1", "item-2"],
            "name": ["A", "B"],
            "stock_quantity": ["3", "not-a-number"],
        }
    )

    result = standardize_inventory(frame)

    assert result.cleaned.loc[0, "stock_quantity"] == 3
    assert result.cleaned["stock_quantity"].dtype == "int64"
    assert result.rejected.iloc[0]["rule_id"] == "S-04"


def test_s05_converts_timestamp_to_entry_hour() -> None:
    frame = pd.DataFrame(
        {
            "entry_id": ["entry-1"],
            "number_of_customer": [4],
            "created_at": [pd.Timestamp("2026-10-15 16:13:54", tz="UTC")],
        }
    )

    result = standardize_traffic(frame, date(2026, 10, 16), TIMEZONE)

    assert result.cleaned.loc[0, "business_date"] == date(2026, 10, 16)
    assert result.cleaned.loc[0, "entry_hour"] == 0


def test_s05_invalid_timestamp_warns_and_keeps_row() -> None:
    frame = pd.DataFrame(
        {
            "entry_id": ["entry-1"],
            "number_of_customer": [4],
            "created_at": ["not-a-date"],
        }
    )

    result = standardize_traffic(frame, date(2026, 10, 16), TIMEZONE)

    assert result.cleaned.loc[0, "business_date"] == date(2026, 10, 16)
    assert pd.isna(result.cleaned.loc[0, "entry_hour"])
    assert result.warnings[0].error_code == "WARN_PEAK_HOUR"


def test_traffic_rejects_negative_customer_count() -> None:
    frame = pd.DataFrame(
        {
            "entry_id": ["entry-1"],
            "number_of_customer": [-1],
            "created_at": [pd.Timestamp("2026-10-16 08:00:00", tz="UTC")],
        }
    )

    result = standardize_traffic(frame, date(2026, 10, 16), TIMEZONE)

    assert result.cleaned.empty
    assert result.rejected.iloc[0]["rule_id"] == "C-03"
