from datetime import date

import pandas as pd

from pipelines.extraction import mock_data

BUSINESS_DATE = date(2026, 10, 12)


def test_transactions_are_deterministic() -> None:
    first = mock_data.transactions(BUSINESS_DATE)
    second = mock_data.transactions(BUSINESS_DATE)

    pd.testing.assert_frame_equal(first, second)


def test_transactions_columns_and_values() -> None:
    frame = mock_data.transactions(BUSINESS_DATE)

    assert list(frame.columns) == [
        "transaction_id",
        "created_at",
        "transaction_number",
        "total",
    ]
    assert len(frame) == 5
    assert frame["created_at"].dt.date.eq(BUSINESS_DATE).all()
    assert frame.loc[0, "transaction_number"] == "  TRX-1001  "


def test_inventory_contains_varied_stock_levels() -> None:
    frame = mock_data.inventory()

    assert list(frame.columns) == ["item_id", "name", "stock_quantity"]
    assert (frame["stock_quantity"] < 0).any()
    assert (frame["stock_quantity"] == 0).any()
    assert frame["name"].str.contains("iced LATTE").any()


def test_customer_traffic_contains_null_count() -> None:
    frame = mock_data.customer_traffic(BUSINESS_DATE)

    assert list(frame.columns) == ["entry_id", "number_of_customer", "created_at"]
    assert frame["number_of_customer"].isna().any()
    assert frame["created_at"].dt.date.eq(BUSINESS_DATE).all()
