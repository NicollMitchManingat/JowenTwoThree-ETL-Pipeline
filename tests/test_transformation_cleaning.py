from datetime import date

import pandas as pd

from pipelines.extraction import mock_data
from pipelines.transformation.cleaning import (
    clean_inventory,
    clean_traffic,
    clean_transactions,
)
from pipelines.transformation.rejects import REJECT_COLUMNS

BUSINESS_DATE = date(2026, 10, 12)


def test_c01_trims_transaction_number() -> None:
    result = clean_transactions(mock_data.transactions(BUSINESS_DATE))

    assert result.cleaned.loc[0, "transaction_number"] == "TRX-1001"
    assert result.rejected.empty


def test_c02_title_cases_inventory_name() -> None:
    result = clean_inventory(mock_data.inventory())

    assert "Iced Latte" in result.cleaned["name"].tolist()


def test_c04_rejects_negative_stock() -> None:
    result = clean_inventory(mock_data.inventory())

    assert len(result.rejected) == 1
    rejected = result.rejected.iloc[0]
    assert rejected["source_table"] == "inventory"
    assert rejected["rule_id"] == "C-04"
    assert rejected["error_code"] == "ERR_NEGATIVE_STOCK"
    assert "Blueberry Muffin" in rejected["payload"]
    assert (result.cleaned["stock_quantity"] >= 0).all()


def test_c03_replaces_null_customer_count_with_zero() -> None:
    result = clean_traffic(mock_data.customer_traffic(BUSINESS_DATE))

    assert result.cleaned["number_of_customer"].isna().sum() == 0
    assert result.rejected.empty


def test_empty_rejects_keep_standard_columns() -> None:
    result = clean_transactions(mock_data.transactions(BUSINESS_DATE))

    assert list(result.rejected.columns) == REJECT_COLUMNS


def test_cleaning_does_not_mutate_input() -> None:
    frame = mock_data.transactions(BUSINESS_DATE)
    original = frame.copy()

    clean_transactions(frame)

    pd.testing.assert_frame_equal(frame, original)
