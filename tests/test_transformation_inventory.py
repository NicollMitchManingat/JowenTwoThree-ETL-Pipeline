from datetime import UTC, date, datetime

import pandas as pd
import pytest

from pipelines.transformation.inventory import build_inventory_summary
from pipelines.transformation.schema import FACT_INVENTORY_SUMMARY_COLUMNS

BUSINESS_DATE = date(2026, 10, 16)
LOADED_AT = datetime(2026, 11, 1, 0, 0, tzinfo=UTC)


def _inventory() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "item_id": ["item-1", "item-2", "item-3", "item-4"],
            "name": ["A", "B", "C", "D"],
            "stock_quantity": [0, 1, 5, 6],
        }
    )


def test_b03_counts_low_and_out_of_stock_items() -> None:
    result = build_inventory_summary(
        _inventory(), BUSINESS_DATE, low_stock_threshold=5, loaded_at=LOADED_AT
    )

    row = result.iloc[0]
    assert row["total_items"] == 4
    assert row["low_stock_items"] == 3
    assert row["out_of_stock_items"] == 1
    assert row["low_stock_threshold"] == 5


def test_inventory_summary_key_and_columns() -> None:
    result = build_inventory_summary(
        _inventory(), BUSINESS_DATE, low_stock_threshold=5, loaded_at=LOADED_AT
    )

    assert list(result.columns) == FACT_INVENTORY_SUMMARY_COLUMNS
    assert result.iloc[0]["inventory_summary_key"] == "INV_20261016"
    assert result.iloc[0]["date_key"] == 20261016


def test_empty_inventory_writes_zero_value_fact() -> None:
    result = build_inventory_summary(
        pd.DataFrame(), BUSINESS_DATE, low_stock_threshold=5, loaded_at=LOADED_AT
    )

    row = result.iloc[0]
    assert row["total_items"] == 0
    assert row["low_stock_items"] == 0
    assert row["out_of_stock_items"] == 0


def test_negative_threshold_is_rejected() -> None:
    with pytest.raises(ValueError, match="low_stock_threshold"):
        build_inventory_summary(
            _inventory(), BUSINESS_DATE, low_stock_threshold=-1, loaded_at=LOADED_AT
        )
