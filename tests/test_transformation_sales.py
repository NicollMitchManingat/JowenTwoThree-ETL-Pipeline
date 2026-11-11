from datetime import UTC, date, datetime

import pandas as pd

from pipelines.transformation.sales import build_sales_summary
from pipelines.transformation.schema import FACT_SALES_SUMMARY_COLUMNS

LOADED_AT = datetime(2026, 11, 1, 0, 0, tzinfo=UTC)


def _transactions() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "transaction_id": ["trx-1", "trx-2", "trx-3", "trx-1"],
            "business_date": [
                date(2026, 10, 16),
                date(2026, 10, 16),
                date(2026, 10, 15),
                date(2026, 10, 16),
            ],
            "total": [245.50, 89.00, 10.25, 245.50],
        }
    )


def test_b01_and_b02_aggregate_per_day() -> None:
    result = build_sales_summary(_transactions(), loaded_at=LOADED_AT)

    row = result.loc[result["date_key"] == 20261016].iloc[0]
    assert row["total_sales"] == 580.00
    assert row["total_orders"] == 2


def test_sales_summary_key_and_columns() -> None:
    result = build_sales_summary(_transactions(), loaded_at=LOADED_AT)

    assert list(result.columns) == FACT_SALES_SUMMARY_COLUMNS
    assert result["sales_summary_key"].tolist() == ["SALES_20261015", "SALES_20261016"]


def test_empty_input_returns_empty_contract() -> None:
    result = build_sales_summary(pd.DataFrame(), loaded_at=LOADED_AT)

    assert result.empty
    assert list(result.columns) == FACT_SALES_SUMMARY_COLUMNS


def test_refund_only_date_retains_negative_net_and_exact_money():
    from decimal import Decimal
    refunds = pd.DataFrame({"business_date": [date(2026, 10, 16)], "refund_amount": [Decimal("450.00")]})
    result = build_sales_summary(pd.DataFrame(), business_date=date(2026, 10, 16), refunds=refunds)
    row = result.iloc[0]
    assert row["total_sales"] == Decimal("0.00")
    assert row["refund_amount"] == Decimal("450.00")
    assert row["net_sales"] == Decimal("-450.00")
    assert row["total_orders"] == 0
