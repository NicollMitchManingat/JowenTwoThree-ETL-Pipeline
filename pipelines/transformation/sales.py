"""Stage 2 sales summary transformation (rules B-01 and B-02)."""

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from .dimensions import date_key
from .schema import FACT_SALES_SUMMARY_COLUMNS


def build_sales_summary(
    frame: pd.DataFrame,
    loaded_at: datetime | None = None,
) -> pd.DataFrame:
    """Aggregate standardized transactions into one row per business date."""

    if frame.empty:
        return pd.DataFrame(columns=FACT_SALES_SUMMARY_COLUMNS)

    grouped = (
        frame.groupby("business_date", as_index=False)
        .agg(
            total_sales=("total", "sum"),
            total_orders=("transaction_id", "nunique"),
        )
        .sort_values("business_date")
        .reset_index(drop=True)
    )

    grouped["total_sales"] = grouped["total_sales"].round(2)
    grouped["date_key"] = grouped["business_date"].map(date_key)
    grouped["sales_summary_key"] = "SALES_" + grouped["date_key"].astype(str)
    grouped["loaded_at"] = pd.Timestamp(loaded_at or datetime.now(UTC))

    return grouped[FACT_SALES_SUMMARY_COLUMNS]
