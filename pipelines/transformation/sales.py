"""Stage 2 sales summary transformation (rules B-01 and B-02)."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pandas as pd

from .dimensions import date_key
from .schema import FACT_SALES_SUMMARY_COLUMNS


def build_sales_summary(
    frame: pd.DataFrame,
    loaded_at: datetime | None = None,
    business_date: date | None = None,
) -> pd.DataFrame:
    """Aggregate standardized transactions into one row per business date."""

    reference = loaded_at or datetime.now(UTC)

    if business_date is not None and "business_date" in frame:
        frame = frame.loc[frame["business_date"] == business_date]

    if frame.empty:
        if business_date is None:
            return pd.DataFrame(columns=FACT_SALES_SUMMARY_COLUMNS)
        row = {
            "sales_summary_key": f"SALES_{date_key(business_date)}",
            "date_key": date_key(business_date),
            "total_sales": 0.0,
            "total_orders": 0,
            "loaded_at": pd.Timestamp(reference),
        }
        return pd.DataFrame([row], columns=FACT_SALES_SUMMARY_COLUMNS)

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
    grouped["loaded_at"] = pd.Timestamp(reference)

    return grouped[FACT_SALES_SUMMARY_COLUMNS]
