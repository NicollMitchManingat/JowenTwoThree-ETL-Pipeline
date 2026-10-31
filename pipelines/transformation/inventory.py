"""Stage 2 inventory summary transformation (rule B-03)."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pandas as pd

from .dimensions import date_key
from .schema import FACT_INVENTORY_SUMMARY_COLUMNS


def build_inventory_summary(
    frame: pd.DataFrame,
    business_date: date,
    low_stock_threshold: int,
    loaded_at: datetime | None = None,
) -> pd.DataFrame:
    """Build the daily inventory snapshot summary."""

    if low_stock_threshold < 0:
        raise ValueError("low_stock_threshold cannot be negative")

    stock = pd.to_numeric(frame.get("stock_quantity", pd.Series(dtype="float64")))
    total_items = len(frame)
    low_stock_items = int((stock <= low_stock_threshold).sum())
    out_of_stock_items = int((stock == 0).sum())
    key = f"INV_{date_key(business_date)}"

    row = {
        "inventory_summary_key": key,
        "date_key": date_key(business_date),
        "total_items": total_items,
        "low_stock_items": low_stock_items,
        "out_of_stock_items": out_of_stock_items,
        "low_stock_threshold": low_stock_threshold,
        "loaded_at": pd.Timestamp(loaded_at or datetime.now(UTC)),
    }
    return pd.DataFrame([row], columns=FACT_INVENTORY_SUMMARY_COLUMNS)
