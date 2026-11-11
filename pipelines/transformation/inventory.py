"""Exact, active inventory snapshots with per-item thresholds."""
from datetime import UTC, date, datetime

import pandas as pd

from pipelines.money import decimal_value

from .dimensions import date_key
from .schema import FACT_INVENTORY_SUMMARY_COLUMNS


def build_inventory_summary(frame: pd.DataFrame, business_date: date,
                            low_stock_threshold: int = 5,
                            loaded_at: datetime | None = None) -> pd.DataFrame:
    if low_stock_threshold < 0:
        raise ValueError("low_stock_threshold cannot be negative")
    active = frame.loc[frame["is_active"]] if "is_active" in frame else frame
    low = out = 0
    for record in active.to_dict("records"):
        stock = decimal_value(record["stock_quantity"])
        threshold = decimal_value(record.get("low_stock_threshold", low_stock_threshold))
        if stock < 0 or threshold < 0:
            raise ValueError("stock and threshold must be nonnegative")
        out += stock == 0
        low += 0 < stock <= threshold
    timestamp = pd.Timestamp(loaded_at or datetime.now(UTC))
    row = {"inventory_summary_key": f"INV_{date_key(business_date)}",
           "date_key": date_key(business_date), "total_items": len(active),
           "low_stock_items": int(low), "out_of_stock_items": int(out),
           "threshold_policy": "PER_ITEM", "snapshot_at": timestamp, "loaded_at": timestamp}
    return pd.DataFrame([row], columns=FACT_INVENTORY_SUMMARY_COLUMNS)
