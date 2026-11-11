"""Stage 2 customer traffic summary transformation (rule B-04)."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pandas as pd

from .dimensions import date_key
from .schema import FACT_TRAFFIC_SUMMARY_COLUMNS


def _row(
    business_date: date,
    total_customers: int,
    peak_hour: str | None,
    loaded_at: datetime,
) -> dict[str, object]:
    return {
        "traffic_summary_key": f"TRAFFIC_{date_key(business_date)}",
        "date_key": date_key(business_date),
        "total_customers": total_customers,
        "peak_hour": peak_hour,
        "loaded_at": pd.Timestamp(loaded_at),
    }


def build_traffic_summary(
    frame: pd.DataFrame,
    business_date: date,
    loaded_at: datetime | None = None,
) -> pd.DataFrame:
    """Build the daily customer traffic summary and peak hour."""

    reference = loaded_at or datetime.now(UTC)

    if frame.empty:
        return pd.DataFrame(
            [_row(business_date, 0, None, reference)],
            columns=FACT_TRAFFIC_SUMMARY_COLUMNS,
        )

    subset = frame
    if "business_date" in subset:
        subset = subset.loc[subset["business_date"] == business_date]

    if subset.empty:
        return pd.DataFrame(
            [_row(business_date, 0, None, reference)],
            columns=FACT_TRAFFIC_SUMMARY_COLUMNS,
        )

    total_customers = int(subset["number_of_customer"].sum())
    hourly = (
        subset.dropna(subset=["entry_hour"])
        .groupby("entry_hour")["number_of_customer"]
        .sum()
    )

    peak_hour: str | None = None
    if not hourly.empty:
        highest = hourly.max()
        hour = int(hourly.loc[hourly == highest].index.min())
        peak_hour = f"{hour:02d}:00:00"

    return pd.DataFrame(
        [_row(business_date, total_customers, peak_hour, reference)],
        columns=FACT_TRAFFIC_SUMMARY_COLUMNS,
    )
