"""Stage 2 date dimension population."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from .parquet_io import read_silver, silver_path, write_silver
from .schema import DIM_DATE_COLUMNS


def date_key(value: date) -> int:
    """Return the integer YYYYMMDD key for a date."""

    return int(f"{value:%Y%m%d}")


def build_dim_date(start: date, end: date) -> pd.DataFrame:
    """Build a contiguous date dimension from ``start`` through ``end``."""

    if end < start:
        raise ValueError("end must be on or after start")

    rows: list[dict[str, object]] = []
    current = start
    while current <= end:
        rows.append(
            {
                "date_key": date_key(current),
                "full_date": current,
                "day_of_month": current.day,
                "month": current.month,
                "month_name": current.strftime("%B"),
                "quarter": (current.month - 1) // 3 + 1,
                "year": current.year,
                "day_of_week": current.weekday(),
                "day_name": current.strftime("%A"),
                "is_weekend": current.weekday() >= 5,
            }
        )
        current += timedelta(days=1)

    return pd.DataFrame(rows, columns=DIM_DATE_COLUMNS)


def ensure_dim_date(start: date, end: date, silver_dir: Path) -> Path:
    """Merge a date range into the silver dim_date table and write it."""

    new_rows = build_dim_date(start, end)
    path = silver_path("dim_date", silver_dir)
    if path.exists():
        existing = read_silver("dim_date", silver_dir)
        combined = pd.concat([existing, new_rows], ignore_index=True)
        combined = combined.drop_duplicates("date_key", keep="last")
        combined = combined.sort_values("date_key").reset_index(drop=True)
    else:
        combined = new_rows

    return write_silver(combined, "dim_date", silver_dir)
