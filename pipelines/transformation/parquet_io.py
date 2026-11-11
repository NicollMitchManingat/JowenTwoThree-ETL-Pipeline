"""Atomic Parquet helpers for Stage 2 silver output."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd


def silver_path(
    table: str,
    silver_dir: Path,
    business_date: date | None = None,
) -> Path:
    """Return the silver path for a table, optionally partitioned by date."""

    if business_date is None:
        return silver_dir / table / "part-000.parquet"
    return (
        silver_dir
        / table
        / f"business_date={business_date.isoformat()}"
        / "part-000.parquet"
    )


def write_silver(
    frame: pd.DataFrame,
    table: str,
    silver_dir: Path,
    business_date: date | None = None,
) -> Path:
    """Write a silver table atomically, replacing any previous partition."""

    path = silver_path(table, silver_dir, business_date)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".parquet.tmp")
    frame.to_parquet(temporary, index=False)
    temporary.replace(path)
    return path


def read_silver(
    table: str,
    silver_dir: Path,
    business_date: date | None = None,
) -> pd.DataFrame:
    """Read a previously written silver table."""

    return pd.read_parquet(silver_path(table, silver_dir, business_date))


def remove_silver(
    table: str,
    silver_dir: Path,
    business_date: date | None = None,
) -> None:
    """Remove a silver partition if it exists."""

    silver_path(table, silver_dir, business_date).unlink(missing_ok=True)
