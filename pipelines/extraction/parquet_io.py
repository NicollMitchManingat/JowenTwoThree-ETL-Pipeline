"""Atomic Parquet staging helpers."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd


def partition_path(table: str, extract_date: date, staging_dir: Path) -> Path:
    """Return the Hive-style partition path for a table and business date."""

    return (
        staging_dir
        / table
        / f"extract_date={extract_date.isoformat()}"
        / "part-000.parquet"
    )


def write_parquet(
    frame: pd.DataFrame,
    table: str,
    extract_date: date,
    staging_dir: Path,
) -> Path:
    """Write a partition atomically, replacing any previous run for that date."""

    path = partition_path(table, extract_date, staging_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".parquet.tmp")
    frame.to_parquet(temporary, index=False)
    temporary.replace(path)
    return path


def read_parquet(table: str, extract_date: date, staging_dir: Path) -> pd.DataFrame:
    """Read a previously staged partition."""

    return pd.read_parquet(partition_path(table, extract_date, staging_dir))
