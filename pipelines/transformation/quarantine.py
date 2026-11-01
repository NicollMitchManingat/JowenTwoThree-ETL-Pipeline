"""Stage 2 rejected-record quarantine persistence."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd

from .schema import QUARANTINE_COLUMNS

SOURCE_TABLES = ("transactions", "inventory", "customer_traffic")


def build_quarantine(
    rejected_frames: Sequence[pd.DataFrame],
    run_id: str,
    parent_run_id: str,
    rejected_at: datetime | None = None,
) -> pd.DataFrame:
    """Combine rejected frames into the quarantine contract."""

    frames = [frame for frame in rejected_frames if not frame.empty]
    if not frames:
        return pd.DataFrame(columns=QUARANTINE_COLUMNS)

    combined = pd.concat(frames, ignore_index=True)
    combined["run_id"] = run_id
    combined["parent_run_id"] = parent_run_id
    combined["rejected_at"] = pd.Timestamp(rejected_at or datetime.now(UTC))
    return combined[QUARANTINE_COLUMNS]


def quarantine_path(
    source_table: str,
    business_date: date,
    quarantine_dir: Path,
) -> Path:
    """Return the quarantine partition path for a source table and date."""

    return (
        quarantine_dir
        / source_table
        / f"transform_date={business_date.isoformat()}"
        / "part-000.parquet"
    )


def write_quarantine(
    frame: pd.DataFrame,
    business_date: date,
    quarantine_dir: Path,
    source_tables: Sequence[str] = SOURCE_TABLES,
) -> dict[str, Path]:
    """Write one quarantine partition per source table, clearing stale ones."""

    written: dict[str, Path] = {}
    for table in source_tables:
        if frame.empty:
            subset = frame.iloc[0:0]
        else:
            subset = frame.loc[frame["source_table"] == table]

        path = quarantine_path(table, business_date, quarantine_dir)
        if subset.empty:
            path.unlink(missing_ok=True)
            continue

        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".parquet.tmp")
        subset.to_parquet(temporary, index=False)
        temporary.replace(path)
        written[table] = path

    return written


def read_quarantine(
    source_table: str,
    business_date: date,
    quarantine_dir: Path,
) -> pd.DataFrame:
    """Read a quarantine partition."""

    return pd.read_parquet(
        quarantine_path(source_table, business_date, quarantine_dir)
    )
