from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd

from pipelines.extraction import mock_data
from pipelines.transformation.cleaning import clean_inventory
from pipelines.transformation.quarantine import (
    build_quarantine,
    quarantine_path,
    read_quarantine,
    write_quarantine,
)
from pipelines.transformation.schema import QUARANTINE_COLUMNS

BUSINESS_DATE = date(2026, 10, 16)
REJECTED_AT = datetime(2026, 11, 1, 0, 0, tzinfo=UTC)


def _rejected() -> pd.DataFrame:
    return clean_inventory(mock_data.inventory()).rejected


def test_build_quarantine_adds_run_context() -> None:
    frame = build_quarantine(
        [_rejected()], "Transform_run", "Run_parent", rejected_at=REJECTED_AT
    )

    assert list(frame.columns) == QUARANTINE_COLUMNS
    assert (frame["run_id"] == "Transform_run").all()
    assert (frame["parent_run_id"] == "Run_parent").all()
    assert frame.iloc[0]["rule_id"] == "C-04"
    assert "Blueberry Muffin" in frame.iloc[0]["payload"]


def test_build_quarantine_empty_input_keeps_contract() -> None:
    frame = build_quarantine([], "Transform_run", "Run_parent")

    assert frame.empty
    assert list(frame.columns) == QUARANTINE_COLUMNS


def test_write_and_read_quarantine_partition(tmp_path: Path) -> None:
    frame = build_quarantine(
        [_rejected()], "Transform_run", "Run_parent", rejected_at=REJECTED_AT
    )
    quarantine_dir = tmp_path / "quarantine"

    written = write_quarantine(frame, BUSINESS_DATE, quarantine_dir)

    assert "inventory" in written
    assert written["inventory"].exists()
    stored = read_quarantine("inventory", BUSINESS_DATE, quarantine_dir)
    assert len(stored) == 1
    assert stored.iloc[0]["source_table"] == "inventory"


def test_quarantine_path_uses_transform_date(tmp_path: Path) -> None:
    path = quarantine_path("inventory", BUSINESS_DATE, tmp_path)

    assert path == (
        tmp_path
        / "inventory"
        / "transform_date=2026-10-16"
        / "part-000.parquet"
    )


def test_empty_rerun_removes_stale_quarantine(tmp_path: Path) -> None:
    frame = build_quarantine(
        [_rejected()], "Transform_run", "Run_parent", rejected_at=REJECTED_AT
    )
    quarantine_dir = tmp_path / "quarantine"
    write_quarantine(frame, BUSINESS_DATE, quarantine_dir)
    stale = quarantine_path("inventory", BUSINESS_DATE, quarantine_dir)
    assert stale.exists()

    write_quarantine(
        build_quarantine([], "Transform_run2", "Run_parent"),
        BUSINESS_DATE,
        quarantine_dir,
    )

    assert not stale.exists()
