from datetime import date
from pathlib import Path

import pandas as pd

from pipelines.transformation.dimensions import (
    build_dim_date,
    date_key,
    ensure_dim_date,
)
from pipelines.transformation.parquet_io import read_silver


def test_date_key_uses_yyyymmdd() -> None:
    assert date_key(date(2026, 10, 12)) == 20261012


def test_build_dim_date_covers_inclusive_range() -> None:
    frame = build_dim_date(date(2026, 10, 30), date(2026, 11, 2))

    assert len(frame) == 4
    assert frame["date_key"].tolist() == [20261030, 20261031, 20261101, 20261102]
    assert frame["month_name"].tolist() == ["October", "October", "November", "November"]


def test_build_dim_date_marks_weekends() -> None:
    frame = build_dim_date(date(2026, 10, 30), date(2026, 11, 2))

    assert frame["is_weekend"].tolist() == [False, True, True, False]
    assert frame["day_name"].tolist() == ["Friday", "Saturday", "Sunday", "Monday"]


def test_build_dim_date_handles_leap_day() -> None:
    frame = build_dim_date(date(2028, 2, 28), date(2028, 3, 1))

    assert frame["date_key"].tolist() == [20280228, 20280229, 20280301]
    assert frame["quarter"].tolist() == [1, 1, 1]


def test_ensure_dim_date_merges_existing_rows(tmp_path: Path) -> None:
    silver_dir = tmp_path / "silver"

    ensure_dim_date(date(2026, 10, 12), date(2026, 10, 13), silver_dir)
    ensure_dim_date(date(2026, 10, 13), date(2026, 10, 14), silver_dir)

    frame = read_silver("dim_date", silver_dir)
    assert frame["date_key"].tolist() == [20261012, 20261013, 20261014]
    assert not frame["date_key"].duplicated().any()


def test_ensure_dim_date_rejects_reversed_range(tmp_path: Path) -> None:
    silver_dir = tmp_path / "silver"

    try:
        ensure_dim_date(date(2026, 10, 14), date(2026, 10, 12), silver_dir)
    except ValueError as exc:
        assert "end must be on or after start" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_dim_date_is_written_as_parquet(tmp_path: Path) -> None:
    silver_dir = tmp_path / "silver"

    path = ensure_dim_date(date(2026, 10, 12), date(2026, 10, 12), silver_dir)

    assert path.exists()
    frame = pd.read_parquet(path)
    assert list(frame.columns) == [
        "date_key",
        "full_date",
        "day_of_month",
        "month",
        "month_name",
        "quarter",
        "year",
        "day_of_week",
        "day_name",
        "is_weekend",
    ]
