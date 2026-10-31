from datetime import date
from pathlib import Path

import pandas as pd

from pipelines.config import load_config
from pipelines.extraction.extract import SALES_COLUMNS, extract_sales
from pipelines.extraction.parquet_io import read_parquet, write_parquet

BUSINESS_DATE = date(2026, 10, 12)


def test_extract_sales_in_mock_mode() -> None:
    config = load_config({"ETL_MOCK_MODE": "true"})

    frame = extract_sales(BUSINESS_DATE, config)

    assert list(frame.columns) == SALES_COLUMNS
    assert len(frame) == 5
    assert frame["created_at"].dt.date.eq(BUSINESS_DATE).all()


def test_extract_sales_requires_connection_outside_mock_mode() -> None:
    config = load_config(
        {
            "ETL_MOCK_MODE": "false",
            "SUPABASE_DB_HOST": "db.example.com",
            "SUPABASE_DB_NAME": "postgres",
            "SUPABASE_DB_USER": "postgres.project",
            "SUPABASE_DB_PASSWORD": "secret",
        }
    )

    try:
        extract_sales(BUSINESS_DATE, config)
    except ValueError as exc:
        assert "connection" in str(exc).lower()
    else:
        raise AssertionError("expected ValueError")


def test_parquet_round_trip_and_overwrite(tmp_path: Path) -> None:
    config = load_config({"ETL_MOCK_MODE": "true"})
    frame = extract_sales(BUSINESS_DATE, config)

    path = write_parquet(frame, "transactions", BUSINESS_DATE, tmp_path)
    assert path.exists()

    write_parquet(frame.iloc[:2], "transactions", BUSINESS_DATE, tmp_path)
    staged = read_parquet("transactions", BUSINESS_DATE, tmp_path)

    pd.testing.assert_frame_equal(staged, frame.iloc[:2].reset_index(drop=True))
