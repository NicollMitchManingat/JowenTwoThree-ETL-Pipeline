from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd

from pipelines.config import load_config
from pipelines.extraction.metadata import read_run_log
from pipelines.extraction.parquet_io import read_parquet
from pipelines.extraction.run import resolve_business_date, run_extraction

BUSINESS_DATE = date(2026, 10, 12)
TABLES = ("transactions", "inventory", "customer_traffic")


def _config(tmp_path: Path):
    return load_config(
        {
            "ETL_MOCK_MODE": "true",
            "ETL_STAGING_DIR": str(tmp_path / "bronze"),
            "ETL_LOG_DIR": str(tmp_path / "logs"),
            "ETL_HANDOVER_DIR": str(tmp_path / "handover"),
        }
    )


def test_mock_run_writes_parquet_and_log(tmp_path: Path) -> None:
    config = _config(tmp_path)

    result = run_extraction(BUSINESS_DATE, config)

    assert result.status == "SUCCESS"
    assert result.decision == "APPROVED"
    assert result.validation_passed is True
    assert set(result.parquet_paths) == set(TABLES)
    for path in result.parquet_paths.values():
        assert path.exists()
    assert result.log_path is not None
    assert result.log_path.exists()
    assert result.handover_path is not None
    assert result.handover_path.exists()

    payload = read_run_log(result.log_path)
    assert payload["pipeline_run_id"] == result.run_id
    assert len(payload["tables"]) == 3


def test_dry_run_writes_nothing(tmp_path: Path) -> None:
    config = _config(tmp_path)

    result = run_extraction(BUSINESS_DATE, config, dry_run=True)

    assert result.status == "SUCCESS"
    assert result.parquet_paths == {}
    assert result.log_path is None
    assert result.handover_path is None
    assert not (tmp_path / "bronze").exists()
    assert not (tmp_path / "logs").exists()
    assert not (tmp_path / "handover").exists()


def test_rerun_overwrites_same_partition(tmp_path: Path) -> None:
    config = _config(tmp_path)

    first = run_extraction(BUSINESS_DATE, config)
    second = run_extraction(BUSINESS_DATE, config)

    assert first.parquet_paths["transactions"] == second.parquet_paths["transactions"]
    staged = read_parquet("transactions", BUSINESS_DATE, config.etl.staging_dir)
    assert len(staged) == 125
    assert not staged["transaction_id"].duplicated().any()


def test_full_mode_runs_in_mock_mode(tmp_path: Path) -> None:
    config = _config(tmp_path)

    result = run_extraction(BUSINESS_DATE, config, full=True)

    assert result.status == "SUCCESS"
    payload = read_run_log(result.log_path) if result.log_path else {}
    assert payload["tables"][0]["extraction_window"] == "full extraction"


def test_resolve_business_date_explicit() -> None:
    config = load_config({"ETL_MOCK_MODE": "true"})

    assert resolve_business_date("2026-10-12", config) == BUSINESS_DATE


def test_resolve_business_date_defaults_to_previous_day() -> None:
    config = load_config({"ETL_MOCK_MODE": "true"})
    reference = datetime(2026, 10, 13, 1, 0, tzinfo=UTC)

    assert resolve_business_date(None, config, now=reference) == BUSINESS_DATE


def test_staged_frames_match_extracted_columns(tmp_path: Path) -> None:
    config = _config(tmp_path)
    result = run_extraction(BUSINESS_DATE, config)

    sales = pd.read_parquet(result.parquet_paths["transactions"])

    assert list(sales.columns) == [
        "transaction_id",
        "created_at",
        "transaction_number",
        "total",
    ]
