from datetime import date
from pathlib import Path

from pipelines.config import load_config
from pipelines.extraction.run import run_extraction
from pipelines.transformation.run import main

BUSINESS_DATE = date(2026, 10, 16)


def _config(tmp_path: Path):
    return load_config(
        {
            "ETL_MOCK_MODE": "true",
            "ETL_STAGING_DIR": str(tmp_path / "bronze"),
            "ETL_LOG_DIR": str(tmp_path / "logs"),
            "ETL_HANDOVER_DIR": str(tmp_path / "handover"),
            "ETL_SILVER_DIR": str(tmp_path / "silver"),
            "ETL_QUARANTINE_DIR": str(tmp_path / "quarantine"),
            "ETL_TRANSFORM_LOG_DIR": str(tmp_path / "logs" / "transformation"),
            "ETL_LOOKBACK_DAYS": "1",
            "ETL_LOW_STOCK_THRESHOLD": "5",
        }
    )


def _set_env(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("ETL_MOCK_MODE", "true")
    monkeypatch.setenv("ETL_STAGING_DIR", str(tmp_path / "bronze"))
    monkeypatch.setenv("ETL_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setenv("ETL_HANDOVER_DIR", str(tmp_path / "handover"))
    monkeypatch.setenv("ETL_SILVER_DIR", str(tmp_path / "silver"))
    monkeypatch.setenv("ETL_QUARANTINE_DIR", str(tmp_path / "quarantine"))
    monkeypatch.setenv(
        "ETL_TRANSFORM_LOG_DIR", str(tmp_path / "logs" / "transformation")
    )
    monkeypatch.setenv("ETL_LOOKBACK_DAYS", "1")
    monkeypatch.setenv("ETL_LOW_STOCK_THRESHOLD", "5")


def test_cli_demo_runs_transformation_end_to_end(tmp_path: Path, monkeypatch) -> None:
    config = _config(tmp_path)
    assert run_extraction(BUSINESS_DATE, config).status == "SUCCESS"
    _set_env(monkeypatch, tmp_path)

    code = main(["--demo", "--date", BUSINESS_DATE.isoformat()])

    assert code == 0
    manifests = list((tmp_path / "handover").glob("Transform_*/manifest.json"))
    assert len(manifests) == 1
    assert (tmp_path / "silver" / "fact_sales_summary").exists()
    assert (tmp_path / "logs" / "transformation").exists()


def test_cli_dry_run_writes_no_transformation_output(
    tmp_path: Path, monkeypatch
) -> None:
    config = _config(tmp_path)
    assert run_extraction(BUSINESS_DATE, config).status == "SUCCESS"
    _set_env(monkeypatch, tmp_path)

    code = main(["--demo", "--date", BUSINESS_DATE.isoformat(), "--dry-run"])

    assert code == 0
    assert not (tmp_path / "silver").exists()
    assert not (tmp_path / "quarantine").exists()
    assert not (tmp_path / "logs" / "transformation").exists()
    assert not list((tmp_path / "handover").glob("Transform_*"))
