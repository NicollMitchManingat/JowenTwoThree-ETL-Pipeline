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



def test_v2_packages_are_immutable_and_detect_changed_input(tmp_path):
    import pytest

    from pipelines.artifacts import read_package
    from pipelines.extraction.batch import stage_extraction
    from pipelines.transformation.batch import transform_batch

    config = load_config({"ETL_MOCK_MODE": "true", "ETL_STAGING_DIR": str(tmp_path / "bronze"),
                          "ETL_SILVER_DIR": str(tmp_path / "silver")})
    first = stage_extraction(date(2026, 10, 15), date(2026, 10, 16), config)
    second = stage_extraction(date(2026, 10, 15), date(2026, 10, 16), config)
    assert first != second
    output = transform_batch(first, config)
    manifest, frames = read_package(output, "transformation")
    assert manifest["decision"] == "APPROVED"
    assert len(frames["fact_sales_summary"]) == 2
    assert len(frames["fact_traffic_summary"]) == 2
    snapshot = frames["fact_inventory_summary"].iloc[0]
    assert str(snapshot["snapshot_at"].date()).replace("-", "") == str(snapshot["date_key"])
    raw_manifest, _ = read_package(first, "extraction")
    target = first.parent / raw_manifest["tables"]["transactions"]["path"]
    original = target.read_bytes()
    target.write_bytes(original + b"changed")
    with pytest.raises(ValueError, match="checksum"):
        transform_batch(first, config)
    read_package(second, "extraction")  # The other run remains intact.


def test_v2_stage2_holds_financial_rejections(tmp_path):
    from pipelines.artifacts import read_package, write_package
    from pipelines.extraction.batch import extract_batch
    from pipelines.transformation.batch import transform_batch
    config = load_config({"ETL_MOCK_MODE": "true", "ETL_STAGING_DIR": str(tmp_path / "bronze"),
                          "ETL_SILVER_DIR": str(tmp_path / "silver")})
    raw, capture = extract_batch(date(2026, 10, 16), date(2026, 10, 16), config)
    raw["transactions"]["total"] = raw["transactions"]["total"].astype(str)
    raw["transactions"].loc[0, "total"] = "invalid"
    source = write_package(config.etl.staging_dir, "Run_bad_money", "extraction", raw,
                           {"dates": ["2026-10-16"], "snapshot_at": capture.isoformat(),
                            "source_mode": "mock"})
    output = transform_batch(source, config)
    result, _ = read_package(output, "transformation", approved=False)
    assert result["decision"] == "REJECTED"



def test_local_warehouse_rolls_back_postload_failure(tmp_path, monkeypatch):
    import os
    from dataclasses import replace

    import pytest

    from pipelines.extraction.batch import stage_extraction
    from pipelines.load import runner
    from pipelines.load.target import warehouse_config
    from pipelines.transformation.batch import transform_batch

    database = os.environ.get("WAREHOUSE_TEST_DATABASE")
    if not database:
        pytest.skip("set WAREHOUSE_TEST_DATABASE to a prepared local demo/test database")
    assert database.endswith(("_test", "_demo")), "never run destructive fault tests on reporting data"
    target = replace(warehouse_config(), dbname=database, allow_demo=True)
    config = load_config({"ETL_MOCK_MODE": "true", "ETL_STAGING_DIR": str(tmp_path / "bronze"),
                          "ETL_SILVER_DIR": str(tmp_path / "silver")})
    source = stage_extraction(date(2026, 10, 16), date(2026, 10, 16), config)
    initial = transform_batch(source, config)
    assert runner.load_batch(initial, target, tmp_path / "logs")["status"] == "SUCCESS"

    def contents():
        with target.connect() as connection:
            return {table: connection.execute(
                f"SELECT row_to_json(t)::text FROM dw_analytics.{table} t ORDER BY date_key").fetchall()
                    for table in runner.TABLES}

    before = contents()
    correction = transform_batch(source, config)
    def reject_after_writes(connection, frames):
        raise ValueError("simulated post-load financial mismatch")
    monkeypatch.setattr(runner, "validate_loaded", reject_after_writes)
    result = runner.load_batch(correction, target, tmp_path / "logs")
    assert result["status"] == "FAILED"
    assert contents() == before
    assert (tmp_path / "logs" / (result["load_run_id"] + ".json")).exists()
    assert all(c["loaded"] == 0 and c["rejected"] == c["received"] for c in result["tables"].values())
    with target.connect() as connection:
        assert connection.execute("SELECT count(*) FROM dw_analytics.load_log WHERE load_run_id=%s AND status='FAILED'",
                                  (result["load_run_id"],)).fetchone()[0] == 4
