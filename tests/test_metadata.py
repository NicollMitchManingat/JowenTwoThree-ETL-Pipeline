import json
import os
import re
from datetime import datetime
from pathlib import Path

from pipelines.extraction.metadata import (
    TableRunMetadata,
    new_run_id,
    purge_old_logs,
    read_run_log,
    write_run_log,
)
from pipelines.logging_setup import configure_logging


def _metadata(run_id: str, table: str = "transactions") -> TableRunMetadata:
    return TableRunMetadata(
        pipeline_run_id=run_id,
        table_name=table,
        extraction_start_timestamp="2026-10-13 23:00:00",
        extraction_end_timestamp="2026-10-13 23:00:15",
        extraction_status="SUCCESS",
        records_read=1250,
        records_extracted=1250,
        records_rejected=5,
        extraction_window="2026-10-12 00:00:00 to 23:59:59",
        validation_results="PASS",
        extraction_duration_seconds=15.0,
    )


def test_new_run_id_format_and_uniqueness() -> None:
    reference = datetime(2026, 10, 13, 23, 0, 0)

    first = new_run_id(reference)
    second = new_run_id(reference)

    assert re.fullmatch(r"Run_20261013_230000_[0-9a-f]{6}", first)
    assert first != second


def test_write_run_log_contains_all_tables(tmp_path: Path) -> None:
    run_id = new_run_id(datetime(2026, 10, 13, 23, 0, 0))
    entries = [_metadata(run_id, "transactions"), _metadata(run_id, "inventory")]

    path = write_run_log(entries, tmp_path)
    payload = read_run_log(path)

    assert path.name == f"{run_id}.json"
    assert payload["pipeline_run_id"] == run_id
    assert len(payload["tables"]) == 2
    assert payload["tables"][0]["records_extracted"] == 1250
    assert payload["tables"][0]["extraction_duration_seconds"] == 15.0


def test_write_run_log_rejects_mixed_run_ids(tmp_path: Path) -> None:
    entries = [_metadata("Run_A"), _metadata("Run_B")]

    try:
        write_run_log(entries, tmp_path)
    except ValueError as exc:
        assert "same pipeline_run_id" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_write_run_log_rejects_empty_entries(tmp_path: Path) -> None:
    try:
        write_run_log([], tmp_path)
    except ValueError as exc:
        assert "at least one" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_purge_old_logs_removes_expired_files(tmp_path: Path) -> None:
    old = tmp_path / "Run_20260101_000000_aaaaaa.json"
    recent = tmp_path / "Run_20261013_000000_bbbbbb.json"
    old.write_text(json.dumps({"pipeline_run_id": "old"}), encoding="utf-8")
    recent.write_text(json.dumps({"pipeline_run_id": "recent"}), encoding="utf-8")
    old_timestamp = datetime(2026, 1, 1).timestamp()
    os.utime(old, (old_timestamp, old_timestamp))

    removed = purge_old_logs(
        tmp_path,
        retention_days=90,
        now=datetime(2026, 10, 13, 12, 0, 0),
    )

    assert old in removed
    assert not old.exists()
    assert recent.exists()


def test_configure_logging_returns_logger() -> None:
    logger = configure_logging("DEBUG")

    assert logger.name == "jowen_etl"
    assert logger.level == 10
