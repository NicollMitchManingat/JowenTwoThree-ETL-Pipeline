import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from pipelines.transformation.metadata import (
    TransformRunMetadata,
    new_transform_run_id,
    purge_old_transform_logs,
    read_transform_log,
    write_transform_log,
)


def _entry(run_id: str, table: str) -> TransformRunMetadata:
    return TransformRunMetadata(
        run_id=run_id,
        parent_run_id="Run_20261101_000000_abcdef",
        table_name=table,
        batch_window="2026-10-16 to 2026-10-16",
        records_read=10,
        records_output=9,
        records_rejected=1,
        status="SUCCESS",
        started_at="2026-11-01T00:00:00+00:00",
        finished_at="2026-11-01T00:00:01+00:00",
        duration_seconds=1.0,
    )


def test_new_transform_run_id_has_prefix() -> None:
    run_id = new_transform_run_id(datetime(2026, 11, 1, 2, 0, tzinfo=UTC))

    assert run_id.startswith("Transform_20261101_020000_")


def test_write_and_read_transform_log(tmp_path: Path) -> None:
    run_id = "Transform_20261101_020000_abcdef"
    entries = [_entry(run_id, "fact_sales_summary"), _entry(run_id, "dim_date")]

    path = write_transform_log(entries, tmp_path / "logs")

    assert path.name == f"{run_id}.json"
    payload = read_transform_log(path)
    assert payload["pipeline_run_id"] == run_id
    assert payload["parent_run_id"] == "Run_20261101_000000_abcdef"
    assert len(payload["tables"]) == 2


def test_write_transform_log_rejects_mixed_run_ids(tmp_path: Path) -> None:
    entries = [_entry("Transform_a", "dim_date"), _entry("Transform_b", "fact_sales_summary")]

    with pytest.raises(ValueError, match="same run_id"):
        write_transform_log(entries, tmp_path / "logs")


def test_write_transform_log_rejects_empty_entries(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="at least one"):
        write_transform_log([], tmp_path / "logs")


def test_purge_old_transform_logs(tmp_path: Path) -> None:
    run_id = "Transform_20261101_020000_abcdef"
    path = write_transform_log([_entry(run_id, "dim_date")], tmp_path)
    old = datetime.now() - timedelta(days=100)
    os.utime(path, (old.timestamp(), old.timestamp()))

    removed = purge_old_transform_logs(tmp_path, retention_days=90)

    assert removed == [path]
    assert not path.exists()


def test_purge_keeps_recent_logs(tmp_path: Path) -> None:
    run_id = "Transform_20261101_020000_abcdef"
    path = write_transform_log([_entry(run_id, "dim_date")], tmp_path)

    removed = purge_old_transform_logs(tmp_path, retention_days=90)

    assert removed == []
    assert path.exists()


def test_purge_rejects_invalid_retention(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="at least 1"):
        purge_old_transform_logs(tmp_path, retention_days=0)


def test_log_payload_is_valid_json(tmp_path: Path) -> None:
    run_id = "Transform_20261101_020000_abcdef"
    path = write_transform_log([_entry(run_id, "dim_date")], tmp_path)

    json.loads(path.read_text(encoding="utf-8"))
