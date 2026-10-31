"""Extraction run metadata and log persistence (Stage 1 section 4)."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4


def new_run_id(now: datetime | None = None) -> str:
    """Return a unique pipeline run identifier."""

    reference = now or datetime.now()
    return f"Run_{reference:%Y%m%d}_{reference:%H%M%S}_{uuid4().hex[:6]}"


@dataclass(frozen=True)
class TableRunMetadata:
    """Metadata recorded for one extracted table."""

    pipeline_run_id: str
    table_name: str
    extraction_start_timestamp: str
    extraction_end_timestamp: str
    extraction_status: str
    records_read: int
    records_extracted: int
    records_rejected: int
    extraction_window: str
    validation_results: str
    error_message: str | None = None
    error_code: str | None = None
    extraction_duration_seconds: float = 0.0

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def write_run_log(entries: Sequence[TableRunMetadata], log_dir: Path) -> Path:
    """Write all table metadata for a run to a single JSON file."""

    if not entries:
        raise ValueError("entries must contain at least one table")

    run_id = entries[0].pipeline_run_id
    if any(entry.pipeline_run_id != run_id for entry in entries):
        raise ValueError("all entries must share the same pipeline_run_id")

    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / f"{run_id}.json"
    payload = {
        "pipeline_run_id": run_id,
        "tables": [entry.to_dict() for entry in entries],
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def read_run_log(path: Path) -> dict[str, object]:
    """Read a previously written run log."""

    return json.loads(path.read_text(encoding="utf-8"))


def purge_old_logs(
    log_dir: Path,
    retention_days: int,
    now: datetime | None = None,
) -> list[Path]:
    """Delete run logs older than the retention period."""

    if retention_days < 1:
        raise ValueError("retention_days must be at least 1")

    if not log_dir.exists():
        return []

    cutoff = (now or datetime.now()) - timedelta(days=retention_days)
    removed: list[Path] = []
    for path in log_dir.glob("Run_*.json"):
        modified = datetime.fromtimestamp(path.stat().st_mtime)
        if modified < cutoff:
            path.unlink()
            removed.append(path)
    return removed
