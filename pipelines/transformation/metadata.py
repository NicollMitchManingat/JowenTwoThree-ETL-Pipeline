"""Stage 2 transformation run metadata and log persistence."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4


def new_transform_run_id(now: datetime | None = None) -> str:
    """Return a unique transformation run identifier."""

    reference = now or datetime.now()
    return (
        f"Transform_{reference:%Y%m%d}_{reference:%H%M%S}_"
        f"{uuid4().hex[:6]}"
    )


@dataclass(frozen=True)
class TransformRunMetadata:
    """Metadata recorded for one transformed table."""

    run_id: str
    parent_run_id: str
    table_name: str
    batch_window: str
    records_read: int
    records_output: int
    records_rejected: int
    status: str
    started_at: str
    finished_at: str
    duration_seconds: float
    error_message: str | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def write_transform_log(
    entries: Sequence[TransformRunMetadata],
    log_dir: Path,
) -> Path:
    """Write all table metadata for a transformation run to one JSON file."""

    if not entries:
        raise ValueError("entries must contain at least one table")

    run_id = entries[0].run_id
    if any(entry.run_id != run_id for entry in entries):
        raise ValueError("all entries must share the same run_id")

    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / f"{run_id}.json"
    payload = {
        "pipeline_run_id": run_id,
        "parent_run_id": entries[0].parent_run_id,
        "tables": [entry.to_dict() for entry in entries],
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def read_transform_log(path: Path) -> dict[str, object]:
    """Read a previously written transformation log."""

    return json.loads(path.read_text(encoding="utf-8"))


def purge_old_transform_logs(
    log_dir: Path,
    retention_days: int,
    now: datetime | None = None,
) -> list[Path]:
    """Delete transformation logs older than the retention period."""

    if retention_days < 1:
        raise ValueError("retention_days must be at least 1")

    if not log_dir.exists():
        return []

    cutoff = (now or datetime.now()) - timedelta(days=retention_days)
    removed: list[Path] = []
    for path in log_dir.glob("Transform_*.json"):
        modified = datetime.fromtimestamp(path.stat().st_mtime)
        if modified < cutoff:
            path.unlink()
            removed.append(path)
    return removed
