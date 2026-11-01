"""Read Stage 1 handover manifests and bronze Parquet partitions."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd

SOURCE_TABLES = ("transactions", "inventory", "customer_traffic")


class TransformationInputError(Exception):
    """Raised when approved bronze input cannot be located."""


def _read_manifest(path: Path) -> dict[str, object]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TransformationInputError(f"cannot read manifest {path}: {exc}") from exc


def find_extraction_manifest(
    business_date: date,
    handover_dir: Path,
    parent_run_id: str | None = None,
) -> tuple[Path, dict[str, object]]:
    """Find the approved extraction manifest for a business date."""

    if parent_run_id is not None:
        path = handover_dir / parent_run_id / "manifest.json"
        if not path.exists():
            raise TransformationInputError(f"manifest not found: {path}")
        manifest = _read_manifest(path)
        if manifest.get("decision") != "APPROVED":
            raise TransformationInputError(
                f"extraction manifest is not APPROVED: {path}"
            )
        return path, manifest

    candidates: list[tuple[str, Path, dict[str, object]]] = []
    for path in handover_dir.glob("Run_*/manifest.json"):
        manifest = _read_manifest(path)
        if (
            manifest.get("processing_date") == business_date.isoformat()
            and manifest.get("decision") == "APPROVED"
        ):
            candidates.append((str(manifest.get("created_at", "")), path, manifest))

    if not candidates:
        raise TransformationInputError(
            f"no APPROVED extraction manifest for {business_date.isoformat()}"
        )

    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1], candidates[0][2]


def load_bronze(manifest: dict[str, object]) -> dict[str, pd.DataFrame]:
    """Read all bronze tables referenced by an extraction manifest."""

    tables = manifest.get("tables")
    if not isinstance(tables, dict):
        raise TransformationInputError("manifest is missing table paths")

    frames: dict[str, pd.DataFrame] = {}
    for table in SOURCE_TABLES:
        entry = tables.get(table)
        if not isinstance(entry, dict) or "path" not in entry:
            raise TransformationInputError(f"manifest is missing {table} path")
        path = Path(str(entry["path"]))
        if not path.exists():
            raise TransformationInputError(f"bronze partition not found: {path}")
        frames[table] = pd.read_parquet(path)
    return frames
