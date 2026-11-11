"""Versioned, immutable run packages; validation never trusts mutable date partitions."""
from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pandas as pd

VERSION = 2


def new_id(stage: str) -> str:
    return f"{stage}_{datetime.now(UTC):%Y%m%d_%H%M%S}_{uuid4().hex[:12]}"


def file_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(value, indent=2, default=str), encoding="utf-8")
    temporary.replace(path)


def write_package(root: Path, run_id: str, stage: str, frames: dict[str, pd.DataFrame],
                  metadata: dict, decision: str = "APPROVED") -> Path:
    folder = root.resolve() / run_id
    folder.mkdir(parents=True, exist_ok=False)
    tables = {}
    for table, frame in frames.items():
        if not table.replace("_", "").isalnum():
            raise ValueError("invalid artifact table name")
        target = folder / f"{table}.parquet"
        frame.to_parquet(target, index=False)
        tables[table] = {"path": target.name, "records": len(frame),
                         "columns": list(frame.columns), "sha256": file_digest(target)}
    manifest = dict(metadata, contract_version=VERSION, stage=stage,
                    pipeline_run_id=run_id, decision=decision,
                    created_at=datetime.now(UTC).isoformat(), tables=tables)
    target = folder / "manifest.json"
    write_json(target, manifest)
    return target


def read_package(path: Path, stage: str, approved: bool = True) -> tuple[dict, dict[str, pd.DataFrame]]:
    path = path.resolve()
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if manifest.get("contract_version") != VERSION or manifest.get("stage") != stage:
        raise ValueError("incompatible handover version or stage; regenerate this batch")
    if approved and manifest.get("decision") != "APPROVED":
        raise ValueError("handover is not APPROVED")
    if not manifest.get("pipeline_run_id") or not manifest.get("dates"):
        raise ValueError("handover identity or business dates missing")
    frames = {}
    for table, entry in manifest["tables"].items():
        target = (path.parent / entry["path"]).resolve()
        if not target.is_relative_to(path.parent) or target == path:
            raise ValueError("artifact path escapes run package")
        if file_digest(target) != entry["sha256"]:
            raise ValueError(f"artifact checksum mismatch: {table}")
        frame = pd.read_parquet(target)
        if len(frame) != entry["records"] or list(frame.columns) != entry["columns"]:
            raise ValueError(f"artifact count or columns mismatch: {table}")
        frames[table] = frame
    return manifest, frames
