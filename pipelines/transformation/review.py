"""Explicit review creates a new approved package; the held package remains immutable."""
from __future__ import annotations

import getpass
from pathlib import Path

from pipelines.artifacts import file_digest, new_id, read_package, write_package


def approve(manifest_path: Path, config, reason: str) -> Path:
    if len(reason.strip()) < 8:
        raise ValueError("a substantive review reason is required")
    manifest, frames = read_package(manifest_path, "transformation", approved=False)
    if manifest["decision"] != "MANUAL_REVIEW":
        raise ValueError("only non-critical MANUAL_REVIEW packages may be approved")
    if (any(value != "PASS" for value in manifest["validation_results"].values())
            or manifest["rejection_rate"] > config.etl.rejection_fail_rate):
        raise ValueError("critical failures cannot be overridden by review")
    metadata = {key: value for key, value in manifest.items()
                if key not in {"tables", "contract_version", "pipeline_run_id", "created_at", "stage", "decision"}}
    metadata["review"] = {"operator": getpass.getuser(), "reason": reason.strip(),
                          "reviewed_run_id": manifest["pipeline_run_id"],
                          "reviewed_manifest_sha256": file_digest(manifest_path)}
    return write_package(config.etl.silver_dir, new_id("Transform"), "transformation", frames, metadata)
