"""Helpers for building rejected-record frames."""

from __future__ import annotations

import json

import pandas as pd

REJECT_COLUMNS = [
    "source_table",
    "source_record_id",
    "rule_id",
    "error_code",
    "reason",
    "payload",
]


def empty_rejects() -> pd.DataFrame:
    """Return an empty rejected-record frame with the standard columns."""

    return pd.DataFrame(columns=REJECT_COLUMNS)


def build_rejects(
    frame: pd.DataFrame,
    mask: pd.Series,
    *,
    source_table: str,
    id_column: str,
    rule_id: str,
    error_code: str,
    reason: str,
) -> pd.DataFrame:
    """Create rejected rows for the records selected by ``mask``."""

    selected = frame.loc[mask]
    if selected.empty:
        return empty_rejects()

    payloads = [
        json.dumps(record, default=str, sort_keys=True)
        for record in selected.to_dict(orient="records")
    ]
    return pd.DataFrame(
        {
            "source_table": source_table,
            "source_record_id": selected[id_column].astype("string").tolist(),
            "rule_id": rule_id,
            "error_code": error_code,
            "reason": reason,
            "payload": payloads,
        }
    )
