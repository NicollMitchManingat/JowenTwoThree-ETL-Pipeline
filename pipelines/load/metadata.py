"""Durable local load outcomes, including failures that rolled back target writes."""
from __future__ import annotations

from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb

from pipelines.artifacts import write_json

from .target import WarehouseConfig

CODE_VERSION = "2.0.0"


def persist_failure(config: WarehouseConfig, record: dict, log_dir: Path) -> None:
    # The local record is durable even if the warehouse itself is unavailable.
    write_json(log_dir / f"{record['load_run_id']}.json", record)
    try:
        with config.connect() as connection:
            persist_db_log(connection, record)
    except (psycopg.Error, OSError, ValueError):
        return


def persist_db_log(connection, record: dict) -> None:
    for table, counts in record["tables"].items():
        connection.execute(
            """INSERT INTO dw_analytics.load_log
               (load_run_id,transformation_run_id,batch_id,batch_window,table_name,
                load_start_ts,load_end_ts,load_duration_sec,records_received,records_inserted,
                records_updated,records_loaded,records_rejected,pre_load_result,post_load_result,
                status,error_code,error_message,load_code_version,details)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (record["load_run_id"], record["transformation_run_id"], record["batch_id"],
             record["batch_window"], table, record["started_at"], record["finished_at"],
             record["duration_seconds"], counts["received"], counts["inserted"], counts["updated"],
             counts["loaded"], counts["rejected"], Jsonb(record["pre_load_result"]),
             Jsonb(record["post_load_result"]), record["status"], record.get("error_code"),
             record.get("error_message"), CODE_VERSION, Jsonb(record.get("details", {}))))
