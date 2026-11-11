"""Atomic load and reconciliation; never extract, clean, or recalculate source data."""
from __future__ import annotations

import json
import shutil
from datetime import UTC, date, datetime, time
from decimal import Decimal
from numbers import Integral
from pathlib import Path

import pandas as pd
import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from pipelines.artifacts import file_digest, new_id, read_package, write_json
from pipelines.money import money
from pipelines.transformation.dimensions import date_key
from pipelines.transformation.schema import FACT_KEY_COLUMNS, TABLE_COLUMNS

from .metadata import persist_db_log, persist_failure
from .target import WarehouseConfig

TABLES = ("dim_date", *FACT_KEY_COLUMNS)
MONEY_COLUMNS = ("total_sales", "refund_amount", "net_sales")


def native(value, column: str):
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if hasattr(value, "item"):
        return value.item()
    if column == "peak_hour" and isinstance(value, str):
        return time.fromisoformat(value)
    return value


def validate_input(manifest: dict, frames: dict) -> None:
    if set(frames) != {*TABLES, "transformation_quarantine"}:
        raise ValueError("handover must contain exactly the date dimension, three facts and quarantine")
    days = [date.fromisoformat(value) for value in manifest["dates"]]
    if not days or len(set(days)) != len(days):
        raise ValueError("missing or duplicate business dates")
    from zoneinfo import ZoneInfo
    capture = datetime.fromisoformat(manifest["snapshot_at"])
    if capture.tzinfo is None:
        raise ValueError("snapshot timestamp must be timezone-aware")
    snapshot_date = capture.astimezone(ZoneInfo("Asia/Manila")).date()
    dim = frames["dim_date"]
    for table in TABLES:
        frame = frames[table]
        if list(frame.columns) != TABLE_COLUMNS[table]:
            raise ValueError(f"column contract mismatch: {table}")
        if frame["date_key"].isna().any() or frame["date_key"].duplicated().any():
            raise ValueError(f"missing or duplicate date keys: {table}")
        for column in frame:
            for value in frame[column]:
                converted = native(value, column)
                if converted is None and column != "peak_hour":
                    raise ValueError(f"required null: {table}.{column}")
                if column in MONEY_COLUMNS:
                    if not isinstance(converted, Decimal) or money(converted) != converted:
                        raise ValueError(f"non-exact monetary value: {table}.{column}")
                elif ((column.startswith("total_") or column in {"low_stock_items", "out_of_stock_items"})
                      and (not isinstance(converted, Integral) or isinstance(converted, bool) or converted < 0)):
                    raise ValueError(f"invalid count: {table}.{column}")
    dim_keys = set(dim["date_key"])
    for row in dim.to_dict("records"):
        if not isinstance(row["full_date"], date) or date_key(row["full_date"]) != row["date_key"]:
            raise ValueError("date dimension key does not match full_date")
    for table, prefix in (("fact_sales_summary", "SALES"), ("fact_inventory_summary", "INV"),
                          ("fact_traffic_summary", "TRAFFIC")):
        frame = frames[table]
        expected = {date_key(snapshot_date)} if prefix == "INV" else {date_key(d) for d in days}
        if set(frame["date_key"]) != expected or not expected <= dim_keys:
            raise ValueError(f"date coverage or referential mismatch: {table}")
        key = FACT_KEY_COLUMNS[table]
        if frame[key].duplicated().any() or any(
                row[key] != f"{prefix}_{row['date_key']}" for row in frame.to_dict("records")):
            raise ValueError(f"summary key mismatch: {table}")
    for row in frames["fact_sales_summary"].to_dict("records"):
        if row["total_sales"] < 0 or row["refund_amount"] < 0:
            raise ValueError("negative transaction or refund total")
        if row["net_sales"] != row["total_sales"] - row["refund_amount"]:
            raise ValueError("financial invariant mismatch")
    for row in frames["fact_inventory_summary"].to_dict("records"):
        if row["threshold_policy"] != "PER_ITEM":
            raise ValueError("inventory threshold policy mismatch")
        if row["low_stock_items"] + row["out_of_stock_items"] > row["total_items"]:
            raise ValueError("inventory categories exceed active item count")


def expected_type(column: str) -> str:
    if column in MONEY_COLUMNS:
        return "numeric"
    if column == "date_key":
        return "int4"
    if column in {"total_orders", "total_items", "low_stock_items", "out_of_stock_items", "total_customers"}:
        return "int8"
    if column in {"day_of_month", "month", "quarter", "year", "day_of_week"}:
        return "int2"
    if column in {"snapshot_at", "loaded_at"}:
        return "timestamptz"
    return {"full_date": "date", "is_weekend": "bool", "peak_hour": "time"}.get(column, "text")


def validate_target(connection, frames: dict) -> None:
    rows = connection.execute(
        """SELECT table_name,column_name,udt_name FROM information_schema.columns
           WHERE table_schema='dw_analytics'""").fetchall()
    actual = {(t, c): kind for t, c, kind in rows}
    for table in TABLES:
        for column in frames[table]:
            if actual.get((table, column)) != expected_type(column):
                raise ValueError(f"target schema mismatch: {table}.{column}")
    for table in (*TABLES, "load_log", "load_quarantine", "load_batches", "transformation_quarantine"):
        for privilege in ("SELECT", "INSERT", "UPDATE"):
            if not connection.execute("SELECT has_table_privilege(current_user,%s,%s)",
                                      (f"dw_analytics.{table}", privilege)).fetchone()[0]:
                raise ValueError(f"missing warehouse privilege: {table} {privilege}")


def write_table(connection, table: str, frame: pd.DataFrame, manifest: dict) -> tuple[int, int]:
    columns = list(frame.columns)
    dates = [int(x) for x in frame["date_key"]]
    existing = {row[0] for row in connection.execute(
        sql.SQL("SELECT date_key FROM dw_analytics.{} WHERE date_key = ANY(%s)").format(sql.Identifier(table)),
        (dates,)).fetchall()}
    target_columns = columns + (["source_run_id", "source_created_at"] if table != "dim_date" else [])
    statement = sql.SQL("INSERT INTO dw_analytics.{} ({}) VALUES ({}) ON CONFLICT(date_key) DO UPDATE SET {}").format(
        sql.Identifier(table), sql.SQL(",").join(map(sql.Identifier, target_columns)),
        sql.SQL(",").join(sql.Placeholder() for _ in target_columns),
        sql.SQL(",").join(sql.SQL("{}=EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
                          for c in target_columns if c != "date_key"))
    with connection.cursor() as cursor:
        for row in frame.to_dict("records"):
            values = [native(row[c], c) for c in columns]
            if table != "dim_date":
                values += [manifest["pipeline_run_id"], datetime.fromisoformat(manifest["snapshot_at"])]
            cursor.execute(statement, values)
    updated = len(existing)
    return len(frame) - updated, updated


def validate_loaded(connection, frames: dict) -> dict:
    checks = {}
    for table in TABLES:
        frame = frames[table]
        columns = list(frame.columns)
        actual = connection.execute(
            sql.SQL("SELECT {} FROM dw_analytics.{} WHERE date_key = ANY(%s) ORDER BY date_key").format(
                sql.SQL(",").join(map(sql.Identifier, columns)), sql.Identifier(table)),
            ([int(x) for x in frame["date_key"]],)).fetchall()
        expected = [tuple(native(row[c], c) for c in columns)
                    for row in frame.sort_values("date_key").to_dict("records")]
        if actual != expected:
            raise ValueError(f"post-load row/count/value reconciliation failed: {table}")
        checks[table] = "PASS"
    # Exact row comparisons include each sales amount; sums additionally expose the audit totals.
    checks["financial_totals"] = {
        column: str(sum(frames["fact_sales_summary"][column], Decimal("0.00")))
        for column in MONEY_COLUMNS}
    return checks


def check_batch(connection, manifest: dict, digest: str, frames: dict) -> bool:
    previous = connection.execute(
        "SELECT manifest_sha256 FROM dw_analytics.load_batches WHERE batch_id=%s OR transformation_run_id=%s",
        (digest, manifest["pipeline_run_id"])).fetchone()
    if previous:
        if previous[0] != digest:
            raise ValueError("a published transformation identity was reused with a changed manifest")
        return True
    captured = datetime.fromisoformat(manifest["snapshot_at"])
    for table in FACT_KEY_COLUMNS:
        newer = connection.execute(
            sql.SQL("SELECT 1 FROM dw_analytics.{} WHERE date_key=ANY(%s) AND source_created_at>%s LIMIT 1")
            .format(sql.Identifier(table)),
            ([int(key) for key in frames[table]["date_key"]], captured)).fetchone()
        if newer:
            raise ValueError(f"stale extraction cannot replace newer published data: {table}")
    return False


def finish(record: dict) -> None:
    record["finished_at"] = datetime.now(UTC).isoformat()
    record["duration_seconds"] = round(
        (datetime.fromisoformat(record["finished_at"]) -
         datetime.fromisoformat(record["started_at"])).total_seconds(), 3)


def load_batch(manifest_path: Path, config: WarehouseConfig, log_dir: Path = Path("data/logs/loading"),
               dry_run: bool = False) -> dict:
    record = {"load_run_id": new_id("Load"), "transformation_run_id": "UNKNOWN", "batch_id": "UNKNOWN",
              "batch_window": "UNKNOWN", "started_at": datetime.now(UTC).isoformat(),
              "tables": {"batch": {"received": 0, "inserted": 0, "updated": 0, "loaded": 0, "rejected": 0}},
              "pre_load_result": {}, "post_load_result": {}, "status": "FAILED"}
    frames = {}
    try:
        manifest, frames = read_package(manifest_path, "transformation")
        record.update(transformation_run_id=manifest["pipeline_run_id"], batch_id=file_digest(manifest_path),
                      batch_window=f"{min(manifest['dates'])} to {max(manifest['dates'])}")
        record["tables"] = {table: {"received": len(frames[table]), "inserted": 0,
                                    "updated": 0, "loaded": 0, "rejected": 0} for table in TABLES}
        validate_input(manifest, frames)
        if manifest["source_mode"] == "mock" and not config.allow_demo:
            raise ValueError("mock packages require a separate demo/test target with WAREHOUSE_ALLOW_DEMO=true")
        if shutil.disk_usage(config.storage_path).free < config.minimum_free_bytes:
            raise ValueError("insufficient free space on the configured local warehouse volume")
        with config.connect() as connection:
            validate_target(connection, frames)
            record["pre_load_result"] = {"handover": "PASS", "schema": "PASS", "storage": "PASS",
                                         "counts": "PASS", "permissions": "PASS"}
            if not dry_run:
                connection.execute("SELECT pg_advisory_xact_lock(21477,3)")
            duplicate = check_batch(connection, manifest, record["batch_id"], frames)
            if dry_run:
                finish(record)
                return dict(record, status="DRY_RUN", duplicate_batch=duplicate)
            if duplicate:
                record["status"] = "SKIPPED"
                record["details"] = {"reason": "identical approved batch already published"}
                for counts in record["tables"].values():
                    counts.update(received=0)
                finish(record)
                persist_db_log(connection, record)
            if duplicate:
                write_json(log_dir / f"{record['load_run_id']}.json", record)
                return record
            for table in TABLES:
                inserted, updated = write_table(connection, table, frames[table], manifest)
                record["tables"][table].update(inserted=inserted, updated=updated, loaded=inserted + updated)
            record["post_load_result"] = validate_loaded(connection, frames)
            for row in frames["transformation_quarantine"].to_dict("records"):
                connection.execute(
                    """INSERT INTO dw_analytics.transformation_quarantine
                       (run_id,parent_run_id,source_table,source_record_id,rule_id,error_code,reason,payload)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                    (manifest["pipeline_run_id"], manifest["parent_run_id"], row["source_table"],
                     native(row["source_record_id"], ""), row["rule_id"], row["error_code"], row["reason"],
                     Jsonb(json.loads(row["payload"]))))
            connection.execute(
                """INSERT INTO dw_analytics.load_batches
                   (batch_id,transformation_run_id,manifest_sha256,load_run_id,source_created_at,business_dates)
                   VALUES (%s,%s,%s,%s,%s,%s)""",
                (record["batch_id"], manifest["pipeline_run_id"], record["batch_id"], record["load_run_id"],
                 datetime.fromisoformat(manifest["snapshot_at"]),
                 [date.fromisoformat(d) for d in manifest["dates"]]))
            record["status"] = "SUCCESS"
            finish(record)
            persist_db_log(connection, record)
        # Publication is now committed together with its SUCCESS log.
    except (ValueError, TypeError, KeyError, OSError, ArithmeticError, psycopg.Error) as exc:
        record.update(status="FAILED", error_code=type(exc).__name__, error_message=str(exc)[:2000])
        for counts in record["tables"].values():
            counts.update(inserted=0, updated=0, loaded=0, rejected=counts["received"])
        finish(record)
        if not dry_run:
            persist_failure(config, record, log_dir)
            try:
                with config.connect() as connection:
                    for table in TABLES:
                        if table in frames:
                            connection.execute(
                                """INSERT INTO dw_analytics.load_quarantine
                                   (load_run_id,table_name,reason,payload) VALUES (%s,%s,%s,%s)""",
                                (record["load_run_id"], table, record["error_code"],
                                 Jsonb(json.loads(json.dumps(frames[table].to_dict("records"), default=str)))))
            except psycopg.Error:
                pass  # Failed files remain in the sealed package and local failure log.
        return record
    write_json(log_dir / f"{record['load_run_id']}.json", record)
    return record
