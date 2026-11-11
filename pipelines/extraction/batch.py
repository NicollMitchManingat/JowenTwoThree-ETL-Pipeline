"""Bounded source extraction under one repeatable-read, read-only transaction."""
from datetime import UTC, date, datetime, timedelta

import pandas as pd

from pipelines.config import AppConfig
from pipelines.db import get_source_connection
from pipelines.money import decimal_value

from . import mock_data
from .windows import business_day_window

CONTRACT = {
    "transactions": {"id": ("transaction_id", "uuid"), "created_at": ("created_at", "timestamptz"),
                     "transaction_number": ("transaction_number", "text"), "total": ("total", "numeric"),
                     "payment_status": ("payment_status", "text")},
    "refunds": {"id": ("refund_id", "uuid"), "transaction_id": ("transaction_id", "uuid"),
                "created_at": ("created_at", "timestamptz"), "refund_amount": ("refund_amount", "numeric")},
    "inventory": {"id": ("item_id", "uuid"), "name": ("name", "text"),
                  "stock_quantity": ("stock_quantity", "numeric"), "unit": ("unit", "text"),
                  "low_stock_threshold": ("low_stock_threshold", "numeric"), "is_active": ("is_active", "bool")},
    "customer_traffic": {"id": ("entry_id", "int8"), "created_at": ("created_at", "timestamptz"),
                         "number_of_customer": ("number_of_customer", "int8")},
}


def dates_between(start: date, end: date) -> list[date]:
    if end < start:
        raise ValueError("end date precedes start date")
    return [start + timedelta(days=n) for n in range((end - start).days + 1)]


def extract_batch(start: date, end: date, config: AppConfig,
                  captured_at: datetime | None = None) -> tuple[dict[str, pd.DataFrame], datetime]:
    captured = captured_at or datetime.now(UTC)
    if config.etl.mock_mode:
        frames = {
            "transactions": pd.concat([mock_data.transactions(d).assign(payment_status="PAID")
                                        for d in dates_between(start, end)], ignore_index=True),
            "customer_traffic": pd.concat([mock_data.customer_traffic(d)
                                          for d in dates_between(start, end)], ignore_index=True),
            "refunds": pd.DataFrame(columns=[x[0] for x in CONTRACT["refunds"].values()]),
            "inventory": mock_data.inventory().assign(unit="units", is_active=True, low_stock_threshold=5),
        }
        # IDs must remain unique when concatenating several sample business dates.
        for table, column in (("transactions", "transaction_id"), ("customer_traffic", "entry_id")):
            frames[table][column] = [f"{value}-{index}" for index, value in enumerate(frames[table][column])]
        return frames, captured
    left, _ = business_day_window(start, config.etl.timezone)
    _, right = business_day_window(end, config.etl.timezone)
    with get_source_connection(config) as connection, connection.cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        cursor.execute("""SELECT table_name,column_name,udt_name FROM information_schema.columns
                          WHERE table_schema='public' AND table_name = ANY(%s)""", (list(CONTRACT),))
        actual = {(t, c): kind for t, c, kind in cursor.fetchall()}
        for table, columns in CONTRACT.items():
            for column, (_, kind) in columns.items():
                if actual.get((table, column)) != kind:
                    raise ValueError(f"source schema mismatch: {table}.{column}; expected {kind}")
        frames = {}
        for table, columns in CONTRACT.items():
            projection = ", ".join(f'"{name}" AS "{alias}"' for name, (alias, _) in columns.items())
            where = "" if table == "inventory" else " WHERE created_at >= %s AND created_at < %s"
            params = () if table == "inventory" else (left, right)
            cursor.execute(f'SELECT COUNT(*) FROM public."{table}"' + where, params)
            expected = cursor.fetchone()[0]
            cursor.execute(f'SELECT {projection} FROM public."{table}"' + where + ' ORDER BY id', params)
            frame = pd.DataFrame(cursor.fetchall(), columns=[x[0] for x in columns.values()])
            if len(frame) != expected:
                raise ValueError(f"source count mismatch: {table}")
            identifier = next(iter(columns.values()))[0]
            if frame[identifier].isna().any() or frame[identifier].duplicated().any():
                raise ValueError(f"missing or duplicate identifiers: {table}")
            frames[table] = frame
    return frames, captured


def validate_inventory(frame: pd.DataFrame) -> None:
    for row in frame.to_dict("records"):
        if not isinstance(row["is_active"], bool):
            raise TypeError("inventory is_active must be boolean")
        if decimal_value(row["low_stock_threshold"]) < 0:
            raise ValueError("inventory threshold must be nonnegative")


def stage_extraction(start: date, end: date, config: AppConfig,
                     dry_run: bool = False):
    """Capture a bounded source window into a new immutable run package."""
    from pipelines.artifacts import new_id, write_package

    frames, captured = extract_batch(start, end, config)
    metadata = {"dates": [d.isoformat() for d in dates_between(start, end)],
                "snapshot_at": captured.isoformat(),
                "source_mode": "mock" if config.etl.mock_mode else "live",
                "validation_results": {"source_types": "PASS", "source_counts": "PASS"}}
    if dry_run:
        return dict(metadata, output_counts={key: len(value) for key, value in frames.items()})
    return write_package(config.etl.staging_dir, new_id("Run"), "extraction", frames, metadata)
