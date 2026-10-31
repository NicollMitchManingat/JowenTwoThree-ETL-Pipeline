"""Stage 1 extraction functions for the Jowen Two Three source tables."""

from __future__ import annotations

from datetime import date

import pandas as pd
import psycopg

from pipelines.config import AppConfig

from . import mock_data
from .queries import sales_query
from .windows import business_day_window

SALES_COLUMNS = [
    "transaction_id",
    "created_at",
    "transaction_number",
    "total",
]


def _run_query(
    connection: psycopg.Connection,
    query: str,
    parameters: dict[str, object] | None = None,
) -> pd.DataFrame:
    with connection.cursor() as cursor:
        cursor.execute(query, parameters)
        rows = cursor.fetchall()
        columns = [description[0] for description in cursor.description or []]
    return pd.DataFrame(rows, columns=columns)


def extract_sales(
    business_date: date,
    config: AppConfig,
    connection: psycopg.Connection | None = None,
) -> pd.DataFrame:
    """Extract sales transactions for a business date."""

    if config.etl.mock_mode:
        return mock_data.transactions(business_date)[SALES_COLUMNS]

    if connection is None:
        raise ValueError("A source connection is required when mock mode is disabled")

    window_start, window_end = business_day_window(business_date, config.etl.timezone)
    return _run_query(
        connection,
        sales_query(),
        {"window_start": window_start, "window_end": window_end},
    )
