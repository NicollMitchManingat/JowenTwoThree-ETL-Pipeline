"""PostgreSQL connection helpers for the Supabase source database."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import TypeVar

import psycopg

from .config import AppConfig, SourceDBConfig


class SourceConnectionError(Exception):
    """Raised when the source database cannot be reached."""


T = TypeVar("T")


def enforce_read_only(connection: psycopg.Connection) -> None:
    """Force the session into read-only mode before running any query."""

    with connection.cursor() as cursor:
        cursor.execute("SET default_transaction_read_only = on")
    connection.commit()


def connect_source(
    source: SourceDBConfig,
    connect_timeout: int = 10,
) -> psycopg.Connection:
    """Open a source connection and enforce read-only mode when configured."""

    try:
        connection = psycopg.connect(
            host=source.host,
            port=source.port,
            dbname=source.name,
            user=source.user,
            password=source.password,
            sslmode=source.sslmode,
            connect_timeout=connect_timeout,
        )
    except psycopg.Error as exc:
        raise SourceConnectionError(str(exc)) from exc

    if source.read_only:
        try:
            enforce_read_only(connection)
        except psycopg.Error as exc:
            connection.close()
            raise SourceConnectionError(str(exc)) from exc

    return connection


def get_source_connection(config: AppConfig) -> psycopg.Connection:
    """Return a source connection, or fail clearly in mock mode."""

    if config.etl.mock_mode:
        raise SourceConnectionError(
            "Mock mode is enabled; no source database connection is available"
        )
    if config.source is None:
        raise SourceConnectionError("Source database configuration is missing")
    return connect_source(config.source)


def with_retry(
    operation: Callable[[], T],
    attempts: int = 3,
    backoff_seconds: float = 2.0,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Run an operation, retrying transient source connection failures."""

    if attempts < 1:
        raise ValueError("attempts must be at least 1")

    last_error: SourceConnectionError | None = None
    for attempt in range(attempts):
        try:
            return operation()
        except SourceConnectionError as exc:
            last_error = exc
            if attempt < attempts - 1:
                sleep(backoff_seconds * (2**attempt))

    if last_error is None:
        raise SourceConnectionError("Operation failed without an error")
    raise last_error


def health_check(config: AppConfig) -> bool:
    """Return True when the source database accepts a simple query."""

    try:
        connection = get_source_connection(config)
    except SourceConnectionError:
        return False

    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()
        return True
    except psycopg.Error:
        return False
    finally:
        connection.close()
