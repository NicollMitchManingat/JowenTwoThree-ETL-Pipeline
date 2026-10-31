"""Verify the live Supabase schema against the documented column contract."""

from __future__ import annotations

from dataclasses import dataclass

import psycopg

from pipelines.config import load_config
from pipelines.db import SourceConnectionError, get_source_connection

from .queries import SOURCE_COLUMNS


@dataclass(frozen=True)
class SchemaIssue:
    table: str
    column: str
    problem: str


def fetch_source_columns(connection: psycopg.Connection) -> dict[str, set[str]]:
    """Return the public columns for each documented source table."""

    query = """
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = ANY(%(tables)s)
    """
    with connection.cursor() as cursor:
        cursor.execute(query, {"tables": list(SOURCE_COLUMNS)})
        rows = cursor.fetchall()

    actual: dict[str, set[str]] = {table: set() for table in SOURCE_COLUMNS}
    for table, column in rows:
        actual.setdefault(table, set()).add(column)
    return actual


def compare_schema(actual: dict[str, set[str]]) -> list[SchemaIssue]:
    """Compare actual columns against the documented contract."""

    issues: list[SchemaIssue] = []
    for table, columns in SOURCE_COLUMNS.items():
        if not actual.get(table):
            issues.append(SchemaIssue(table, "*", "table missing or inaccessible"))
            continue
        for column in columns:
            if column not in actual[table]:
                issues.append(SchemaIssue(table, column, "column missing"))
    return issues


def main() -> int:
    config = load_config()

    if config.etl.mock_mode:
        print("Mock mode is enabled; set ETL_MOCK_MODE=false to verify the real schema.")
        return 2

    try:
        connection = get_source_connection(config)
    except SourceConnectionError as exc:
        print(f"Connection failed: {exc}")
        return 1

    try:
        actual = fetch_source_columns(connection)
    finally:
        connection.close()

    issues = compare_schema(actual)
    if not issues:
        print("PASS: all documented source columns exist.")
        return 0

    print("FAIL: the source schema does not match the documented contract.")
    for issue in issues:
        print(f" - {issue.table}.{issue.column}: {issue.problem}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
