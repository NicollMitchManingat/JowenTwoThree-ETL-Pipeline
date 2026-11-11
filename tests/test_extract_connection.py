from datetime import UTC, date, datetime

from pipelines.config import load_config
from pipelines.extraction.extract import (
    INVENTORY_COLUMNS,
    SALES_COLUMNS,
    TRAFFIC_COLUMNS,
    extract_inventory,
    extract_sales,
    extract_traffic,
)

BUSINESS_DATE = date(2026, 10, 12)


class FakeCursor:
    def __init__(self, rows, columns):
        self._rows = rows
        self.description = [(column,) for column in columns]
        self.queries: list[str] = []
        self.parameters: dict[str, object] | None = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, query, parameters=None):
        self.queries.append(query)
        self.parameters = parameters

    def fetchall(self):
        return self._rows


class FakeConnection:
    def __init__(self, rows, columns):
        self.cursor_instance = FakeCursor(rows, columns)

    def cursor(self):
        return self.cursor_instance


def _real_config():
    return load_config(
        {
            "ETL_MOCK_MODE": "false",
            "SUPABASE_DB_HOST": "db.example.com",
            "SUPABASE_DB_NAME": "postgres",
            "SUPABASE_DB_USER": "postgres.project",
            "SUPABASE_DB_PASSWORD": "secret",
        }
    )


def test_extract_sales_uses_connection_and_window() -> None:
    timestamp = datetime(2026, 10, 12, 8, 15, tzinfo=UTC)
    connection = FakeConnection(
        [(("trx-1"), timestamp, "TRX-1", 245.50)],
        SALES_COLUMNS,
    )

    frame = extract_sales(BUSINESS_DATE, _real_config(), connection=connection)

    assert list(frame.columns) == SALES_COLUMNS
    assert len(frame) == 1
    assert connection.cursor_instance.parameters is not None
    assert "window_start" in connection.cursor_instance.parameters


def test_extract_inventory_uses_connection() -> None:
    connection = FakeConnection(
        [("item-1", "Iced Coffee", 12)],
        INVENTORY_COLUMNS,
    )

    frame = extract_inventory(_real_config(), connection=connection)

    assert list(frame.columns) == INVENTORY_COLUMNS
    assert len(frame) == 1
    assert "inventory" in connection.cursor_instance.queries[0]


def test_extract_traffic_uses_connection_and_window() -> None:
    timestamp = datetime(2026, 10, 12, 7, 30, tzinfo=UTC)
    connection = FakeConnection(
        [("traffic-1", 4, timestamp)],
        TRAFFIC_COLUMNS,
    )

    frame = extract_traffic(BUSINESS_DATE, _real_config(), connection=connection)

    assert list(frame.columns) == TRAFFIC_COLUMNS
    assert len(frame) == 1
    assert connection.cursor_instance.parameters is not None
    assert "window_end" in connection.cursor_instance.parameters
