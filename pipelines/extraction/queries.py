"""Column-mapped SQL for the documented Jowen Two Three source tables."""

from __future__ import annotations

SOURCE_COLUMNS: dict[str, dict[str, str]] = {
    "transactions": {
        "id": "transaction_id",
        "created_at": "created_at",
        "transaction_number": "transaction_number",
        "total": "total",
    },
    "inventory": {
        "id": "item_id",
        "name": "name",
        "stock quantity": "stock_quantity",
    },
    "customer_traffic": {
        "id": "entry_id",
        "number_of_customer": "number_of_customer",
        "created_at": "created_at",
    },
}


def _quote(identifier: str) -> str:
    """Quote a PostgreSQL identifier safely."""

    return '"' + identifier.replace('"', '""') + '"'


def _select(table: str, columns: dict[str, str]) -> str:
    projections = ", ".join(
        f"{_quote(source)} AS {_quote(target)}" for source, target in columns.items()
    )
    return f"SELECT {projections} FROM {_quote(table)}"


def _window_clause(column: str) -> str:
    return (
        f" WHERE {_quote(column)} >= %(window_start)s"
        f" AND {_quote(column)} < %(window_end)s"
    )


def sales_query(apply_window: bool = True) -> str:
    """Return the sales extraction query, optionally filtered by business date."""

    query = _select("transactions", SOURCE_COLUMNS["transactions"])
    if apply_window:
        query += _window_clause("created_at")
    return query + ' ORDER BY "created_at", "id"'


def inventory_query() -> str:
    """Return the full inventory snapshot query."""

    return _select("inventory", SOURCE_COLUMNS["inventory"]) + ' ORDER BY "id"'


def traffic_query(apply_window: bool = True) -> str:
    """Return the customer traffic query, optionally filtered by business date."""

    query = _select("customer_traffic", SOURCE_COLUMNS["customer_traffic"])
    if apply_window:
        query += _window_clause("created_at")
    return query + ' ORDER BY "created_at", "id"'
