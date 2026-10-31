from pipelines.extraction.queries import (
    SOURCE_COLUMNS,
    inventory_query,
    sales_query,
    traffic_query,
)


def test_documented_contract_lists_all_tables() -> None:
    assert set(SOURCE_COLUMNS) == {"transactions", "inventory", "customer_traffic"}


def test_sales_query_maps_columns_and_filters_window() -> None:
    query = sales_query()

    assert 'FROM "transactions"' in query
    assert '"id" AS "transaction_id"' in query
    assert '"created_at" AS "created_at"' in query
    assert '"transaction_number" AS "transaction_number"' in query
    assert '"total" AS "total"' in query
    assert "%(window_start)s" in query
    assert "%(window_end)s" in query


def test_sales_query_can_skip_window() -> None:
    query = sales_query(apply_window=False)

    assert "%(window_start)s" not in query
    assert 'ORDER BY "created_at", "id"' in query


def test_inventory_query_quotes_stock_quantity() -> None:
    query = inventory_query()

    assert 'FROM "inventory"' in query
    assert '"stock quantity" AS "stock_quantity"' in query
    assert "%(window_start)s" not in query


def test_traffic_query_maps_columns_and_filters_window() -> None:
    query = traffic_query()

    assert 'FROM "customer_traffic"' in query
    assert '"id" AS "entry_id"' in query
    assert '"number_of_customer" AS "number_of_customer"' in query
    assert "%(window_start)s" in query
