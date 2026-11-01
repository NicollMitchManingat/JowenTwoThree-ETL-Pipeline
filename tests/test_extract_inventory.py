from pipelines.config import load_config
from pipelines.extraction.extract import INVENTORY_COLUMNS, extract_inventory


def test_extract_inventory_in_mock_mode() -> None:
    config = load_config({"ETL_MOCK_MODE": "true"})

    frame = extract_inventory(config)

    assert list(frame.columns) == INVENTORY_COLUMNS
    assert len(frame) == 15
    assert (frame["stock_quantity"] < 0).any()
    assert (frame["stock_quantity"] == 0).any()


def test_extract_inventory_requires_connection_outside_mock_mode() -> None:
    config = load_config(
        {
            "ETL_MOCK_MODE": "false",
            "SUPABASE_DB_HOST": "db.example.com",
            "SUPABASE_DB_NAME": "postgres",
            "SUPABASE_DB_USER": "postgres.project",
            "SUPABASE_DB_PASSWORD": "secret",
        }
    )

    try:
        extract_inventory(config)
    except ValueError as exc:
        assert "connection" in str(exc).lower()
    else:
        raise AssertionError("expected ValueError")
