from datetime import date

from pipelines.config import load_config
from pipelines.extraction.extract import TRAFFIC_COLUMNS, extract_traffic

BUSINESS_DATE = date(2026, 10, 12)


def test_extract_traffic_in_mock_mode() -> None:
    config = load_config({"ETL_MOCK_MODE": "true"})

    frame = extract_traffic(BUSINESS_DATE, config)

    assert list(frame.columns) == TRAFFIC_COLUMNS
    assert len(frame) == 22
    assert frame["created_at"].dt.date.eq(BUSINESS_DATE).all()
    assert frame["number_of_customer"].isna().any()


def test_extract_traffic_requires_connection_outside_mock_mode() -> None:
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
        extract_traffic(BUSINESS_DATE, config)
    except ValueError as exc:
        assert "connection" in str(exc).lower()
    else:
        raise AssertionError("expected ValueError")
