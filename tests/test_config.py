from pathlib import Path

import pytest

from pipelines.config import ConfigError, load_config


def test_mock_mode_uses_defaults() -> None:
    config = load_config({"ETL_MOCK_MODE": "true"})

    assert config.source is None
    assert config.etl.mock_mode is True
    assert config.etl.timezone == "Asia/Manila"
    assert config.etl.staging_dir == Path("data/bronze")
    assert config.etl.log_dir == Path("data/logs")
    assert config.etl.handover_dir == Path("data/handover")
    assert config.etl.retry_attempts == 3
    assert config.etl.retry_backoff_seconds == 2.0
    assert config.etl.log_level == "INFO"
    assert config.etl.log_retention_days == 90


def test_real_mode_requires_source_fields() -> None:
    with pytest.raises(ConfigError, match="SUPABASE_DB_HOST"):
        load_config({"ETL_MOCK_MODE": "false"})


def test_real_mode_parses_source_settings() -> None:
    config = load_config(
        {
            "ETL_MOCK_MODE": "false",
            "SUPABASE_DB_HOST": "db.example.com",
            "SUPABASE_DB_PORT": "6543",
            "SUPABASE_DB_NAME": "postgres",
            "SUPABASE_DB_USER": "postgres.project",
            "SUPABASE_DB_PASSWORD": "secret",
            "SUPABASE_DB_SSLMODE": "require",
            "SUPABASE_READ_ONLY": "true",
        }
    )

    assert config.source is not None
    assert config.source.host == "db.example.com"
    assert config.source.port == 6543
    assert config.source.name == "postgres"
    assert config.source.user == "postgres.project"
    assert config.source.password == "secret"
    assert config.source.sslmode == "require"
    assert config.source.read_only is True


def test_read_only_defaults_to_true() -> None:
    config = load_config(
        {
            "ETL_MOCK_MODE": "false",
            "SUPABASE_DB_HOST": "db.example.com",
            "SUPABASE_DB_NAME": "postgres",
            "SUPABASE_DB_USER": "postgres.project",
            "SUPABASE_DB_PASSWORD": "secret",
        }
    )

    assert config.source is not None
    assert config.source.read_only is True


def test_invalid_integer_setting_is_rejected() -> None:
    with pytest.raises(ConfigError, match="ETL_RETRY_ATTEMPTS"):
        load_config({"ETL_MOCK_MODE": "true", "ETL_RETRY_ATTEMPTS": "many"})


def test_retry_attempts_must_be_positive() -> None:
    with pytest.raises(ConfigError, match="at least 1"):
        load_config({"ETL_MOCK_MODE": "true", "ETL_RETRY_ATTEMPTS": "0"})


def test_invalid_boolean_setting_is_rejected() -> None:
    with pytest.raises(ConfigError, match="ETL_MOCK_MODE"):
        load_config({"ETL_MOCK_MODE": "maybe"})
