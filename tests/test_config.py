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
    assert config.etl.silver_dir == Path("data/silver")
    assert config.etl.quarantine_dir == Path("data/quarantine")
    assert config.etl.transform_log_dir == Path("data/logs/transformation")
    assert config.etl.retry_attempts == 3
    assert config.etl.retry_backoff_seconds == 2.0
    assert config.etl.log_level == "INFO"
    assert config.etl.log_retention_days == 90
    assert config.etl.lookback_days == 3
    assert config.etl.low_stock_threshold == 5
    assert config.etl.rejection_warn_rate == 0.01
    assert config.etl.rejection_fail_rate == 0.05


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


def test_lookback_days_must_be_positive() -> None:
    with pytest.raises(ConfigError, match="ETL_LOOKBACK_DAYS"):
        load_config({"ETL_MOCK_MODE": "true", "ETL_LOOKBACK_DAYS": "0"})


def test_low_stock_threshold_cannot_be_negative() -> None:
    with pytest.raises(ConfigError, match="ETL_LOW_STOCK_THRESHOLD"):
        load_config({"ETL_MOCK_MODE": "true", "ETL_LOW_STOCK_THRESHOLD": "-1"})


def test_rejection_rates_must_be_ordered() -> None:
    with pytest.raises(ConfigError, match="Rejection rates"):
        load_config(
            {
                "ETL_MOCK_MODE": "true",
                "ETL_REJECTION_WARN_RATE": "0.10",
                "ETL_REJECTION_FAIL_RATE": "0.05",
            }
        )


def test_live_pipeline_uses_local_profile_and_ignores_cancelled_hosted_profile(tmp_path, monkeypatch):
    import os

    from pipelines.load.target import warehouse_config
    from pipelines.run import configuration

    monkeypatch.chdir(tmp_path)
    for key in list(os.environ):
        if key.startswith(("WAREHOUSE_", "ETL_")):
            monkeypatch.delenv(key)
    (tmp_path / ".env").write_text("ETL_MOCK_MODE=true\nWAREHOUSE_DB_PASSWORD=\n")
    (tmp_path / ".env.warehouse.local").write_text(
        "WAREHOUSE_DB_HOST=127.0.0.1\nWAREHOUSE_DB_NAME=jowen_analytics\n"
        "WAREHOUSE_DB_USER=jowen_etl_local\nWAREHOUSE_DB_PASSWORD=local-secret\n")
    (tmp_path / ".env.warehouse").write_text(
        "WAREHOUSE_DB_HOST=db.cancelled.supabase.co\nWAREHOUSE_DB_PASSWORD=hosted-secret\n")
    monkeypatch.setenv("WAREHOUSE_CONFIG_FILE", ".env.warehouse")
    config = configuration()
    target = warehouse_config()
    assert config.etl.mock_mode
    assert "WAREHOUSE_DB_PASSWORD" not in os.environ
    assert target.host == "127.0.0.1"
    assert target.dbname == "jowen_analytics"
    assert target.password == "local-secret"
    assert "local-secret" not in repr(target)


def test_local_warehouse_rejects_remote_environment_override(tmp_path, monkeypatch):
    from pipelines.load.target import warehouse_config

    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env.warehouse.local").write_text("WAREHOUSE_DB_PASSWORD=local-secret\n")
    monkeypatch.setenv("WAREHOUSE_DB_HOST", "db.cancelled.supabase.co")
    with pytest.raises(ValueError, match="local PostgreSQL only"):
        warehouse_config()
