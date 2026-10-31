"""Environment-driven configuration for the ETL pipeline."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


class ConfigError(Exception):
    """Raised when the pipeline configuration is missing or invalid."""


@dataclass(frozen=True)
class SourceDBConfig:
    """Connection settings for the Supabase PostgreSQL source database."""

    host: str
    port: int
    name: str
    user: str
    password: str
    sslmode: str
    read_only: bool = True


@dataclass(frozen=True)
class ETLConfig:
    """Pipeline behaviour settings."""

    mock_mode: bool
    timezone: str
    staging_dir: Path
    log_dir: Path
    handover_dir: Path
    silver_dir: Path
    quarantine_dir: Path
    transform_log_dir: Path
    retry_attempts: int
    retry_backoff_seconds: float
    log_level: str
    log_retention_days: int
    lookback_days: int
    low_stock_threshold: int
    rejection_warn_rate: float
    rejection_fail_rate: float


@dataclass(frozen=True)
class AppConfig:
    """Top-level application configuration."""

    source: SourceDBConfig | None
    etl: ETLConfig


def _get(env: Mapping[str, str], key: str, default: str | None = None) -> str | None:
    value = env.get(key, default)
    if value is None:
        return None
    value = value.strip()
    return value or None


def _require(env: Mapping[str, str], key: str) -> str:
    value = _get(env, key)
    if value is None:
        raise ConfigError(f"Missing required environment variable: {key}")
    return value


def _get_bool(env: Mapping[str, str], key: str, default: bool) -> bool:
    raw = _get(env, key)
    if raw is None:
        return default
    normalized = raw.lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ConfigError(f"{key} must be a boolean, got: {raw!r}")


def _get_int(env: Mapping[str, str], key: str, default: int) -> int:
    raw = _get(env, key)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{key} must be an integer, got: {raw!r}") from exc


def _get_float(env: Mapping[str, str], key: str, default: float) -> float:
    raw = _get(env, key)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ConfigError(f"{key} must be a number, got: {raw!r}") from exc


def load_config(env: Mapping[str, str] | None = None) -> AppConfig:
    """Build configuration from the environment (or an explicit mapping)."""

    if env is None:
        load_dotenv()
        env = os.environ

    retry_attempts = _get_int(env, "ETL_RETRY_ATTEMPTS", 3)
    if retry_attempts < 1:
        raise ConfigError("ETL_RETRY_ATTEMPTS must be at least 1")

    retry_backoff = _get_float(env, "ETL_RETRY_BACKOFF_SECONDS", 2.0)
    if retry_backoff < 0:
        raise ConfigError("ETL_RETRY_BACKOFF_SECONDS cannot be negative")

    log_retention = _get_int(env, "ETL_LOG_RETENTION_DAYS", 90)
    if log_retention < 1:
        raise ConfigError("ETL_LOG_RETENTION_DAYS must be at least 1")

    lookback_days = _get_int(env, "ETL_LOOKBACK_DAYS", 3)
    if lookback_days < 1:
        raise ConfigError("ETL_LOOKBACK_DAYS must be at least 1")

    low_stock_threshold = _get_int(env, "ETL_LOW_STOCK_THRESHOLD", 5)
    if low_stock_threshold < 0:
        raise ConfigError("ETL_LOW_STOCK_THRESHOLD cannot be negative")

    rejection_warn_rate = _get_float(env, "ETL_REJECTION_WARN_RATE", 0.01)
    rejection_fail_rate = _get_float(env, "ETL_REJECTION_FAIL_RATE", 0.05)
    if not 0.0 <= rejection_warn_rate <= rejection_fail_rate <= 1.0:
        raise ConfigError(
            "Rejection rates must satisfy 0 <= ETL_REJECTION_WARN_RATE "
            "<= ETL_REJECTION_FAIL_RATE <= 1"
        )

    etl = ETLConfig(
        mock_mode=_get_bool(env, "ETL_MOCK_MODE", False),
        timezone=_get(env, "ETL_TIMEZONE", "Asia/Manila") or "Asia/Manila",
        staging_dir=Path(_get(env, "ETL_STAGING_DIR", "data/bronze") or "data/bronze"),
        log_dir=Path(_get(env, "ETL_LOG_DIR", "data/logs") or "data/logs"),
        handover_dir=Path(_get(env, "ETL_HANDOVER_DIR", "data/handover") or "data/handover"),
        silver_dir=Path(_get(env, "ETL_SILVER_DIR", "data/silver") or "data/silver"),
        quarantine_dir=Path(
            _get(env, "ETL_QUARANTINE_DIR", "data/quarantine") or "data/quarantine"
        ),
        transform_log_dir=Path(
            _get(env, "ETL_TRANSFORM_LOG_DIR", "data/logs/transformation")
            or "data/logs/transformation"
        ),
        retry_attempts=retry_attempts,
        retry_backoff_seconds=retry_backoff,
        log_level=(_get(env, "LOG_LEVEL", "INFO") or "INFO").upper(),
        log_retention_days=log_retention,
        lookback_days=lookback_days,
        low_stock_threshold=low_stock_threshold,
        rejection_warn_rate=rejection_warn_rate,
        rejection_fail_rate=rejection_fail_rate,
    )

    source: SourceDBConfig | None = None
    if not etl.mock_mode:
        source = SourceDBConfig(
            host=_require(env, "SUPABASE_DB_HOST"),
            port=_get_int(env, "SUPABASE_DB_PORT", 5432),
            name=_require(env, "SUPABASE_DB_NAME"),
            user=_require(env, "SUPABASE_DB_USER"),
            password=_require(env, "SUPABASE_DB_PASSWORD"),
            sslmode=_get(env, "SUPABASE_DB_SSLMODE", "require") or "require",
            read_only=_get_bool(env, "SUPABASE_READ_ONLY", True),
        )

    return AppConfig(source=source, etl=etl)
