import pytest

from pipelines import db
from pipelines.config import load_config


def test_with_retry_retries_then_succeeds() -> None:
    calls = {"count": 0}
    sleeps: list[float] = []

    def operation() -> str:
        calls["count"] += 1
        if calls["count"] < 3:
            raise db.SourceConnectionError("transient failure")
        return "ok"

    result = db.with_retry(operation, attempts=3, backoff_seconds=1.0, sleep=sleeps.append)

    assert result == "ok"
    assert calls["count"] == 3
    assert sleeps == [1.0, 2.0]


def test_with_retry_raises_after_exhausting_attempts() -> None:
    calls = {"count": 0}
    sleeps: list[float] = []

    def operation() -> str:
        calls["count"] += 1
        raise db.SourceConnectionError("still down")

    with pytest.raises(db.SourceConnectionError, match="still down"):
        db.with_retry(operation, attempts=3, backoff_seconds=1.0, sleep=sleeps.append)

    assert calls["count"] == 3
    assert sleeps == [1.0, 2.0]


def test_with_retry_rejects_invalid_attempts() -> None:
    with pytest.raises(ValueError, match="at least 1"):
        db.with_retry(lambda: "ok", attempts=0)


def test_get_source_connection_fails_in_mock_mode() -> None:
    config = load_config({"ETL_MOCK_MODE": "true"})

    with pytest.raises(db.SourceConnectionError, match="Mock mode"):
        db.get_source_connection(config)


def test_health_check_is_false_in_mock_mode() -> None:
    config = load_config({"ETL_MOCK_MODE": "true"})

    assert db.health_check(config) is False
