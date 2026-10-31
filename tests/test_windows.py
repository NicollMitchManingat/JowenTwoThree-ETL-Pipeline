from datetime import UTC, date, datetime

from pipelines.extraction.windows import business_day_window, previous_business_day


def test_business_day_window_is_start_of_day_to_next_day() -> None:
    start, end = business_day_window(date(2026, 10, 12), "Asia/Manila")

    assert start.isoformat() == "2026-10-12T00:00:00+08:00"
    assert end.isoformat() == "2026-10-13T00:00:00+08:00"
    assert (end - start).total_seconds() == 86400


def test_previous_business_day_uses_local_timezone() -> None:
    reference = datetime(2026, 10, 13, 1, 0, tzinfo=UTC)

    assert previous_business_day(reference, "Asia/Manila") == date(2026, 10, 12)
