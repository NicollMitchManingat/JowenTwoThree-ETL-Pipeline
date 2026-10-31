"""Business-date window helpers for extraction."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo


def business_day_window(business_date: date, timezone: str) -> tuple[datetime, datetime]:
    """Return the [start, end) window for a business date in a timezone."""

    tz = ZoneInfo(timezone)
    start = datetime.combine(business_date, time.min, tzinfo=tz)
    end = start + timedelta(days=1)
    return start, end


def previous_business_day(reference: datetime, timezone: str) -> date:
    """Return the business date before the reference instant."""

    tz = ZoneInfo(timezone)
    local_date = reference.astimezone(tz).date()
    return local_date - timedelta(days=1)
