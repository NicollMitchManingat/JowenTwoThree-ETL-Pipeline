"""Exact numeric parsing shared by transformation and reconciliation."""

import re
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation

ZERO = Decimal("0.00")


def decimal_value(value: object) -> Decimal:
    text = re.sub(r"PHP|[₱,\s]", "", str(value), flags=re.IGNORECASE)
    try:
        number = Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("not a decimal") from exc
    if not number.is_finite():
        raise ValueError("decimal must be finite")
    return number


def money(value: object) -> Decimal:
    number = decimal_value(value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    if abs(number) >= Decimal(10000000000000000):
        raise ValueError("amount exceeds NUMERIC(18,2)")
    return number
