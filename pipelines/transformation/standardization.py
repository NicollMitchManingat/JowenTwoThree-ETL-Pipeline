"""Stage 2 standardization and data type rules S-01 to S-05."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import ROUND_HALF_UP, Decimal

import pandas as pd

from .rejects import build_rejects, empty_rejects


@dataclass(frozen=True)
class WarningEvent:
    """A non-critical issue that should be visible in validation and logs."""

    rule_id: str
    error_code: str
    count: int
    message: str


@dataclass(frozen=True)
class StandardizationResult:
    """Standardized records, rejected rows, and non-critical warnings."""

    cleaned: pd.DataFrame
    rejected: pd.DataFrame
    warnings: list[WarningEvent] = field(default_factory=list)


def _parse_business_datetime(series: pd.Series, timezone: str) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce")
    if getattr(parsed.dt, "tz", None) is None:
        return parsed.dt.tz_localize(
            timezone, ambiguous="NaT", nonexistent="NaT"
        )
    return parsed.dt.tz_convert(timezone)


def _parse_money(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce")
    text = series.astype("string").str.replace(
        r"[Pp][Hh][Pp]|[₱,\s]", "", regex=True
    )
    return pd.to_numeric(text, errors="coerce")


def _round_half_up(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")

    def round_value(value: float) -> float:
        if pd.isna(value):
            return value
        return float(
            Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        )

    return values.map(round_value)


def standardize_transactions(
    frame: pd.DataFrame,
    timezone: str,
) -> StandardizationResult:
    """Apply S-01 date conversion, S-02 money casting, and ID casing."""

    cleaned = frame.copy()
    rejected_parts: list[pd.DataFrame] = []

    parsed_dates = _parse_business_datetime(cleaned["created_at"], timezone)
    invalid_dates = parsed_dates.isna()
    rejected_parts.append(
        build_rejects(
            cleaned,
            invalid_dates,
            source_table="transactions",
            id_column="transaction_id",
            rule_id="S-01",
            error_code="ERR_DATE_CONVERSION",
            reason="created_at cannot be converted to a business date",
        )
    )
    cleaned = cleaned.loc[~invalid_dates].copy()
    cleaned["business_date"] = parsed_dates.loc[~invalid_dates].dt.date

    parsed_totals = _parse_money(cleaned["total"])
    invalid_totals = parsed_totals.isna()
    rejected_parts.append(
        build_rejects(
            cleaned,
            invalid_totals,
            source_table="transactions",
            id_column="transaction_id",
            rule_id="S-02",
            error_code="ERR_TYPE_CONVERSION",
            reason="total cannot be converted to NUMERIC(10,2)",
        )
    )
    cleaned = cleaned.loc[~invalid_totals].copy()
    cleaned["total"] = _round_half_up(parsed_totals.loc[~invalid_totals])

    if "transaction_number" in cleaned:
        cleaned["transaction_number"] = (
            cleaned["transaction_number"].astype("string").str.strip().str.upper()
        )

    return StandardizationResult(
        cleaned=cleaned.reset_index(drop=True),
        rejected=_combine_rejects(rejected_parts),
    )


def standardize_inventory(frame: pd.DataFrame) -> StandardizationResult:
    """Apply S-03 name fallback and S-04 integer stock casting."""

    cleaned = frame.copy()
    rejected_parts: list[pd.DataFrame] = []
    warnings: list[WarningEvent] = []

    if "name" in cleaned:
        names = cleaned["name"].astype("string").str.strip()
        unnamed = names.isna() | names.eq("")
        if unnamed.any():
            warnings.append(
                WarningEvent(
                    "S-03",
                    "WARN_UNNAMED_ITEM",
                    int(unnamed.sum()),
                    "inventory rows received the UNNAMED ITEM fallback",
                )
            )
        cleaned["name"] = names.mask(unnamed, "UNNAMED ITEM")

    numeric_stock = pd.to_numeric(cleaned["stock_quantity"], errors="coerce")
    negative = numeric_stock < 0
    rejected_parts.append(
        build_rejects(
            cleaned,
            negative,
            source_table="inventory",
            id_column="item_id",
            rule_id="C-04",
            error_code="ERR_NEGATIVE_STOCK",
            reason="stock_quantity is negative",
        )
    )
    invalid = numeric_stock.isna() & ~negative
    rejected_parts.append(
        build_rejects(
            cleaned,
            invalid,
            source_table="inventory",
            id_column="item_id",
            rule_id="S-04",
            error_code="ERR_TYPE_CONVERSION",
            reason="stock_quantity cannot be converted to INTEGER",
        )
    )

    keep = ~(negative | invalid)
    cleaned = cleaned.loc[keep].copy()
    cleaned["stock_quantity"] = (
        numeric_stock.loc[keep].round().astype("int64")
    )

    return StandardizationResult(
        cleaned=cleaned.reset_index(drop=True),
        rejected=_combine_rejects(rejected_parts),
        warnings=warnings,
    )


def standardize_traffic(
    frame: pd.DataFrame,
    business_date: date,
    timezone: str,
) -> StandardizationResult:
    """Apply S-05 timestamp/peak-hour conversion and customer-count casting."""

    cleaned = frame.copy()
    rejected_parts: list[pd.DataFrame] = []
    warnings: list[WarningEvent] = []

    parsed_dates = _parse_business_datetime(cleaned["created_at"], timezone)
    invalid_dates = parsed_dates.isna()
    if invalid_dates.any():
        warnings.append(
            WarningEvent(
                "S-05",
                "WARN_PEAK_HOUR",
                int(invalid_dates.sum()),
                "customer_traffic rows have an invalid created_at and no peak hour",
            )
        )

    cleaned["business_date"] = (
        parsed_dates.dt.date.astype("object").where(~invalid_dates, business_date)
    )
    entry_hour = parsed_dates.dt.hour.astype("Int64")
    cleaned["entry_hour"] = entry_hour

    customer_counts = pd.to_numeric(cleaned["number_of_customer"], errors="coerce")
    invalid_counts = customer_counts.isna() | (customer_counts < 0)
    rejected_parts.append(
        build_rejects(
            cleaned,
            invalid_counts,
            source_table="customer_traffic",
            id_column="entry_id",
            rule_id="C-03",
            error_code="ERR_TYPE_CONVERSION",
            reason="number_of_customer cannot be converted to a non-negative INTEGER",
        )
    )
    cleaned = cleaned.loc[~invalid_counts].copy()
    cleaned["number_of_customer"] = (
        customer_counts.loc[~invalid_counts].astype("int64")
    )

    return StandardizationResult(
        cleaned=cleaned.reset_index(drop=True),
        rejected=_combine_rejects(rejected_parts),
        warnings=warnings,
    )


def _combine_rejects(parts: list[pd.DataFrame]) -> pd.DataFrame:
    frames = [part for part in parts if not part.empty]
    if not frames:
        return empty_rejects()
    return pd.concat(frames, ignore_index=True)
