"""Stage 2 data cleaning rules C-01 to C-04."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from .rejects import build_rejects, empty_rejects


@dataclass(frozen=True)
class CleaningResult:
    """Cleaned records plus the rows rejected by cleaning rules."""

    cleaned: pd.DataFrame
    rejected: pd.DataFrame


def clean_transactions(frame: pd.DataFrame) -> CleaningResult:
    """Apply C-01 whitespace trimming to transaction numbers."""

    cleaned = frame.copy()
    if "transaction_number" in cleaned:
        cleaned["transaction_number"] = (
            cleaned["transaction_number"].astype("string").str.strip()
        )
    return CleaningResult(cleaned=cleaned, rejected=empty_rejects())


def clean_inventory(frame: pd.DataFrame) -> CleaningResult:
    """Apply C-02 title casing and C-04 negative-stock rejection."""

    cleaned = frame.copy()
    if "name" in cleaned:
        cleaned["name"] = cleaned["name"].astype("string").str.strip().str.title()

    numeric_stock = pd.to_numeric(cleaned["stock_quantity"], errors="coerce")
    negative = numeric_stock < 0
    rejected = build_rejects(
        cleaned,
        negative,
        source_table="inventory",
        id_column="item_id",
        rule_id="C-04",
        error_code="ERR_NEGATIVE_STOCK",
        reason="stock_quantity is negative",
    )
    cleaned = cleaned.loc[~negative].reset_index(drop=True)
    return CleaningResult(cleaned=cleaned, rejected=rejected)


def clean_traffic(frame: pd.DataFrame) -> CleaningResult:
    """Apply C-03 null customer-count handling."""

    cleaned = frame.copy()
    if "number_of_customer" in cleaned:
        cleaned["number_of_customer"] = cleaned["number_of_customer"].fillna(0)
    return CleaningResult(cleaned=cleaned, rejected=empty_rejects())
