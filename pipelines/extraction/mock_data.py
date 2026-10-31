"""Deterministic sample data used when mock mode is enabled."""

from __future__ import annotations

from datetime import date

import pandas as pd


def transactions(business_date: date) -> pd.DataFrame:
    """Return a deterministic set of sales transactions for a business date."""

    base = pd.Timestamp(business_date)
    rows = [
        {
            "transaction_id": "trx-0001",
            "created_at": base + pd.Timedelta(hours=8, minutes=15),
            "transaction_number": "  TRX-1001  ",
            "total": 245.50,
        },
        {
            "transaction_id": "trx-0002",
            "created_at": base + pd.Timedelta(hours=10, minutes=42),
            "transaction_number": "TRX-1002",
            "total": 89.00,
        },
        {
            "transaction_id": "trx-0003",
            "created_at": base + pd.Timedelta(hours=12, minutes=5),
            "transaction_number": "trx-1003",
            "total": 1320.75,
        },
        {
            "transaction_id": "trx-0004",
            "created_at": base + pd.Timedelta(hours=18, minutes=30),
            "transaction_number": "TRX-1004",
            "total": 45.25,
        },
        {
            "transaction_id": "trx-0005",
            "created_at": base + pd.Timedelta(hours=21, minutes=58),
            "transaction_number": "TRX-1005",
            "total": 610.00,
        },
    ]
    return pd.DataFrame(rows)


def inventory() -> pd.DataFrame:
    """Return a deterministic inventory snapshot with varied stock levels."""

    rows = [
        {"item_id": "item-0001", "name": "Iced Coffee", "stock_quantity": 12},
        {"item_id": "item-0002", "name": "iced LATTE", "stock_quantity": 3},
        {"item_id": "item-0003", "name": "Croissant", "stock_quantity": 0},
        {"item_id": "item-0004", "name": "Blueberry Muffin", "stock_quantity": -5},
        {"item_id": "item-0005", "name": "Matcha Tea", "stock_quantity": 50},
        {"item_id": "item-0006", "name": "Bottled Water", "stock_quantity": 1},
    ]
    return pd.DataFrame(rows)


def customer_traffic(business_date: date) -> pd.DataFrame:
    """Return a deterministic set of customer traffic entries."""

    base = pd.Timestamp(business_date)
    rows = [
        {
            "entry_id": "traffic-0001",
            "number_of_customer": 4,
            "created_at": base + pd.Timedelta(hours=7, minutes=30),
        },
        {
            "entry_id": "traffic-0002",
            "number_of_customer": 7,
            "created_at": base + pd.Timedelta(hours=11, minutes=0),
        },
        {
            "entry_id": "traffic-0003",
            "number_of_customer": None,
            "created_at": base + pd.Timedelta(hours=12, minutes=45),
        },
        {
            "entry_id": "traffic-0004",
            "number_of_customer": 12,
            "created_at": base + pd.Timedelta(hours=18, minutes=15),
        },
        {
            "entry_id": "traffic-0005",
            "number_of_customer": 3,
            "created_at": base + pd.Timedelta(hours=21, minutes=40),
        },
    ]
    frame = pd.DataFrame(rows)
    frame["number_of_customer"] = frame["number_of_customer"].astype("Int64")
    return frame
