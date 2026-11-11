"""Daily paid transaction totals and refunds on their own business dates."""
from datetime import UTC, date, datetime

import pandas as pd

from pipelines.money import ZERO, money

from .dimensions import date_key
from .schema import FACT_SALES_SUMMARY_COLUMNS


def build_sales_summary(frame: pd.DataFrame, loaded_at: datetime | None = None,
                        business_date: date | None = None,
                        refunds: pd.DataFrame | None = None) -> pd.DataFrame:
    reference = loaded_at or datetime.now(UTC)
    refunds = refunds if refunds is not None else pd.DataFrame()
    dates = {business_date} if business_date else set(frame.get("business_date", []))
    if business_date is None:
        dates.update(refunds.get("business_date", []))
    rows = []
    for day in sorted(dates):
        sales = frame.loc[frame["business_date"] == day] if not frame.empty else frame
        returned = refunds.loc[refunds["business_date"] == day] if not refunds.empty else refunds
        total = sum((money(x) for x in sales.get("total", [])), ZERO)
        refund = sum((money(x) for x in returned.get("refund_amount", [])), ZERO)
        rows.append({"sales_summary_key": f"SALES_{date_key(day)}", "date_key": date_key(day),
                     "total_sales": money(total), "refund_amount": money(refund),
                     "net_sales": money(total - refund),
                     "total_orders": int(sales["transaction_id"].nunique()) if not sales.empty else 0,
                     "loaded_at": pd.Timestamp(reference)})
    return pd.DataFrame(rows, columns=FACT_SALES_SUMMARY_COLUMNS)
