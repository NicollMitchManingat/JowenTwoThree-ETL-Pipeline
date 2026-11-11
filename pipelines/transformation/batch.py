"""Stage 2: transform only a sealed Stage 1 package, then seal the output."""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from pipelines.artifacts import new_id, read_package, write_package
from pipelines.config import AppConfig
from pipelines.extraction.batch import CONTRACT, validate_inventory

from .cleaning import clean_inventory, clean_traffic, clean_transactions
from .dimensions import build_dim_date
from .inventory import build_inventory_summary
from .sales import build_sales_summary
from .schema import TABLE_COLUMNS
from .standardization import (
    standardize_inventory,
    standardize_traffic,
    standardize_transactions,
)
from .traffic import build_traffic_summary


def transform_batch(manifest_path: Path, config: AppConfig, dry_run: bool = False) -> Path | dict:
    manifest, raw = read_package(manifest_path, "extraction")
    if set(raw) != set(CONTRACT):
        raise ValueError("extraction package has missing or unexpected tables")
    days = [date.fromisoformat(value) for value in manifest["dates"]]
    capture = datetime.fromisoformat(manifest["snapshot_at"])
    capture_day = capture.astimezone(ZoneInfo(config.etl.timezone)).date()
    transaction_source = raw["transactions"]
    paid = transaction_source.loc[transaction_source["payment_status"] == "PAID"]
    txn = standardize_transactions(clean_transactions(paid).cleaned, config.etl.timezone)
    refund_source = raw["refunds"].drop(columns=["transaction_id"]).rename(
        columns={"refund_id": "transaction_id", "refund_amount": "total"})
    refund = standardize_transactions(refund_source, config.etl.timezone)
    refund.rejected["source_table"] = "refunds"
    returned = refund.cleaned.rename(columns={"total": "refund_amount", "transaction_id": "refund_id"})
    active = raw["inventory"].loc[raw["inventory"]["is_active"]].copy()
    validate_inventory(active)
    clean_inv = clean_inventory(active)
    inv = standardize_inventory(clean_inv.cleaned)
    traffic = standardize_traffic(clean_traffic(raw["customer_traffic"]).cleaned,
                                  days[0], config.etl.timezone)
    counts = {}
    for table, source, result, excluded, extra_rejected in [
        ("transactions", transaction_source, txn, len(transaction_source) - len(paid), 0),
        ("refunds", raw["refunds"], refund, 0, 0),
        ("inventory", raw["inventory"], inv, len(raw["inventory"]) - len(active), len(clean_inv.rejected)),
        ("customer_traffic", raw["customer_traffic"], traffic, 0, 0),
    ]:
        counts[table] = {"read": len(source), "output": len(result.cleaned),
                         "excluded": excluded, "rejected": len(result.rejected) + extra_rejected}
        if counts[table]["read"] != sum(counts[table][key] for key in ("output", "excluded", "rejected")):
            raise ValueError(f"source reconciliation failed: {table}")
    financial_rejects = len(txn.rejected) + len(refund.rejected)
    rejected = sum(value["rejected"] for value in counts.values())
    read = sum(value["read"] for value in counts.values())
    rate = rejected / read if read else 0
    warnings = [event.__dict__ for event in inv.warnings + traffic.warnings]
    decision = ("REJECTED" if financial_rejects or rate > config.etl.rejection_fail_rate else
                "MANUAL_REVIEW" if warnings or rate > config.etl.rejection_warn_rate else "APPROVED")
    facts = {
        "fact_sales_summary": pd.concat([build_sales_summary(txn.cleaned, business_date=day,
                                                            refunds=returned) for day in days], ignore_index=True),
        "fact_inventory_summary": build_inventory_summary(inv.cleaned, capture_day, loaded_at=capture),
        "fact_traffic_summary": pd.concat([build_traffic_summary(traffic.cleaned, day) for day in days],
                                          ignore_index=True),
    }
    # Do not silently drop rows whose dates disagree with the approved input bounds.
    for table, frame in (("transactions", txn.cleaned), ("refunds", returned),
                         ("customer_traffic", traffic.cleaned)):
        if not frame.empty and not frame["business_date"].isin(days).all():
            raise ValueError(f"out-of-window standardized rows: {table}")
    dim = build_dim_date(min(days + [capture_day]), max(days + [capture_day]))
    for table, frame in facts.items():
        if list(frame.columns) != TABLE_COLUMNS[table] or frame["date_key"].duplicated().any():
            raise ValueError(f"fact contract or grain failure: {table}")
    rejects = [frame for frame in (txn.rejected, refund.rejected, clean_inv.rejected,
                                   inv.rejected, traffic.rejected) if not frame.empty]
    quarantine = pd.concat(rejects, ignore_index=True) if rejects else pd.DataFrame(
        columns=["source_table", "source_record_id", "rule_id", "error_code", "reason", "payload"])
    frames = {"dim_date": dim, **facts, "transformation_quarantine": quarantine}
    metadata = {"dates": manifest["dates"], "snapshot_at": manifest["snapshot_at"],
                "source_mode": manifest["source_mode"], "parent_run_id": manifest["pipeline_run_id"],
                "parent_manifest_sha256": __import__("hashlib").sha256(manifest_path.read_bytes()).hexdigest(),
                "source_counts": counts, "rejection_rate": rate, "warnings": warnings,
                "validation_results": {"source_reconciliation": "PASS", "fact_grain": "PASS",
                                       "financial_records": "FAIL" if financial_rejects else "PASS"}}
    if dry_run:
        return dict(metadata, decision=decision, output_counts={k: len(v) for k, v in frames.items()})
    return write_package(config.etl.silver_dir, new_id("Transform"), "transformation",
                         frames, metadata, decision)
