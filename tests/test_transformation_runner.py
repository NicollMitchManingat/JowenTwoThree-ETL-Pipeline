from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from pipelines.config import load_config
from pipelines.extraction.extract import (
    INVENTORY_COLUMNS,
    SALES_COLUMNS,
    TRAFFIC_COLUMNS,
)
from pipelines.extraction.parquet_io import read_parquet, write_parquet
from pipelines.extraction.run import run_extraction
from pipelines.transformation.input import TransformationInputError
from pipelines.transformation.metadata import read_transform_log
from pipelines.transformation.parquet_io import read_silver
from pipelines.transformation.runner import run_transformation

BUSINESS_DATE = date(2026, 10, 16)
PRIOR_DATE = date(2026, 10, 15)


def _config(tmp_path: Path):
    return load_config(
        {
            "ETL_MOCK_MODE": "true",
            "ETL_STAGING_DIR": str(tmp_path / "bronze"),
            "ETL_LOG_DIR": str(tmp_path / "logs"),
            "ETL_HANDOVER_DIR": str(tmp_path / "handover"),
            "ETL_SILVER_DIR": str(tmp_path / "silver"),
            "ETL_QUARANTINE_DIR": str(tmp_path / "quarantine"),
            "ETL_TRANSFORM_LOG_DIR": str(tmp_path / "logs" / "transformation"),
            "ETL_LOOKBACK_DAYS": "3",
            "ETL_LOW_STOCK_THRESHOLD": "5",
        }
    )


def _extract(config, business_date: date) -> None:
    result = run_extraction(business_date, config)
    assert result.status == "SUCCESS"


def test_mock_transformation_writes_silver_quarantine_and_log(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    _extract(config, BUSINESS_DATE)

    result = run_transformation(BUSINESS_DATE, config)

    assert result.status == "SUCCESS"
    assert result.validation_passed is True
    assert result.log_path is not None and result.log_path.exists()
    assert result.silver_paths["dim_date"].exists()
    assert result.silver_paths["fact_sales_summary/2026-10-16"].exists()
    assert result.silver_paths["fact_inventory_summary"].exists()
    assert result.silver_paths["fact_traffic_summary/2026-10-16"].exists()
    assert "inventory/2026-10-16" in result.quarantine_paths

    sales = read_silver(
        "fact_sales_summary", config.etl.silver_dir, BUSINESS_DATE
    )
    assert sales.iloc[0]["total_sales"] == pytest.approx(2310.50)
    assert sales.iloc[0]["total_orders"] == 5

    inventory = read_silver(
        "fact_inventory_summary", config.etl.silver_dir, BUSINESS_DATE
    )
    assert inventory.iloc[0]["total_items"] == 5
    assert inventory.iloc[0]["low_stock_items"] == 3
    assert inventory.iloc[0]["out_of_stock_items"] == 1

    traffic = read_silver(
        "fact_traffic_summary", config.etl.silver_dir, BUSINESS_DATE
    )
    assert traffic.iloc[0]["total_customers"] == 26
    assert traffic.iloc[0]["peak_hour"] == "18:00:00"

    payload = read_transform_log(result.log_path)
    assert payload["pipeline_run_id"] == result.run_id
    assert len(payload["tables"]) == 3


def test_rerun_is_idempotent(tmp_path: Path) -> None:
    config = _config(tmp_path)
    _extract(config, BUSINESS_DATE)

    first = run_transformation(BUSINESS_DATE, config)
    second = run_transformation(BUSINESS_DATE, config)

    sales = read_silver(
        "fact_sales_summary", config.etl.silver_dir, BUSINESS_DATE
    )
    assert len(sales) == 1
    assert not sales["sales_summary_key"].duplicated().any()
    assert first.silver_paths == second.silver_paths
    assert first.silver_paths["fact_sales_summary/2026-10-16"] == second.silver_paths[
        "fact_sales_summary/2026-10-16"
    ]


def test_lookback_reprocesses_late_arriving_transaction(tmp_path: Path) -> None:
    config = _config(tmp_path)
    _extract(config, PRIOR_DATE)
    _extract(config, BUSINESS_DATE)

    prior = read_parquet("transactions", PRIOR_DATE, config.etl.staging_dir)
    extra = pd.DataFrame(
        [
            {
                "transaction_id": "trx-late",
                "created_at": pd.Timestamp("2026-10-15 10:00:00"),
                "transaction_number": "TRX-LATE",
                "total": 100.00,
            }
        ]
    )
    updated = pd.concat([prior, extra], ignore_index=True)
    write_parquet(updated, "transactions", PRIOR_DATE, config.etl.staging_dir)

    run_transformation(BUSINESS_DATE, config, lookback_days=2)

    prior_sales = read_silver(
        "fact_sales_summary", config.etl.silver_dir, PRIOR_DATE
    )
    current_sales = read_silver(
        "fact_sales_summary", config.etl.silver_dir, BUSINESS_DATE
    )
    assert prior_sales.iloc[0]["total_sales"] == pytest.approx(2410.50)
    assert prior_sales.iloc[0]["total_orders"] == 6
    assert current_sales.iloc[0]["total_sales"] == pytest.approx(2310.50)


def test_empty_business_date_writes_zero_value_facts(tmp_path: Path) -> None:
    config = _config(tmp_path)
    _extract(config, BUSINESS_DATE)

    write_parquet(
        pd.DataFrame(columns=SALES_COLUMNS),
        "transactions",
        BUSINESS_DATE,
        config.etl.staging_dir,
    )
    write_parquet(
        pd.DataFrame(columns=INVENTORY_COLUMNS),
        "inventory",
        BUSINESS_DATE,
        config.etl.staging_dir,
    )
    write_parquet(
        pd.DataFrame(columns=TRAFFIC_COLUMNS),
        "customer_traffic",
        BUSINESS_DATE,
        config.etl.staging_dir,
    )

    result = run_transformation(BUSINESS_DATE, config)

    assert result.status == "SUCCESS"
    sales = read_silver(
        "fact_sales_summary", config.etl.silver_dir, BUSINESS_DATE
    )
    traffic = read_silver(
        "fact_traffic_summary", config.etl.silver_dir, BUSINESS_DATE
    )
    inventory = read_silver(
        "fact_inventory_summary", config.etl.silver_dir, BUSINESS_DATE
    )
    assert sales.iloc[0]["total_sales"] == 0
    assert traffic.iloc[0]["total_customers"] == 0
    assert inventory.iloc[0]["total_items"] == 0


def test_dry_run_writes_nothing(tmp_path: Path) -> None:
    config = _config(tmp_path)
    _extract(config, BUSINESS_DATE)

    result = run_transformation(BUSINESS_DATE, config, dry_run=True)

    assert result.status == "SUCCESS"
    assert result.silver_paths == {}
    assert result.quarantine_paths == {}
    assert result.log_path is None
    assert not (tmp_path / "silver").exists()
    assert not (tmp_path / "quarantine").exists()
    assert not (tmp_path / "logs" / "transformation").exists()


def test_missing_manifest_raises(tmp_path: Path) -> None:
    config = _config(tmp_path)

    with pytest.raises(TransformationInputError, match="no APPROVED"):
        run_transformation(BUSINESS_DATE, config)
