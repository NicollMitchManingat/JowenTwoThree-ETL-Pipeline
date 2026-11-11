"""Independent, local-only warehouse credentials and bounded connections."""
from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

import psycopg
from dotenv import dotenv_values


@dataclass(frozen=True)
class WarehouseConfig:
    host: str = "127.0.0.1"
    port: int = 55432
    dbname: str = "jowen_analytics"
    user: str = "jowen_etl_local"
    password: str = field(default="", repr=False)
    storage_path: Path = Path("data/warehouse")
    minimum_free_bytes: int = 1024 ** 3
    allow_demo: bool = False

    def connect(self):
        if self.host not in {"127.0.0.1", "localhost", "::1"}:
            raise ValueError("this warehouse deployment supports local PostgreSQL only")
        return psycopg.connect(host=self.host, port=self.port, dbname=self.dbname,
                               user=self.user, password=self.password, connect_timeout=10,
                               options="-c lock_timeout=10000 -c statement_timeout=180000")


def warehouse_config(env: Mapping[str, str] | None = None) -> WarehouseConfig:
    if env is None:
        env = {**dotenv_values(".env.warehouse.local"),
               **{key: value for key, value in os.environ.items() if key.startswith("WAREHOUSE_DB_")
                  or key in {"WAREHOUSE_STORAGE_PATH", "WAREHOUSE_MINIMUM_FREE_BYTES", "WAREHOUSE_ALLOW_DEMO"}}}
    password = env.get("WAREHOUSE_DB_PASSWORD", "")
    if not password:
        raise ValueError("WAREHOUSE_DB_PASSWORD is required; source credentials are never reused")
    config = WarehouseConfig(
        host=env.get("WAREHOUSE_DB_HOST", "127.0.0.1"),
        port=int(env.get("WAREHOUSE_DB_PORT", "55432")),
        dbname=env.get("WAREHOUSE_DB_NAME", "jowen_analytics"),
        user=env.get("WAREHOUSE_DB_USER", "jowen_etl_local"), password=password,
        storage_path=Path(env.get("WAREHOUSE_STORAGE_PATH", "data/warehouse")),
        minimum_free_bytes=int(env.get("WAREHOUSE_MINIMUM_FREE_BYTES", str(1024 ** 3))),
        allow_demo=env.get("WAREHOUSE_ALLOW_DEMO", "false").lower() == "true")
    if config.minimum_free_bytes < 0 or config.port < 1 or config.port > 65535:
        raise ValueError("invalid warehouse storage threshold or port")
    if config.host not in {"127.0.0.1", "localhost", "::1"}:
        raise ValueError("this warehouse deployment supports local PostgreSQL only")
    return config
