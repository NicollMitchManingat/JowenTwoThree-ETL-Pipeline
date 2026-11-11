"""Portable PostgreSQL lifecycle inside this repository; never installs a system service."""
from __future__ import annotations

import argparse
import json
import os
import secrets
import subprocess
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from urllib.request import urlretrieve

import psycopg
from dotenv import dotenv_values
from psycopg import sql

ROOT = Path(__file__).resolve().parents[1]
HOME = ROOT / "data" / "warehouse"
BIN = ROOT / "data" / "tools" / "pgsql" / "bin"
DATABASES = ("jowen_analytics", "jowen_analytics_demo")
DOWNLOAD = "https://get.enterprisedb.com/postgresql/postgresql-17.11-3-windows-x64-binaries.zip"


def execute(arguments, environment=None):
    HOME.mkdir(parents=True, exist_ok=True)
    with (HOME / "bootstrap.log").open("a", encoding="utf-8") as log:
        subprocess.run([str(arg) for arg in arguments], check=True, stdin=subprocess.DEVNULL,
                       stdout=log, stderr=log, env=environment, timeout=120,
                       creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--download", action="store_true", help="download pinned Windows binaries from EDB")
    parser.add_argument("--stop", action="store_true")
    parser.add_argument("--backup", action="store_true")
    args = parser.parse_args()
    os.chdir(ROOT)
    HOME.mkdir(parents=True, exist_ok=True)
    if args.download:
        tools = ROOT / "data" / "tools"
        tools.mkdir(parents=True, exist_ok=True)
        archive = tools / "postgresql-17.11-3.zip"
        if not archive.exists():
            urlretrieve(DOWNLOAD, archive)
        with zipfile.ZipFile(archive) as bundle:
            for entry in bundle.infolist():
                if entry.filename.startswith(("pgsql/bin/", "pgsql/lib/", "pgsql/share/")):
                    if not (tools / entry.filename).resolve().is_relative_to(tools.resolve()):
                        raise ValueError("archive entry escapes the tools directory")
                    bundle.extract(entry, tools)
    if not (BIN / "pg_ctl.exe").exists():
        raise SystemExit("Portable PostgreSQL missing; on Windows rerun with --download")
    config_path = HOME / "admin.json"
    if not config_path.exists():
        config_path.write_text(json.dumps({"host": "127.0.0.1", "port": 55432, "user": "etl_local_admin",
                                           "password": secrets.token_urlsafe(32), "dbname": DATABASES[0]}))
    settings = json.loads(config_path.read_text())
    cluster = HOME / "pgdata"
    if args.stop:
        execute([BIN / "pg_ctl.exe", "-D", cluster, "-m", "fast", "-w", "stop"])
        print("Stopped the ETL-local PostgreSQL cluster")
        return
    if not (cluster / "PG_VERSION").exists():
        password_file = HOME / "init-password.txt"
        password_file.write_text(settings["password"])
        try:
            execute([BIN / "initdb.exe", "-D", cluster, "-U", settings["user"], "--pwfile", password_file,
                     "--auth=scram-sha-256", "--locale=C", "--encoding=UTF8"])
        finally:
            password_file.unlink(missing_ok=True)
    status = subprocess.run([str(BIN / "pg_ctl.exe"), "-D", str(cluster), "status"], check=False,
                            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
    if status.returncode:
        execute([BIN / "pg_ctl.exe", "-D", cluster, "-l", HOME / "server.log",
                 "-o", "-h 127.0.0.1 -p 55432", "-w", "start"])
    with psycopg.connect(**dict(settings, dbname="postgres"), autocommit=True) as connection:
        for database in DATABASES:
            if not connection.execute("SELECT 1 FROM pg_database WHERE datname=%s", (database,)).fetchone():
                connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    for database in DATABASES:
        with psycopg.connect(**dict(settings, dbname=database)) as connection:
            for script in ("schema.sql", "roles.sql", "load.sql"):
                connection.execute((ROOT / "warehouse" / script).read_text(encoding="utf-8"))
    login = "jowen_etl_local"
    environment_path = ROOT / ".env.warehouse.local"
    with psycopg.connect(**settings) as connection:
        exists = connection.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (login,)).fetchone()
        if not exists:
            password = secrets.token_urlsafe(32)
            connection.execute(sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                sql.Identifier(login), sql.Literal(password)))
            environment_path.write_text(
                "WAREHOUSE_DB_HOST=127.0.0.1\nWAREHOUSE_DB_PORT=55432\n"
                f"WAREHOUSE_DB_NAME={DATABASES[0]}\nWAREHOUSE_DB_USER={login}\n"
                f"WAREHOUSE_DB_PASSWORD={password}\nWAREHOUSE_STORAGE_PATH=data/warehouse\n")
        elif not environment_path.exists() or not dotenv_values(environment_path).get("WAREHOUSE_DB_PASSWORD"):
            raise ValueError("existing login credentials are missing; recover them without resetting the account")
        connection.execute(sql.SQL("GRANT etl_warehouse_writer TO {}").format(sql.Identifier(login)))
    if args.backup:
        backup = HOME / "backups"
        backup.mkdir(exist_ok=True)
        environment = dict(os.environ, PGPASSWORD=settings["password"])
        for database in DATABASES:
            execute([BIN / "pg_dump.exe", "-h", settings["host"], "-p", settings["port"],
                     "-U", settings["user"], "-Fc", "-f",
                     backup / f"{database}_{datetime.now(UTC):%Y%m%d_%H%M%S}.dump", database], environment)
    print("Local PostgreSQL ready on 127.0.0.1:55432; separate reporting/demo databases and restricted login verified")


if __name__ == "__main__":
    main()
