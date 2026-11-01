"""Command-line entry point for the Stage 2 transformation batch."""

from __future__ import annotations

import argparse
import os

from dotenv import load_dotenv

from pipelines.config import ConfigError, load_config
from pipelines.extraction.run import resolve_business_date

from .input import TransformationInputError
from .runner import run_transformation


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the Jowen Two Three transformation batch."
    )
    parser.add_argument("--date", help="business date in YYYY-MM-DD format")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help="number of business dates to reprocess (default: config)",
    )
    parser.add_argument("--demo", action="store_true", help="force mock mode")
    parser.add_argument("--dry-run", action="store_true", help="skip all writes")
    parser.add_argument(
        "--parent-run-id",
        default=None,
        help="explicit extraction run ID to consume",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    load_dotenv()

    env = dict(os.environ)
    if args.demo:
        env["ETL_MOCK_MODE"] = "true"

    try:
        config = load_config(env)
    except ConfigError as exc:
        print(f"Configuration error: {exc}")
        return 2

    business_date = resolve_business_date(args.date, config)
    try:
        result = run_transformation(
            business_date,
            config,
            lookback_days=args.lookback_days,
            dry_run=args.dry_run,
            parent_run_id=args.parent_run_id,
        )
    except TransformationInputError as exc:
        print(f"Input error: {exc}")
        return 2

    print(
        f"{result.run_id} {result.business_date.isoformat()} "
        f"parent={result.parent_run_id} status={result.status} "
        f"validation={'PASS' if result.validation_passed else 'FAIL'}"
    )
    for name, path in result.silver_paths.items():
        print(f" - {name}: {path}")
    for name, path in result.quarantine_paths.items():
        print(f" - quarantine {name}: {path}")
    if result.log_path is not None:
        print(f" log: {result.log_path}")

    return 0 if result.status in {"SUCCESS", "WARNING"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
