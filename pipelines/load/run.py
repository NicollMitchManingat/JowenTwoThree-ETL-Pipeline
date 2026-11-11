"""Load only a verified, approved Stage 2 manifest."""
import argparse
import json
from dataclasses import replace
from pathlib import Path

from .runner import load_batch
from .target import warehouse_config


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--demo", action="store_true", help="use the separate local _demo database")
    args = parser.parse_args(argv)
    try:
        target = warehouse_config()
        if args.demo:
            target = replace(target, dbname=target.dbname if target.dbname.endswith("_demo")
                             else target.dbname + "_demo", allow_demo=True)
        report = load_batch(args.manifest, target, dry_run=args.dry_run)
        print(json.dumps(report, default=str, indent=2))
        return 0 if report["status"] in {"SUCCESS", "SKIPPED", "DRY_RUN"} else 1
    except ValueError as exc:
        print(f"Configuration error: {exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
