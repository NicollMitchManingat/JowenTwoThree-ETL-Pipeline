"""Transform or review an immutable Stage 1/2 handover; no source connection is needed."""
import argparse
import json
import os
from pathlib import Path

from dotenv import load_dotenv

from pipelines.config import ConfigError, load_config

from .batch import transform_batch
from .review import approve


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--review-reason", help="explicitly approve a held non-critical Stage 2 package")
    args = parser.parse_args(argv)
    load_dotenv()
    config = load_config(dict(os.environ, ETL_MOCK_MODE="true"))
    try:
        if args.review_reason:
            if args.dry_run:
                raise ValueError("review approval cannot be combined with dry-run")
            result = approve(args.manifest, config, args.review_reason)
        else:
            result = transform_batch(args.manifest, config, args.dry_run)
        if isinstance(result, dict):
            result.pop("_frames", None)
            metadata = result
        else:
            metadata = json.loads(result.read_text(encoding="utf-8"))
            metadata["manifest_path"] = str(result)
        print(json.dumps(metadata, default=str, indent=2))
        return 0 if metadata["decision"] == "APPROVED" else 1
    except (ConfigError, ValueError, KeyError, TypeError, OSError, ArithmeticError) as exc:
        print(f"Transformation stopped: {type(exc).__name__}: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
