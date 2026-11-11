"""Version-2 extraction CLI: bounded dates and immutable source packages."""
import argparse
import json
import sys
from datetime import UTC, datetime

import psycopg

from pipelines.artifacts import new_id, write_json
from pipelines.config import ConfigError
from pipelines.db import SourceConnectionError
from pipelines.run import add_window_arguments, configuration, retry, window

from .batch import stage_extraction


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    add_window_arguments(parser)
    args = parser.parse_args(argv)
    config = None
    try:
        config = configuration(args.demo)
        start, end = window(args, config)
        result = retry(lambda: stage_extraction(start, end, config, args.dry_run),
                       config.etl.retry_attempts, config.etl.retry_backoff_seconds)
        print(json.dumps(result, default=str, indent=2))
        return 0
    except (ConfigError, ValueError, OSError, psycopg.Error, SourceConnectionError) as exc:
        failure = {"stage": "extraction", "status": "FAILED", "error_code": type(exc).__name__,
                   "error_message": str(exc)[:2000], "created_at": datetime.now(UTC).isoformat()}
        if config is not None and not args.dry_run:
            write_json(config.etl.log_dir / "pipeline" / f"{new_id('FailedExtraction')}.json", failure)
        print(json.dumps(failure), file=sys.stderr)
        return 1
