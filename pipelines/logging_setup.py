"""Shared logging configuration for the ETL pipeline."""

from __future__ import annotations

import logging
import sys

LOGGER_NAME = "jowen_etl"


def configure_logging(level: str = "INFO") -> logging.Logger:
    """Configure and return the pipeline logger."""

    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(level.upper())
    logger.propagate = False

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
        )
        logger.addHandler(handler)

    return logger
