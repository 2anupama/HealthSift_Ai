"""Centralized logging utilities for HealthSift AI."""

from __future__ import annotations

import logging
from datetime import datetime

from config import LOG_DIR

# One timestamped file per process run/session.
_SESSION_LOG_FILE = LOG_DIR / f"healthsift_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
_HANDLERS_CONFIGURED = False


def _configure_root_logger() -> None:
    """Configure root logger handlers once for console + file output."""
    global _HANDLERS_CONFIGURED

    if _HANDLERS_CONFIGURED:
        return

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter(_FORMAT, datefmt=_DATE_FORMAT)

    file_handler = logging.FileHandler(_SESSION_LOG_FILE, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    _HANDLERS_CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """Return a configured logger instance for the given module name."""
    _configure_root_logger()
    return logging.getLogger(name)
