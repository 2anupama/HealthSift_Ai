"""Schema and structural validation utilities for HealthSift AI."""

from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd

from config import ERROR_DIR, REQUIRED_COLUMNS
from src.logger import get_logger

logger = get_logger(__name__)


def _move_to_error(file_path: Path) -> Path:
    """Move an invalid file to the error directory, preserving uniqueness."""
    ERROR_DIR.mkdir(parents=True, exist_ok=True)

    destination = ERROR_DIR / file_path.name
    if destination.exists():
        destination = ERROR_DIR / f"{file_path.stem}_{file_path.stat().st_mtime_ns}{file_path.suffix}"

    shutil.move(str(file_path), str(destination))
    return destination


def validate(df: pd.DataFrame, filepath: str | Path) -> tuple[bool, str | None]:
    """Validate a loaded DataFrame against required rules.

    Validation checks:
    1) DataFrame is not empty
    2) All required columns are present

    If validation fails, file is moved to `error/` and reason is logged.
    """
    file_path = Path(filepath)

    try:
        if df is None:
            message = "Validation failed: DataFrame is None or file is unreadable."
            moved_to = _move_to_error(file_path)
            logger.error("%s File moved to: %s", message, moved_to)
            return False, message

        if df.empty:
            message = "Validation failed: File is empty."
            moved_to = _move_to_error(file_path)
            logger.error("%s File moved to: %s", message, moved_to)
            return False, message

        missing_columns = [column for column in REQUIRED_COLUMNS if column not in df.columns]
        if missing_columns:
            message = f"Validation failed: Missing required columns: {missing_columns}"
            moved_to = _move_to_error(file_path)
            logger.error("%s File moved to: %s", message, moved_to)
            return False, message

        logger.info("Validation succeeded for file: %s", file_path)
        return True, None
    except Exception as exc:
        # Catch corrupted/unreadable edge cases gracefully and quarantine the file.
        message = f"Validation error for {file_path}: {exc}"
        try:
            moved_to = _move_to_error(file_path)
            logger.error("%s File moved to: %s", message, moved_to)
        except Exception as move_exc:
            logger.error(
                "%s Additionally failed to move file to error directory: %s",
                message,
                move_exc,
            )
        return False, message
