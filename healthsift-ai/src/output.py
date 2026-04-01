"""Output writing utilities for HealthSift AI."""

from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd

from config import (
    ERROR_DIR,
    INPUT_FILE_SUCCESS_ACTION,
    PROCESSED_DIR,
    PROCESSED_INPUT_DIR,
)
from src.logger import get_logger

logger = get_logger(__name__)


def _unique_destination(base_path: Path) -> Path:
    """Return a unique destination path by appending a numeric suffix if needed."""
    if not base_path.exists():
        return base_path

    counter = 1
    while True:
        candidate = base_path.with_name(f"{base_path.stem}_{counter}{base_path.suffix}")
        if not candidate.exists():
            return candidate
        counter += 1


def _move_to_error(file_path: Path) -> Path:
    """Move source file to error directory with collision-safe naming."""
    ERROR_DIR.mkdir(parents=True, exist_ok=True)
    destination = _unique_destination(ERROR_DIR / file_path.name)
    shutil.move(str(file_path), str(destination))
    return destination


def _handle_source_file_on_success(file_path: Path) -> None:
    """Delete or archive original source file after successful processing."""
    action = INPUT_FILE_SUCCESS_ACTION.strip().lower()
    if not file_path.exists():
        logger.warning("Source file not found after processing; skip post-action: %s", file_path)
        return

    if action == "delete":
        file_path.unlink()
        logger.info("Deleted original input file: %s", file_path)
        return

    if action == "archive":
        PROCESSED_INPUT_DIR.mkdir(parents=True, exist_ok=True)
        destination = _unique_destination(PROCESSED_INPUT_DIR / file_path.name)
        shutil.move(str(file_path), str(destination))
        logger.info("Archived original input file to: %s", destination)
        return

    logger.warning(
        "Unknown INPUT_FILE_SUCCESS_ACTION '%s'. Defaulting to archive.",
        INPUT_FILE_SUCCESS_ACTION,
    )
    PROCESSED_INPUT_DIR.mkdir(parents=True, exist_ok=True)
    fallback_destination = _unique_destination(PROCESSED_INPUT_DIR / file_path.name)
    shutil.move(str(file_path), str(fallback_destination))
    logger.info("Archived original input file to: %s", fallback_destination)


def save_output(df: pd.DataFrame, original_filepath: str | Path) -> Path | None:
    """Save processed DataFrame to processed/ and move original input file.

    The output filename format is: <original_name>_processed<original_extension>.
    Supported extensions: .csv and .xlsx.
    """
    source_file = Path(original_filepath)
    extension = source_file.suffix.lower()
    output_name = f"{source_file.stem}_processed{source_file.suffix}"
    output_path = PROCESSED_DIR / output_name

    try:
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

        output_df = df.copy()
        if "health_category" in output_df.columns:
            ordered_cols = [col for col in output_df.columns if col != "health_category"]
            ordered_cols.append("health_category")
            output_df = output_df[ordered_cols]

        if extension == ".csv":
            output_df.to_csv(output_path, index=False)
        elif extension == ".xlsx":
            output_df.to_excel(output_path, index=False, engine="openpyxl")
        else:
            raise ValueError(f"Unsupported output format: {source_file.suffix}")

        logger.info("Saved processed output: %s", output_path)
        logger.info("Output row count: %d", len(output_df))

        _handle_source_file_on_success(source_file)
        return output_path
    except Exception as exc:
        logger.error("Failed to save output for %s: %s", source_file, exc)
        try:
            if source_file.exists():
                moved_to = _move_to_error(source_file)
                logger.error("Moved source file to error directory: %s", moved_to)
        except Exception as move_exc:
            logger.error("Failed moving source file to error directory: %s", move_exc)
        return None
