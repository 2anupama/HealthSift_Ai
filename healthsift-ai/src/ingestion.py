"""File ingestion and watch utilities for HealthSift AI."""

from __future__ import annotations

from pathlib import Path
from typing import Callable, Optional

import pandas as pd
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

from config import INPUT_DIR
from src.logger import get_logger

logger = get_logger(__name__)
SUPPORTED_EXTENSIONS = {".csv", ".xlsx"}


def load_file(filepath: str | Path) -> pd.DataFrame:
    """Load a CSV or XLSX file into a pandas DataFrame.

    Args:
        filepath: Path to the input data file.

    Returns:
        Loaded pandas DataFrame.

    Raises:
        ValueError: If the file extension is unsupported.
        Exception: If pandas cannot read the file.
    """
    file_path = Path(filepath)
    suffix = file_path.suffix.lower()

    if suffix not in SUPPORTED_EXTENSIONS:
        raise ValueError(f"Unsupported file format: {file_path.name}")

    if suffix == ".csv":
        dataframe = pd.read_csv(file_path, encoding="utf-8-sig")
    else:
        dataframe = pd.read_excel(file_path)

    logger.info("Loaded file successfully: %s", file_path)
    return dataframe


def _default_pipeline_trigger(file_path: Path) -> None:
    """Default placeholder for pipeline trigger callback."""
    logger.info("Pipeline trigger placeholder called for: %s", file_path)


class InputFileHandler(FileSystemEventHandler):
    """Watchdog handler that reacts to new input files."""

    def __init__(self, on_file_detected: Optional[Callable[[Path], None]] = None) -> None:
        self.on_file_detected = on_file_detected or _default_pipeline_trigger
        super().__init__()

    def on_created(self, event: FileSystemEvent) -> None:
        """Handle newly created files in the input directory."""
        if event.is_directory:
            return

        file_path = Path(event.src_path)
        if file_path.suffix.lower() not in SUPPORTED_EXTENSIONS:
            return

        logger.info("Detected new input file: %s", file_path)
        self.on_file_detected(file_path)


def start_watcher(on_file_detected: Optional[Callable[[Path], None]] = None) -> Observer:
    """Start monitoring the input directory for new data files.

    Args:
        on_file_detected: Callback invoked with the detected file path.

    Returns:
        Active watchdog observer instance.
    """
    INPUT_DIR.mkdir(parents=True, exist_ok=True)

    event_handler = InputFileHandler(on_file_detected=on_file_detected)
    observer = Observer()
    observer.schedule(event_handler, str(INPUT_DIR), recursive=False)
    observer.start()

    logger.info("Started input watcher on directory: %s", INPUT_DIR)
    return observer
