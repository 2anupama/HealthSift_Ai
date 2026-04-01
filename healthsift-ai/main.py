"""Main entry point for HealthSift AI pipeline orchestration."""

from __future__ import annotations

import shutil
import time
from pathlib import Path

from config import ERROR_DIR, INPUT_DIR
from src import classifier, cleaning, ingestion, output, validation
from src.logger import get_logger

logger = get_logger(__name__)


def _move_file_to_error(file_path: Path) -> None:
    """Move a file to error/ directory if it still exists."""
    if not file_path.exists():
        return

    ERROR_DIR.mkdir(parents=True, exist_ok=True)
    destination = ERROR_DIR / file_path.name
    if destination.exists():
        destination = ERROR_DIR / f"{file_path.stem}_{int(time.time() * 1000)}{file_path.suffix}"

    shutil.move(str(file_path), str(destination))
    logger.error("Moved file to error directory: %s", destination)


def process_file(filepath: str | Path) -> None:
    """Run the full HealthSift processing pipeline for one file."""
    file_path = Path(filepath)
    start_time = time.perf_counter()

    logger.info("Starting pipeline for file: %s", file_path)

    try:
        dataframe = ingestion.load_file(file_path)

        is_valid, validation_error = validation.validate(dataframe, file_path)
        if not is_valid:
            logger.warning("Validation failed for %s: %s", file_path, validation_error)
            elapsed = time.perf_counter() - start_time
            logger.info("Processing time for %s: %.2f seconds", file_path.name, elapsed)
            return

        cleaned_df = cleaning.clean(dataframe)
        classified_df = classifier.classify_dataframe(cleaned_df)
        saved_path = output.save_output(classified_df, file_path)

        if saved_path is None:
            logger.error("Output save failed for file: %s", file_path)
        else:
            logger.info("Pipeline completed successfully for file: %s", file_path)
    except Exception as exc:
        logger.exception("Unhandled pipeline error for %s: %s", file_path, exc)
        try:
            _move_file_to_error(file_path)
        except Exception as move_exc:
            logger.exception("Failed to move errored file %s: %s", file_path, move_exc)
    finally:
        elapsed = time.perf_counter() - start_time
        logger.info("Processing time for %s: %.2f seconds", file_path.name, elapsed)


def _process_backlog() -> None:
    """Process any existing files in input/ at startup."""
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    backlog_files = sorted(
        (
            path
            for path in INPUT_DIR.iterdir()
            if path.is_file() and path.suffix.lower() in ingestion.SUPPORTED_EXTENSIONS
        ),
        key=lambda path: path.stat().st_mtime,
    )

    if not backlog_files:
        logger.info("No backlog files found in input directory.")
        return

    logger.info("Found %d backlog file(s) in input directory.", len(backlog_files))
    for file_path in backlog_files:
        process_file(file_path)


def main() -> None:
    """Start backlog processing and continuous file watching."""
    logger.info("Starting HealthSift AI pipeline service.")

    observer = None
    try:
        _process_backlog()

        def _safe_process_file(file_path: Path) -> None:
            """Ensure watcher callback errors are contained and logged."""
            try:
                process_file(file_path)
            except Exception as exc:
                logger.exception("Watcher callback failed for %s: %s", file_path, exc)

        observer = ingestion.start_watcher(on_file_detected=_safe_process_file)
        logger.info("Watcher is active. Press Ctrl+C to stop.")

        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutdown requested via Ctrl+C.")
    except Exception as exc:
        logger.exception("Fatal runtime error in main service loop: %s", exc)
    finally:
        if observer is not None:
            observer.stop()
            observer.join()
            logger.info("Watcher stopped cleanly.")
        logger.info("HealthSift AI service stopped.")


if __name__ == "__main__":
    main()
