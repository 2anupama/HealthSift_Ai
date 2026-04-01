"""Main entry point for HealthSift AI pipeline orchestration."""

from __future__ import annotations

import shutil
import time
from pathlib import Path

from config import ERROR_DIR, INPUT_DIR, validate_environment
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


def _log_and_print_file_summary(
    file_path: Path,
    total_records: int,
    category_counts: dict[str, int],
    source_counts: dict[str, int],
    elapsed_seconds: float,
) -> None:
    """Print and log a processing summary without sensitive field values."""
    summary_lines = [
        f"Summary for {file_path.name}",
        f"Total records processed: {total_records}",
        (
            "Health categories: "
            f"Diabetic={category_counts.get('Diabetic', 0)}, "
            f"Pregnant={category_counts.get('Pregnant', 0)}, "
            f"Diabetic & Pregnant={category_counts.get('Diabetic & Pregnant', 0)}, "
            f"Neither={category_counts.get('Neither', 0)}"
        ),
        (
            "Classification source: "
            f"rule-based={source_counts.get('rule-based', 0)}, "
            f"llm={source_counts.get('llm', 0)}"
        ),
        f"Processing time: {elapsed_seconds:.2f}s",
    ]

    summary_text = "\n".join(summary_lines)
    print(summary_text)
    logger.info(summary_text)


def process_file(filepath: str | Path) -> None:
    """Run the full HealthSift processing pipeline for one file."""
    file_path = Path(filepath)
    start_time = time.perf_counter()
    total_records = 0
    category_counts = {"Diabetic": 0, "Pregnant": 0, "Diabetic & Pregnant": 0, "Neither": 0}
    source_counts = {"rule-based": 0, "llm": 0}

    logger.info("Starting pipeline for file: %s", file_path)

    try:
        dataframe = ingestion.load_file(file_path)
        total_records = len(dataframe)

        is_valid, validation_error = validation.validate(dataframe, file_path)
        if not is_valid:
            logger.warning("Validation failed for %s: %s", file_path, validation_error)
            return

        cleaned_df = cleaning.clean(dataframe)
        classified_df = classifier.classify_dataframe(cleaned_df)
        category_counts = {
            "Diabetic": int((classified_df["health_category"] == "Diabetic").sum()),
            "Pregnant": int((classified_df["health_category"] == "Pregnant").sum()),
            "Diabetic & Pregnant": int(
                (classified_df["health_category"] == "Diabetic & Pregnant").sum()
            ),
            "Neither": int((classified_df["health_category"] == "Neither").sum()),
        }
        source_counts = {
            "rule-based": int((classified_df["classification_source"] == "rule-based").sum()),
            "llm": int((classified_df["classification_source"] == "llm").sum()),
        }
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
        _log_and_print_file_summary(
            file_path=file_path,
            total_records=total_records,
            category_counts=category_counts,
            source_counts=source_counts,
            elapsed_seconds=elapsed,
        )
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
        validate_environment()
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
