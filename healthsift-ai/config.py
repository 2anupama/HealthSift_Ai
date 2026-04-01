"""Central configuration for the HealthSift AI project."""

from __future__ import annotations

import logging
from pathlib import Path
from urllib.parse import urlparse

import requests

# Project root is the directory containing this config file.
PROJECT_ROOT = Path(__file__).resolve().parent

# Folder paths used by the pipeline.
INPUT_DIR = PROJECT_ROOT / "input"
PROCESSED_DIR = PROJECT_ROOT / "processed"
ERROR_DIR = PROJECT_ROOT / "error"
LOG_DIR = PROJECT_ROOT / "logs"
PROCESSED_INPUT_DIR = PROJECT_ROOT / "processed_input"

# What to do with the original file after successful output save.
# Supported values: "archive", "delete"
INPUT_FILE_SUCCESS_ACTION = "archive"

# Input validation requirements.
REQUIRED_COLUMNS = ["Patient ID", "Diagnosis"]

# Ollama model API configuration.
OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "llama3"

# Rule-based keyword lists for quick classification.
DIABETIC_KEYWORDS = [
    "diabetes",
    "diabetic",
    "type 1 diabetes",
    "type 2 diabetes",
    "type 1",
    "type 2",
    "gestational diabetes",
    "hyperglycemia",
    "insulin",
    "hba1c",
    "metformin",
    "glipizide",
    "t1dm",
    "t2dm",
]

PREGNANT_KEYWORDS = [
    "pregnant",
    "pregnancy",
    "antenatal",
    "prenatal",
    "gestation",
    "gestational",
    "obstetric",
    "trimester",
    "expecting",
    "gravida",
    "maternity",
]


def validate_environment() -> None:
    """Ensure required directories exist and warn if Ollama is unreachable."""
    config_logger = logging.getLogger("healthsift.config")

    required_dirs = [INPUT_DIR, PROCESSED_DIR, ERROR_DIR, LOG_DIR, PROCESSED_INPUT_DIR]
    for directory in required_dirs:
        directory.mkdir(parents=True, exist_ok=True)

    parsed_url = urlparse(OLLAMA_URL)
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    ping_url = f"{base_url}/api/tags"

    try:
        response = requests.get(ping_url, timeout=3)
        if response.ok:
            config_logger.info("Environment check: Ollama endpoint reachable.")
        else:
            config_logger.warning(
                "Environment check: Ollama responded with HTTP %s. "
                "System will continue with rule-based classification.",
                response.status_code,
            )
    except requests.RequestException:
        config_logger.warning(
            "Environment check: Ollama is unreachable at %s. "
            "System will continue with rule-based classification.",
            OLLAMA_URL,
        )
