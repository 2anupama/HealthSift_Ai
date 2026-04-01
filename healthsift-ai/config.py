"""Central configuration for the HealthSift AI project."""

from pathlib import Path

# Project root is the directory containing this config file.
PROJECT_ROOT = Path(__file__).resolve().parent

# Folder paths used by the pipeline.
INPUT_DIR = PROJECT_ROOT / "input"
PROCESSED_DIR = PROJECT_ROOT / "processed"
ERROR_DIR = PROJECT_ROOT / "error"
LOG_DIR = PROJECT_ROOT / "logs"

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
    "gestational diabetes",
    "hyperglycemia",
    "insulin",
    "hba1c",
]

PREGNANT_KEYWORDS = [
    "pregnant",
    "pregnancy",
    "antenatal",
    "prenatal",
    "gestation",
    "obstetric",
    "trimester",
    "expecting",
]
