"""Local Ollama integration utilities for HealthSift AI."""

from __future__ import annotations

from typing import Any

import requests

from config import OLLAMA_MODEL, OLLAMA_URL
from src.logger import get_logger

logger = get_logger(__name__)

VALID_CATEGORIES = {"Diabetic", "Pregnant", "Diabetic & Pregnant", "Neither"}
MAX_RETRIES = 2


def _normalize_category(raw_text: str) -> str:
    """Normalize model output to one valid category or default to Neither."""
    lowered = raw_text.strip().lower()

    if "diabetic & pregnant" in lowered:
        return "Diabetic & Pregnant"
    if "diabetic and pregnant" in lowered:
        return "Diabetic & Pregnant"
    if "pregnant" in lowered and "diabet" in lowered:
        return "Diabetic & Pregnant"
    if lowered == "diabetic" or "diabetic" in lowered:
        return "Diabetic"
    if lowered == "pregnant" or "pregnant" in lowered:
        return "Pregnant"
    if lowered == "neither" or "neither" in lowered:
        return "Neither"
    return "Neither"


def _extract_response_text(payload: dict[str, Any]) -> str:
    """Extract text response from Ollama payload safely."""
    response_text = payload.get("response", "")
    if isinstance(response_text, str):
        return response_text
    return str(response_text)


def query_ollama(diagnosis_text: str) -> str:
    """Query local Ollama model and return a normalized health category."""
    prompt = (
        "You are a medical data classifier. Based only on the following diagnosis text, "
        "classify the patient into exactly one of these categories: Diabetic, Pregnant, "
        "Diabetic & Pregnant, or Neither. Reply with only the category name, nothing else. "
        f"Diagnosis: {diagnosis_text}"
    )

    request_body = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
    }

    for attempt in range(MAX_RETRIES + 1):
        try:
            response = requests.post(OLLAMA_URL, json=request_body, timeout=20)
            response.raise_for_status()

            payload = response.json()
            model_reply = _extract_response_text(payload)
            category = _normalize_category(model_reply)

            if category not in VALID_CATEGORIES:
                logger.warning(
                    "Ollama returned unexpected category '%s'; defaulting to Neither.",
                    model_reply,
                )
                return "Neither"

            logger.info("Ollama classification resolved as: %s", category)
            return category
        except requests.exceptions.RequestException as exc:
            if attempt < MAX_RETRIES:
                logger.warning(
                    "Ollama query attempt %d/%d failed: %s. Retrying...",
                    attempt + 1,
                    MAX_RETRIES + 1,
                    exc,
                )
            else:
                logger.warning(
                    "Ollama unavailable after %d attempts: %s. Defaulting to Neither.",
                    MAX_RETRIES + 1,
                    exc,
                )
                return "Neither"
        except ValueError as exc:
            logger.warning("Invalid Ollama response JSON: %s. Defaulting to Neither.", exc)
            return "Neither"

    return "Neither"
