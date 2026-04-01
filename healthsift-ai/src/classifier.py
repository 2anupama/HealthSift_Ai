"""Classification utilities for HealthSift AI."""

from __future__ import annotations

import pandas as pd

from config import DIABETIC_KEYWORDS, PREGNANT_KEYWORDS
from src.logger import get_logger
from src.llm_handler import query_ollama

logger = get_logger(__name__)


def rule_based_classify(diagnosis_text: str) -> str | None:
    """Classify diagnosis text using keyword rules.

    Returns:
        - "Diabetic"
        - "Pregnant"
        - "Diabetic & Pregnant"
        - None (when no rule matches; should be sent to LLM)
    """
    if diagnosis_text is None or pd.isna(diagnosis_text):
        return None

    text = str(diagnosis_text).lower()

    has_diabetic_keyword = any(keyword.lower() in text for keyword in DIABETIC_KEYWORDS)
    has_pregnant_keyword = any(keyword.lower() in text for keyword in PREGNANT_KEYWORDS)

    if has_diabetic_keyword and has_pregnant_keyword:
        return "Diabetic & Pregnant"
    if has_diabetic_keyword:
        return "Diabetic"
    if has_pregnant_keyword:
        return "Pregnant"
    return None


def classify_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Apply rule-based classification, then use LLM for unresolved rows."""
    diagnosis_column = None
    if "Diagnosis" in df.columns:
        diagnosis_column = "Diagnosis"
    elif "diagnosis" in df.columns:
        diagnosis_column = "diagnosis"
    else:
        raise ValueError("Diagnosis column not found. Expected 'Diagnosis' or 'diagnosis'.")

    classified_df = df.copy()
    classified_df["health_category"] = classified_df[diagnosis_column].apply(rule_based_classify)

    llm_pending = classified_df[classified_df["health_category"].isna()]
    classified_df.attrs["llm_pending_indices"] = llm_pending.index.tolist()
    llm_pending_count = len(llm_pending)
    rules_resolved_count = len(classified_df) - llm_pending_count

    if llm_pending_count > 0:
        logger.info("Sending %d unresolved records to Ollama.", llm_pending_count)
        classified_df.loc[llm_pending.index, "health_category"] = llm_pending[diagnosis_column].apply(
            lambda text: query_ollama(str(text))
        )

    diabetic_count = int((classified_df["health_category"] == "Diabetic").sum())
    pregnant_count = int((classified_df["health_category"] == "Pregnant").sum())
    both_count = int((classified_df["health_category"] == "Diabetic & Pregnant").sum())
    neither_count = int((classified_df["health_category"] == "Neither").sum())

    logger.info("Classification complete.")
    logger.info("Resolved by rules: %d", rules_resolved_count)
    logger.info("Sent to LLM: %d", llm_pending_count)
    logger.info("Category counts | Diabetic: %d", diabetic_count)
    logger.info("Category counts | Pregnant: %d", pregnant_count)
    logger.info("Category counts | Diabetic & Pregnant: %d", both_count)
    logger.info("Category counts | Neither: %d", neither_count)

    return classified_df
