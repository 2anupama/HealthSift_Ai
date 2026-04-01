"""Classification utilities for HealthSift AI."""

from __future__ import annotations

import pandas as pd

from config import DIABETIC_KEYWORDS, PREGNANT_KEYWORDS
from src.logger import get_logger

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
    """Apply rule-based classification and mark records for LLM fallback."""
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

    diabetic_count = int((classified_df["health_category"] == "Diabetic").sum())
    pregnant_count = int((classified_df["health_category"] == "Pregnant").sum())
    both_count = int((classified_df["health_category"] == "Diabetic & Pregnant").sum())
    llm_count = int(classified_df["health_category"].isna().sum())

    logger.info("Rule-based classification complete.")
    logger.info("Category counts | Diabetic: %d", diabetic_count)
    logger.info("Category counts | Pregnant: %d", pregnant_count)
    logger.info("Category counts | Diabetic & Pregnant: %d", both_count)
    logger.info("Category counts | Needs LLM: %d", llm_count)

    return classified_df
