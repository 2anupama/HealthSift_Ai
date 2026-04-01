"""Data cleaning and preprocessing utilities for HealthSift AI."""

from __future__ import annotations

import re

import numpy as np
import pandas as pd

from src.logger import get_logger

logger = get_logger(__name__)


def _normalize_column_name(column_name: str) -> str:
    """Normalize column names: trim, lowercase, and replace spaces with underscores."""
    return re.sub(r"\s+", "_", column_name.strip().lower())


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Clean an input DataFrame using HealthSift AI preprocessing rules.

    Steps:
    1) Strip whitespace from all string columns
    2) Normalize column names internally
    3) Drop duplicate rows
    4) Clean diagnosis values
    5) Clean age values (if present)
    6) Standardize gender values (if present)

    Returns:
        Cleaned DataFrame with display-style column names preserved.
    """
    cleaned_df = df.copy()
    logger.info("Starting data cleaning pipeline for %d rows.", len(cleaned_df))

    # 1) Strip leading/trailing whitespace from all string columns.
    string_columns = cleaned_df.select_dtypes(include=["object", "string"]).columns
    whitespace_changed_rows: set[int] = set()
    for column in string_columns:
        column_as_string = cleaned_df[column].astype("string")
        stripped = column_as_string.str.strip()
        changed_mask = (column_as_string.notna()) & (stripped != column_as_string)
        whitespace_changed_rows.update(cleaned_df.index[changed_mask].tolist())
        cleaned_df[column] = stripped
    logger.info(
        "Step 1 complete: stripped whitespace in string fields for %d affected rows.",
        len(whitespace_changed_rows),
    )

    # 2) Standardize column names internally while preserving display names for output.
    display_columns = [str(col).strip() for col in cleaned_df.columns]
    normalized_columns = [_normalize_column_name(col) for col in display_columns]
    renamed_columns_count = sum(
        old != new for old, new in zip(cleaned_df.columns.tolist(), normalized_columns)
    )
    cleaned_df.columns = normalized_columns
    logger.info(
        "Step 2 complete: standardized %d column names for internal processing.",
        renamed_columns_count,
    )

    # 3) Drop fully duplicate rows.
    rows_before_dedup = len(cleaned_df)
    cleaned_df = cleaned_df.drop_duplicates()
    duplicates_removed = rows_before_dedup - len(cleaned_df)
    logger.info("Step 3 complete: removed %d fully duplicate rows.", duplicates_removed)

    # 4) Clean diagnosis field.
    if "diagnosis" in cleaned_df.columns:
        diagnosis_series = cleaned_df["diagnosis"].astype("string").str.strip()
        invalid_values = {"", "n/a", "none", "null", "-", "unknown"}
        replace_mask = diagnosis_series.str.lower().isin(invalid_values)
        replaced_count = int(replace_mask.sum())
        diagnosis_series = diagnosis_series.mask(replace_mask, np.nan)

        fill_mask = diagnosis_series.isna()
        filled_count = int(fill_mask.sum())
        diagnosis_series = diagnosis_series.fillna("unspecified")

        cleaned_df["diagnosis"] = diagnosis_series
        logger.info(
            "Step 4 complete: diagnosis invalids replaced with NaN for %d rows; "
            "filled %d missing diagnosis values with 'unspecified'.",
            replaced_count,
            filled_count,
        )
    else:
        logger.warning("Step 4 skipped: 'Diagnosis' column not present.")

    # 5) Clean age field if present.
    if "age" in cleaned_df.columns:
        original_age = cleaned_df["age"]
        numeric_age = pd.to_numeric(original_age, errors="coerce")
        coerced_to_nan_mask = original_age.notna() & numeric_age.isna()
        coerced_count = int(coerced_to_nan_mask.sum())

        missing_age_count = int(numeric_age.isna().sum())
        median_age = numeric_age.median()
        if pd.notna(median_age):
            numeric_age = numeric_age.fillna(median_age)
            logger.info(
                "Step 5 complete: coerced %d non-numeric age values to NaN and "
                "filled %d NaN values with median age %.2f.",
                coerced_count,
                missing_age_count,
                float(median_age),
            )
        else:
            logger.warning(
                "Step 5 complete: coerced %d non-numeric age values to NaN; "
                "%d NaN values could not be filled because median age is unavailable.",
                coerced_count,
                missing_age_count,
            )
        cleaned_df["age"] = numeric_age
    else:
        logger.info("Step 5 skipped: 'Age' column not present.")

    # 6) Standardize gender field if present.
    if "gender" in cleaned_df.columns:
        normalized_gender = cleaned_df["gender"].astype("string").str.strip().str.lower()
        gender_map = {
            "m": "Male",
            "male": "Male",
            "f": "Female",
            "female": "Female",
        }
        standardized_gender = normalized_gender.map(gender_map).fillna("Unknown")
        changed_gender_count = int(
            (cleaned_df["gender"].astype("string") != standardized_gender.astype("string")).sum()
        )
        cleaned_df["gender"] = standardized_gender
        logger.info(
            "Step 6 complete: standardized gender values for %d rows.",
            changed_gender_count,
        )
    else:
        logger.info("Step 6 skipped: 'Gender' column not present.")

    # Restore display names (trimmed originals) before returning.
    display_name_map = dict(zip(normalized_columns, display_columns))
    cleaned_df = cleaned_df.rename(columns=display_name_map)
    logger.info("Cleaning pipeline completed successfully for %d rows.", len(cleaned_df))
    return cleaned_df
