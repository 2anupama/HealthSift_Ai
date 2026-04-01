"""Generate sample messy healthcare data for HealthSift AI pipeline testing."""

from __future__ import annotations

import csv
import random
from pathlib import Path


def generate_sample_data() -> Path:
    """Create a 50-row CSV with intentionally messy healthcare records."""
    random.seed(42)

    project_root = Path(__file__).resolve().parents[1]
    input_dir = project_root / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    output_path = input_dir / "sample_test.csv"

    diagnoses = [
        "",
        "Metformin 500mg twice daily",
        "28 weeks pregnant, iron supplements",
        "Gestational diabetes, insulin therapy",
        "CBC test ordered, follow up in 2 weeks",
        "Type 2 diabetes follow-up",
        "Prenatal vitamins started",
        "Routine annual checkup",
        "Glipizide dose adjusted",
        "N/A",
        "blood pressure review",
        "unknown",
    ]
    genders = ["m", "F", "Male", "female", "unknown"]
    ages = ["29", "45", "31", "27", "38", "42", "n/a", "forty", "", "33", "50", "thirty-five"]

    rows: list[list[str]] = []
    for idx in range(1, 46):
        rows.append(
            [
                f"P{idx:04d}",
                random.choice(ages),
                random.choice(genders),
                random.choice(diagnoses),
            ]
        )

    # Add deliberate duplicate rows.
    rows.append(rows[3].copy())
    rows.append(rows[10].copy())
    rows.append(rows[20].copy())
    rows.append(rows[0].copy())
    rows.append(rows[1].copy())

    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["Patient ID", "Age", "Gender", "Diagnosis"])
        writer.writerows(rows)

    return output_path


if __name__ == "__main__":
    generated_file = generate_sample_data()
    print(f"Generated sample file: {generated_file}")
