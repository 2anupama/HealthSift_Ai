"""Simple assert-based tests for rule-based classifier behavior."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.classifier import rule_based_classify


def run_tests() -> None:
    """Run simple rule-based classification assertions."""
    assert rule_based_classify("diabetes type 2") == "Diabetic"
    assert rule_based_classify("prenatal vitamins") == "Pregnant"
    assert rule_based_classify("gestational diabetes") == "Diabetic & Pregnant"
    assert rule_based_classify("blood pressure check") is None
    print("All classifier tests passed.")


if __name__ == "__main__":
    run_tests()
