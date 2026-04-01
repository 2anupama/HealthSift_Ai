# HealthSift AI

## Project Overview

HealthSift AI is a Python-based healthcare data processing pipeline for CSV/Excel files.
It monitors the `input/` folder, validates and cleans records, classifies patient diagnoses
using rule-based logic with local Ollama fallback, and writes results to `processed/`.

## Setup Instructions

```bash
pip install -r requirements.txt
ollama pull llama3
```

## Run the Pipeline

```bash
python main.py
```

## Test Workflow

1. Generate messy sample input data:
```bash
python tests/generate_sample_data.py
```
2. Run the pipeline:
```bash
python main.py
```
3. Run classifier unit checks:
```bash
python tests/test_classifier.py
```
