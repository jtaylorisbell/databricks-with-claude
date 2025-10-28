# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository contains PySpark ETL modules for Databricks, implementing the medallion architecture pattern (bronze → silver → gold layers).

## Prerequisites

Before working with this project, ensure the following tools are installed and configured:

### Required Tools

1. **uv (Python Package Manager)**
   - This project uses `uv` exclusively for Python package management
   - Install: `curl -LsSf https://astral.sh/uv/install.sh | sh` (macOS/Linux)
   - Alternative: `pip install uv`

2. **Databricks CLI**
   - Required for interacting with Databricks workspaces
   - Install: `curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh`
   - Alternative: `brew install databricks` (macOS)

### Databricks Configuration

The project requires a configured Databricks profile for workspace connectivity:

1. **Create a profile**: `databricks configure --profile <profile-name>`
   - You'll need the workspace URL (e.g., `https://dbc-abc123-def4.cloud.databricks.com`)
   - Authentication method (Personal Access Token recommended for development)

2. **Set environment variable**: `DATABRICKS_CONFIG_PROFILE`
   - Add to shell profile: `export DATABRICKS_CONFIG_PROFILE=<profile-name>`
   - Or use a `.env` file in the project root
   - The `.env` file is gitignored for security

3. **Verify configuration**: `databricks workspace list` should successfully connect

## Development Setup

### Python Environment
- Use `uv` for all Python package management (not pip)
- Install dependencies: `uv sync`
- Add new dependencies: `uv add <package_name>`

### PySpark Dependencies
When adding PySpark-related dependencies, note that PySpark is typically provided by the Databricks runtime. For local development and testing, add it as a dev dependency:
```bash
uv add --dev pyspark
```

## Common Commands

### Running Tests
```bash
# Run all tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=src --cov-report=html

# Run specific test file
uv run pytest tests/etl/test_base.py

# Run tests matching a pattern
uv run pytest -k "test_name_pattern"
```

### Code Quality
```bash
# Format code (if using ruff or black)
uv run ruff format .

# Lint code
uv run ruff check .
```

## Architecture

### Medallion Architecture
The ETL pipeline follows the medallion architecture with three layers:

**Bronze Layer** (`src/etl/bronze/`)
- Raw data ingestion from source systems
- Minimal transformations (add metadata like ingestion timestamp, source file)
- Data stored in original format with audit columns
- Append-only for full history

**Silver Layer** (`src/etl/silver/`)
- Cleansed and validated data
- Remove duplicates, handle nulls, standardize formats
- Data quality validations and flags
- Typically overwrite mode for current state

**Gold Layer** (`src/etl/gold/`)
- Business-level aggregations and metrics
- Optimized for reporting and analytics
- Denormalized for query performance
- Business logic applied

### Base ETL Class
All ETL jobs inherit from `BaseETL` (`src/etl/base.py`) which provides:
- `extract()`: Read data from source
- `transform()`: Apply transformations
- `load()`: Write data to target
- `run()`: Execute full pipeline

### Module Structure
```
src/etl/
├── base.py              # BaseETL abstract class
├── bronze/              # Raw data ingestion modules
├── silver/              # Data cleansing modules
└── gold/                # Aggregation and business logic modules
```

### Creating New ETL Jobs
1. Create a new module in the appropriate layer directory (bronze/silver/gold)
2. Inherit from `BaseETL` and implement `extract()`, `transform()`, and `load()` methods
3. Initialize with source path and optional SparkSession
4. Call `run(target_path)` to execute the pipeline

Example:
```python
from src.etl.bronze.example_ingestion import ExampleBronzeETL

etl = ExampleBronzeETL(source_path="/path/to/source")
etl.run(target_path="/path/to/bronze/table")
```

### Data Formats
- All layers use Delta Lake format for ACID transactions and time travel
- Bronze layer preserves original data structure
- Silver and gold layers may reshape data for optimization

### Testing Strategy
- Unit tests in `tests/` mirror the `src/` structure
- Use local Spark sessions for testing
- Mock external dependencies
- Test each ETL method (extract, transform, load) independently
- We do not need PySpark installed locally. We are using Databricks Connect to run all PySpark code remotely against a Databricks cluster. So along those same lines, do not ever use pyspark.sql.SparkSession. Instead use databricks.connect.DatabricksSession!