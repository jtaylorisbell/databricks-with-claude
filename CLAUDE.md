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

2. **Databricks CLI (Optional)**
   - Optional tool for workspace management tasks
   - Install: `curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh`
   - Alternative: `brew install databricks` (macOS)

### Databricks Configuration

This project uses **Databricks Connect** (Python package, installed as a core dependency) to run all PySpark code remotely on your Databricks cluster. Authentication is handled via environment variables.

#### Required Environment Variables

Create a `.env` file in the project root with these variables:

```bash
# Required: Your Databricks workspace URL
DATABRICKS_HOST=https://dbc-abc123-def4.cloud.databricks.com

# Required: Personal Access Token
# PAT authentication is recommended because Databricks managed MCP Servers
# don't easily support Claude's OAuth flow
DATABRICKS_TOKEN=your-personal-access-token-here

# Choose ONE compute option:

# For serverless compute (recommended for quick development):
DATABRICKS_SERVERLESS_COMPUTE_ID=auto

# OR for classic compute cluster:
# DATABRICKS_CLUSTER_ID=your-cluster-id-here
```

The `.env` file is gitignored for security.

#### Generate Personal Access Token

1. Go to your Databricks workspace
2. Click your username → Settings → Developer → Access tokens
3. Generate new token and copy it to your `.env` file

## Development Setup

### Python Environment
- Use `uv` for all Python package management (not pip)
- Install dependencies: `uv sync`
- Add new dependencies: `uv add <package_name>`

### PySpark and Databricks Connect
**IMPORTANT**: This project uses Databricks Connect to run all PySpark code remotely on Databricks clusters.
- Do NOT install PySpark locally
- ALWAYS use `databricks.connect.DatabricksSession` instead of `pyspark.sql.SparkSession`
- All Spark operations execute remotely on your configured Databricks cluster
- Databricks Connect is already included as a core project dependency

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
- Use DatabricksSession fixtures for integration tests (see `tests/conftest.py`)
- Test data transformations with sample data
- Test each ETL method (extract, transform, load) independently
- All tests run against your remote Databricks cluster via Databricks Connect