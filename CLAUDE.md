# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a template repository for building PySpark applications on Databricks with Claude Code support. Customize this file with your project-specific requirements and architecture patterns.

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

### PySpark and Databricks Connect
**IMPORTANT**: This project uses Databricks Connect to run all PySpark code remotely on Databricks clusters.
- Do NOT install PySpark locally
- ALWAYS use `databricks.connect.DatabricksSession` instead of `pyspark.sql.SparkSession`
- All Spark operations execute remotely on your configured Databricks cluster

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

### Project Structure
Organize your code according to your project needs. Common patterns include:

**Medallion Architecture (ETL Projects)**
```
src/
├── etl/
│   ├── base.py          # Base ETL class
│   ├── bronze/          # Raw data ingestion
│   ├── silver/          # Data cleansing
│   └── gold/            # Business aggregations
```

**Application Structure**
```
src/
├── models/              # Data models
├── services/            # Business logic
├── utils/               # Helper functions
└── pipelines/           # Data pipelines
```

### Best Practices

**Data Processing**
- Use Delta Lake format for ACID transactions and time travel
- Implement proper error handling and logging
- Write modular, testable code

**Testing Strategy**
- Unit tests in `tests/` should mirror the `src/` structure
- Use DatabricksSession fixtures for integration tests (see `tests/conftest.py`)
- Test data transformations with sample data
- All tests run against your remote Databricks cluster via Databricks Connect

**Code Organization**
- Keep business logic separate from data access code
- Use type hints for better code clarity
- Document complex transformations and business rules