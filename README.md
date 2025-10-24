# Databricks with Claude

PySpark ETL modules for Databricks workflows.

## Setup

This project uses `uv` for Python package management.

### Install dependencies

```bash
uv sync
```

### Add new dependencies

```bash
uv add <package_name>
```

## Project Structure

```
databricks-with-claude/
├── src/
│   └── etl/           # ETL modules
│       ├── bronze/    # Raw data ingestion
│       ├── silver/    # Cleaned and validated data
│       └── gold/      # Business-level aggregates
├── tests/             # Test files
├── notebooks/         # Databricks notebooks (optional)
└── pyproject.toml     # Project dependencies
```

## Running Tests

```bash
uv run pytest
```

## Development

Each ETL module should be self-contained and follow the medallion architecture pattern (bronze → silver → gold).
