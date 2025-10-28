# Databricks with Claude

PySpark ETL modules for Databricks workflows.

## Setup

### Prerequisites

#### Install uv (Python Package Manager)

Install `uv` for fast, reliable Python package management:

```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# Or via pip
pip install uv
```

#### Install Databricks CLI (Optional)

The Databricks CLI is optional and useful for workspace management tasks:

```bash
# macOS/Linux
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Or via brew (macOS)
brew tap databricks/tap
brew install databricks
```

### Configure Databricks Connection

This project uses **Databricks Connect** (Python package) to run PySpark code remotely on your Databricks cluster. Authentication is handled via environment variables.

#### Required Environment Variables

Create a `.env` file in the project root with the following variables:

```bash
# Required: Your Databricks workspace URL
DATABRICKS_HOST=https://dbc-abc123-def4.cloud.databricks.com

# Required: Personal Access Token (PAT authentication recommended)
# PAT is recommended because Databricks managed MCP Servers don't easily support OAuth
DATABRICKS_TOKEN=your-personal-access-token-here

# Optional: Choose your compute type (use ONE of the following)

# For serverless compute (recommended for quick development):
DATABRICKS_SERVERLESS_COMPUTE_ID=auto

# OR for classic compute cluster:
# DATABRICKS_CLUSTER_ID=your-cluster-id-here
```

**Note**: The `.env` file is gitignored for security.

#### Generate a Personal Access Token

1. Go to your Databricks workspace
2. Click your username → Settings → Developer → Access tokens
3. Click "Generate new token"
4. Set an optional comment and lifetime
5. Copy the generated token and add it to your `.env` file

### Install Project Dependencies

Once uv is installed and environment variables are configured:

```bash
# Sync dependencies
uv sync

# Add new dependencies as needed
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
