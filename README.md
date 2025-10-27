# Databricks with Claude

PySpark ETL modules for Databricks workflows.

## Setup

### Prerequisites

This project requires two main tools to be installed:

#### 1. Install uv (Python Package Manager)

Install `uv` for fast, reliable Python package management:

```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# Or via pip
pip install uv
```

#### 2. Install Databricks CLI

Install the Databricks CLI to manage your Databricks workspace:

```bash
# macOS/Linux
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Windows
winget install Databricks.DatabricksCLI

# Or via brew (macOS)
brew tap databricks/tap
brew install databricks
```

### Configure Databricks Connection

#### Create a Databricks Profile

Configure a connection profile to your Databricks workspace:

```bash
databricks configure --profile <profile-name>
```

You'll be prompted to enter:
- **Databricks Host**: Your workspace URL (e.g., `https://dbc-abc123-def4.cloud.databricks.com`)
- **Authentication**: Choose from:
  - Personal Access Token (recommended for development)
  - OAuth (for interactive workflows)
  - Azure CLI (for Azure Databricks)

To generate a Personal Access Token:
1. Go to your Databricks workspace
2. Click your username → Settings → Developer → Access tokens
3. Generate new token and copy it

#### Set Environment Variable

Set the `DATABRICKS_CONFIG_PROFILE` environment variable to use your profile:

```bash
# Add to your shell profile (~/.bashrc, ~/.zshrc, etc.)
export DATABRICKS_CONFIG_PROFILE=<profile-name>

# Or create a .env file in the project root
echo "DATABRICKS_CONFIG_PROFILE=<profile-name>" > .env
```

### Install Project Dependencies

Once uv and Databricks CLI are configured:

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
