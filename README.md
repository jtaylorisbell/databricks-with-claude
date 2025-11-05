# Databricks with Claude ⚡

PySpark ETL modules for Databricks workflows.

## 🚀 Setup

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

#### Install Databricks CLI

The Databricks CLI is useful for workspace management tasks:

```bash
# macOS/Linux
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Or via brew (macOS)
brew tap databricks/tap
brew install databricks
```

### 🔐 Configure Databricks Connection

This project uses **Databricks Connect** (Python package) to run PySpark code remotely on your Databricks cluster. Authentication is handled via environment variables.

#### Required Environment Variables

Create a `.env` file in the project root with the following variables:

```bash
# Required: Your Databricks workspace URL
DATABRICKS_HOST=https://dbc-abc123-def4.cloud.databricks.com

# Required if using PAT, not necessary if using OAuth U2M
DATABRICKS_TOKEN=your-personal-access-token-here

# Optional: Choose your compute type (use ONE of the following)

# For serverless compute (recommended for quick development):
DATABRICKS_SERVERLESS_COMPUTE_ID=auto

# OR for classic compute cluster:
# DATABRICKS_CLUSTER_ID=your-cluster-id-here
```

> [!NOTE] 
> If using OAuth U2M, run `databricks auth login --host $DATABRICKS_HOST` to authenticate.

#### To Generate a Personal Access Token

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

## 📁 Project Structure

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

## 🧪 Running Tests

```bash
uv run pytest
```

## 💻 Development

Each ETL module should be self-contained and follow the medallion architecture pattern (bronze → silver → gold).

## 🤖 Claude Code Tooling

This project includes specialized Claude Code tooling to enhance development workflows when using [Claude Code](https://claude.com/code).

### MCP Servers

The project is configured with two Model Context Protocol (MCP) servers in `.mcp.json`:

1. **databricks-sql**: Query Unity Catalog tables directly using SQL
   - Provides read-only access to your Databricks SQL warehouse
   - Useful for exploring table schemas, sampling data, and validating transformations
   - Uses the same DATABRICKS_HOST and DATABRICKS_TOKEN from your `.env` file
   - Authentication is handled automatically via environment variables

2. **sequential-thinking**: Advanced problem-solving through chain-of-thought reasoning
   - Enables dynamic, reflective problem-solving for complex tasks
   - Helps break down multi-step problems with iterative refinement
   - Useful for planning ETL pipelines, debugging issues, and architectural decisions
   - Automatically available when working with Claude Code

### Slash Commands

Custom slash commands are available in `.claude/commands/`:

- **`/dev-docs [description]`**: Create comprehensive strategic plans with structured task breakdown
  - Example: `/dev-docs implement incremental loading for silver layer`
  - Creates a task directory in `dev/active/[task-name]/` with:
    - `[task-name]-plan.md`: Comprehensive implementation plan
    - `[task-name]-context.md`: Key files, decisions, dependencies
    - `[task-name]-tasks.md`: Checklist for tracking progress
  - Ideal for planning complex features before implementation

- **`/dev-docs-update [context]`**: Update dev documentation before context compaction
  - Updates active task documentation with current progress
  - Captures session context and architectural decisions
  - Creates handoff notes for context resets
  - Optional: Provide specific context to focus on

### Domain Skills

The project includes specialized skills that activate automatically based on your work:

1. **pyspark-best-practices**: PySpark optimization and coding patterns
   - Triggers: Working with PySpark code, optimization queries
   - Provides: Performance tips, caching strategies, anti-patterns to avoid

2. **databricks-etl-patterns**: ETL pipeline development guidance
   - Triggers: Working with ETL jobs, BaseETL class, data quality
   - Provides: Incremental load patterns, error handling, testing strategies

3. **medallion-architecture**: Bronze/silver/gold layer guidance
   - Triggers: Working with specific layers (bronze/silver/gold)
   - Provides: Layer responsibilities, data flow patterns, naming conventions

4. **unity-catalog-data-discovery**: Table schema exploration
   - Triggers: Queries about table schemas, metadata, column types
   - Provides: DESCRIBE TABLE patterns, schema inspection techniques
   - Critical: Use before writing ETL transformations to understand source data

5. **skill-developer**: Meta-skill for managing Claude Code skills
   - Triggers: Working with skill-rules.json or creating new skills
   - Provides: Skill creation guidance, trigger patterns, debugging

### 💡 Usage Tips

**For Data Discovery:**
```
"What is the schema of catalog.schema.table_name?"
```
This will activate the unity-catalog-data-discovery skill and provide table metadata.

**For ETL Development:**
When working in `src/etl/bronze/`, `src/etl/silver/`, or `src/etl/gold/`, relevant skills activate automatically to provide:
- Layer-specific guidance
- BaseETL implementation patterns
- PySpark optimization tips

**For Strategic Planning:**
```
/dev-docs implement incremental loading for NYC taxi silver layer
```
This creates a structured plan with task breakdown, acceptance criteria, and progress tracking.

**For Context Preservation:**
Before reaching context limits:
```
/dev-docs-update current ETL implementation and architectural decisions
```
This documents your progress for seamless continuation after context reset.
