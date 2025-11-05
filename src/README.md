# Source Code Overview

This directory contains PySpark ETL modules for Databricks, implementing the medallion architecture pattern for data processing.

## Architecture Pattern

The codebase follows the **medallion architecture** (also known as the multi-hop architecture), which organizes data into three layers:

```
Bronze (Raw)  →  Silver (Refined)  →  Gold (Aggregated)
```

- **Bronze Layer**: Raw data ingestion with minimal transformation
- **Silver Layer**: Cleaned, validated, and deduplicated data
- **Gold Layer**: Business-level aggregations and metrics optimized for analytics

## Directory Structure

```
src/
├── etl/
│   ├── base.py                      # BaseETL abstract class
│   ├── bronze/                      # Raw data ingestion modules
│   ├── silver/                      # Data cleansing modules
│   └── gold/                        # Business aggregation modules
│       └── daily_trips_aggregation.py
└── README.md                        # This file
```

## BaseETL Class

All ETL jobs inherit from the `BaseETL` abstract class (src/etl/base.py:10), which provides a standardized pipeline pattern:

### Core Methods

1. **`extract()`**: Read data from source (table, file, or API)
2. **`transform()`**: Apply transformations, validations, and business logic
3. **`load()`**: Write data to target location in Delta format
4. **`run()`**: Execute the full ETL pipeline (extract → transform → load)

### Key Features

- **Databricks Connect Integration**: Uses `DatabricksSession` instead of standard SparkSession
- **Remote Execution**: All PySpark code runs on your configured Databricks cluster
- **Consistent Pattern**: All ETL jobs follow the same extract-transform-load structure
- **Type Safety**: Uses type hints for better IDE support and documentation

### Example Usage

```python
from src.etl.gold.daily_trips_aggregation import DailyTripsAggregationETL

# Initialize ETL job with source path
etl = DailyTripsAggregationETL(source_path="samples.nyctaxi.trips")

# Run full pipeline
etl.run(target_path="main.default.gold_daily_trips")
```

## Current Implementations

### Gold Layer

#### DailyTripsAggregationETL
**File**: src/etl/gold/daily_trips_aggregation.py:9

Aggregates NYC taxi trip data by day to produce business metrics.

**Source**: Unity Catalog table (e.g., `samples.nyctaxi.trips`)

**Transformations**:
- Extracts date from pickup timestamp
- Groups trips by pickup date
- Calculates daily metrics:
  - `avg_fare_amount`: Average fare per day
  - `avg_trip_distance`: Average trip distance per day
  - `total_trips`: Total trip count per day

**Output**: Delta table in overwrite mode

**Use Case**: Daily reporting, trend analysis, and business intelligence

## Creating New ETL Jobs

To create a new ETL job:

1. **Choose the appropriate layer**:
   - Bronze: For raw data ingestion
   - Silver: For data cleansing and validation
   - Gold: For business-level aggregations

2. **Create a new module** in the corresponding directory:
   ```python
   from src.etl.base import BaseETL
   from pyspark.sql import DataFrame

   class MyNewETL(BaseETL):
       def extract(self) -> DataFrame:
           # Read from source
           return self.spark.read.table(self.source_path)

       def transform(self, df: DataFrame) -> DataFrame:
           # Apply transformations
           return df.filter(...).select(...)

       def load(self, df: DataFrame, target_path: str) -> None:
           # Write to target
           df.write.format("delta").mode("overwrite").saveAsTable(target_path)
   ```

3. **Instantiate and run**:
   ```python
   etl = MyNewETL(source_path="/path/to/source")
   etl.run(target_path="catalog.schema.table")
   ```

## Design Principles

1. **Single Responsibility**: Each ETL class handles one specific transformation
2. **Reusability**: Common patterns are abstracted into the BaseETL class
3. **Testability**: Each method (extract, transform, load) can be tested independently
4. **Delta Lake**: All layers use Delta format for ACID transactions and time travel
5. **Remote Execution**: All Spark operations run on Databricks clusters via Databricks Connect

## Testing

ETL jobs are tested in `tests/etl/` with the same directory structure:

```python
# tests/etl/gold/test_daily_trips_aggregation.py
def test_transform():
    etl = DailyTripsAggregationETL("source")
    result = etl.transform(input_df)
    assert result.columns == ["pickup_date", "avg_fare_amount", ...]
```

See the project root `tests/` directory for examples.

## Development Guidelines

- **Always inherit from BaseETL** for consistency
- **Use descriptive class names** ending with "ETL" (e.g., `CustomerSilverETL`)
- **Document transformations** with clear docstrings
- **Test each method** independently before running full pipeline
- **Use Delta Lake format** for all writes to enable ACID transactions
- **Follow medallion architecture** - respect layer boundaries

## Next Steps

To extend this codebase:

1. **Bronze Layer**: Create ingestion jobs for raw data sources
2. **Silver Layer**: Add data cleansing and validation modules
3. **Gold Layer**: Build additional aggregations for specific business needs
4. **Utilities**: Add common helper functions for data quality, logging, etc.

For more information, see the project root `CLAUDE.md` and main `README.md`.
