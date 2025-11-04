---
name: medallion-architecture
description: Medallion architecture pattern guidance for bronze, silver, and gold data layers including layer responsibilities, data flow, transformations, storage strategies, naming conventions, and when to use each layer. Use when designing ETL pipelines, creating new layers, or understanding data flow through the architecture.
---

# Medallion Architecture Guide

## Purpose

Comprehensive guide for implementing the medallion architecture (Bronze → Silver → Gold) pattern in this Databricks project. Defines responsibilities, best practices, and patterns for each layer.

## When to Use This Skill

Automatically activates when:
- Creating new bronze/silver/gold ETL modules
- Designing data pipeline flow
- Deciding which layer for transformations
- Understanding layer responsibilities
- Working with data quality across layers
- Implementing incremental loads by layer

---

## Architecture Overview

### Three-Layer Pattern

```
Bronze (Raw)          Silver (Cleansed)         Gold (Curated)
     │                       │                        │
     │  Minimal transforms   │  Cleansing            │  Aggregations
     ▼                       ▼                        ▼
  [Raw Data]  ──────────>  [Clean Data]  ──────>  [Business Metrics]
  Append-only              Overwrite/Merge         Overwrite
  All history              Current state           Optimized for analytics
```

### Directory Structure

```
src/etl/
├── base.py                    # BaseETL abstract class
├── bronze/                    # Raw ingestion layer
│   ├── __init__.py
│   └── [source]_ingestion.py
├── silver/                    # Cleansing layer
│   ├── __init__.py
│   └── [entity]_cleansed.py
└── gold/                      # Aggregation layer
    ├── __init__.py
    └── [metric]_aggregated.py
```

---

## Bronze Layer (Raw)

### Purpose
- **Raw data ingestion** from source systems
- **Minimal transformation** - preserve original data
- **Audit trail** - full history of all data received
- **Data lake** - append-only storage

### Characteristics

**Storage:**
- Format: Delta Lake
- Mode: **Append-only** (preserves all history)
- Partitioning: By ingestion_date or source_date

**Transformations:**
- Add metadata columns (ingestion_timestamp, source_file)
- Type casting only if necessary
- No business logic
- No filtering (except technical failures)
- No deduplication

**Schema:**
```
Original columns + metadata:
- etl_inserted_at: TIMESTAMP
- etl_source_file: STRING
- etl_job_name: STRING
- ingestion_date: DATE (for partitioning)
```

### Bronze Layer Example

```python
from src.etl.base import BaseETL
from pyspark.sql import DataFrame, functions as F

class TaxiTripsBronzeETL(BaseETL):
    """Ingest raw taxi trip data into Bronze layer."""

    def extract(self) -> DataFrame:
        """Read raw CSV files."""
        return (self.spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(self.source_path)
        )

    def transform(self, df: DataFrame) -> DataFrame:
        """Add metadata, minimal transformation."""
        return (df
            # Add metadata columns
            .withColumn("etl_inserted_at", F.current_timestamp())
            .withColumn("etl_source_file", F.input_file_name())
            .withColumn("etl_job_name", F.lit(self.__class__.__name__))
            .withColumn("ingestion_date", F.current_date())
        )

    def load(self, df: DataFrame, target_path: str) -> None:
        """Append to Bronze Delta table."""
        (df.write
            .format("delta")
            .mode("append")  # Always append in Bronze
            .partitionBy("ingestion_date")
            .save(target_path)
        )
```

### Bronze Layer Best Practices

✅ **DO:**
- Preserve all source data (even if duplicate/invalid)
- Add standard metadata columns
- Partition by ingestion_date for incremental processing
- Use schema-on-write when possible
- Log source file names
- Keep transformations minimal

❌ **DON'T:**
- Filter out "bad" data (move to Silver)
- Deduplicate records
- Apply business logic
- Join with other tables
- Aggregate data
- Use overwrite mode (loses history)

---

## Silver Layer (Cleansed)

### Purpose
- **Data quality** - validated and cleansed records
- **Deduplication** - one version of truth
- **Standardization** - consistent formats
- **Current state** - latest valid records

### Characteristics

**Storage:**
- Format: Delta Lake
- Mode: **Overwrite** or **Merge** (upsert)
- Partitioning: By business key (date, category, etc.)

**Transformations:**
- Data quality validations
- Remove duplicates
- Handle nulls and defaults
- Standardize formats (dates, strings)
- Type conversions
- Data quality flags
- Quarantine invalid records

**Schema:**
```
Cleansed columns + quality metadata:
- is_valid: BOOLEAN
- validation_errors: ARRAY<STRING>
- cleansed_at: TIMESTAMP
- source_record_count: BIGINT
```

### Silver Layer Example

```python
from src.etl.base import BaseETL
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

class TaxiTripsSilverETL(BaseETL):
    """Cleanse and validate taxi trip data for Silver layer."""

    def extract(self) -> DataFrame:
        """Read from Bronze layer."""
        return self.spark.read.format("delta").load(self.source_path)

    def transform(self, df: DataFrame) -> DataFrame:
        """Apply data quality and cleansing."""

        # Data quality validations
        df = (df
            .withColumn("is_valid",
                (F.col("trip_distance") > 0) &
                (F.col("fare_amount") >= 0) &
                (F.col("pickup_datetime").isNotNull()) &
                (F.col("dropoff_datetime").isNotNull())
            )
            .withColumn("cleansed_at", F.current_timestamp())
        )

        # Remove duplicates (keep most recent)
        window = Window.partitionBy("trip_id").orderBy(F.col("etl_inserted_at").desc())
        df = (df
            .withColumn("row_num", F.row_number().over(window))
            .filter(F.col("row_num") == 1)
            .drop("row_num")
        )

        # Standardize formats
        df = (df
            .withColumn("pickup_datetime", F.to_timestamp("pickup_datetime"))
            .withColumn("passenger_count", F.col("passenger_count").cast("int"))
        )

        # Split valid/invalid
        df_valid = df.filter(F.col("is_valid") == True)
        df_invalid = df.filter(F.col("is_valid") == False)

        # Quarantine invalid records
        if df_invalid.count() > 0:
            self.quarantine_records(df_invalid)

        return df_valid

    def load(self, df: DataFrame, target_path: str) -> None:
        """Overwrite Silver Delta table."""
        (df.write
            .format("delta")
            .mode("overwrite")  # Current state only
            .save(target_path)
        )

    def quarantine_records(self, df: DataFrame) -> None:
        """Write invalid records to quarantine."""
        quarantine_path = self.source_path.replace("/silver/", "/quarantine/")
        (df.write
            .format("delta")
            .mode("append")
            .save(quarantine_path)
        )
```

### Silver Layer Best Practices

✅ **DO:**
- Validate data quality with flags
- Remove duplicates systematically
- Handle nulls with business rules
- Quarantine invalid records
- Add cleansing metadata
- Use overwrite or merge mode
- Keep current state only

❌ **DON'T:**
- Delete invalid records silently
- Apply complex business aggregations
- Denormalize for reporting
- Keep full history (that's Bronze)
- Use append mode (unless incremental merge)

---

## Gold Layer (Curated)

### Purpose
- **Business metrics** - KPIs and aggregations
- **Denormalized** - optimized for analytics
- **Performance** - fast query execution
- **Business-ready** - matches reporting needs

### Characteristics

**Storage:**
- Format: Delta Lake
- Mode: **Overwrite** (full refresh) or **Merge** (incremental)
- Partitioning: By report dimensions (date, region, product)
- Optimization: Z-ordering, optimize, vacuum

**Transformations:**
- Aggregations (sum, avg, count)
- Business calculations
- Denormalization (flatten hierarchies)
- Joining dimension tables
- Window functions for trends
- Metric calculations

**Schema:**
```
Business metrics + dimensions:
- metric_date: DATE
- [dimension_columns]: STRING/INT
- [metric_columns]: DOUBLE/DECIMAL
- aggregated_at: TIMESTAMP
- record_count: BIGINT
```

### Gold Layer Example

```python
from src.etl.base import BaseETL
from pyspark.sql import DataFrame, functions as F

class DailyTripMetricsGoldETL(BaseETL):
    """Calculate daily trip metrics for Gold layer."""

    def extract(self) -> DataFrame:
        """Read from Silver layer."""
        return self.spark.read.format("delta").load(self.source_path)

    def transform(self, df: DataFrame) -> DataFrame:
        """Calculate business metrics."""

        return (df
            # Extract date for aggregation
            .withColumn("trip_date", F.to_date("pickup_datetime"))

            # Group and aggregate
            .groupBy("trip_date")
            .agg(
                F.count("*").alias("total_trips"),
                F.sum("fare_amount").alias("total_revenue"),
                F.avg("fare_amount").alias("avg_fare"),
                F.avg("trip_distance").alias("avg_distance"),
                F.sum("tip_amount").alias("total_tips"),
                F.avg(F.col("tip_amount") / F.col("fare_amount")).alias("avg_tip_percentage")
            )

            # Add metadata
            .withColumn("aggregated_at", F.current_timestamp())

            # Sort for readability
            .orderBy("trip_date")
        )

    def load(self, df: DataFrame, target_path: str) -> None:
        """Write and optimize Gold table."""
        (df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("trip_date")
            .option("overwriteSchema", "true")
            .save(target_path)
        )

        # Optimize after write
        self.spark.sql(f"OPTIMIZE delta.`{target_path}`")
```

### Gold Layer Best Practices

✅ **DO:**
- Aggregate to business grain
- Denormalize for query performance
- Add calculated metrics
- Partition by query patterns
- Optimize tables after write
- Include aggregation metadata
- Match reporting requirements

❌ **DON'T:**
- Keep raw detail (use Silver)
- Over-aggregate (lose flexibility)
- Skip optimization
- Forget about partitioning
- Include PII unless necessary

---

## Layer Decision Guide

### Which Layer for Your Transformation?

| Question | Bronze | Silver | Gold |
|----------|--------|--------|------|
| Is this raw data from source? | ✅ | ❌ | ❌ |
| Need full historical audit trail? | ✅ | ❌ | ❌ |
| Cleaning/validating data? | ❌ | ✅ | ❌ |
| Removing duplicates? | ❌ | ✅ | ❌ |
| Aggregating metrics? | ❌ | ❌ | ✅ |
| Building KPIs/dashboards? | ❌ | ❌ | ✅ |
| Joining multiple entities? | ❌ | Maybe | ✅ |

### Data Flow Examples

**Example 1: Sales Data**
```
Bronze: Raw sales transactions (all records, all time)
   ↓
Silver: Validated sales (deduplicated, current, quality-checked)
   ↓
Gold: Daily sales metrics by product/region
```

**Example 2: User Events**
```
Bronze: Raw clickstream events (append-only)
   ↓
Silver: Cleansed events (valid sessions, standardized)
   ↓
Gold: User engagement metrics, cohort analysis
```

---

## Common Patterns

### Incremental Processing

**Bronze:** Always append new data
```python
# Process only new files
df = spark.read.csv(f"{source_path}/{today}")
df.write.mode("append").save(bronze_path)
```

**Silver:** Process new Bronze data
```python
# Get last processed timestamp
last_run = get_last_run()
df = spark.read.delta(bronze_path).filter(F.col("etl_inserted_at") > last_run)
# Clean and overwrite Silver
df_clean.write.mode("overwrite").save(silver_path)
```

**Gold:** Recompute aggregations
```python
# Read all Silver data (or incremental window)
df = spark.read.delta(silver_path)
# Aggregate and overwrite Gold
df_agg.write.mode("overwrite").save(gold_path)
```

### Testing Across Layers

```python
def test_bronze_to_silver_to_gold():
    """Integration test across all layers."""

    # Setup: Write test data to Bronze
    bronze_etl = BronzeETL(source="test_input")
    bronze_etl.run("test_bronze")

    # Test Silver processing
    silver_etl = SilverETL(source="test_bronze")
    silver_etl.run("test_silver")

    # Test Gold aggregation
    gold_etl = GoldETL(source="test_silver")
    gold_etl.run("test_gold")

    # Verify end-to-end
    result = spark.read.delta("test_gold")
    assert result.count() > 0
```

---

## Naming Conventions

### File Naming
```
src/etl/bronze/taxi_trips_ingestion.py
src/etl/silver/taxi_trips_cleansed.py
src/etl/gold/daily_trip_metrics.py
```

### Table Naming
```
bronze.raw_taxi_trips
silver.taxi_trips
gold.daily_trip_metrics
```

### Path Naming
```
/bronze/taxi_trips/
/silver/taxi_trips/
/gold/daily_trip_metrics/
```

---

## Performance Optimization by Layer

### Bronze Optimization
- Partition by ingestion_date
- Optimize file sizes (128MB-1GB)
- Use efficient file formats (Delta)
- Minimal transformations for speed

### Silver Optimization
- Partition by business keys
- Use Z-ordering for query columns
- Regular OPTIMIZE and VACUUM
- Index on join columns

### Gold Optimization
- Denormalize for query performance
- Aggressive partitioning
- Z-order by filter columns
- Cache frequently accessed tables
- Pre-aggregate where possible

```python
# Optimize Gold tables
spark.sql(f"OPTIMIZE gold.daily_metrics ZORDER BY (date, region)")
spark.sql(f"VACUUM gold.daily_metrics RETAIN 168 HOURS")
```

---

## Quick Reference

### Layer Comparison

| Aspect | Bronze | Silver | Gold |
|--------|--------|--------|------|
| **Purpose** | Raw ingestion | Cleansing | Aggregation |
| **Write Mode** | Append | Overwrite/Merge | Overwrite/Merge |
| **History** | Full | Current | Aggregated |
| **Quality** | All records | Valid only | Business-ready |
| **Complexity** | Low | Medium | High |
| **Query Speed** | Slow | Medium | Fast |

### Standard Metadata by Layer

**Bronze:**
```python
.withColumn("etl_inserted_at", F.current_timestamp())
.withColumn("etl_source_file", F.input_file_name())
.withColumn("ingestion_date", F.current_date())
```

**Silver:**
```python
.withColumn("is_valid", validation_expression)
.withColumn("cleansed_at", F.current_timestamp())
```

**Gold:**
```python
.withColumn("aggregated_at", F.current_timestamp())
.withColumn("record_count", F.lit(source_count))
```

---

**For ETL implementation patterns, see the databricks-etl-patterns skill.**
**For PySpark performance tips, see the pyspark-best-practices skill.**
