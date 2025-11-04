---
name: databricks-etl-patterns
description: ETL patterns and best practices for Databricks pipelines including BaseETL class usage, incremental loads, error handling, data quality checks, monitoring, idempotency, Delta Lake operations, and testing strategies. Use when building ETL jobs, implementing data pipelines, or troubleshooting data processing workflows.
---

# Databricks ETL Patterns

## Purpose

Comprehensive guide for building robust, scalable ETL pipelines in Databricks using the medallion architecture and BaseETL pattern established in this project.

## When to Use This Skill

Automatically activates when:
- Creating new ETL jobs or pipelines
- Working with BaseETL class
- Implementing extract, transform, load methods
- Handling incremental data loads
- Adding data quality validations
- Troubleshooting ETL failures
- Optimizing pipeline performance

---

## BaseETL Pattern

### Core Structure

All ETL jobs in this project inherit from `BaseETL` (`src/etl/base.py`):

```python
from src.etl.base import BaseETL
from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame

class MyETL(BaseETL):
    """ETL job description."""

    def __init__(self, source_path: str, spark: DatabricksSession = None):
        super().__init__(source_path, spark)

    def extract(self) -> DataFrame:
        """Extract data from source."""
        return self.spark.read.format("delta").load(self.source_path)

    def transform(self, df: DataFrame) -> DataFrame:
        """Apply transformations."""
        # Your transformation logic
        return df

    def load(self, df: DataFrame, target_path: str) -> None:
        """Load data to target."""
        df.write.format("delta").mode("overwrite").save(target_path)
```

### Usage Pattern

```python
# Initialize ETL job
etl = MyETL(source_path="/path/to/source")

# Execute pipeline
etl.run(target_path="/path/to/target")
```

---

## Extract Patterns

### Reading from Different Sources

```python
def extract(self) -> DataFrame:
    """Extract from various sources."""

    # Delta table
    df = self.spark.table("catalog.schema.table")

    # CSV with schema
    df = self.spark.read.schema(self.get_schema()).csv(self.source_path)

    # Parquet
    df = self.spark.read.parquet(self.source_path)

    # JSON with multiline support
    df = self.spark.read.option("multiline", "true").json(self.source_path)

    return df
```

### Incremental Extraction

```python
from pyspark.sql import functions as F

def extract(self) -> DataFrame:
    """Extract only new/changed records."""

    # Get last processed timestamp
    last_run = self.get_last_run_timestamp()

    df = self.spark.table(self.source_path)

    if last_run:
        # Incremental load
        df = df.filter(F.col("updated_at") > last_run)

    return df

def get_last_run_timestamp(self):
    """Get timestamp from last successful run."""
    try:
        control_table = self.spark.table("control.etl_runs")
        return (control_table
            .filter(F.col("job_name") == self.__class__.__name__)
            .filter(F.col("status") == "success")
            .agg(F.max("run_timestamp"))
            .collect()[0][0]
        )
    except:
        return None  # First run
```

---

## Transform Patterns

### Data Quality Checks

```python
from pyspark.sql import functions as F

def transform(self, df: DataFrame) -> DataFrame:
    """Transform with quality checks."""

    # Add data quality flags
    df = self.add_quality_flags(df)

    # Apply business transformations
    df = self.apply_business_logic(df)

    # Filter invalid records (or move to quarantine)
    df_valid = df.filter(F.col("is_valid") == True)
    df_invalid = df.filter(F.col("is_valid") == False)

    # Write invalid records to quarantine
    if df_invalid.count() > 0:
        self.quarantine_invalid_records(df_invalid)

    return df_valid

def add_quality_flags(self, df: DataFrame) -> DataFrame:
    """Add data quality validation flags."""
    return (df
        .withColumn("is_valid",
            (F.col("id").isNotNull()) &
            (F.col("amount") >= 0) &
            (F.col("email").rlike(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'))
        )
        .withColumn("validation_timestamp", F.current_timestamp())
    )
```

### Adding Metadata Columns

```python
from pyspark.sql import functions as F

def transform(self, df: DataFrame) -> DataFrame:
    """Add standard metadata columns."""
    return (df
        .withColumn("etl_inserted_at", F.current_timestamp())
        .withColumn("etl_source_file", F.input_file_name())
        .withColumn("etl_job_name", F.lit(self.__class__.__name__))
        .withColumn("etl_run_id", F.lit(self.run_id))
    )
```

### Handling Duplicates

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def transform(self, df: DataFrame) -> DataFrame:
    """Remove duplicates keeping most recent."""

    # Define window for deduplication
    window = Window.partitionBy("id").orderBy(F.col("updated_at").desc())

    return (df
        .withColumn("row_num", F.row_number().over(window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )
```

---

## Load Patterns

### Delta Lake Writes

```python
def load(self, df: DataFrame, target_path: str) -> None:
    """Load with Delta Lake optimizations."""

    # Overwrite mode (Silver/Gold layers)
    df.write.format("delta").mode("overwrite").save(target_path)

    # Append mode (Bronze layer)
    df.write.format("delta").mode("append").save(target_path)

    # Merge mode for upserts
    self.merge_data(df, target_path)
```

### Merge (Upsert) Pattern

```python
from delta.tables import DeltaTable

def merge_data(self, df: DataFrame, target_path: str) -> None:
    """Merge new data with existing Delta table."""

    if DeltaTable.isDeltaTable(self.spark, target_path):
        target = DeltaTable.forPath(self.spark, target_path)

        (target.alias("target")
            .merge(
                df.alias("source"),
                "target.id = source.id"
            )
            .whenMatchedUpdate(set={
                "amount": "source.amount",
                "updated_at": "source.updated_at"
            })
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        # First load, just write
        df.write.format("delta").save(target_path)
```

### Partitioned Writes

```python
def load(self, df: DataFrame, target_path: str) -> None:
    """Write data partitioned by date."""

    (df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("date")  # Or year, month, etc.
        .option("overwriteSchema", "true")  # Allow schema evolution
        .save(target_path)
    )
```

---

## Error Handling

### Comprehensive Error Handling

```python
from pyspark.sql.utils import AnalysisException
import logging

logger = logging.getLogger(__name__)

def run(self, target_path: str) -> bool:
    """Execute ETL with error handling."""
    try:
        logger.info(f"Starting {self.__class__.__name__}")

        # Extract
        df = self.extract()
        logger.info(f"Extracted {df.count()} records")

        # Transform
        df_transformed = self.transform(df)
        logger.info(f"Transformed to {df_transformed.count()} records")

        # Load
        self.load(df_transformed, target_path)
        logger.info(f"Loaded to {target_path}")

        # Update control table
        self.update_control_table("success")

        return True

    except AnalysisException as e:
        logger.error(f"Schema or table error: {str(e)}")
        self.update_control_table("failed", error=str(e))
        raise

    except Exception as e:
        logger.error(f"ETL failed: {str(e)}")
        self.update_control_table("failed", error=str(e))
        raise
```

### Retry Logic

```python
from time import sleep

def run_with_retry(self, target_path: str, max_retries: int = 3) -> bool:
    """Execute ETL with retry logic."""
    for attempt in range(max_retries):
        try:
            return self.run(target_path)
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                logger.warning(f"Attempt {attempt + 1} failed, retrying in {wait_time}s")
                sleep(wait_time)
            else:
                logger.error(f"All {max_retries} attempts failed")
                raise
```

---

## Testing ETL Jobs

### Unit Testing Transformations

```python
import pytest
from databricks.connect import DatabricksSession

@pytest.fixture(scope="session")
def spark():
    """Create Spark session for tests."""
    return DatabricksSession.builder.getOrCreate()

def test_transform_logic(spark):
    """Test transformation with sample data."""
    # Create sample input
    sample_data = [
        (1, "active", 100.0),
        (2, "inactive", -50.0),  # Invalid
        (3, "active", 200.0)
    ]
    df = spark.createDataFrame(sample_data, ["id", "status", "amount"])

    # Apply transformation
    etl = MyETL(source_path="dummy")
    result = etl.transform(df)

    # Verify results
    assert result.count() == 2  # One invalid filtered
    assert result.filter("status = 'active'").count() == 2
```

### Integration Testing

```python
def test_full_etl_pipeline(spark):
    """Test complete ETL pipeline with sample data."""
    source_path = "/test/source/"
    target_path = "/test/target/"

    # Write test source data
    test_data = spark.createDataFrame([
        (1, "test", 100.0)
    ], ["id", "name", "amount"])
    test_data.write.format("delta").save(source_path)

    # Run ETL
    etl = MyETL(source_path=source_path)
    etl.run(target_path=target_path)

    # Verify results
    result = spark.read.format("delta").load(target_path)
    assert result.count() == 1
    assert "etl_inserted_at" in result.columns
```

---

## Monitoring and Observability

### Logging Best Practices

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run(self, target_path: str) -> bool:
    """Run with comprehensive logging."""
    logger.info(f"Starting ETL: {self.__class__.__name__}")
    logger.info(f"Source: {self.source_path}")
    logger.info(f"Target: {target_path}")

    start_time = datetime.now()

    # Extract
    df = self.extract()
    logger.info(f"Extracted {df.count()} records in {datetime.now() - start_time}")

    # Log schema
    logger.debug(f"Schema: {df.schema}")

    # Continue with transform/load...
```

### Metrics Collection

```python
from datetime import datetime

class ETLMetrics:
    """Track ETL execution metrics."""

    def __init__(self, job_name: str):
        self.job_name = job_name
        self.start_time = datetime.now()
        self.metrics = {}

    def record_count(self, stage: str, count: int):
        """Record row count for stage."""
        self.metrics[f"{stage}_count"] = count

    def record_duration(self, stage: str, duration: float):
        """Record stage duration."""
        self.metrics[f"{stage}_duration_seconds"] = duration

    def publish_metrics(self):
        """Write metrics to monitoring table."""
        # Implementation to write to Delta table or external system
        pass
```

---

## Idempotency Pattern

### Making ETL Jobs Idempotent

```python
def run(self, target_path: str) -> bool:
    """Idempotent ETL execution."""

    # Generate deterministic run_id based on date/params
    self.run_id = self.generate_run_id()

    # Check if already processed
    if self.is_already_processed(self.run_id):
        logger.info(f"Run {self.run_id} already processed, skipping")
        return True

    # Process data
    df = self.extract()
    df_transformed = self.transform(df)
    self.load(df_transformed, target_path)

    # Mark as processed
    self.mark_as_processed(self.run_id)

    return True

def generate_run_id(self) -> str:
    """Generate deterministic run ID."""
    from hashlib import md5
    key = f"{self.__class__.__name__}_{self.source_path}_{datetime.now().date()}"
    return md5(key.encode()).hexdigest()
```

---

## Performance Optimization

### Checkpoint Strategy

```python
def run(self, target_path: str) -> bool:
    """ETL with checkpointing for large datasets."""

    # Extract
    df = self.extract()

    # Checkpoint after expensive operations
    df = df.checkpoint()

    # Continue processing
    df_transformed = self.transform(df)
    self.load(df_transformed, target_path)
```

### Adaptive Query Execution

```python
# Enable AQE in Databricks (usually on by default)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

---

## Quick Reference

### Essential ETL Checklist
- [ ] Inherit from BaseETL
- [ ] Implement extract(), transform(), load()
- [ ] Add data quality validations
- [ ] Include error handling
- [ ] Add metadata columns (etl_inserted_at, etc.)
- [ ] Write to Delta Lake format
- [ ] Add logging statements
- [ ] Make pipeline idempotent
- [ ] Write unit tests
- [ ] Handle incremental loads

### Common Imports
```python
from src.etl.base import BaseETL
from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame, functions as F
from delta.tables import DeltaTable
import logging
```

### Metadata Columns Standard
```python
.withColumn("etl_inserted_at", F.current_timestamp())
.withColumn("etl_source_file", F.input_file_name())
.withColumn("etl_job_name", F.lit(self.__class__.__name__))
```

---

**For PySpark-specific optimizations, see the pyspark-best-practices skill.**
**For layer-specific guidance, see the medallion-architecture skill.**
