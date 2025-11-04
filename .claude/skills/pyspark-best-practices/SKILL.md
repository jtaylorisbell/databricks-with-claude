---
name: pyspark-best-practices
description: Best practices for writing PySpark code including performance optimization, DataFrame operations, UDFs, caching strategies, partition management, broadcast joins, schema handling, and common anti-patterns to avoid. Use when writing PySpark transformations, optimizing queries, or troubleshooting performance issues.
---

# PySpark Best Practices

## Purpose

Provides comprehensive guidance for writing efficient, maintainable PySpark code in Databricks environments. Covers performance optimization, common patterns, and anti-patterns to avoid.

## When to Use This Skill

Automatically activates when:
- Writing PySpark transformations or queries
- Optimizing DataFrame operations
- Working with spark.sql() or DataFrame API
- Debugging PySpark performance issues
- Implementing data transformations
- Using UDFs or complex operations

---

## Core Principles

### 1. Prefer DataFrame Operations Over RDDs
**Always use DataFrame API** - It's optimized by Catalyst and Tungsten engines.

```python
# Good: DataFrame API
df.filter(F.col("age") > 18).select("name", "age")

# Avoid: RDD operations (unless absolutely necessary)
df.rdd.filter(lambda x: x.age > 18).map(lambda x: (x.name, x.age))
```

### 2. Use Built-in Functions Over UDFs
Built-in functions are optimized and run faster than Python UDFs.

```python
from pyspark.sql import functions as F

# Good: Built-in function
df.withColumn("full_name", F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")))

# Avoid: Python UDF for simple operations
@udf(StringType())
def concatenate_names(first, last):
    return f"{first} {last}"

df.withColumn("full_name", concatenate_names("first_name", "last_name"))
```

**When UDFs are necessary:**
- Use Pandas UDFs (vectorized) for better performance
- Define return types explicitly
- Keep logic simple and testable

```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf("double")
def calculate_score(scores: pd.Series) -> pd.Series:
    return scores * 1.5

df.withColumn("adjusted_score", calculate_score("raw_score"))
```

### 3. Avoid Collect() on Large Datasets
`collect()` brings all data to the driver - use only for small results.

```python
# Good: Count records
count = df.count()

# Good: Show sample
df.show(10)

# Avoid: Collect large dataset
all_data = df.collect()  # Can cause OOM errors!

# If you need to iterate, use foreachPartition
def process_partition(partition):
    for row in partition:
        # Process row
        pass

df.foreachPartition(process_partition)
```

---

## Performance Optimization

### Caching Strategies

Cache DataFrames that are reused multiple times:

```python
# Cache when DataFrame is used multiple times
df_cached = df.filter(F.col("status") == "active").cache()

# Use in multiple operations
result1 = df_cached.groupBy("category").count()
result2 = df_cached.filter(F.col("amount") > 1000)

# Unpersist when done
df_cached.unpersist()
```

**Cache Levels:**
- `cache()` - Memory only (default: MEMORY_AND_DISK)
- `persist(StorageLevel.MEMORY_ONLY)` - Memory only, fail if OOM
- `persist(StorageLevel.MEMORY_AND_DISK)` - Spill to disk if needed
- `persist(StorageLevel.DISK_ONLY)` - Disk only

### Partition Management

**Repartition vs Coalesce:**

```python
# Repartition: Full shuffle, use for increasing partitions or balancing
df_repartitioned = df.repartition(200)

# Coalesce: No shuffle, use for reducing partitions
df_coalesced = df.coalesce(50)

# Repartition by column (for downstream joins/groupBy)
df_partitioned = df.repartition(100, "user_id")
```

**Optimal partition size:** 128MB - 1GB per partition

### Broadcast Joins

Use broadcast for small dimension tables (<10GB):

```python
from pyspark.sql.functions import broadcast

# Broadcast small dimension table
large_df.join(broadcast(small_df), "id")

# Configure broadcast threshold (default: 10MB)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")
```

---

## Schema Management

### Define Schemas Explicitly

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Good: Explicit schema
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

df = spark.read.schema(schema).json("path/to/data")

# Avoid: Schema inference (slower and can be incorrect)
df = spark.read.json("path/to/data")
```

### Schema Evolution

Handle schema changes gracefully:

```python
# Check if column exists before accessing
if "new_column" in df.columns:
    df = df.withColumn("processed", F.col("new_column") * 2)
else:
    df = df.withColumn("processed", F.lit(0))

# Or use select with error handling
df.select([F.col(c) for c in ["id", "name"] if c in df.columns])
```

---

## Common Anti-Patterns to Avoid

### 1. Don't Use Row-by-Row Operations

```python
# BAD: Iterating over rows
for row in df.collect():
    # Process row
    result = process(row)

# GOOD: Use DataFrame operations
df.withColumn("result", process_udf(F.col("input")))
```

### 2. Avoid Multiple Writes in Loops

```python
# BAD: Writing in a loop
for category in categories:
    df.filter(F.col("category") == category).write.save(f"output/{category}")

# GOOD: Use partitionBy
df.write.partitionBy("category").save("output/")
```

### 3. Don't Create New SparkContext

```python
# BAD: Creating new context
from pyspark import SparkContext
sc = SparkContext()

# GOOD: Use existing session in Databricks
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()
```

### 4. Avoid SELECT * When Possible

```python
# BAD: Reading all columns
df = spark.table("large_table")

# GOOD: Select only needed columns
df = spark.table("large_table").select("id", "name", "amount")
```

---

## Databricks-Specific Best Practices

### Use Databricks Connect for Remote Execution

```python
from databricks.connect import DatabricksSession

# Always use DatabricksSession in this project
spark = DatabricksSession.builder.getOrCreate()
```

### Delta Lake Operations

```python
from delta.tables import DeltaTable

# Read Delta table
df = spark.read.format("delta").load("/path/to/delta/table")

# Or use table name
df = spark.table("catalog.schema.table")

# Optimize Delta tables regularly
spark.sql("OPTIMIZE delta.`/path/to/table`")

# Vacuum old files (default: 7 days)
spark.sql("VACUUM delta.`/path/to/table` RETAIN 168 HOURS")
```

### Optimize File Sizes

```python
# Configure target file size for writes (default: 128MB)
spark.conf.set("spark.databricks.delta.targetFileSize", "128MB")

# Repartition before writing to control file count
df.repartition(100).write.format("delta").save("path")
```

---

## Testing Best Practices

### Write Testable Code

```python
# Good: Separate transformation logic
def transform_sales_data(df):
    """Transform raw sales data."""
    return (df
        .filter(F.col("status") == "completed")
        .withColumn("total", F.col("quantity") * F.col("price"))
        .groupBy("product_id")
        .agg(F.sum("total").alias("total_revenue"))
    )

# Easy to test with sample data
def test_transform_sales_data():
    sample_df = spark.createDataFrame([
        (1, "completed", 10, 5.0),
        (2, "pending", 5, 10.0)
    ], ["product_id", "status", "quantity", "price"])

    result = transform_sales_data(sample_df)
    assert result.count() == 1
```

### Use Small Sample Data

```python
# For development/testing, use sample
df_sample = df.sample(fraction=0.01, seed=42)

# Or limit for quick iterations
df_dev = df.limit(1000)
```

---

## Debugging Tips

### Explain Query Plans

```python
# See physical plan
df.explain(mode="extended")

# See cost-based optimization
df.explain(mode="cost")

# Simple explain
df.explain()
```

### Monitor Execution

```python
# Check number of partitions
print(f"Partitions: {df.rdd.getNumPartitions()}")

# Check data skew
df.groupBy(F.spark_partition_id()).count().show()
```

### Enable Query Logging

```python
# Set log level
spark.sparkContext.setLogLevel("INFO")

# Use display for Databricks notebooks
display(df)  # Interactive visualization
```

---

## Quick Reference

### Essential Imports
```python
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from databricks.connect import DatabricksSession
```

### Common Transformations
- **Filter:** `df.filter(condition)` or `df.where(condition)`
- **Select:** `df.select("col1", "col2")` or `df.select(F.col("col1"))`
- **Add Column:** `df.withColumn("new_col", expression)`
- **Rename:** `df.withColumnRenamed("old", "new")`
- **Drop:** `df.drop("col1", "col2")`
- **Join:** `df1.join(df2, on="key", how="left")`
- **GroupBy:** `df.groupBy("col").agg(F.sum("amount"))`
- **Order:** `df.orderBy(F.col("col").desc())`

### Performance Checklist
- [ ] Using DataFrame API (not RDDs)
- [ ] Using built-in functions (not UDFs when possible)
- [ ] Explicit schemas defined
- [ ] Avoiding collect() on large data
- [ ] Caching reused DataFrames
- [ ] Appropriate partition count
- [ ] Broadcasting small dimension tables
- [ ] Selecting only needed columns
- [ ] Using Delta Lake optimization

---

**For more details on ETL patterns, see the databricks-etl-patterns skill.**
**For medallion architecture guidance, see the medallion-architecture skill.**
