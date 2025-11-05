---
name: unity-catalog-data-discovery
description: Guide for discovering Unity Catalog table metadata including schemas, column names, data types, descriptions, and table properties. Essential when writing ETL pipelines, data transformations, or queries. Covers DESCRIBE commands, SELECT LIMIT patterns, catalog exploration, schema inspection, and metadata discovery using PySpark (DatabricksSession) and SQL. Triggers on unity catalog, table schema, describe table, column names, data types, table metadata, catalog tables, schema discovery, inspect schema, table structure, catalog exploration.
---

# Unity Catalog Data Discovery

## Purpose

Provides comprehensive guidance for discovering and inspecting Unity Catalog table metadata, which is **critical before writing ETL pipelines**. Understanding table schemas, column types, descriptions, and properties ensures correct transformations and prevents runtime errors.

## When to Use This Skill

Use this skill when you need to:

- Discover what tables exist in a catalog/schema
- Inspect table schemas before writing ETL code
- Understand column names, data types, and descriptions
- View sample data to understand content and structure
- Check table properties, partitioning, and metadata
- Verify table structure before transformations
- Explore Unity Catalog hierarchies (catalog → schema → table)

**Golden Rule**: Always inspect table metadata BEFORE writing ETL transformations.

---

## Unity Catalog Hierarchy

Unity Catalog uses a three-level namespace:

```
catalog.schema.table
```

**Example**: `samples.nyctaxi.trips`
- **Catalog**: `samples`
- **Schema**: `nyctaxi`
- **Table**: `trips`

---

## Discovery Methods

### 1. DESCRIBE TABLE (Recommended for Schema)

**Purpose**: Get complete table schema with column names, types, and descriptions

**SQL Syntax**:
```sql
DESCRIBE TABLE catalog.schema.table;
DESCRIBE TABLE EXTENDED catalog.schema.table;  -- More detailed metadata
```

**PySpark Syntax**:
```python
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

# Method 1: Using SQL
schema_df = spark.sql("DESCRIBE TABLE catalog.schema.table")
schema_df.show(truncate=False)

# Method 2: Using DataFrame API
df = spark.table("catalog.schema.table")
df.printSchema()  # Shows schema in tree format
```

**Example Output**:
```
col_name            data_type    comment
tpep_pickup_datetime timestamp   The date and time when the meter was engaged
tpep_dropoff_datetime timestamp  The date and time when the meter was disengaged
passenger_count     bigint       The number of passengers in the vehicle
trip_distance       double       The elapsed trip distance in miles
fare_amount         double       The time-and-distance fare calculated by the meter
```

### 2. DESCRIBE TABLE EXTENDED

**Purpose**: Get comprehensive metadata including storage location, partitions, table properties

**SQL Syntax**:
```sql
DESCRIBE TABLE EXTENDED catalog.schema.table;
```

**PySpark Syntax**:
```python
extended_info = spark.sql("DESCRIBE TABLE EXTENDED catalog.schema.table")
extended_info.show(truncate=False)
```

**Additional Metadata Includes**:
- Storage location
- File format (Delta, Parquet, etc.)
- Partition columns
- Table properties
- Statistics
- Created/modified timestamps
- Owner

### 3. SELECT LIMIT (Recommended for Sample Data)

**Purpose**: View actual data to understand content, patterns, and edge cases

**SQL Syntax**:
```sql
SELECT * FROM catalog.schema.table LIMIT 10;
```

**PySpark Syntax**:
```python
# Method 1: Using SQL
sample_df = spark.sql("SELECT * FROM catalog.schema.table LIMIT 10")
sample_df.show()

# Method 2: Using DataFrame API
df = spark.table("catalog.schema.table")
df.limit(10).show()

# Method 3: Show with full column width
df.limit(10).show(truncate=False)

# Method 4: Pretty print as pandas (small samples only)
df.limit(10).toPandas()
```

**Best Practice**: Combine with specific columns if table is very wide:
```python
df.select("col1", "col2", "col3").limit(10).show()
```

### 4. List Catalogs and Schemas

**Discover available catalogs**:
```sql
SHOW CATALOGS;
```

```python
spark.sql("SHOW CATALOGS").show()
```

**Discover schemas in a catalog**:
```sql
SHOW SCHEMAS IN catalog_name;
```

```python
spark.sql("SHOW SCHEMAS IN catalog_name").show()
```

**Discover tables in a schema**:
```sql
SHOW TABLES IN catalog.schema;
```

```python
spark.sql("SHOW TABLES IN catalog.schema").show()
```

### 5. Information Schema (Advanced)

Unity Catalog provides SQL information schema for programmatic metadata access:

```sql
-- List all tables in a schema
SELECT * FROM catalog.information_schema.tables
WHERE table_schema = 'schema_name';

-- List all columns in a table
SELECT * FROM catalog.information_schema.columns
WHERE table_schema = 'schema_name'
  AND table_name = 'table_name';

-- Get detailed column metadata
SELECT
  column_name,
  data_type,
  is_nullable,
  column_default,
  comment
FROM catalog.information_schema.columns
WHERE table_schema = 'schema_name'
  AND table_name = 'table_name'
ORDER BY ordinal_position;
```

---

## Complete Discovery Workflow

When starting a new ETL pipeline, follow this workflow:

### Step 1: Verify Catalog and Schema Exist

```python
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

# List available catalogs
spark.sql("SHOW CATALOGS").show()

# List schemas in target catalog
spark.sql("SHOW SCHEMAS IN my_catalog").show()
```

### Step 2: Find Tables in Schema

```python
# List all tables
tables_df = spark.sql("SHOW TABLES IN my_catalog.my_schema")
tables_df.show(truncate=False)
```

### Step 3: Inspect Table Schema

```python
# Get schema details
schema_df = spark.sql("DESCRIBE TABLE my_catalog.my_schema.my_table")
schema_df.show(truncate=False)

# Or use printSchema for quick overview
df = spark.table("my_catalog.my_schema.my_table")
df.printSchema()
```

### Step 4: View Sample Data

```python
# See actual data
df.limit(10).show(truncate=False)

# Check for nulls and data quality
df.summary().show()  # Statistical summary
```

### Step 5: Document Findings

Create a comment in your ETL code:

```python
"""
Source Table: samples.nyctaxi.trips

Schema:
- tpep_pickup_datetime: timestamp (pickup time)
- tpep_dropoff_datetime: timestamp (dropoff time)
- passenger_count: bigint (number of passengers)
- trip_distance: double (miles)
- fare_amount: double (USD)
- tip_amount: double (USD)
- total_amount: double (USD)

Notes:
- Partitioned by pickup date
- ~1.5M rows per month
- Nulls present in passenger_count (~2%)
"""
```

---

## Best Practices

### DO ✅

1. **Always inspect schema before writing transformations**
   - Prevents column name typos
   - Ensures correct data type casting
   - Identifies nullable columns

2. **Use DESCRIBE TABLE for comprehensive schema**
   - Shows column types and comments
   - Faster than reading full table

3. **Use SELECT LIMIT for data patterns**
   - Understand actual data values
   - Identify edge cases (nulls, special characters)
   - Validate assumptions

4. **Document schema in ETL code**
   - Add schema comments at top of file
   - Helps future maintainers
   - Serves as validation reference

5. **Check for partitioning**
   - Use DESCRIBE TABLE EXTENDED
   - Partition-aware queries are much faster
   - Important for incremental loads

6. **Verify table exists before reading**
   ```python
   # Check table exists
   tables = spark.sql("SHOW TABLES IN catalog.schema").collect()
   table_names = [row.tableName for row in tables]

   if "my_table" not in table_names:
       raise ValueError("Table catalog.schema.my_table does not exist")
   ```

### DON'T ❌

1. **Don't hardcode column names without verification**
   - Always check spelling and case sensitivity
   - Column names might change between environments

2. **Don't assume data types**
   - Inspect actual types with DESCRIBE
   - Avoid implicit casting errors

3. **Don't skip sample data inspection**
   - Schema alone doesn't show data quality issues
   - Edge cases appear in actual data

4. **Don't query full tables unnecessarily**
   - Always use LIMIT for exploration
   - Full scans are expensive and slow

5. **Don't ignore table comments/descriptions**
   - Often contain crucial business logic
   - May explain non-obvious column meanings

---

## Integration with ETL Development

### Example: Bronze Layer ETL with Metadata Discovery

```python
"""
Bronze Layer: NYC Taxi Trip Ingestion

Source: samples.nyctaxi.trips
Target: bronze.taxi.raw_trips

Schema verified on: 2025-01-04
Key columns:
- tpep_pickup_datetime: timestamp (NOT NULL based on sample)
- trip_distance: double (nullable, contains 0 values)
- fare_amount: double (nullable, negative values present for refunds)
"""

from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit
from src.etl.base import BaseETL

class TaxiTripsBronzeETL(BaseETL):
    def __init__(self, source_table: str):
        super().__init__()
        self.source_table = source_table

    def extract(self) -> DataFrame:
        """Extract from Unity Catalog source table"""
        # Verify table exists
        df = self.spark.table(self.source_table)

        # Log schema for validation
        print(f"Source schema for {self.source_table}:")
        df.printSchema()

        return df

    def transform(self, df: DataFrame) -> DataFrame:
        """Add bronze layer metadata columns"""
        return df.withColumn("ingestion_timestamp", current_timestamp()) \
                 .withColumn("source_table", lit(self.source_table))

    def load(self, df: DataFrame, target_path: str) -> None:
        """Write to bronze layer as Delta"""
        df.write \
          .format("delta") \
          .mode("append") \
          .save(target_path)
```

### Example: Discovering Schema for Silver Layer Transformation

```python
"""
Before writing silver layer transformations, verify bronze schema:
"""

from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

# Step 1: Describe bronze table
bronze_schema = spark.sql("DESCRIBE TABLE bronze.taxi.raw_trips")
print("Bronze Schema:")
bronze_schema.show(truncate=False)

# Step 2: Check sample data
bronze_df = spark.table("bronze.taxi.raw_trips")
print("\nSample Data:")
bronze_df.limit(5).show(truncate=False)

# Step 3: Check for nulls
from pyspark.sql.functions import col, sum as _sum

null_counts = bronze_df.select([
    _sum(col(c).isNull().cast("int")).alias(c)
    for c in bronze_df.columns
])
print("\nNull Counts:")
null_counts.show()

# Now write silver transformation with confidence in schema
```

---

## Common Patterns

### Pattern 1: Dynamic Column Selection

```python
# Get all numeric columns programmatically
schema_df = spark.sql("DESCRIBE TABLE catalog.schema.table")
numeric_cols = schema_df.filter(
    col("data_type").isin(["bigint", "int", "double", "float", "decimal"])
).select("col_name").rdd.flatMap(lambda x: x).collect()

# Use in transformation
df = spark.table("catalog.schema.table")
numeric_df = df.select(*numeric_cols)
```

### Pattern 2: Schema Validation in ETL

```python
def validate_expected_schema(df: DataFrame, expected_columns: list) -> bool:
    """Validate DataFrame has expected columns"""
    actual_columns = set(df.columns)
    expected_columns_set = set(expected_columns)

    missing = expected_columns_set - actual_columns
    if missing:
        raise ValueError(f"Missing expected columns: {missing}")

    return True

# In ETL extract method
df = spark.table("source.table")
validate_expected_schema(df, ["id", "name", "created_at"])
```

### Pattern 3: Incremental Load with Partition Discovery

```python
# Discover partition columns
extended_info = spark.sql("DESCRIBE TABLE EXTENDED catalog.schema.table")
extended_info.filter(col("col_name") == "# Partition Information").show()

# Use partition column for incremental load
from datetime import datetime, timedelta

yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

incremental_df = spark.table("catalog.schema.table") \
    .filter(col("partition_date") == yesterday)
```

---

## Troubleshooting

### Issue: Table Not Found

**Error**: `Table or view not found: catalog.schema.table`

**Solution**:
1. Verify catalog exists: `SHOW CATALOGS`
2. Verify schema exists: `SHOW SCHEMAS IN catalog`
3. Verify table exists: `SHOW TABLES IN catalog.schema`
4. Check permissions (must have SELECT privilege)

### Issue: Permission Denied

**Error**: `Permission denied on table`

**Solution**:
- Contact workspace admin for GRANT SELECT permissions
- Verify you're using correct identity (check `SELECT current_user()`)

### Issue: Column Not Found After Inspection

**Error**: `Column 'myColumn' does not exist`

**Solution**:
- Column names are case-sensitive in some contexts
- Verify exact spelling with DESCRIBE TABLE
- Use backticks for special characters: `` `column-name` ``

---

## Quick Reference

| Task | SQL Command | PySpark Method |
|------|-------------|----------------|
| List catalogs | `SHOW CATALOGS` | `spark.sql("SHOW CATALOGS")` |
| List schemas | `SHOW SCHEMAS IN catalog` | `spark.sql("SHOW SCHEMAS IN catalog")` |
| List tables | `SHOW TABLES IN catalog.schema` | `spark.sql("SHOW TABLES IN catalog.schema")` |
| Get schema | `DESCRIBE TABLE catalog.schema.table` | `df.printSchema()` |
| Get extended metadata | `DESCRIBE TABLE EXTENDED ...` | `spark.sql("DESCRIBE TABLE EXTENDED ...")` |
| Sample data | `SELECT * FROM ... LIMIT 10` | `df.limit(10).show()` |
| Column info | `information_schema.columns` | `spark.sql("SELECT ... FROM information_schema.columns")` |

---

## Related Skills

- **databricks-etl-patterns**: BaseETL usage, incremental loads, error handling
- **medallion-architecture**: Understanding bronze/silver/gold layer responsibilities
- **pyspark-best-practices**: Efficient PySpark code patterns

---

**Skill Status**: Complete ✅
**Line Count**: < 500 ✅
**Use Before**: Writing any ETL transformation or query
