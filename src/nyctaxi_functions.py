from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame, SparkSession

def get_spark() -> SparkSession:
  spark = DatabricksSession.builder.getOrCreate()
  return spark

def get_nyctaxi_trips() -> DataFrame:
  spark = get_spark()
  df = spark.read.table("samples.nyctaxi.trips")
  return df