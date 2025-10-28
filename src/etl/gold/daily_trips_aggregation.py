"""Gold layer ETL for daily NYC taxi trip aggregations."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.etl.base import BaseETL


class DailyTripsAggregationETL(BaseETL):
    """ETL job to aggregate NYC taxi trips by day.

    This gold layer job reads from the NYC taxi trips table and creates
    daily aggregations including average fare amount and trip distance.

    Example:
        >>> etl = DailyTripsAggregationETL("samples.nyctaxi.trips")
        >>> etl.run("main.default.gold_trips")
    """

    def extract(self) -> DataFrame:
        """Extract data from the NYC taxi trips table.

        Returns:
            DataFrame containing trip data with pickup/dropoff times,
            trip distance, and fare amount
        """
        return self.spark.read.table(self.source_path)

    def transform(self, df: DataFrame) -> DataFrame:
        """Transform trips data into daily aggregations.

        Calculates the following metrics by pickup date:
        - Average fare amount
        - Average trip distance
        - Total number of trips

        Args:
            df: Input DataFrame with trip-level data

        Returns:
            DataFrame with daily aggregations containing:
            - pickup_date: Date of trips
            - avg_fare_amount: Average fare for the day
            - avg_trip_distance: Average distance for the day
            - total_trips: Count of trips for the day
        """
        return (
            df.withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
            .groupBy("pickup_date")
            .agg(
                F.avg("fare_amount").alias("avg_fare_amount"),
                F.avg("trip_distance").alias("avg_trip_distance"),
                F.count("*").alias("total_trips"),
            )
            .orderBy("pickup_date")
        )

    def load(self, df: DataFrame, target_path: str) -> None:
        """Load aggregated data to the target table.

        Writes data in Delta format using overwrite mode to ensure
        the table contains the latest aggregations.

        Args:
            df: Transformed DataFrame with daily aggregations
            target_path: Destination table name (e.g., 'main.default.gold_trips')
        """
        df.write.format("delta").mode("overwrite").saveAsTable(target_path)
