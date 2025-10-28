"""Main script to run the NYC taxi trips daily aggregation ETL job."""

from src.etl.gold.daily_trips_aggregation import DailyTripsAggregationETL


def main():
    """Run the daily trips aggregation ETL pipeline."""
    print("Starting NYC taxi trips daily aggregation ETL job...")

    # Source and target table configuration
    source_table = "samples.nyctaxi.trips"
    target_table = "main.default.gold_trips"

    # Initialize and run the ETL job
    etl = DailyTripsAggregationETL(source_table)

    print(f"Extracting data from {source_table}...")
    print(f"Transforming and aggregating trips by day...")
    print(f"Loading results to {target_table}...")

    etl.run(target_table)

    print(f"✓ ETL job completed successfully!")
    print(f"Results written to {target_table}")

    # Display sample results
    print("\nSample results:")
    result_df = etl.spark.table(target_table)
    result_df.orderBy("pickup_date", ascending=False).show(10)


if __name__ == "__main__":
    main()