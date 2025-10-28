"""Base ETL class for all data pipeline jobs."""

from abc import ABC, abstractmethod
from typing import Optional

from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame


class BaseETL(ABC):
    """Abstract base class for ETL jobs in the medallion architecture.

    All ETL jobs inherit from this class and implement the extract, transform,
    and load methods.

    Attributes:
        spark: DatabricksSession instance for data processing
        source_path: Path to the source data
    """

    def __init__(self, source_path: str, spark: Optional[DatabricksSession] = None):
        """Initialize the ETL job.

        Args:
            source_path: Path or table name to read source data from
            spark: Optional DatabricksSession. If not provided, creates new session
        """
        self.source_path = source_path
        self.spark = spark or DatabricksSession.builder.getOrCreate()

        if self.spark is None:
            raise RuntimeError(
                "No active DatabricksSession found. Please provide a DatabricksSession "
                "or ensure Databricks Connect is configured."
            )

    @abstractmethod
    def extract(self) -> DataFrame:
        """Extract data from the source.

        Returns:
            DataFrame containing raw data from source
        """
        pass

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Transform the extracted data.

        Args:
            df: Input DataFrame from extract step

        Returns:
            Transformed DataFrame
        """
        pass

    @abstractmethod
    def load(self, df: DataFrame, target_path: str) -> None:
        """Load transformed data to target location.

        Args:
            df: Transformed DataFrame to write
            target_path: Destination path or table name
        """
        pass

    def run(self, target_path: str) -> None:
        """Execute the full ETL pipeline.

        Args:
            target_path: Destination path or table name for output
        """
        # Extract
        raw_df = self.extract()

        # Transform
        transformed_df = self.transform(raw_df)

        # Load
        self.load(transformed_df, target_path)
