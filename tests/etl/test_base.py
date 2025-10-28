"""Tests for the base ETL class."""

from databricks.connect import DatabricksSession

from src.etl.base import BaseETL


class ConcreteETL(BaseETL):
    """Concrete implementation of BaseETL for testing."""

    def extract(self):
        return self.spark.createDataFrame([(1, "test")], ["id", "value"])

    def transform(self, df):
        return df

    def load(self, df, target_path):
        pass


def test_base_etl_initialization(spark_session: DatabricksSession):
    """Test that BaseETL initializes with a DatabricksSession."""
    etl = ConcreteETL("test_path", spark=spark_session)
    assert etl.source_path == "test_path"
    assert etl.spark is not None


def test_base_etl_run_executes_pipeline(spark_session: DatabricksSession):
    """Test that run() executes the full ETL pipeline."""
    etl = ConcreteETL("test_path", spark=spark_session)

    # This should not raise any errors
    etl.run("target_path")
