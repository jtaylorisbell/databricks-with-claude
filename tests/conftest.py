"""Pytest configuration and shared fixtures."""

import pytest
from databricks.connect import DatabricksSession


@pytest.fixture(scope="session")
def spark_session():
    """Create a Databricks session for testing.

    This fixture creates a Databricks Connect session that can be used across
    all tests. It's session-scoped to avoid recreating the session for
    each test, which improves performance.

    Yields:
        DatabricksSession: A configured Databricks session for testing
    """
    spark = DatabricksSession.builder.getOrCreate()

    yield spark

    # Cleanup
    spark.stop()
