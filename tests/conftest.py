import pytest
from unittest.mock import MagicMock
from pyspark.sql import SparkSession
from fastapi.testclient import TestClient
from api import app

@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    return SparkSession.builder \
        .master("local[1]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
        .getOrCreate()

@pytest.fixture
def client(monkeypatch):
    """API client with a mocked database connection."""
    mock_conn = MagicMock()
    # Mocking the connection function in api.py
    monkeypatch.setattr("api.get_db_conn", lambda: mock_conn)
    
    with TestClient(app) as c:
        c.mock_db = mock_conn # Attach to client for easy access in tests
        yield c