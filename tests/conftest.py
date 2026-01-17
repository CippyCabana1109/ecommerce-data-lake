"""
Pytest configuration and fixtures for ecommerce data lake tests.
"""

import pytest
import os
import sys
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    """Create a Spark session for testing"""
    spark = (SparkSession.builder
        .appName("EcommerceDataLakeTest")
        .master("local[2]")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate())
    
    yield spark
    spark.stop()


@pytest.fixture(autouse=True)
def setup_test_environment():
    """Setup test environment before each test"""
    # Add src directory to Python path
    src_path = os.path.join(os.path.dirname(__file__), '..', 'src')
    if src_path not in sys.path:
        sys.path.insert(0, src_path)
    
    yield
    
    # Cleanup after test
    pass
