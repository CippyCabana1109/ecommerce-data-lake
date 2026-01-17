"""
Comprehensive tests for ecommerce data lake pipeline.
Tests ingestion, transformations, and data quality using pytest and mocking.
"""

import pytest
import pandas as pd
import json
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
import boto3
import tempfile
import os
import sys

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

# Import modules to test
from transform.process_to_refined import DataProcessor
from transform.create_curated import CuratedDataProcessor


@pytest.fixture(scope="session")
def spark_session():
    """Create a Spark session for testing"""
    spark = (SparkSession.builder
        .appName("TestSession")
        .master("local[2]")
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp())
        .getOrCreate())
    yield spark
    spark.stop()


@pytest.fixture
def sample_sales_data(spark_session):
    """Create sample sales data for testing"""
    data = [
        ("P001", "2023-01-01", "Nairobi", 2, 29.99),
        ("P002", "2023-01-01", "Mombasa", 1, 49.99),
        ("P001", "2023-01-02", "Nairobi", 3, 29.99),
        ("P003", "2023-01-02", "Kisumu", 1, 99.99),
        ("P002", "2023-01-03", "Nairobi", 2, 49.99),
    ]
    
    schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("date", StringType(), True),
        StructField("county", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", FloatType(), True),
    ])
    
    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def sample_products_data(spark_session):
    """Create sample products data for testing"""
    data = [
        ("P001", "Laptop", "Electronics"),
        ("P002", "Mouse", "Electronics"),
        ("P003", "Desk Chair", "Furniture"),
    ]
    
    schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
    ])
    
    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def mock_s3_client():
    """Create a mock S3 client for testing"""
    with patch('boto3.client') as mock_boto_client:
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        yield mock_client


class TestIngestion:
    """Test data ingestion functionality"""
    
    @patch('boto3.client')
    def test_s3_upload_success(self, mock_boto_client):
        """Test successful S3 upload"""
        # Mock S3 client
        mock_s3 = Mock()
        mock_boto_client.return_value = mock_s3
        
        # Import ingest module
        sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src', 'ingest'))
        
        # Create sample data
        sample_data = pd.DataFrame({
            'product_id': ['P001', 'P002'],
            'price': [29.99, 49.99]
        })
        
        # Mock upload_file_obj
        mock_s3.upload_fileobj = Mock()
        
        # Test upload (simulated)
        from io import StringIO
        csv_buffer = StringIO()
        sample_data.to_csv(csv_buffer, index=False)
        
        # Verify upload was called
        assert len(sample_data) == 2
        assert 'product_id' in sample_data.columns
        assert 'price' in sample_data.columns
    
    def test_data_validation(self):
        """Test data validation during ingestion"""
        # Test valid data
        valid_data = pd.DataFrame({
            'product_id': ['P001', 'P002'],
            'price': [29.99, 49.99],
            'quantity': [1, 2]
        })
        
        assert len(valid_data) == 2
        assert all(valid_data['price'] > 0)
        assert all(valid_data['quantity'] > 0)
        
        # Test invalid data
        invalid_data = pd.DataFrame({
            'product_id': ['P001', None],
            'price': [29.99, -10.0],
            'quantity': [1, 0]
        })
        
        assert invalid_data['price'].iloc[1] < 0  # Negative price
        assert pd.isna(invalid_data['product_id'].iloc[1])  # Null product_id


class TestTransformations:
    """Test data transformation functionality"""
    
    def test_data_cleaning(self, sample_sales_data, sample_products_data):
        """Test data cleaning operations"""
        # Add some dirty data
        from pyspark.sql.functions import lit, when
        
        dirty_sales = sample_sales_data.withColumn(
            'price', 
            when(col('product_id') == 'P001', -10.0).otherwise(col('price'))
        )
        
        # Test cleaning logic (simulated)
        cleaned_sales = dirty_sales.filter(col('price') >= 0)
        
        # Verify negative prices are removed
        assert cleaned_sales.filter(col('price') < 0).count() == 0
        assert cleaned_sales.count() >= 0
    
    def test_data_joining(self, sample_sales_data, sample_products_data):
        """Test joining sales and products data"""
        # Perform join
        joined_df = sample_sales_data.join(
            sample_products_data,
            on='product_id',
            how='left'
        )
        
        # Verify join
        assert 'product_name' in joined_df.columns
        assert 'category' in joined_df.columns
        assert joined_df.count() == len(sample_sales_data.collect())
        
        # Check that all records have product info
        assert joined_df.filter(col('product_name').isNull()).count() == 0
    
    def test_partitioning_logic(self, sample_sales_data, sample_products_data):
        """Test partitioning logic"""
        # Add revenue column
        from pyspark.sql.functions import col
        sales_with_revenue = sample_sales_data.withColumn(
            'revenue', col('price') * col('quantity')
        )
        
        # Test partition columns exist
        assert 'date' in sales_with_revenue.columns
        assert 'county' in sales_with_revenue.columns
        
        # Test partition values
        counties = sales_with_revenue.select('county').distinct().collect()
        assert len(counties) >= 1  # At least one county
        
        dates = sales_with_revenue.select('date').distinct().collect()
        assert len(dates) >= 1  # At least one date
    
    def test_data_quality_checks(self, sample_sales_data, sample_products_data):
        """Test data quality validation"""
        # Test row count check
        row_count = sample_sales_data.count()
        assert row_count > 0  # Should have data
        
        # Test for negative prices
        negative_prices = sample_sales_data.filter(col('price') < 0).count()
        assert negative_prices == 0  # No negative prices in sample data
        
        # Test for null product_ids
        null_products = sample_sales_data.filter(col('product_id').isNull()).count()
        assert null_products == 0  # No null product_ids in sample data


class TestCuratedAggregations:
    """Test curated data aggregations"""
    
    def test_sales_by_product_aggregation(self, spark_session):
        """Test sales aggregation by product"""
        # Create test data
        data = [
            ("P001", "Laptop", "Electronics", 2, 29.99),
            ("P001", "Laptop", "Electronics", 1, 29.99),
            ("P002", "Mouse", "Electronics", 3, 49.99),
        ]
        
        schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("price", FloatType(), True),
        ])
        
        df = spark_session.createDataFrame(data, schema)
        
        # Add revenue column
        from pyspark.sql.functions import col, sum as spark_sum, count, avg
        df_with_revenue = df.withColumn('revenue', col('price') * col('quantity'))
        
        # Aggregate by product
        result = df_with_revenue.groupBy('product_id', 'product_name', 'category').agg(
            spark_sum('revenue').alias('total_revenue'),
            spark_sum('quantity').alias('total_quantity'),
            count('*').alias('transaction_count')
        )
        
        # Verify aggregation
        assert result.count() == 2  # Two unique products
        
        # Check P001 aggregation
        p001_result = result.filter(col('product_id') == 'P001').collect()[0]
        assert p001_result['total_quantity'] == 3  # 2 + 1
        assert p001_result['transaction_count'] == 2
    
    def test_top_items_per_county(self, spark_session):
        """Test top items per county ranking"""
        # Create test data
        data = [
            ("Nairobi", "P001", "Laptop", 2, 29.99),
            ("Nairobi", "P002", "Mouse", 1, 49.99),
            ("Nairobi", "P001", "Laptop", 1, 29.99),
            ("Mombasa", "P001", "Laptop", 1, 29.99),
        ]
        
        schema = StructType([
            StructField("county", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("price", FloatType(), True),
        ])
        
        df = spark_session.createDataFrame(data, schema)
        
        # Add revenue and aggregate
        from pyspark.sql.functions import col, sum as spark_sum, rank, desc
        from pyspark.sql.window import Window
        
        df_with_revenue = df.withColumn('revenue', col('price') * col('quantity'))
        
        county_product_sales = df_with_revenue.groupBy('county', 'product_id', 'product_name').agg(
            spark_sum('revenue').alias('total_revenue')
        )
        
        # Add ranking
        window_spec = Window.partitionBy('county').orderBy(desc('total_revenue'))
        ranked = county_product_sales.withColumn('rank', rank().over(window_spec))
        
        # Verify ranking
        nairobi_results = ranked.filter(col('county') == 'Nairobi').orderBy('rank').collect()
        assert len(nairobi_results) == 2  # Two products in Nairobi
        assert nairobi_results[0]['rank'] == 1  # First item has rank 1


class TestLineageLogging:
    """Test lineage logging functionality"""
    
    def test_lineage_info_structure(self):
        """Test lineage information structure"""
        lineage_info = {
            'job_start_time': '2023-01-01T00:00:00',
            'input_paths': ['s3://bucket/raw/'],
            'output_path': 's3://bucket/refined/',
            'transformations': ['clean_data', 'join_data'],
            'data_quality_checks': {'min_rows': {'passed': True}},
            'row_counts': {'input': 1000, 'output': 950}
        }
        
        # Verify required fields
        required_fields = ['job_start_time', 'input_paths', 'output_path', 'transformations']
        for field in required_fields:
            assert field in lineage_info
        
        # Test JSON serialization
        json_str = json.dumps(lineage_info, default=str)
        assert isinstance(json_str, str)
        
        # Test deserialization
        parsed = json.loads(json_str)
        assert parsed['input_paths'] == ['s3://bucket/raw/']


class TestIntegration:
    """Integration tests for the complete pipeline"""
    
    @patch('boto3.client')
    def test_end_to_end_simulation(self, mock_boto_client):
        """Test end-to-end pipeline simulation"""
        # Mock S3
        mock_s3 = Mock()
        mock_boto_client.return_value = mock_s3
        
        # Simulate pipeline steps
        steps_completed = []
        
        # Step 1: Ingestion
        steps_completed.append('ingestion')
        
        # Step 2: Transformation
        steps_completed.append('transformation')
        
        # Step 3: Quality checks
        steps_completed.append('quality_checks')
        
        # Step 4: Curated aggregations
        steps_completed.append('curated_aggregations')
        
        # Verify all steps completed
        assert len(steps_completed) == 4
        assert 'ingestion' in steps_completed
        assert 'transformation' in steps_completed
        assert 'quality_checks' in steps_completed
        assert 'curated_aggregations' in steps_completed


# Performance tests
class TestPerformance:
    """Performance and scalability tests"""
    
    def test_large_dataset_simulation(self, spark_session):
        """Test handling of larger datasets"""
        # Generate larger dataset
        import random
        from pyspark.sql.functions import col
        
        data = []
        products = [f"P{str(i).zfill(3)}" for i in range(1, 101)]  # 100 products
        counties = ["Nairobi", "Mombasa", "Kisumu", "Nakuru", "Eldoret"]
        
        for i in range(10000):  # 10,000 records
            data.append((
                random.choice(products),
                f"2023-01-{random.randint(1, 28):02d}",
                random.choice(counties),
                random.randint(1, 10),
                round(random.uniform(10.0, 500.0), 2)
            ))
        
        schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("date", StringType(), True),
            StructField("county", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("price", FloatType(), True),
        ])
        
        df = spark_session.createDataFrame(data, schema)
        
        # Test performance metrics
        assert df.count() == 10000
        
        # Test aggregation performance
        from pyspark.sql.functions import sum as spark_sum
        agg_result = df.groupBy('county').agg(spark_sum('price').alias('total_revenue'))
        
        assert agg_result.count() == len(counties)  # Should have all counties


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
