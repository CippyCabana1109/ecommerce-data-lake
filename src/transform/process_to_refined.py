"""
PySpark script to process raw sales and products data to refined format.
Includes data cleaning, joining, partitioning, Delta Lake versioning, 
data quality checks, and lineage logging.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, when, isnan, isnull
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DateType
from pyspark.sql.utils import AnalysisException
import logging
import json
from datetime import datetime
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_processing.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.spark = None
        self.lineage_info = {
            'job_start_time': datetime.now().isoformat(),
            'input_paths': [],
            'output_path': '',
            'transformations': [],
            'data_quality_checks': {},
            'row_counts': {}
        }
        
    def create_spark_session(self):
        """Create SparkSession with Delta Lake support"""
        try:
            self.spark = (SparkSession.builder
                .appName("Ecommerce-Data-Lake-Processing")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .getOrCreate())
            
            logger.info("SparkSession created successfully with Delta Lake support")
            return self.spark
            
        except Exception as e:
            logger.error(f"Failed to create SparkSession: {str(e)}")
            raise
            
    def read_raw_data(self):
        """Read raw sales and products data from S3"""
        try:
            # Define input paths
            sales_path = f"s3a://{self.bucket_name}/raw/sales/"
            products_path = f"s3a://{self.bucket_name}/raw/products/"
            
            self.lineage_info['input_paths'] = [sales_path, products_path]
            
            # Read sales data
            logger.info(f"Reading sales data from {sales_path}")
            sales_df = (self.spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(f"{sales_path}*.csv"))
            
            # Read products data
            logger.info(f"Reading products data from {products_path}")
            products_df = (self.spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(f"{products_path}*.csv"))
            
            # Log initial row counts
            self.lineage_info['row_counts']['sales_raw'] = sales_df.count()
            self.lineage_info['row_counts']['products_raw'] = products_df.count()
            
            logger.info(f"Read {self.lineage_info['row_counts']['sales_raw']} sales records")
            logger.info(f"Read {self.lineage_info['row_counts']['products_raw']} product records")
            
            return sales_df, products_df
            
        except Exception as e:
            logger.error(f"Error reading raw data: {str(e)}")
            raise
            
    def clean_data(self, sales_df, products_df):
        """Clean and transform the raw data"""
        try:
            logger.info("Starting data cleaning and transformation")
            
            # Clean sales data
            sales_clean = (sales_df
                .fillna({'quantity': 0, 'price': 0.0})
                .withColumn('price', col('price').cast(FloatType()))
                .withColumn('quantity', col('quantity').cast(IntegerType()))
                .filter(col('price') >= 0)  # Remove negative prices
                .filter(col('quantity') >= 0)  # Remove negative quantities
            )
            
            # Clean products data
            products_clean = (products_df
                .fillna({'product_name': 'Unknown', 'category': 'Unknown'})
                .withColumn('product_id', col('product_id').cast(StringType()))
            )
            
            # Log transformation steps
            self.lineage_info['transformations'].append("Filled null values in sales and products")
            self.lineage_info['transformations'].append("Cast price to float, quantity to integer")
            self.lineage_info['transformations'].append("Filtered out negative prices and quantities")
            
            # Log row counts after cleaning
            self.lineage_info['row_counts']['sales_cleaned'] = sales_clean.count()
            self.lineage_info['row_counts']['products_cleaned'] = products_clean.count()
            
            logger.info(f"Sales data after cleaning: {self.lineage_info['row_counts']['sales_cleaned']} records")
            logger.info(f"Products data after cleaning: {self.lineage_info['row_counts']['products_cleaned']} records")
            
            return sales_clean, products_clean
            
        except Exception as e:
            logger.error(f"Error during data cleaning: {str(e)}")
            raise
            
    def join_data(self, sales_df, products_df):
        """Join sales and products data on product_id"""
        try:
            logger.info("Joining sales and products data")
            
            # Join datasets
            joined_df = sales_df.join(
                products_df,
                on='product_id',
                how='left'
            )
            
            # Log transformation
            self.lineage_info['transformations'].append("Left joined sales with products on product_id")
            
            # Log row count after join
            self.lineage_info['row_counts']['joined'] = joined_df.count()
            
            logger.info(f"Joined dataset: {self.lineage_info['row_counts']['joined']} records")
            
            return joined_df
            
        except Exception as e:
            logger.error(f"Error during data joining: {str(e)}")
            raise
            
    def perform_data_quality_checks(self, df):
        """Perform data quality checks on the processed data"""
        try:
            logger.info("Performing data quality checks")
            
            total_rows = df.count()
            
            # Check 1: Minimum row count
            min_rows_check = total_rows > 1000
            self.lineage_info['data_quality_checks']['min_rows_check'] = {
                'passed': min_rows_check,
                'actual_count': total_rows,
                'threshold': 1000
            }
            
            # Check 2: No negative prices
            negative_prices = df.filter(col('price') < 0).count()
            no_negative_prices_check = negative_prices == 0
            self.lineage_info['data_quality_checks']['no_negative_prices_check'] = {
                'passed': no_negative_prices_check,
                'negative_price_count': negative_prices
            }
            
            # Check 3: No null product_ids
            null_product_ids = df.filter(col('product_id').isNull() | (col('product_id') == '')).count()
            no_null_product_ids_check = null_product_ids == 0
            self.lineage_info['data_quality_checks']['no_null_product_ids_check'] = {
                'passed': no_null_product_ids_check,
                'null_product_id_count': null_product_ids
            }
            
            # Check 4: Valid date format (if date column exists)
            if 'date' in df.columns:
                invalid_dates = df.filter(col('date').isNull() | isnan(col('date'))).count()
                valid_dates_check = invalid_dates == 0
                self.lineage_info['data_quality_checks']['valid_dates_check'] = {
                    'passed': valid_dates_check,
                    'invalid_date_count': invalid_dates
                }
            
            # Overall quality check
            all_checks_passed = all(
                check['passed'] for check in self.lineage_info['data_quality_checks'].values()
            )
            
            logger.info(f"Data quality checks completed. Overall status: {'PASSED' if all_checks_passed else 'FAILED'}")
            
            for check_name, check_result in self.lineage_info['data_quality_checks'].items():
                status = 'PASSED' if check_result['passed'] else 'FAILED'
                logger.info(f"  {check_name}: {status}")
            
            return all_checks_passed
            
        except Exception as e:
            logger.error(f"Error during data quality checks: {str(e)}")
            raise
            
    def write_to_refined(self, df):
        """Write processed data to refined layer as Delta Lake"""
        try:
            output_path = f"s3a://{self.bucket_name}/refined/"
            self.lineage_info['output_path'] = output_path
            
            logger.info(f"Writing processed data to {output_path}")
            
            # Write as Delta Lake with partitioning
            (df.write
                .format("delta")
                .mode("overwrite")
                .partitionBy("date", "county")
                .option("overwriteSchema", "true")
                .save(output_path))
            
            # Optimize the Delta table
            self.spark.sql(f"OPTIMIZE delta.`{output_path}`")
            
            # Vacuum old versions (keep last 7 days)
            self.spark.sql(f"VACUUM delta.`{output_path}` RETAIN 168 HOURS")
            
            logger.info("Data successfully written to refined layer as Delta Lake")
            
        except Exception as e:
            logger.error(f"Error writing to refined layer: {str(e)}")
            raise
            
    def log_lineage(self):
        """Log lineage information"""
        try:
            self.lineage_info['job_end_time'] = datetime.now().isoformat()
            
            # Write lineage info to log file
            with open('lineage_log.json', 'w') as f:
                json.dump(self.lineage_info, f, indent=2, default=str)
            
            logger.info("Lineage information logged successfully")
            logger.info(f"Processing summary:")
            logger.info(f"  - Input paths: {', '.join(self.lineage_info['input_paths'])}")
            logger.info(f"  - Output path: {self.lineage_info['output_path']}")
            logger.info(f"  - Start time: {self.lineage_info['job_start_time']}")
            logger.info(f"  - End time: {self.lineage_info['job_end_time']}")
            logger.info(f"  - Transformations: {len(self.lineage_info['transformations'])}")
            logger.info(f"  - Data quality checks: {len(self.lineage_info['data_quality_checks'])}")
            
        except Exception as e:
            logger.error(f"Error logging lineage: {str(e)}")
            raise
            
    def run_pipeline(self):
        """Run the complete data processing pipeline"""
        try:
            logger.info("Starting data processing pipeline")
            
            # Create Spark session
            self.create_spark_session()
            
            # Read raw data
            sales_df, products_df = self.read_raw_data()
            
            # Clean data
            sales_clean, products_clean = self.clean_data(sales_df, products_df)
            
            # Join data
            joined_df = self.join_data(sales_clean, products_clean)
            
            # Perform data quality checks
            quality_checks_passed = self.perform_data_quality_checks(joined_df)
            
            if not quality_checks_passed:
                logger.warning("Some data quality checks failed. Proceeding with caution.")
            
            # Write to refined layer
            self.write_to_refined(joined_df)
            
            # Log lineage
            self.log_lineage()
            
            logger.info("Data processing pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise
            
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("SparkSession stopped")

def main():
    """Main function to run the data processor"""
    # Configuration - replace with your actual bucket name
    BUCKET_NAME = "[your-bucket]"
    
    try:
        processor = DataProcessor(BUCKET_NAME)
        processor.run_pipeline()
        
    except Exception as e:
        logger.error(f"Application failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
