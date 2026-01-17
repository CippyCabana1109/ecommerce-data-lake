"""
PySpark script to create curated views from refined data.
Aggregates sales by product, county, and category with various metrics.
Includes top-selling items per county analysis and comprehensive lineage logging.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg, max as spark_max, min as spark_min, rank, desc
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, LongType
import logging
import json
from datetime import datetime
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('curated_processing.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class CuratedDataProcessor:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.spark = None
        self.lineage_info = {
            'job_start_time': datetime.now().isoformat(),
            'input_paths': [],
            'output_paths': [],
            'aggregations': [],
            'metrics': {},
            'row_counts': {}
        }
        
    def create_spark_session(self):
        """Create SparkSession with Delta Lake support"""
        try:
            self.spark = (SparkSession.builder
                .appName("Ecommerce-Curated-Data-Processing")
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
            
    def read_refined_data(self):
        """Read refined data from Delta Lake"""
        try:
            refined_path = f"s3a://{self.bucket_name}/refined/"
            self.lineage_info['input_paths'] = [refined_path]
            
            logger.info(f"Reading refined data from {refined_path}")
            
            # Read refined Delta table
            refined_df = (self.spark.read
                .format("delta")
                .load(refined_path))
            
            # Log initial row count
            initial_count = refined_df.count()
            self.lineage_info['row_counts']['refined_input'] = initial_count
            
            logger.info(f"Read {initial_count} records from refined layer")
            
            # Show schema for verification
            logger.info("Refined data schema:")
            refined_df.printSchema()
            
            return refined_df
            
        except Exception as e:
            logger.error(f"Error reading refined data: {str(e)}")
            raise
            
    def create_sales_by_product_aggregation(self, df):
        """Aggregate sales data by product"""
        try:
            logger.info("Creating sales aggregation by product")
            
            # Calculate revenue (price * quantity)
            df_with_revenue = df.withColumn('revenue', col('price') * col('quantity'))
            
            # Aggregate by product
            sales_by_product = (df_with_revenue
                .groupBy('product_id', 'product_name', 'category')
                .agg(
                    spark_sum('revenue').alias('total_revenue'),
                    spark_sum('quantity').alias('total_quantity'),
                    avg('price').alias('avg_price'),
                    count('*').alias('transaction_count'),
                    spark_max('price').alias('max_price'),
                    spark_min('price').alias('min_price')
                )
                .orderBy(desc('total_revenue'))
            )
            
            # Log aggregation details
            self.lineage_info['aggregations'].append("Sales by product: revenue, quantity, avg_price, transaction_count")
            self.lineage_info['row_counts']['sales_by_product'] = sales_by_product.count()
            
            # Calculate metrics
            product_metrics = {
                'total_products': sales_by_product.count(),
                'total_revenue_all_products': sales_by_product.agg(spark_sum('total_revenue')).collect()[0][0],
                'avg_revenue_per_product': sales_by_product.agg(avg('total_revenue')).collect()[0][0]
            }
            self.lineage_info['metrics']['sales_by_product'] = product_metrics
            
            logger.info(f"Created sales by product aggregation: {self.lineage_info['row_counts']['sales_by_product']} products")
            
            return sales_by_product
            
        except Exception as e:
            logger.error(f"Error creating sales by product aggregation: {str(e)}")
            raise
            
    def create_sales_by_county_aggregation(self, df):
        """Aggregate sales data by county"""
        try:
            logger.info("Creating sales aggregation by county")
            
            # Calculate revenue
            df_with_revenue = df.withColumn('revenue', col('price') * col('quantity'))
            
            # Aggregate by county
            sales_by_county = (df_with_revenue
                .groupBy('county')
                .agg(
                    spark_sum('revenue').alias('total_revenue'),
                    spark_sum('quantity').alias('total_quantity'),
                    count('*').alias('transaction_count'),
                    count('product_id').alias('unique_products'),
                    avg('revenue').alias('avg_transaction_value')
                )
                .orderBy(desc('total_revenue'))
            )
            
            # Log aggregation details
            self.lineage_info['aggregations'].append("Sales by county: revenue, quantity, transaction_count, unique_products")
            self.lineage_info['row_counts']['sales_by_county'] = sales_by_county.count()
            
            # Calculate metrics
            county_metrics = {
                'total_counties': sales_by_county.count(),
                'total_revenue_all_counties': sales_by_county.agg(spark_sum('total_revenue')).collect()[0][0],
                'avg_revenue_per_county': sales_by_county.agg(avg('total_revenue')).collect()[0][0]
            }
            self.lineage_info['metrics']['sales_by_county'] = county_metrics
            
            logger.info(f"Created sales by county aggregation: {self.lineage_info['row_counts']['sales_by_county']} counties")
            
            return sales_by_county
            
        except Exception as e:
            logger.error(f"Error creating sales by county aggregation: {str(e)}")
            raise
            
    def create_sales_by_category_aggregation(self, df):
        """Aggregate sales data by category"""
        try:
            logger.info("Creating sales aggregation by category")
            
            # Calculate revenue
            df_with_revenue = df.withColumn('revenue', col('price') * col('quantity'))
            
            # Aggregate by category
            sales_by_category = (df_with_revenue
                .groupBy('category')
                .agg(
                    spark_sum('revenue').alias('total_revenue'),
                    spark_sum('quantity').alias('total_quantity'),
                    count('*').alias('transaction_count'),
                    count('product_id').alias('unique_products'),
                    avg('price').alias('avg_price')
                )
                .orderBy(desc('total_revenue'))
            )
            
            # Log aggregation details
            self.lineage_info['aggregations'].append("Sales by category: revenue, quantity, unique_products, avg_price")
            self.lineage_info['row_counts']['sales_by_category'] = sales_by_category.count()
            
            # Calculate metrics
            category_metrics = {
                'total_categories': sales_by_category.count(),
                'total_revenue_all_categories': sales_by_category.agg(spark_sum('total_revenue')).collect()[0][0],
                'avg_revenue_per_category': sales_by_category.agg(avg('total_revenue')).collect()[0][0]
            }
            self.lineage_info['metrics']['sales_by_category'] = category_metrics
            
            logger.info(f"Created sales by category aggregation: {self.lineage_info['row_counts']['sales_by_category']} categories")
            
            return sales_by_category
            
        except Exception as e:
            logger.error(f"Error creating sales by category aggregation: {str(e)}")
            raise
            
    def create_top_selling_items_per_county(self, df):
        """Create top-selling items per Kenyan county analysis"""
        try:
            logger.info("Creating top-selling items per county analysis")
            
            # Calculate revenue
            df_with_revenue = df.withColumn('revenue', col('price') * col('quantity'))
            
            # Aggregate by county and product
            county_product_sales = (df_with_revenue
                .groupBy('county', 'product_id', 'product_name', 'category')
                .agg(
                    spark_sum('revenue').alias('total_revenue'),
                    spark_sum('quantity').alias('total_quantity'),
                    count('*').alias('transaction_count')
                )
            )
            
            # Define window specification for ranking within each county
            window_spec = Window.partitionBy('county').orderBy(desc('total_revenue'))
            
            # Add rank column
            top_items_per_county = (county_product_sales
                .withColumn('revenue_rank', rank().over(window_spec))
                .filter(col('revenue_rank') <= 10)  # Top 10 items per county
                .orderBy('county', 'revenue_rank')
            )
            
            # Log aggregation details
            self.lineage_info['aggregations'].append("Top 10 selling items per county by revenue")
            self.lineage_info['row_counts']['top_items_per_county'] = top_items_per_county.count()
            
            # Calculate metrics
            top_items_metrics = {
                'total_county_item_combinations': top_items_per_county.count(),
                'counties_analyzed': top_items_per_county.select('county').distinct().count(),
                'avg_items_per_county': top_items_per_county.count() / top_items_per_county.select('county').distinct().count()
            }
            self.lineage_info['metrics']['top_items_per_county'] = top_items_metrics
            
            logger.info(f"Created top items per county analysis: {self.lineage_info['row_counts']['top_items_per_county']} records")
            
            return top_items_per_county
            
        except Exception as e:
            logger.error(f"Error creating top items per county analysis: {str(e)}")
            raise
            
    def create_comprehensive_curated_view(self, df):
        """Create a comprehensive curated view with multiple aggregations"""
        try:
            logger.info("Creating comprehensive curated view")
            
            # Calculate revenue
            df_with_revenue = df.withColumn('revenue', col('price') * col('quantity'))
            
            # Multi-dimensional aggregation
            comprehensive_view = (df_with_revenue
                .groupBy('county', 'category', 'product_id', 'product_name')
                .agg(
                    spark_sum('revenue').alias('total_revenue'),
                    spark_sum('quantity').alias('total_quantity'),
                    count('*').alias('transaction_count'),
                    avg('price').alias('avg_price'),
                    spark_max('price').alias('max_price'),
                    spark_min('price').alias('min_price')
                )
                .orderBy('county', 'category', desc('total_revenue'))
            )
            
            # Log aggregation details
            self.lineage_info['aggregations'].append("Comprehensive curated view: county, category, product dimensions")
            self.lineage_info['row_counts']['comprehensive_view'] = comprehensive_view.count()
            
            logger.info(f"Created comprehensive curated view: {self.lineage_info['row_counts']['comprehensive_view']} records")
            
            return comprehensive_view
            
        except Exception as e:
            logger.error(f"Error creating comprehensive curated view: {str(e)}")
            raise
            
    def write_curated_data(self, sales_by_product, sales_by_county, sales_by_category, 
                          top_items_per_county, comprehensive_view):
        """Write all curated datasets to curated layer"""
        try:
            curated_path = f"s3a://{self.bucket_name}/curated/"
            self.lineage_info['output_paths'] = [curated_path]
            
            logger.info(f"Writing curated data to {curated_path}")
            
            # Write sales by product
            (sales_by_product.write
                .format("delta")
                .mode("overwrite")
                .partitionBy("category")
                .save(f"{curated_path}sales_by_product/"))
            
            # Write sales by county (partitioned by county)
            (sales_by_county.write
                .format("delta")
                .mode("overwrite")
                .partitionBy("county")
                .save(f"{curated_path}sales_by_county/"))
            
            # Write sales by category
            (sales_by_category.write
                .format("delta")
                .mode("overwrite")
                .save(f"{curated_path}sales_by_category/"))
            
            # Write top items per county (partitioned by county)
            (top_items_per_county.write
                .format("delta")
                .mode("overwrite")
                .partitionBy("county")
                .save(f"{curated_path}top_items_per_county/"))
            
            # Write comprehensive view (partitioned by county)
            (comprehensive_view.write
                .format("delta")
                .mode("overwrite")
                .partitionBy("county")
                .save(f"{curated_path}comprehensive_view/"))
            
            # Optimize Delta tables
            for table_name in ["sales_by_product", "sales_by_county", "sales_by_category", 
                              "top_items_per_county", "comprehensive_view"]:
                table_path = f"{curated_path}{table_name}/"
                try:
                    self.spark.sql(f"OPTIMIZE delta.`{table_path}`")
                    logger.info(f"Optimized {table_name} Delta table")
                except Exception as e:
                    logger.warning(f"Could not optimize {table_name}: {str(e)}")
            
            logger.info("All curated datasets successfully written to curated layer")
            
        except Exception as e:
            logger.error(f"Error writing curated data: {str(e)}")
            raise
            
    def log_lineage(self):
        """Log lineage information"""
        try:
            self.lineage_info['job_end_time'] = datetime.now().isoformat()
            
            # Write lineage info to log file
            with open('curated_lineage_log.json', 'w') as f:
                json.dump(self.lineage_info, f, indent=2, default=str)
            
            logger.info("Lineage information logged successfully")
            logger.info(f"Curated processing summary:")
            logger.info(f"  - Input paths: {', '.join(self.lineage_info['input_paths'])}")
            logger.info(f"  - Output paths: {', '.join(self.lineage_info['output_paths'])}")
            logger.info(f"  - Start time: {self.lineage_info['job_start_time']}")
            logger.info(f"  - End time: {self.lineage_info['job_end_time']}")
            logger.info(f"  - Aggregations created: {len(self.lineage_info['aggregations'])}")
            
            # Log metrics summary
            logger.info("Aggregation metrics:")
            for agg_name, metrics in self.lineage_info['metrics'].items():
                logger.info(f"  {agg_name}:")
                for metric_name, metric_value in metrics.items():
                    logger.info(f"    - {metric_name}: {metric_value}")
            
            # Log row counts
            logger.info("Row counts:")
            for table_name, count in self.lineage_info['row_counts'].items():
                logger.info(f"  - {table_name}: {count}")
            
        except Exception as e:
            logger.error(f"Error logging lineage: {str(e)}")
            raise
            
    def run_pipeline(self):
        """Run the complete curated data processing pipeline"""
        try:
            logger.info("Starting curated data processing pipeline")
            
            # Create Spark session
            self.create_spark_session()
            
            # Read refined data
            refined_df = self.read_refined_data()
            
            # Create various aggregations
            sales_by_product = self.create_sales_by_product_aggregation(refined_df)
            sales_by_county = self.create_sales_by_county_aggregation(refined_df)
            sales_by_category = self.create_sales_by_category_aggregation(refined_df)
            top_items_per_county = self.create_top_selling_items_per_county(refined_df)
            comprehensive_view = self.create_comprehensive_curated_view(refined_df)
            
            # Write curated data
            self.write_curated_data(sales_by_product, sales_by_county, sales_by_category,
                                  top_items_per_county, comprehensive_view)
            
            # Log lineage
            self.log_lineage()
            
            logger.info("Curated data processing pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise
            
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("SparkSession stopped")

def main():
    """Main function to run the curated data processor"""
    # Configuration - replace with your actual bucket name
    BUCKET_NAME = "[your-bucket]"
    
    try:
        processor = CuratedDataProcessor(BUCKET_NAME)
        processor.run_pipeline()
        
    except Exception as e:
        logger.error(f"Application failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
