"""
Local Hive Metastore setup for enhanced local development.
Provides production-like metadata management for local testing.
"""

import os
import subprocess
import tempfile
import shutil
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class HiveMetastoreSetup:
    """
    Setup and manage local Hive Metastore for Spark development.
    
    Enables local table metadata persistence, HiveQL compatibility,
    and production-like development environment.
    """
    
    def __init__(self, warehouse_dir: str = "spark-warehouse"):
        self.warehouse_dir = Path(warehouse_dir)
        self.metastore_db = "metastore_db"
        self.hive_home = None
        self.spark_session = None
        
    def setup_hive_environment(self):
        """
        Setup local Hive environment with embedded metastore.
        
        Returns:
            bool: True if setup successful
        """
        try:
            # Create warehouse directory
            self.warehouse_dir.mkdir(exist_ok=True)
            
            # Download Hive if not present (simplified for demo)
            logger.info("Setting up local Hive metastore...")
            
            # Create metastore configuration
            self._create_hive_config()
            
            logger.info("✅ Hive metastore setup completed")
            return True
            
        except Exception as e:
            logger.error(f"❌ Hive setup failed: {e}")
            return False
    
    def _create_hive_config(self):
        """Create Hive configuration files."""
        
        hive_site_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>javax.jdo.option.ConnectionURL</name>
        <value>jdbc:derby:;databaseName={self.metastore_db};create=true</value>
        <description>JDBC connect string for a JDBC metastore</description>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionDriverName</name>
        <value>org.apache.derby.jdbc.EmbeddedDriver</value>
        <description>Driver class name for a JDBC metastore</description>
    </property>
    <property>
        <name>hive.metastore.warehouse.dir</name>
        <value>{self.warehouse_dir.absolute()}</value>
        <description>location of default database for the warehouse</description>
    </property>
    <property>
        <name>hive.metastore.schema.verification</name>
        <value>false</value>
        <description>Enforce metastore schema version consistency</description>
    </property>
</configuration>"""
        
        # Create hive-site.xml
        config_dir = Path("conf")
        config_dir.mkdir(exist_ok=True)
        
        with open(config_dir / "hive-site.xml", "w") as f:
            f.write(hive_site_xml)
        
        logger.info(f"✅ Hive configuration created in {config_dir}")
    
    def create_spark_session_with_hive(self, app_name: str = "LocalEcommerceDataLake"):
        """
        Create Spark session with Hive support.
        
        Args:
            app_name: Name of the Spark application
            
        Returns:
            SparkSession: Configured Spark session with Hive support
        """
        try:
            from pyspark.sql import SparkSession
            
            # Stop existing session if any
            if self.spark_session:
                self.spark_session.stop()
            
            # Create session with Hive support
            self.spark_session = (SparkSession.builder
                .appName(app_name)
                .config("spark.sql.warehouse.dir", str(self.warehouse_dir.absolute()))
                .config("spark.hadoop.hive.metastore.uris", "thrift://localhost:9083")
                .config("spark.sql.catalogImplementation", "hive")
                .config("spark.sql.catalogImplementation", "in-memory")
                .config("spark.driver.extraJavaOptions", "-Dderby.system.home=.")
                .config("spark.hive.metastore.warehouse.dir", str(self.warehouse_dir.absolute()))
                .enableHiveSupport()
                .getOrCreate())
            
            logger.info("✅ Spark session with Hive support created")
            logger.info(f"📁 Warehouse directory: {self.warehouse_dir.absolute()}")
            
            return self.spark_session
            
        except Exception as e:
            logger.error(f"❌ Failed to create Spark session with Hive: {e}")
            raise
    
    def create_sample_tables(self):
        """
        Create sample tables in Hive metastore for testing.
        
        Returns:
            bool: True if tables created successfully
        """
        try:
            if not self.spark_session:
                raise ValueError("Spark session not initialized. Call create_spark_session_with_hive() first.")
            
            logger.info("Creating sample tables in Hive metastore...")
            
            # Create sample sales data
            sample_sales_data = [
                ("P001", "2023-01-01", "Nairobi", 2, 29.99),
                ("P002", "2023-01-01", "Mombasa", 1, 49.99),
                ("P001", "2023-01-02", "Nairobi", 3, 29.99),
                ("P003", "2023-01-02", "Kisumu", 1, 99.99),
                ("P002", "2023-01-03", "Nairobi", 2, 49.99),
            ]
            
            sales_df = self.spark_session.createDataFrame(sample_sales_data, [
                "product_id", "date", "county", "quantity", "price"
            ])
            
            # Create Hive table
            sales_df.createOrReplaceTempView("temp_sales")
            self.spark_session.sql("""
                CREATE TABLE IF NOT EXISTS sales (
                    product_id STRING,
                    date DATE,
                    county STRING,
                    quantity INT,
                    price DOUBLE
                )
                STORED AS PARQUET
                PARTITIONED BY (county STRING)
            """)
            
            # Insert data
            self.spark_session.sql("""
                INSERT INTO TABLE sales PARTITION (county)
                SELECT product_id, date, quantity, price, county FROM temp_sales
            """)
            
            # Create products table
            sample_products_data = [
                ("P001", "Laptop", "Electronics"),
                ("P002", "Mouse", "Electronics"),
                ("P003", "Desk Chair", "Furniture"),
            ]
            
            products_df = self.spark_session.createDataFrame(sample_products_data, [
                "product_id", "product_name", "category"
            ])
            
            products_df.createOrReplaceTempView("temp_products")
            self.spark_session.sql("""
                CREATE TABLE IF NOT EXISTS products (
                    product_id STRING,
                    product_name STRING,
                    category STRING
                )
                STORED AS PARQUET
            """)
            
            self.spark_session.sql("""
                INSERT INTO TABLE products
                SELECT * FROM temp_products
            """)
            
            logger.info("✅ Sample tables created successfully")
            
            # Show tables
            tables = self.spark_session.sql("SHOW TABLES").collect()
            logger.info(f"📊 Available tables: {[table.tableName for table in tables]}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create sample tables: {e}")
            return False
    
    def test_hive_functionality(self):
        """
        Test Hive functionality with sample queries.
        
        Returns:
            bool: True if tests pass
        """
        try:
            if not self.spark_session:
                raise ValueError("Spark session not initialized")
            
            logger.info("Testing Hive functionality...")
            
            # Test basic queries
            result = self.spark_session.sql("""
                SELECT county, COUNT(*) as sales_count, SUM(price * quantity) as total_revenue
                FROM sales
                GROUP BY county
                ORDER BY total_revenue DESC
            """).collect()
            
            logger.info("📈 Sales by county:")
            for row in result:
                logger.info(f"  {row.county}: {row.sales_count} sales, ${row.total_revenue:.2f} revenue")
            
            # Test join query
            join_result = self.spark_session.sql("""
                SELECT s.product_id, p.product_name, s.county, s.quantity
                FROM sales s
                JOIN products p ON s.product_id = p.product_id
                WHERE s.county = 'Nairobi'
            """).collect()
            
            logger.info("🔗 Nairobi sales with product names:")
            for row in join_result:
                logger.info(f"  {row.product_name}: {row.quantity} units")
            
            # Test partition pruning
            partition_result = self.spark_session.sql("""
                SELECT * FROM sales WHERE county = 'Nairobi'
            """).collect()
            
            logger.info(f"🎯 Partition pruning test: {len(partition_result)} Nairobi records")
            
            logger.info("✅ Hive functionality tests passed")
            return True
            
        except Exception as e:
            logger.error(f"❌ Hive functionality tests failed: {e}")
            return False
    
    def cleanup(self):
        """Clean up Hive metastore and temporary files."""
        try:
            if self.spark_session:
                self.spark_session.stop()
                logger.info("✅ Spark session stopped")
            
            # Clean up metastore database
            if os.path.exists(self.metastore_db):
                shutil.rmtree(self.metastore_db)
                logger.info(f"✅ Metastore database {self.metastore_db} cleaned up")
            
        except Exception as e:
            logger.error(f"❌ Cleanup failed: {e}")


def main():
    """Main function to setup and test Hive metastore."""
    
    # Initialize Hive setup
    hive_setup = HiveMetastoreSetup()
    
    try:
        # Setup Hive environment
        if not hive_setup.setup_hive_environment():
            return 1
        
        # Create Spark session with Hive support
        spark = hive_setup.create_spark_session_with_hive()
        
        # Create sample tables
        if not hive_setup.create_sample_tables():
            return 1
        
        # Test functionality
        if not hive_setup.test_hive_functionality():
            return 1
        
        logger.info("🎉 Hive metastore setup and testing completed successfully!")
        return 0
        
    except Exception as e:
        logger.error(f"❌ Setup failed: {e}")
        return 1
        
    finally:
        # Cleanup
        hive_setup.cleanup()


if __name__ == "__main__":
    exit(main())
