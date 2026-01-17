"""
AWS Glue catalog setup script for ecommerce data lake.
Creates database and crawlers for raw, refined, and curated zones.
Uses boto3 to configure AWS Glue service.
"""

import boto3
import json
import time
import logging
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GlueCatalogSetup:
    def __init__(self, bucket_name, region='us-east-1'):
        self.bucket_name = bucket_name
        self.region = region
        self.glue_client = boto3.client('glue', region_name=region)
        self.iam_client = boto3.client('iam', region_name=region)
        
        # Configuration
        self.database_name = 'ecommerce_db'
        self.crawlers = {
            'raw_crawler': {
                'path': f's3://{bucket_name}/raw/',
                'description': 'Crawler for raw ecommerce data (CSV files)',
                'classification': 'csv'
            },
            'refined_crawler': {
                'path': f's3://{bucket_name}/refined/',
                'description': 'Crawler for refined ecommerce data (Delta Lake)',
                'classification': 'delta'
            },
            'curated_crawler': {
                'path': f's3://{bucket_name}/curated/',
                'description': 'Crawler for curated ecommerce data (Delta Lake)',
                'classification': 'delta'
            }
        }
        
    def create_glue_database(self):
        """Create Glue database"""
        try:
            logger.info(f"Creating Glue database: {self.database_name}")
            
            response = self.glue_client.create_database(
                DatabaseInput={
                    'Name': self.database_name,
                    'Description': 'Ecommerce data lake database containing raw, refined, and curated data',
                    'LocationUri': f's3://{self.bucket_name}/',
                    'Parameters': {
                        'created_by': 'setup_catalog.py',
                        'environment': 'production'
                    }
                }
            )
            
            logger.info(f"Successfully created database: {self.database_name}")
            return response
            
        except self.glue_client.exceptions.AlreadyExistsException:
            logger.info(f"Database {self.database_name} already exists")
            return None
        except ClientError as e:
            logger.error(f"Error creating database: {e}")
            raise
            
    def get_crawler_role_arn(self):
        """Get or create IAM role for Glue crawler"""
        role_name = 'AWSGlueServiceRole-EcommerceCrawler'
        
        try:
            # Try to get existing role
            response = self.iam_client.get_role(RoleName=role_name)
            logger.info(f"Using existing IAM role: {role_name}")
            return response['Role']['Arn']
            
        except self.iam_client.exceptions.NoSuchEntityException:
            # Create new role
            logger.info(f"Creating new IAM role: {role_name}")
            
            # Trust policy for Glue service
            trust_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "glue.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            }
            
            # Create role
            response = self.iam_client.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description='IAM role for Glue crawlers to access S3 data lake'
            )
            
            role_arn = response['Role']['Arn']
            
            # Attach necessary policies
            policies_to_attach = [
                'arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole',
                'arn:aws:iam::aws:policy/AmazonS3FullAccess'
            ]
            
            for policy_arn in policies_to_attach:
                self.iam_client.attach_role_policy(
                    RoleName=role_name,
                    PolicyArn=policy_arn
                )
                logger.info(f"Attached policy: {policy_arn}")
            
            logger.info(f"Successfully created IAM role: {role_name}")
            return role_arn
            
    def create_crawler(self, crawler_name, crawler_config, role_arn):
        """Create a Glue crawler"""
        try:
            logger.info(f"Creating crawler: {crawler_name}")
            
            # Extract table prefix from path
            table_prefix = crawler_name.replace('_crawler', '')
            
            crawler_params = {
                'Name': crawler_name,
                'Role': role_arn,
                'DatabaseName': self.database_name,
                'Description': crawler_config['description'],
                'Targets': {
                    'S3Targets': [
                        {
                            'Path': crawler_config['path'],
                            'Exclusions': ['**/_temporary/**', '**/_SUCCESS']
                        }
                    ]
                },
                'TablePrefix': table_prefix,
                'Schedule': {
                    'ScheduleExpression': 'cron(0 2 * * ? *)',  # Daily at 2 AM UTC
                    'State': 'SCHEDULED'
                },
                'SchemaChangePolicy': {
                    'UpdateBehavior': 'UPDATE_IN_DATABASE',
                    'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
                },
                'RecrawlPolicy': {
                    'RecrawlBehavior': 'CRAWL_NEW_FOLDERS_ONLY'
                },
                'LineageConfiguration': {
                    'CrawlerLineageSettings': 'ENABLE'
                }
            }
            
            # Add classification for Delta Lake
            if crawler_config.get('classification') == 'delta':
                crawler_params['Targets']['S3Targets'][0]['ConnectionName'] = None
                crawler_params['Configuration'] = json.dumps({
                    "version": "1.0",
                    "classifierDelta": {
                        "version": "1.0",
                        "type": "delta"
                    }
                })
            
            response = self.glue_client.create_crawler(**crawler_params)
            logger.info(f"Successfully created crawler: {crawler_name}")
            return response
            
        except self.glue_client.exceptions.AlreadyExistsException:
            logger.info(f"Crawler {crawler_name} already exists")
            return None
        except ClientError as e:
            logger.error(f"Error creating crawler {crawler_name}: {e}")
            raise
            
    def start_crawler(self, crawler_name):
        """Start a Glue crawler"""
        try:
            logger.info(f"Starting crawler: {crawler_name}")
            
            self.glue_client.start_crawler(Name=crawler_name)
            
            # Wait for crawler to complete
            logger.info(f"Waiting for crawler {crawler_name} to complete...")
            
            while True:
                response = self.glue_client.get_crawler(Name=crawler_name)
                state = response['Crawler']['State']
                
                if state == 'READY':
                    logger.info(f"Crawler {crawler_name} completed successfully")
                    return True
                elif state == 'FAILED':
                    error_message = response['Crawler'].get('LastCrawl', {}).get('ErrorMessage', 'Unknown error')
                    logger.error(f"Crawler {crawler_name} failed: {error_message}")
                    return False
                else:
                    logger.info(f"Crawler {crawler_name} status: {state}")
                    time.sleep(30)  # Wait 30 seconds before checking again
                    
        except ClientError as e:
            logger.error(f"Error starting crawler {crawler_name}: {e}")
            raise
            
    def list_database_tables(self):
        """List all tables in the database"""
        try:
            response = self.glue_client.get_tables(DatabaseName=self.database_name)
            tables = [table['Name'] for table in response['TableList']]
            logger.info(f"Tables in database {self.database_name}: {tables}")
            return tables
            
        except ClientError as e:
            logger.error(f"Error listing tables: {e}")
            raise
            
    def setup_complete_catalog(self):
        """Set up the complete Glue catalog with database and crawlers"""
        try:
            logger.info("Starting Glue catalog setup")
            
            # Create database
            self.create_glue_database()
            
            # Get IAM role
            role_arn = self.get_crawler_role_arn()
            
            # Create crawlers
            for crawler_name, crawler_config in self.crawlers.items():
                self.create_crawler(crawler_name, crawler_config, role_arn)
            
            logger.info("Glue catalog setup completed successfully")
            logger.info("Note: Run start_all_crawlers() to populate the catalog with table metadata")
            
        except Exception as e:
            logger.error(f"Catalog setup failed: {e}")
            raise
            
    def start_all_crawlers(self):
        """Start all crawlers and wait for completion"""
        try:
            logger.info("Starting all crawlers")
            
            for crawler_name in self.crawlers.keys():
                success = self.start_crawler(crawler_name)
                if not success:
                    logger.warning(f"Crawler {crawler_name} did not complete successfully")
            
            # List tables after crawlers complete
            self.list_database_tables()
            
            logger.info("All crawlers processing completed")
            
        except Exception as e:
            logger.error(f"Error running crawlers: {e}")
            raise

def main():
    """Main function to set up Glue catalog"""
    # Configuration - replace with your actual bucket name
    BUCKET_NAME = "[your-bucket]"
    REGION = "us-east-1"  # Change to your preferred AWS region
    
    try:
        # Initialize setup
        setup = GlueCatalogSetup(BUCKET_NAME, REGION)
        
        # Setup catalog (database and crawlers)
        setup.setup_complete_catalog()
        
        # Optional: Start crawlers immediately
        # Uncomment the line below to start crawlers after setup
        # setup.start_all_crawlers()
        
        logger.info("Glue catalog setup completed!")
        logger.info("Next steps:")
        logger.info("1. Verify crawlers in AWS Glue console")
        logger.info("2. Run crawlers manually or wait for scheduled execution")
        logger.info("3. Use Athena to query the created tables")
        logger.info("4. Check src/queries/insights.sql for sample Athena queries")
        
    except Exception as e:
        logger.error(f"Setup failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
