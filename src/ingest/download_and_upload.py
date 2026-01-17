"""
Download and Upload Kaggle Datasets to S3

This script downloads e-commerce datasets from Kaggle, converts XLSX to CSV,
adds county mapping for Kenyan context, and uploads to S3 raw layer.

Datasets:
- Kenya Supermarkets Data (XLSX) -> raw/products/
- E-commerce Sales Data (CSV) -> raw/sales/

Requirements:
- kaggle library (pip install kaggle)
- kaggle.json credentials file in ~/.kaggle/
- AWS credentials via environment variables or AWS CLI
"""

import os
import sys
import logging
import pandas as pd
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import random
from pathlib import Path
from typing import Optional

# Add parent directory to path for imports if needed
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ingest.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Kenyan counties list
KENYAN_COUNTIES = [
    'Nairobi', 'Mombasa', 'Kisumu', 'Nakuru', 'Eldoret', 'Thika', 'Malindi',
    'Kitale', 'Garissa', 'Kakamega', 'Nyeri', 'Meru', 'Machakos', 'Embu',
    'Nanyuki', 'Kilifi', 'Lamu', 'Turkana', 'Marsabit', 'Isiolo', 'Mandera',
    'Wajir', 'Samburu', 'West Pokot', 'Baringo', 'Laikipia', 'Nyandarua',
    'Muranga', 'Kiambu', 'Uasin Gishu', 'Elgeyo-Marakwet', 'Nandi',
    'Bomet', 'Kericho', 'Narok', 'Kajiado', 'Makueni', 'Kitui', 'Taita-Taveta',
    'Kwale', 'Kilifi', 'Tana River', 'Lamu', 'Homa Bay', 'Migori', 'Kisii',
    'Nyamira', 'Siaya', 'Busia', 'Bungoma', 'Trans Nzoia', 'Uasin Gishu'
]

# Kaggle dataset URLs (format: username/dataset-name)
KAGGLE_DATASETS = {
    'products': {
        'url': 'emmanuelkens/kenya-supermarkets-data',
        'filename': 'Supermarket Data.xlsx',
        's3_path': 'raw/products/',
        'type': 'xlsx'
    },
    'sales': {
        'url': 'kaninigichuyia/ecommerce-sales-data',
        'filename': 'sales.csv',  # May vary, will try common names
        's3_path': 'raw/sales/',
        'type': 'csv'
    }
}

# AWS S3 Configuration
S3_BUCKET = os.getenv('S3_BUCKET', 'your-bucket-name')
AWS_REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')


def get_kaggle_api() -> Optional[object]:
    """Initialize Kaggle API client if credentials are available."""
    try:
        from kaggle.api.kaggle_api_extended import KaggleApi
        api = KaggleApi()
        api.authenticate()
        logger.info("Kaggle API authenticated successfully")
        return api
    except Exception as e:
        logger.warning(f"Kaggle API not available: {e}")
        logger.warning("Please install kaggle library and set up ~/.kaggle/kaggle.json")
        return None


def download_kaggle_dataset(api: object, dataset_url: str, filename: str, 
                           download_dir: str = 'data/temp') -> Optional[str]:
    """
    Download dataset from Kaggle.
    
    Args:
        api: Kaggle API client
        dataset_url: Dataset URL in format 'username/dataset-name'
        filename: Specific file to download
        download_dir: Local directory to save file
        
    Returns:
        Path to downloaded file or None if failed
    """
    try:
        # Create download directory
        Path(download_dir).mkdir(parents=True, exist_ok=True)
        
        # Download dataset files
        logger.info(f"Downloading dataset: {dataset_url}")
        api.dataset_download_files(dataset_url, path=download_dir, unzip=True)
        
        # Look for the file (Kaggle downloads may have different structure)
        download_path = Path(download_dir) / filename
        
        # If exact filename not found, search for similar files
        if not download_path.exists():
            possible_files = list(Path(download_dir).glob('*.xlsx')) + \
                           list(Path(download_dir).glob('*.csv'))
            if possible_files:
                download_path = possible_files[0]
                logger.info(f"Found file: {download_path}")
        
        if download_path.exists():
            logger.info(f"Successfully downloaded: {download_path}")
            return str(download_path)
        else:
            logger.error(f"File not found after download: {filename}")
            return None
            
    except Exception as e:
        logger.error(f"Error downloading dataset {dataset_url}: {e}")
        return None


def convert_xlsx_to_csv(xlsx_path: str, output_dir: str = 'data/temp') -> Optional[str]:
    """
    Convert XLSX file to CSV.
    
    Args:
        xlsx_path: Path to XLSX file
        output_dir: Directory to save CSV file
        
    Returns:
        Path to converted CSV file or None if failed
    """
    try:
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Converting XLSX to CSV: {xlsx_path}")
        
        # Read XLSX file (try first sheet if multiple sheets)
        df = pd.read_excel(xlsx_path, sheet_name=0)
        
        # Generate CSV filename
        csv_filename = Path(xlsx_path).stem + '.csv'
        csv_path = Path(output_dir) / csv_filename
        
        # Save as CSV
        df.to_csv(csv_path, index=False, encoding='utf-8')
        
        logger.info(f"Successfully converted to: {csv_path}")
        return str(csv_path)
        
    except Exception as e:
        logger.error(f"Error converting XLSX to CSV: {e}")
        return None


def add_county_column(df: pd.DataFrame, county_col_name: str = 'county') -> pd.DataFrame:
    """
    Add a random county column to dataframe for Kenyan context.
    
    Args:
        df: DataFrame to add county column to
        county_col_name: Name of the county column
        
    Returns:
        DataFrame with county column added
    """
    try:
        if county_col_name in df.columns:
            logger.warning(f"Column '{county_col_name}' already exists. Skipping addition.")
            return df
        
        # Generate random county values weighted towards major cities
        major_cities = ['Nairobi', 'Mombasa', 'Kisumu', 'Nakuru', 'Eldoret']
        major_weights = [0.3, 0.15, 0.1, 0.1, 0.05]
        remaining_weight = 1.0 - sum(major_weights)
        
        # Create weights for all counties (major cities get higher weights)
        weights = []
        for county in KENYAN_COUNTIES:
            if county in major_cities:
                idx = major_cities.index(county)
                weights.append(major_weights[idx])
            else:
                # Distribute remaining weight evenly among other counties
                remaining_counties = len(KENYAN_COUNTIES) - len(major_cities)
                weights.append(remaining_weight / remaining_counties if remaining_counties > 0 else 0)
        
        # Normalize weights to sum to 1
        total_weight = sum(weights)
        if total_weight > 0:
            weights = [w / total_weight for w in weights]
        
        df[county_col_name] = random.choices(KENYAN_COUNTIES, weights=weights, k=len(df))
        
        logger.info(f"Added '{county_col_name}' column with {len(df)} records")
        return df
        
    except Exception as e:
        logger.error(f"Error adding county column: {e}")
        return df


def get_s3_client():
    """Initialize and return S3 client."""
    try:
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        # Test credentials by listing buckets
        s3_client.list_buckets()
        logger.info(f"S3 client initialized for region: {AWS_REGION}")
        return s3_client
    except NoCredentialsError:
        logger.error("AWS credentials not found. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
        raise
    except Exception as e:
        logger.error(f"Error initializing S3 client: {e}")
        raise


def upload_to_s3(s3_client, local_file_path: str, bucket: str, s3_key: str) -> bool:
    """
    Upload file to S3.
    
    Args:
        s3_client: Boto3 S3 client
        local_file_path: Path to local file
        bucket: S3 bucket name
        s3_key: S3 object key (path)
        
    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info(f"Uploading {local_file_path} to s3://{bucket}/{s3_key}")
        
        # Read file and convert to CSV if it's a DataFrame
        if local_file_path.endswith('.csv'):
            s3_client.upload_file(
                local_file_path,
                bucket,
                s3_key,
                ExtraArgs={'ContentType': 'text/csv'}
            )
        else:
            s3_client.upload_file(local_file_path, bucket, s3_key)
        
        logger.info(f"Successfully uploaded to s3://{bucket}/{s3_key}")
        return True
        
    except FileNotFoundError:
        logger.error(f"File not found: {local_file_path}")
        return False
    except ClientError as e:
        logger.error(f"Error uploading to S3: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error uploading to S3: {e}")
        return False


def process_dataset(dataset_name: str, dataset_config: dict, api: Optional[object] = None,
                   s3_client=None, use_local_file: Optional[str] = None) -> bool:
    """
    Process a single dataset: download, convert, add county, upload to S3.
    
    Args:
        dataset_name: Name of the dataset (e.g., 'products', 'sales')
        dataset_config: Dataset configuration dictionary
        api: Kaggle API client (optional)
        s3_client: S3 client
        use_local_file: Use local file instead of downloading (optional)
        
    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info(f"Processing dataset: {dataset_name}")
        
        # Step 1: Download or use local file
        if use_local_file:
            file_path = use_local_file
            logger.info(f"Using local file: {file_path}")
        elif api:
            file_path = download_kaggle_dataset(
                api, 
                dataset_config['url'], 
                dataset_config['filename']
            )
            if not file_path:
                logger.error(f"Failed to download dataset: {dataset_name}")
                return False
        else:
            logger.error(f"Neither Kaggle API nor local file provided for {dataset_name}")
            return False
        
        # Step 2: Convert XLSX to CSV if needed
        if file_path.endswith('.xlsx') or file_path.endswith('.xls'):
            csv_path = convert_xlsx_to_csv(file_path)
            if not csv_path:
                logger.error(f"Failed to convert XLSX to CSV: {file_path}")
                return False
            file_path = csv_path
        
        # Step 3: Read CSV, add county column
        logger.info(f"Reading and processing CSV: {file_path}")
        df = pd.read_csv(file_path)
        df = add_county_column(df)
        
        # Step 4: Save processed CSV
        processed_path = file_path.replace('.csv', '_with_county.csv')
        df.to_csv(processed_path, index=False, encoding='utf-8')
        logger.info(f"Saved processed file: {processed_path}")
        
        # Step 5: Upload to S3
        s3_filename = Path(file_path).name.replace('_with_county', '')
        s3_key = f"{dataset_config['s3_path']}{s3_filename}"
        
        success = upload_to_s3(s3_client, processed_path, S3_BUCKET, s3_key)
        
        if success:
            logger.info(f"Successfully processed and uploaded {dataset_name}")
            return True
        else:
            logger.error(f"Failed to upload {dataset_name} to S3")
            return False
            
    except Exception as e:
        logger.error(f"Error processing dataset {dataset_name}: {e}", exc_info=True)
        return False


def main():
    """Main function to orchestrate download and upload process."""
    logger.info("=" * 60)
    logger.info("Starting Kaggle dataset download and S3 upload process")
    logger.info("=" * 60)
    
    # Check S3 bucket configuration
    if S3_BUCKET == 'your-bucket-name':
        logger.warning(f"S3_BUCKET not set. Using default: {S3_BUCKET}")
        logger.warning("Please set S3_BUCKET environment variable")
        response = input("Continue with default bucket name? (y/n): ")
        if response.lower() != 'y':
            logger.info("Exiting...")
            return
    
    # Initialize S3 client
    try:
        s3_client = get_s3_client()
    except Exception as e:
        logger.error(f"Failed to initialize S3 client: {e}")
        logger.error("Please configure AWS credentials via environment variables or AWS CLI")
        return
    
    # Initialize Kaggle API (optional - can use local files instead)
    api = get_kaggle_api()
    
    if not api:
        logger.warning("Kaggle API not available. You can:")
        logger.warning("1. Install kaggle: pip install kaggle")
        logger.warning("2. Set up ~/.kaggle/kaggle.json with your API credentials")
        logger.warning("3. Or place files manually in data/temp/ directory")
        use_local = input("Continue with local files only? (y/n): ")
        if use_local.lower() != 'y':
            return
    
    # Process each dataset
    results = {}
    for dataset_name, dataset_config in KAGGLE_DATASETS.items():
        success = process_dataset(dataset_name, dataset_config, api, s3_client)
        results[dataset_name] = success
    
    # Summary
    logger.info("=" * 60)
    logger.info("Processing Summary:")
    logger.info("=" * 60)
    for dataset_name, success in results.items():
        status = "✓ SUCCESS" if success else "✗ FAILED"
        logger.info(f"{dataset_name}: {status}")
    
    total = len(results)
    successful = sum(results.values())
    logger.info(f"Total: {successful}/{total} datasets processed successfully")
    
    if successful == total:
        logger.info("All datasets processed successfully!")
    else:
        logger.warning(f"{total - successful} dataset(s) failed to process")


if __name__ == "__main__":
    main()
