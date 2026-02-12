"""
S3 Data Lake Upload Script
Uploads healthcare analytics CSV files to S3 bronze layer with proper organization.
"""

import boto3
import os
from pathlib import Path
from datetime import datetime, timezone
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'healthcare-analytics-datalake')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')

# Data lake folder structure
BRONZE_LAYER = 'bronze/raw/cms-data'
SILVER_LAYER = 'silver/processed'
GOLD_LAYER = 'gold/curated'

# Files to upload with metadata
FILES_TO_UPLOAD = [
    {
        'local_path': 'data/ipps_charges.csv',
        's3_key': f'{BRONZE_LAYER}/ipps_charges/ipps_charges.csv',
        'description': 'Medicare Inpatient Hospitals - by Provider and Service',
        'expected_rows': 146427,
        'expected_size_mb': 40
    },
    {
        'local_path': 'data/hospital_general_info.csv',
        's3_key': f'{BRONZE_LAYER}/hospital_general_info/hospital_general_info.csv',
        'description': 'Hospital General Information master list',
        'expected_rows': 5421,
        'expected_size_mb': 1
    },
    {
        'local_path': 'data/readmissions.csv',
        's3_key': f'{BRONZE_LAYER}/readmissions/readmissions.csv',
        'description': 'Hospital Readmissions Reduction Program data',
        'expected_rows': 18510,
        'expected_size_mb': 2
    }
]


def initialize_s3_client():
    """Initialize S3 client with credentials from environment or AWS config."""
    try:
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        # Test connection - try to list bucket or create if doesn't exist
        try:
            s3_client.head_bucket(Bucket=S3_BUCKET_NAME)
            logger.info(f"Successfully connected to S3 bucket: {S3_BUCKET_NAME}")
        except s3_client.exceptions.ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                # Bucket doesn't exist, try to create it
                logger.info(f"Bucket {S3_BUCKET_NAME} not found. Attempting to create...")
                try:
                    if AWS_REGION == 'us-east-1':
                        s3_client.create_bucket(Bucket=S3_BUCKET_NAME)
                    else:
                        s3_client.create_bucket(
                            Bucket=S3_BUCKET_NAME,
                            CreateBucketConfiguration={'LocationConstraint': AWS_REGION}
                        )
                    logger.info(f"✓ Created bucket: {S3_BUCKET_NAME}")
                except s3_client.exceptions.ClientError as create_error:
                    create_code = create_error.response['Error']['Code']
                    if create_code == 'BucketAlreadyExists':
                        logger.error(f"Bucket name {S3_BUCKET_NAME} is already taken globally. Please use a unique name.")
                    elif create_code == 'BucketAlreadyOwnedByYou':
                        logger.info(f"Bucket {S3_BUCKET_NAME} already exists and is owned by you.")
                    else:
                        logger.error(f"Failed to create bucket: {str(create_error)}")
                        raise
            elif error_code == '403':
                logger.error(f"Access denied to bucket {S3_BUCKET_NAME}. Check IAM permissions.")
                logger.error("Your IAM user needs: s3:CreateBucket, s3:ListBucket, s3:PutObject")
                raise
            else:
                raise
        return s3_client
    except Exception as e:
        logger.error(f"Failed to connect to S3: {str(e)}")
        raise


def create_s3_folder_structure(s3_client):
    """Create folder structure in S3 (using empty objects as folder markers)."""
    folders = [
        f'{BRONZE_LAYER}/ipps_charges/',
        f'{BRONZE_LAYER}/hospital_general_info/',
        f'{BRONZE_LAYER}/readmissions/',
        f'{SILVER_LAYER}/',
        f'{GOLD_LAYER}/',
        'metadata/',
        'logs/'
    ]
    
    for folder in folders:
        try:
            # Create folder marker (empty object)
            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=f'{folder}.keep',
                Body=b'',
                Metadata={'purpose': 'folder_marker'}
            )
            logger.info(f"Created folder structure: {folder}")
        except Exception as e:
            logger.warning(f"Could not create folder {folder}: {str(e)}")


def upload_file_to_s3(s3_client, local_path, s3_key, metadata):
    """Upload a single file to S3 with metadata and validation."""
    file_path = Path(local_path)
    
    if not file_path.exists():
        logger.error(f"File not found: {local_path}")
        return False
    
    file_size_mb = file_path.stat().st_size / (1024 * 1024)
    logger.info(f"Uploading {file_path.name} ({file_size_mb:.2f} MB) to {s3_key}")
    
    try:
        # Upload with metadata
        s3_client.upload_file(
            local_path,
            S3_BUCKET_NAME,
            s3_key,
            ExtraArgs={
                'Metadata': {
                    'upload_date': datetime.now(timezone.utc).isoformat(),
                    'description': metadata['description'],
                    'expected_rows': str(metadata['expected_rows']),
                    'source': 'CMS',
                    'data_layer': 'bronze',
                    'file_type': 'csv'
                },
                'ContentType': 'text/csv',
                'ServerSideEncryption': 'AES256'  # Enable encryption
            }
        )
        
        # Verify upload
        response = s3_client.head_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
        uploaded_size_mb = response['ContentLength'] / (1024 * 1024)
        
        logger.info(f"✓ Successfully uploaded {file_path.name}")
        logger.info(f"  Size: {uploaded_size_mb:.2f} MB")
        logger.info(f"  S3 Path: s3://{S3_BUCKET_NAME}/{s3_key}")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to upload {file_path.name}: {str(e)}")
        return False


def create_upload_manifest(s3_client, upload_results):
    """Create a manifest file documenting the upload."""
    manifest = {
        'upload_timestamp': datetime.now(timezone.utc).isoformat(),
        'bucket': S3_BUCKET_NAME,
        'region': AWS_REGION,
        'files': []
    }
    
    for result in upload_results:
        manifest['files'].append({
            's3_key': result['s3_key'],
            'local_path': result['local_path'],
            'description': result['description'],
            'status': result['status'],
            'size_mb': result.get('size_mb', 0)
        })
    
    manifest_key = f"metadata/upload_manifest_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    
    import json
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=manifest_key,
        Body=json.dumps(manifest, indent=2),
        ContentType='application/json',
        Metadata={'purpose': 'upload_manifest'}
    )
    
    logger.info(f"Created upload manifest: {manifest_key}")
    return manifest_key


def main():
    """Main execution function."""
    logger.info("=" * 60)
    logger.info("S3 Data Lake Upload - Healthcare Analytics Platform")
    logger.info("=" * 60)
    
    # Initialize S3 client
    try:
        s3_client = initialize_s3_client()
    except Exception as e:
        logger.error(f"Cannot proceed without S3 connection: {str(e)}")
        return
    
    # Create folder structure
    logger.info("\nCreating S3 folder structure...")
    create_s3_folder_structure(s3_client)
    
    # Upload files
    logger.info("\nStarting file uploads...")
    upload_results = []
    
    for file_config in FILES_TO_UPLOAD:
        success = upload_file_to_s3(
            s3_client,
            file_config['local_path'],
            file_config['s3_key'],
            {
                'description': file_config['description'],
                'expected_rows': file_config['expected_rows']
            }
        )
        
        upload_results.append({
            's3_key': file_config['s3_key'],
            'local_path': file_config['local_path'],
            'description': file_config['description'],
            'status': 'success' if success else 'failed',
            'size_mb': file_config['expected_size_mb']
        })
    
    # Create manifest
    logger.info("\nCreating upload manifest...")
    manifest_key = create_upload_manifest(s3_client, upload_results)
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("Upload Summary")
    logger.info("=" * 60)
    successful = sum(1 for r in upload_results if r['status'] == 'success')
    logger.info(f"Successfully uploaded: {successful}/{len(upload_results)} files")
    logger.info(f"Manifest: s3://{S3_BUCKET_NAME}/{manifest_key}")
    logger.info("\nNext steps:")
    logger.info("1. Verify files in S3 console")
    logger.info("2. Set up Snowflake external stage pointing to bronze layer")
    logger.info("3. Run COPY INTO commands to load data to Snowflake raw schema")


if __name__ == '__main__':
    main()

