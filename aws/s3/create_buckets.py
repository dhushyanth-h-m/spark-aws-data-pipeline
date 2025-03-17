#!/usr/bin/env python3
"""
Create S3 buckets for the NYC Taxi Data Pipeline.
This script creates two S3 buckets:
1. Raw data bucket: Stores the raw NYC Taxi data
2. Processed data bucket: Stores the processed data in Parquet format
"""

import boto3
import logging
import os
import sys
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_bucket(bucket_name, region=None):
    """Create an S3 bucket in a specified region

    Args:
        bucket_name: Bucket to create
        region: String region to create bucket in, e.g., 'us-west-2'

    Returns:
        True if bucket created, else False
    """
    try:
        if region is None:
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3', region_name=region)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration=location
            )
        
        # Add default encryption
        s3_client.put_bucket_encryption(
            Bucket=bucket_name,
            ServerSideEncryptionConfiguration={
                'Rules': [
                    {
                        'ApplyServerSideEncryptionByDefault': {
                            'SSEAlgorithm': 'AES256'
                        }
                    }
                ]
            }
        )
        
        # Add lifecycle policy to transition to Infrequent Access after 30 days
        s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration={
                'Rules': [
                    {
                        'ID': 'Transition to IA',
                        'Status': 'Enabled',
                        'Prefix': '',
                        'Transitions': [
                            {
                                'Days': 30,
                                'StorageClass': 'STANDARD_IA'
                            }
                        ]
                    }
                ]
            }
        )
        
        logger.info(f"Created bucket {bucket_name}")
        return True
    except ClientError as e:
        logger.error(f"Error creating bucket {bucket_name}: {e}")
        return False

def main():
    """Main function to create S3 buckets for the NYC Taxi Data Pipeline"""
    # Load environment variables or use defaults
    aws_region = os.environ.get('AWS_REGION', 'us-east-1')
    project_name = os.environ.get('PROJECT_NAME', 'nyc-taxi-pipeline')
    environment = os.environ.get('ENVIRONMENT', 'dev')
    
    # Create bucket names with proper naming convention
    raw_bucket_name = f"{project_name}-{environment}-raw"
    processed_bucket_name = f"{project_name}-{environment}-processed"
    
    # Create buckets
    if create_bucket(raw_bucket_name, aws_region):
        logger.info(f"Successfully created raw data bucket: {raw_bucket_name}")
    else:
        logger.error(f"Failed to create raw data bucket: {raw_bucket_name}")
        sys.exit(1)
    
    if create_bucket(processed_bucket_name, aws_region):
        logger.info(f"Successfully created processed data bucket: {processed_bucket_name}")
    else:
        logger.error(f"Failed to create processed data bucket: {processed_bucket_name}")
        sys.exit(1)
    
    logger.info("S3 bucket setup completed successfully")
    
    # Output bucket names for use in other scripts
    print(f"Raw data bucket: {raw_bucket_name}")
    print(f"Processed data bucket: {processed_bucket_name}")

if __name__ == "__main__":
    main() 