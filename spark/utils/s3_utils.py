#!/usr/bin/env python3
"""
Utility functions for S3 operations in Spark.
This module provides helper functions for working with S3 in PySpark jobs.
"""

import logging
import boto3
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def list_s3_files(bucket_name, prefix="", suffix=""):
    """
    List files in an S3 bucket with optional prefix and suffix filtering
    
    Args:
        bucket_name: Name of the S3 bucket
        prefix: Prefix to filter objects by
        suffix: Suffix to filter objects by
        
    Returns:
        List of S3 object keys
    """
    try:
        s3_client = boto3.client('s3')
        paginator = s3_client.get_paginator('list_objects_v2')
        
        # Create a paginator for listing objects
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        
        # Filter objects by suffix if provided
        file_list = []
        for page in page_iterator:
            if "Contents" in page:
                for obj in page["Contents"]:
                    key = obj["Key"]
                    if not suffix or key.endswith(suffix):
                        file_list.append(key)
        
        return file_list
    
    except ClientError as e:
        logger.error(f"Error listing files in bucket {bucket_name}: {e}")
        return []

def get_latest_partition(bucket_name, prefix):
    """
    Get the latest partition in an S3 bucket based on prefix
    
    Args:
        bucket_name: Name of the S3 bucket
        prefix: Prefix to filter partitions by
        
    Returns:
        Latest partition key
    """
    try:
        s3_client = boto3.client('s3')
        paginator = s3_client.get_paginator('list_objects_v2')
        
        # List all partitions with the given prefix
        page_iterator = paginator.paginate(
            Bucket=bucket_name,
            Prefix=prefix,
            Delimiter='/'
        )
        
        # Find the latest partition
        partitions = []
        for page in page_iterator:
            if "CommonPrefixes" in page:
                for obj in page["CommonPrefixes"]:
                    partitions.append(obj["Prefix"])
        
        if not partitions:
            return None
        
        # Sort partitions and return the latest one
        return sorted(partitions)[-1]
    
    except ClientError as e:
        logger.error(f"Error getting latest partition in bucket {bucket_name}: {e}")
        return None

def check_s3_path_exists(bucket_name, prefix):
    """
    Check if an S3 path exists
    
    Args:
        bucket_name: Name of the S3 bucket
        prefix: Prefix to check
        
    Returns:
        True if the path exists, False otherwise
    """
    try:
        s3_client = boto3.client('s3')
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix,
            MaxKeys=1
        )
        
        return 'Contents' in response
    
    except ClientError as e:
        logger.error(f"Error checking if path exists in bucket {bucket_name}: {e}")
        return False

def delete_s3_path(bucket_name, prefix):
    """
    Delete all objects under an S3 path
    
    Args:
        bucket_name: Name of the S3 bucket
        prefix: Prefix of objects to delete
        
    Returns:
        True if deletion was successful, False otherwise
    """
    try:
        s3_client = boto3.client('s3')
        paginator = s3_client.get_paginator('list_objects_v2')
        
        # List all objects with the given prefix
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        
        # Delete all objects
        for page in page_iterator:
            if "Contents" in page:
                objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
                s3_client.delete_objects(
                    Bucket=bucket_name,
                    Delete={"Objects": objects}
                )
        
        logger.info(f"Successfully deleted objects with prefix {prefix} from bucket {bucket_name}")
        return True
    
    except ClientError as e:
        logger.error(f"Error deleting objects with prefix {prefix} from bucket {bucket_name}: {e}")
        return False

def copy_s3_objects(source_bucket, source_prefix, dest_bucket, dest_prefix):
    """
    Copy objects from one S3 location to another
    
    Args:
        source_bucket: Source bucket name
        source_prefix: Source prefix
        dest_bucket: Destination bucket name
        dest_prefix: Destination prefix
        
    Returns:
        Number of objects copied
    """
    try:
        s3_client = boto3.client('s3')
        paginator = s3_client.get_paginator('list_objects_v2')
        
        # List all objects with the given prefix
        page_iterator = paginator.paginate(Bucket=source_bucket, Prefix=source_prefix)
        
        # Copy all objects
        copied_count = 0
        for page in page_iterator:
            if "Contents" in page:
                for obj in page["Contents"]:
                    source_key = obj["Key"]
                    dest_key = source_key.replace(source_prefix, dest_prefix, 1)
                    
                    s3_client.copy_object(
                        CopySource={"Bucket": source_bucket, "Key": source_key},
                        Bucket=dest_bucket,
                        Key=dest_key
                    )
                    
                    copied_count += 1
        
        logger.info(f"Successfully copied {copied_count} objects from {source_bucket}/{source_prefix} to {dest_bucket}/{dest_prefix}")
        return copied_count
    
    except ClientError as e:
        logger.error(f"Error copying objects from {source_bucket}/{source_prefix} to {dest_bucket}/{dest_prefix}: {e}")
        return 0

def get_s3_path(bucket_name, key):
    """
    Get the S3 path in the format s3://bucket/key
    
    Args:
        bucket_name: Name of the S3 bucket
        key: Object key
        
    Returns:
        S3 path
    """
    return f"s3://{bucket_name}/{key}"

def parse_s3_url(s3_url):
    """
    Parse an S3 URL into bucket and key
    
    Args:
        s3_url: S3 URL in the format s3://bucket/key
        
    Returns:
        Tuple of (bucket_name, key)
    """
    if not s3_url.startswith("s3://"):
        raise ValueError(f"Invalid S3 URL: {s3_url}")
    
    # Remove the "s3://" prefix
    s3_path = s3_url[5:]
    
    # Split into bucket and key
    parts = s3_path.split("/", 1)
    bucket_name = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    
    return bucket_name, key 