#!/usr/bin/env python3
"""
Run the NYC Taxi data pipeline locally for testing.
This script downloads a sample of NYC Taxi data, processes it using Spark,
and saves the results to the local filesystem.
"""

import argparse
import logging
import os
import tempfile
import subprocess
import sys
from datetime import datetime

import requests

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def download_sample_data(year, month, output_dir):
    """
    Download a sample of NYC Taxi data
    
    Args:
        year: Year of the data
        month: Month of the data
        output_dir: Directory to save the data
        
    Returns:
        Path to the downloaded file
    """
    # URL pattern for Yellow Taxi data
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
    
    logger.info(f"Downloading data from {url}")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Download the file
    output_file = os.path.join(output_dir, f"yellow_tripdata_{year}-{month:02d}.parquet")
    
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(output_file, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        logger.info(f"Downloaded data to {output_file}")
        return output_file
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading data: {e}")
        sys.exit(1)

def run_spark_job(input_file, output_dir):
    """
    Run the Spark job to process the data
    
    Args:
        input_file: Path to the input file
        output_dir: Directory to save the output
        
    Returns:
        Exit code of the Spark job
    """
    logger.info(f"Running Spark job on {input_file}")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Build the spark-submit command
    spark_job = os.path.join(os.path.dirname(__file__), "spark", "jobs", "process_taxi_data.py")
    
    cmd = [
        "spark-submit",
        "--master", "local[*]",
        "--conf", "spark.sql.parquet.compression=snappy",
        "--conf", "spark.dynamicAllocation.enabled=true",
        spark_job,
        "--input-path", input_file,
        "--output-path", output_dir,
        "--file-format", "parquet"
    ]
    
    logger.info(f"Running command: {' '.join(cmd)}")
    
    # Run the command
    process = subprocess.run(cmd, check=False)
    
    if process.returncode == 0:
        logger.info(f"Spark job completed successfully")
    else:
        logger.error(f"Spark job failed with exit code {process.returncode}")
    
    return process.returncode

def main():
    """Main function to run the pipeline locally"""
    parser = argparse.ArgumentParser(description="Run the NYC Taxi data pipeline locally")
    parser.add_argument("--year", type=int, default=datetime.now().year, help="Year of the data")
    parser.add_argument("--month", type=int, default=datetime.now().month - 1, help="Month of the data")
    parser.add_argument("--data-dir", default="./data", help="Directory to store the data")
    parser.add_argument("--output-dir", default="./output", help="Directory to store the output")
    
    args = parser.parse_args()
    
    # Download the data
    input_file = download_sample_data(args.year, args.month, args.data_dir)
    
    # Run the Spark job
    exit_code = run_spark_job(input_file, args.output_dir)
    
    if exit_code == 0:
        logger.info(f"Pipeline completed successfully. Results saved to {args.output_dir}")
    else:
        logger.error(f"Pipeline failed with exit code {exit_code}")
        sys.exit(exit_code)

if __name__ == "__main__":
    main() 