"""
NYC Taxi Data Ingestion DAG
This DAG downloads NYC Taxi Trip data from the NYC TLC website and uploads it to S3.
"""

import os
import tempfile
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

# Environment variables
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')
PROJECT_NAME = os.environ.get('PROJECT_NAME', 'nyc-taxi-pipeline')
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'dev')
RAW_BUCKET = f"{PROJECT_NAME}-{ENVIRONMENT}-raw"

# Create the DAG
dag = DAG(
    'nyc_taxi_ingest',
    default_args=default_args,
    description='Download NYC Taxi Trip data and upload to S3',
    schedule_interval='0 0 1 * *',  # Run monthly on the 1st day
    catchup=True,
    max_active_runs=1,
    tags=['nyc-taxi', 'aws', 's3', 'ingest'],
    params={
        'raw_bucket': RAW_BUCKET,
        'year': '{{ execution_date.strftime("%Y") }}',
        'month': '{{ execution_date.strftime("%m") }}'
    }
)

def _get_data_url(**context):
    """Generate the URL for the NYC Taxi data based on execution date"""
    year = context['params']['year']
    month = context['params']['month']
    
    # URL pattern for Yellow Taxi data
    # Example: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet"
    
    return url

def _download_and_upload_to_s3(**context):
    """Download the NYC Taxi data and upload to S3"""
    # Get parameters
    year = context['params']['year']
    month = context['params']['month']
    bucket_name = context['params']['raw_bucket']
    data_url = context['ti'].xcom_pull(task_ids='get_data_url')
    
    # Create S3 hook
    s3_hook = S3Hook(aws_conn_id='aws_default')
    
    # Create temporary file
    with tempfile.NamedTemporaryFile(suffix='.parquet') as temp_file:
        # Download the data
        import requests
        response = requests.get(data_url, stream=True)
        response.raise_for_status()
        
        # Write to temporary file
        for chunk in response.iter_content(chunk_size=8192):
            temp_file.write(chunk)
        
        # Flush to ensure all data is written
        temp_file.flush()
        
        # Upload to S3
        s3_key = f"{year}/{month}/yellow_tripdata_{year}-{month}.parquet"
        s3_hook.load_file(
            filename=temp_file.name,
            key=s3_key,
            bucket_name=bucket_name,
            replace=True
        )
        
        # Log success
        print(f"Successfully uploaded NYC Taxi data to s3://{bucket_name}/{s3_key}")
        
        # Return the S3 path for downstream tasks
        return f"s3://{bucket_name}/{s3_key}"

def _check_data_availability(**context):
    """Check if the NYC Taxi data is available for download"""
    data_url = context['ti'].xcom_pull(task_ids='get_data_url')
    
    import requests
    response = requests.head(data_url)
    
    if response.status_code == 200:
        print(f"Data is available at {data_url}")
        return True
    else:
        print(f"Data is not available at {data_url}. Status code: {response.status_code}")
        return False

# Define tasks
get_data_url = PythonOperator(
    task_id='get_data_url',
    python_callable=_get_data_url,
    provide_context=True,
    dag=dag
)

check_data_availability = PythonOperator(
    task_id='check_data_availability',
    python_callable=_check_data_availability,
    provide_context=True,
    dag=dag
)

download_and_upload_to_s3 = PythonOperator(
    task_id='download_and_upload_to_s3',
    python_callable=_download_and_upload_to_s3,
    provide_context=True,
    dag=dag
)

# Define task dependencies
get_data_url >> check_data_availability >> download_and_upload_to_s3 