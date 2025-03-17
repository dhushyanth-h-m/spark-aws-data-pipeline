"""
NYC Taxi Data Pipeline DAG
This DAG orchestrates the processing of NYC Taxi Trip data using AWS EMR and Apache Spark.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

# Environment variables
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')
PROJECT_NAME = os.environ.get('PROJECT_NAME', 'nyc-taxi-pipeline')
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'dev')
RAW_BUCKET = f"{PROJECT_NAME}-{ENVIRONMENT}-raw"
PROCESSED_BUCKET = f"{PROJECT_NAME}-{ENVIRONMENT}-processed"
LOGS_BUCKET = f"{PROJECT_NAME}-{ENVIRONMENT}-logs"
EMR_LOGS_PATH = f"s3://{LOGS_BUCKET}/emr-logs/"

# EMR cluster configuration
EMR_CLUSTER_CONFIG = {
    'Name': f"{PROJECT_NAME}-{ENVIRONMENT}-cluster",
    'ReleaseLabel': 'emr-6.5.0',
    'Applications': [
        {'Name': 'Spark'},
        {'Name': 'Hadoop'},
        {'Name': 'Hive'},
        {'Name': 'Livy'}
    ],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Master',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },
            {
                'Name': 'Core',
                'Market': 'SPOT',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 2,
                'BidPrice': '0.1'
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
    },
    'BootstrapActions': [
        {
            'Name': 'Install Python packages',
            'ScriptBootstrapAction': {
                'Path': 's3://aws-bigdata-blog/artifacts/aws-blog-emr-jupyter/bootstrap_python.sh'
            }
        }
    ],
    'Configurations': [
        {
            'Classification': 'spark',
            'Properties': {
                'maximizeResourceAllocation': 'true'
            }
        },
        {
            'Classification': 'spark-defaults',
            'Properties': {
                'spark.dynamicAllocation.enabled': 'true',
                'spark.executor.instances': '2',
                'spark.executor.memory': '4g'
            }
        }
    ],
    'VisibleToAllUsers': True,
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
    'LogUri': EMR_LOGS_PATH,
    'Tags': [
        {
            'Key': 'Project',
            'Value': PROJECT_NAME
        },
        {
            'Key': 'Environment',
            'Value': ENVIRONMENT
        }
    ]
}

# Spark job steps
SPARK_STEPS = [
    {
        'Name': 'Process NYC Taxi Data',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                '--master', 'yarn',
                '--conf', 'spark.sql.parquet.compression=snappy',
                '--conf', 'spark.dynamicAllocation.enabled=true',
                's3://{{ params.code_bucket }}/spark/jobs/process_taxi_data.py',
                '--input-path', 's3://{{ params.raw_bucket }}/{{ params.year }}/{{ params.month }}/',
                '--output-path', 's3://{{ params.processed_bucket }}/{{ params.year }}/{{ params.month }}/',
                '--file-format', 'csv'
            ]
        }
    }
]

# Create the DAG
dag = DAG(
    'nyc_taxi_pipeline',
    default_args=default_args,
    description='Process NYC Taxi Trip data using AWS EMR and Apache Spark',
    schedule_interval='0 0 1 * *',  # Run monthly on the 1st day
    catchup=False,
    tags=['nyc-taxi', 'aws', 'spark', 'emr'],
    params={
        'code_bucket': f"{PROJECT_NAME}-{ENVIRONMENT}-code",
        'raw_bucket': RAW_BUCKET,
        'processed_bucket': PROCESSED_BUCKET,
        'year': '{{ execution_date.strftime("%Y") }}',
        'month': '{{ execution_date.strftime("%m") }}'
    }
)

# Define tasks
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id='create_emr_cluster',
    job_flow_overrides=EMR_CLUSTER_CONFIG,
    aws_conn_id='aws_default',
    dag=dag
)

add_spark_steps = EmrAddStepsOperator(
    task_id='add_spark_steps',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    steps=SPARK_STEPS,
    aws_conn_id='aws_default',
    dag=dag
)

watch_spark_step = EmrStepSensor(
    task_id='watch_spark_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_spark_steps', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag
)

terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id='terminate_emr_cluster',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    dag=dag
)

# Define task dependencies
create_emr_cluster >> add_spark_steps >> watch_spark_step >> terminate_emr_cluster 