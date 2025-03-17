# NYC Taxi Data Pipeline Architecture

## Overview

This document describes the architecture of the NYC Taxi Data Pipeline, a cloud-scale data processing system built on AWS services, Apache Spark, Airflow, and Docker.

## Architecture Diagram

```
+------------------+     +------------------+     +------------------+
|                  |     |                  |     |                  |
|  NYC Taxi Data   +---->+  Amazon S3       +---->+  AWS EMR         |
|  Source          |     |  (Raw Data)      |     |  (Spark Cluster) |
|                  |     |                  |     |                  |
+------------------+     +------------------+     +--------+---------+
                                                           |
                                                           v
+------------------+     +------------------+     +------------------+
|                  |     |                  |     |                  |
|  Visualization   +<----+  Amazon S3       +<----+  Processed Data  |
|  & Analytics     |     |  (Processed)     |     |  (Parquet Format)|
|                  |     |                  |     |                  |
+------------------+     +------------------+     +------------------+
        ^
        |
        |                 +------------------+
        |                 |                  |
        +-----------------+  Apache Airflow  |
                          |  (Orchestration) |
                          |                  |
                          +------------------+
```

## Component Details

### 1. Data Source
- NYC Taxi Trip dataset containing records of taxi operations
- Data includes pickup/dropoff times, locations, trip distances, fares, etc.

### 2. Data Storage
- **Raw Data**: Amazon S3 bucket stores the raw NYC Taxi data
- **Processed Data**: Amazon S3 bucket stores the processed data in Parquet format

### 3. Data Processing
- **AWS EMR Cluster**: Managed Hadoop framework running Apache Spark
- **Apache Spark**: Distributed processing engine for large-scale data analytics
- **Processing Jobs**: PySpark jobs that transform, clean, and analyze the taxi data

### 4. Workflow Orchestration
- **Apache Airflow**: Manages the scheduling and execution of data pipeline tasks
- **DAGs**: Define the sequence and dependencies of tasks in the pipeline
- **Operators**: Connect to AWS services and trigger Spark jobs on EMR

### 5. Containerization
- **Docker**: Provides consistent environments for development and deployment
- **Docker Compose**: Orchestrates multi-container applications

## Data Flow

1. **Ingestion**: NYC Taxi data is ingested from the source and stored in the raw S3 bucket
2. **Processing**: Airflow triggers EMR cluster creation and Spark jobs
3. **Transformation**: Spark jobs process the data (cleaning, aggregation, feature engineering)
4. **Storage**: Processed data is stored in Parquet format in the processed S3 bucket
5. **Termination**: EMR cluster is terminated to optimize costs
6. **Analytics**: Processed data is available for visualization and analytics

## Cost Optimization Strategies

- **EMR Cluster Management**: Auto-scaling and automatic termination
- **Spot Instances**: Utilize spot instances for worker nodes to reduce costs
- **Storage Optimization**: Parquet format for efficient storage and retrieval
- **Scheduled Processing**: Run jobs during off-peak hours
- **Resource Monitoring**: Track usage to stay within free tier limits where possible 