#!/usr/bin/env python3
"""
Process NYC Taxi Trip data using Apache Spark.
This script reads NYC Taxi data from S3, performs transformations,
and writes the processed data back to S3 in Parquet format.
"""

import argparse
import logging
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, month, year, when, lit, expr
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session(app_name="NYC-Taxi-Data-Processing"):
    """
    Create a Spark session with appropriate configurations
    
    Args:
        app_name: Name of the Spark application
        
    Returns:
        SparkSession object
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.parquet.compression", "snappy") \
        .config("spark.sql.files.maxPartitionBytes", 134217728) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .getOrCreate()
    
    logger.info("Spark session created")
    return spark

def read_taxi_data(spark, input_path, file_format="csv", header=True, inferSchema=True):
    """
    Read NYC Taxi data from S3
    
    Args:
        spark: SparkSession object
        input_path: S3 path to the input data
        file_format: Format of the input files (csv, parquet, etc.)
        header: Whether the input files have a header
        inferSchema: Whether to infer the schema from the data
        
    Returns:
        Spark DataFrame containing the taxi data
    """
    logger.info(f"Reading taxi data from {input_path}")
    
    if file_format.lower() == "csv":
        df = spark.read.format("csv") \
            .option("header", header) \
            .option("inferSchema", inferSchema) \
            .load(input_path)
    elif file_format.lower() == "parquet":
        df = spark.read.parquet(input_path)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")
    
    logger.info(f"Read {df.count()} records from {input_path}")
    return df

def clean_taxi_data(df):
    """
    Clean and transform the taxi data
    
    Args:
        df: Spark DataFrame containing the taxi data
        
    Returns:
        Cleaned Spark DataFrame
    """
    logger.info("Cleaning and transforming taxi data")
    
    # Rename columns to standardized format
    # Assuming the data has columns like pickup_datetime, dropoff_datetime, etc.
    # Adjust column names based on the actual data schema
    
    # Cast columns to appropriate types
    df = df.withColumn("pickup_datetime", col("pickup_datetime").cast(TimestampType())) \
           .withColumn("dropoff_datetime", col("dropoff_datetime").cast(TimestampType())) \
           .withColumn("passenger_count", col("passenger_count").cast(IntegerType())) \
           .withColumn("trip_distance", col("trip_distance").cast(DoubleType())) \
           .withColumn("fare_amount", col("fare_amount").cast(DoubleType())) \
           .withColumn("tip_amount", col("tip_amount").cast(DoubleType())) \
           .withColumn("total_amount", col("total_amount").cast(DoubleType()))
    
    # Filter out invalid records
    df = df.filter(
        (col("pickup_datetime").isNotNull()) &
        (col("dropoff_datetime").isNotNull()) &
        (col("trip_distance") > 0) &
        (col("fare_amount") >= 0) &
        (col("passenger_count") > 0)
    )
    
    # Add derived columns
    df = df.withColumn("pickup_hour", hour(col("pickup_datetime"))) \
           .withColumn("pickup_day", dayofweek(col("pickup_datetime"))) \
           .withColumn("pickup_month", month(col("pickup_datetime"))) \
           .withColumn("pickup_year", year(col("pickup_datetime"))) \
           .withColumn("trip_duration_minutes", 
                       (col("dropoff_datetime").cast("long") - col("pickup_datetime").cast("long")) / 60)
    
    # Categorize trips
    df = df.withColumn(
        "trip_type",
        when(col("trip_distance") < 3, "short")
        .when(col("trip_distance") < 10, "medium")
        .otherwise("long")
    )
    
    # Calculate cost per mile
    df = df.withColumn(
        "cost_per_mile",
        when(col("trip_distance") > 0, col("fare_amount") / col("trip_distance"))
        .otherwise(0)
    )
    
    logger.info(f"Cleaned data has {df.count()} records")
    return df

def analyze_taxi_data(df):
    """
    Perform analysis on the taxi data
    
    Args:
        df: Spark DataFrame containing the cleaned taxi data
        
    Returns:
        Dictionary of analysis DataFrames
    """
    logger.info("Analyzing taxi data")
    
    # Hourly trip counts
    hourly_trips = df.groupBy("pickup_hour") \
                     .count() \
                     .orderBy("pickup_hour")
    
    # Average fare by trip type
    avg_fare_by_trip_type = df.groupBy("trip_type") \
                              .agg({"fare_amount": "avg", "trip_distance": "avg"}) \
                              .orderBy("trip_type")
    
    # Passenger count distribution
    passenger_distribution = df.groupBy("passenger_count") \
                               .count() \
                               .orderBy("passenger_count")
    
    # Monthly trends
    monthly_trends = df.groupBy("pickup_year", "pickup_month") \
                       .agg({"fare_amount": "sum", "trip_distance": "sum", "passenger_count": "sum"}) \
                       .orderBy("pickup_year", "pickup_month")
    
    analysis = {
        "hourly_trips": hourly_trips,
        "avg_fare_by_trip_type": avg_fare_by_trip_type,
        "passenger_distribution": passenger_distribution,
        "monthly_trends": monthly_trends
    }
    
    return analysis

def write_data_to_s3(df, output_path, partition_cols=None, mode="overwrite"):
    """
    Write the processed data to S3 in Parquet format
    
    Args:
        df: Spark DataFrame to write
        output_path: S3 path to write the data to
        partition_cols: List of columns to partition by
        mode: Write mode (overwrite, append, etc.)
    """
    logger.info(f"Writing data to {output_path}")
    
    writer = df.write.mode(mode).format("parquet")
    
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    
    writer.save(output_path)
    logger.info(f"Successfully wrote data to {output_path}")

def main():
    """Main function to process NYC Taxi data"""
    parser = argparse.ArgumentParser(description="Process NYC Taxi data")
    parser.add_argument("--input-path", required=True, help="S3 path to the input data")
    parser.add_argument("--output-path", required=True, help="S3 path to write the output data")
    parser.add_argument("--file-format", default="csv", help="Format of the input files (csv, parquet)")
    parser.add_argument("--partition-by", nargs="+", default=["pickup_year", "pickup_month"], 
                        help="Columns to partition the output by")
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Read data
        df = read_taxi_data(spark, args.input_path, args.file_format)
        
        # Clean and transform data
        cleaned_df = clean_taxi_data(df)
        
        # Analyze data
        analysis = analyze_taxi_data(cleaned_df)
        
        # Write cleaned data to S3
        write_data_to_s3(
            cleaned_df, 
            f"{args.output_path}/cleaned", 
            partition_cols=args.partition_by
        )
        
        # Write analysis results to S3
        for name, analysis_df in analysis.items():
            write_data_to_s3(
                analysis_df,
                f"{args.output_path}/analysis/{name}"
            )
        
        logger.info("Data processing completed successfully")
    
    except Exception as e:
        logger.error(f"Error processing data: {e}")
        raise
    
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main() 