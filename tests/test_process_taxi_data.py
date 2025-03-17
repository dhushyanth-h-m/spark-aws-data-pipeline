#!/usr/bin/env python3
"""
Unit tests for the NYC Taxi data processing job.
"""

import os
import sys
import unittest
from unittest.mock import patch, MagicMock

# Add the spark directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'spark'))

# Import the functions to test
from jobs.process_taxi_data import (
    create_spark_session,
    clean_taxi_data,
    analyze_taxi_data
)

class TestProcessTaxiData(unittest.TestCase):
    """Test cases for the NYC Taxi data processing job."""
    
    @classmethod
    def setUpClass(cls):
        """Set up the test environment."""
        # Create a mock Spark session
        cls.spark = MagicMock()
        cls.spark.read.format.return_value.option.return_value.option.return_value.load.return_value = MagicMock()
        
        # Create a mock DataFrame
        cls.df = MagicMock()
        cls.df.count.return_value = 100
        cls.df.filter.return_value = cls.df
        cls.df.withColumn.return_value = cls.df
        cls.df.groupBy.return_value.count.return_value.orderBy.return_value = MagicMock()
        cls.df.groupBy.return_value.agg.return_value.orderBy.return_value = MagicMock()
    
    @patch('jobs.process_taxi_data.SparkSession')
    def test_create_spark_session(self, mock_spark_session):
        """Test creating a Spark session."""
        # Set up the mock
        mock_builder = MagicMock()
        mock_spark_session.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = MagicMock()
        
        # Call the function
        spark = create_spark_session()
        
        # Verify the result
        self.assertIsNotNone(spark)
        mock_builder.appName.assert_called_once_with("NYC-Taxi-Data-Processing")
        self.assertEqual(mock_builder.config.call_count, 4)
        mock_builder.getOrCreate.assert_called_once()
    
    def test_clean_taxi_data(self):
        """Test cleaning the taxi data."""
        # Call the function
        result = clean_taxi_data(self.df)
        
        # Verify the result
        self.assertIsNotNone(result)
        self.df.withColumn.assert_called()
        self.df.filter.assert_called()
    
    def test_analyze_taxi_data(self):
        """Test analyzing the taxi data."""
        # Call the function
        result = analyze_taxi_data(self.df)
        
        # Verify the result
        self.assertIsNotNone(result)
        self.assertIn("hourly_trips", result)
        self.assertIn("avg_fare_by_trip_type", result)
        self.assertIn("passenger_distribution", result)
        self.assertIn("monthly_trends", result)
        self.df.groupBy.assert_called()

if __name__ == '__main__':
    unittest.main() 