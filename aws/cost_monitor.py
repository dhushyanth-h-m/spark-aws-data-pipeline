#!/usr/bin/env python3
"""
Monitor AWS costs for the NYC Taxi Data Pipeline.
This script retrieves cost information from AWS Cost Explorer
and provides recommendations for cost optimization.
"""

import argparse
import boto3
import logging
import os
import sys
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_cost_and_usage(start_date, end_date, granularity='DAILY', metrics=['UnblendedCost']):
    """
    Get cost and usage data from AWS Cost Explorer
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        granularity: Time granularity (DAILY, MONTHLY, etc.)
        metrics: Cost metrics to retrieve
        
    Returns:
        Cost and usage data
    """
    try:
        client = boto3.client('ce')
        
        response = client.get_cost_and_usage(
            TimePeriod={
                'Start': start_date,
                'End': end_date
            },
            Granularity=granularity,
            Metrics=metrics,
            GroupBy=[
                {
                    'Type': 'DIMENSION',
                    'Key': 'SERVICE'
                }
            ]
        )
        
        return response
    
    except ClientError as e:
        logger.error(f"Error retrieving cost data: {e}")
        return None

def get_cost_forecast(start_date, end_date, granularity='MONTHLY', metrics=['UnblendedCost']):
    """
    Get cost forecast from AWS Cost Explorer
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        granularity: Time granularity (DAILY, MONTHLY, etc.)
        metrics: Cost metrics to retrieve
        
    Returns:
        Cost forecast data
    """
    try:
        client = boto3.client('ce')
        
        response = client.get_cost_forecast(
            TimePeriod={
                'Start': start_date,
                'End': end_date
            },
            Granularity=granularity,
            Metric=metrics[0]
        )
        
        return response
    
    except ClientError as e:
        logger.error(f"Error retrieving cost forecast: {e}")
        return None

def analyze_costs(cost_data):
    """
    Analyze cost data and provide recommendations
    
    Args:
        cost_data: Cost and usage data from AWS Cost Explorer
        
    Returns:
        Dictionary of cost analysis and recommendations
    """
    if not cost_data or 'ResultsByTime' not in cost_data:
        return None
    
    # Extract service costs
    service_costs = {}
    total_cost = 0.0
    
    for result in cost_data['ResultsByTime']:
        for group in result['Groups']:
            service = group['Keys'][0]
            amount = float(group['Metrics']['UnblendedCost']['Amount'])
            
            if service in service_costs:
                service_costs[service] += amount
            else:
                service_costs[service] = amount
            
            total_cost += amount
    
    # Sort services by cost
    sorted_services = sorted(service_costs.items(), key=lambda x: x[1], reverse=True)
    
    # Generate recommendations
    recommendations = []
    
    for service, cost in sorted_services:
        if service == 'Amazon Elastic Compute Cloud - Compute':
            recommendations.append("Consider using Spot Instances for EMR worker nodes to reduce EC2 costs.")
            recommendations.append("Ensure EMR clusters are terminated when not in use.")
        elif service == 'Amazon Simple Storage Service':
            recommendations.append("Implement S3 lifecycle policies to transition infrequently accessed data to cheaper storage classes.")
            recommendations.append("Clean up temporary data and logs regularly.")
    
    # Add general recommendations
    recommendations.append("Schedule pipeline runs during off-peak hours to minimize costs.")
    recommendations.append("Monitor and optimize Spark jobs to reduce processing time and resource usage.")
    
    return {
        'total_cost': total_cost,
        'service_costs': dict(sorted_services),
        'recommendations': recommendations
    }

def print_cost_report(analysis, forecast=None):
    """
    Print a cost report
    
    Args:
        analysis: Cost analysis data
        forecast: Cost forecast data
    """
    if not analysis:
        logger.error("No cost analysis data available")
        return
    
    print("\n===== AWS COST REPORT =====\n")
    
    print(f"Total Cost: ${analysis['total_cost']:.2f}\n")
    
    print("Cost by Service:")
    for service, cost in analysis['service_costs'].items():
        print(f"  {service}: ${cost:.2f}")
    
    if forecast and 'Total' in forecast:
        print(f"\nForecast for Next Month: ${float(forecast['Total']['Amount']):.2f}")
    
    print("\nRecommendations for Cost Optimization:")
    for i, recommendation in enumerate(analysis['recommendations'], 1):
        print(f"  {i}. {recommendation}")
    
    print("\n===========================\n")

def main():
    """Main function to monitor AWS costs"""
    parser = argparse.ArgumentParser(description="Monitor AWS costs for the NYC Taxi Data Pipeline")
    parser.add_argument("--days", type=int, default=30, help="Number of days to analyze")
    parser.add_argument("--forecast", action="store_true", help="Include cost forecast")
    
    args = parser.parse_args()
    
    # Calculate date range
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=args.days)
    
    # Format dates for AWS Cost Explorer
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    # Get cost data
    cost_data = get_cost_and_usage(start_date_str, end_date_str)
    
    if not cost_data:
        logger.error("Failed to retrieve cost data")
        sys.exit(1)
    
    # Get forecast if requested
    forecast_data = None
    if args.forecast:
        forecast_start = end_date + timedelta(days=1)
        forecast_end = forecast_start + timedelta(days=30)
        
        forecast_start_str = forecast_start.strftime('%Y-%m-%d')
        forecast_end_str = forecast_end.strftime('%Y-%m-%d')
        
        forecast_data = get_cost_forecast(forecast_start_str, forecast_end_str)
    
    # Analyze costs
    analysis = analyze_costs(cost_data)
    
    # Print report
    print_cost_report(analysis, forecast_data)

if __name__ == "__main__":
    main() 