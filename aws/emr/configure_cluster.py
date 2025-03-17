#!/usr/bin/env python3
"""
Configure and launch an EMR cluster for the NYC Taxi Data Pipeline.
This script creates an EMR cluster with Apache Spark installed,
configured for cost optimization using spot instances and auto-termination.
"""

import boto3
import logging
import os
import sys
import json
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_emr_cluster(cluster_name, log_uri, ec2_key_name=None, keep_alive=False, 
                       instance_count=3, master_instance_type='m5.xlarge', 
                       slave_instance_type='m5.xlarge', use_spot=True):
    """
    Create an EMR cluster with Spark installed
    
    Args:
        cluster_name: Name of the cluster
        log_uri: S3 URI for EMR logs
        ec2_key_name: EC2 key pair name for SSH access
        keep_alive: Whether to keep the cluster alive after job completion
        instance_count: Number of instances in the cluster
        master_instance_type: EC2 instance type for the master node
        slave_instance_type: EC2 instance type for the core and task nodes
        use_spot: Whether to use spot instances for core and task nodes
        
    Returns:
        EMR cluster ID if successful, None otherwise
    """
    try:
        emr_client = boto3.client('emr')
        
        # Configure instance groups with spot instances for cost optimization
        instance_groups = [
            {
                'Name': 'Master',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': master_instance_type,
                'InstanceCount': 1,
            }
        ]
        
        # Add core nodes
        if instance_count > 1:
            core_group = {
                'Name': 'Core',
                'InstanceRole': 'CORE',
                'InstanceType': slave_instance_type,
                'InstanceCount': instance_count - 1,
            }
            
            if use_spot:
                core_group['Market'] = 'SPOT'
                core_group['BidPrice'] = '0.1'  # Set a reasonable bid price
            else:
                core_group['Market'] = 'ON_DEMAND'
                
            instance_groups.append(core_group)
        
        # Configure applications
        applications = [
            {'Name': 'Spark'},
            {'Name': 'Hadoop'},
            {'Name': 'Hive'},
            {'Name': 'Livy'}
        ]
        
        # Configure bootstrap actions
        bootstrap_actions = [
            {
                'Name': 'Install Python packages',
                'ScriptBootstrapAction': {
                    'Path': 's3://aws-bigdata-blog/artifacts/aws-blog-emr-jupyter/bootstrap_python.sh'
                }
            }
        ]
        
        # Configure EMR configurations
        configurations = [
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
        ]
        
        # Create the cluster
        response = emr_client.run_job_flow(
            Name=cluster_name,
            LogUri=log_uri,
            ReleaseLabel='emr-6.5.0',
            Applications=applications,
            Instances={
                'InstanceGroups': instance_groups,
                'Ec2KeyName': ec2_key_name,
                'KeepJobFlowAliveWhenNoSteps': keep_alive,
                'TerminationProtected': False,
                'Ec2SubnetId': 'subnet-12345678'  # Replace with your subnet ID
            },
            BootstrapActions=bootstrap_actions,
            Configurations=configurations,
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
            Tags=[
                {
                    'Key': 'Project',
                    'Value': 'NYC-Taxi-Pipeline'
                },
                {
                    'Key': 'Environment',
                    'Value': os.environ.get('ENVIRONMENT', 'dev')
                }
            ]
        )
        
        cluster_id = response['JobFlowId']
        logger.info(f"Created EMR cluster with ID: {cluster_id}")
        
        # Save the cluster configuration to a file for reference
        config = {
            'cluster_id': cluster_id,
            'cluster_name': cluster_name,
            'instance_count': instance_count,
            'master_instance_type': master_instance_type,
            'slave_instance_type': slave_instance_type,
            'use_spot': use_spot,
            'keep_alive': keep_alive
        }
        
        with open('emr_cluster_config.json', 'w') as f:
            json.dump(config, f, indent=2)
            
        return cluster_id
    
    except ClientError as e:
        logger.error(f"Error creating EMR cluster: {e}")
        return None

def main():
    """Main function to configure and launch an EMR cluster"""
    # Load environment variables or use defaults
    aws_region = os.environ.get('AWS_REGION', 'us-east-1')
    project_name = os.environ.get('PROJECT_NAME', 'nyc-taxi-pipeline')
    environment = os.environ.get('ENVIRONMENT', 'dev')
    
    # Set cluster parameters
    cluster_name = f"{project_name}-{environment}-cluster"
    log_uri = f"s3://{project_name}-{environment}-logs/emr-logs/"
    ec2_key_name = os.environ.get('EC2_KEY_NAME')  # Optional for SSH access
    
    # Cost optimization parameters
    keep_alive = os.environ.get('KEEP_ALIVE', 'false').lower() == 'true'
    instance_count = int(os.environ.get('INSTANCE_COUNT', '3'))
    master_instance_type = os.environ.get('MASTER_INSTANCE_TYPE', 'm5.xlarge')
    slave_instance_type = os.environ.get('SLAVE_INSTANCE_TYPE', 'm5.xlarge')
    use_spot = os.environ.get('USE_SPOT', 'true').lower() == 'true'
    
    # Create the EMR cluster
    cluster_id = create_emr_cluster(
        cluster_name=cluster_name,
        log_uri=log_uri,
        ec2_key_name=ec2_key_name,
        keep_alive=keep_alive,
        instance_count=instance_count,
        master_instance_type=master_instance_type,
        slave_instance_type=slave_instance_type,
        use_spot=use_spot
    )
    
    if cluster_id:
        logger.info(f"EMR cluster setup completed successfully. Cluster ID: {cluster_id}")
        print(f"EMR Cluster ID: {cluster_id}")
    else:
        logger.error("Failed to create EMR cluster")
        sys.exit(1)

if __name__ == "__main__":
    main() 