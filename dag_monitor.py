"""
DAG monitoring service for Marathon Agent
Monitors S3 and CloudWatch for DAG errors
"""
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

from .db_models import dag_db

# Configure logging
logger = logging.getLogger("marathon-agent-dag-monitor")
logger.setLevel(logging.INFO)

class DAGMonitor:
    """Service to monitor DAGs for errors"""
    
    def __init__(self, 
                region_name: Optional[str] = None,
                aws_access_key_id: Optional[str] = None,
                aws_secret_access_key: Optional[str] = None,
                aws_session_token: Optional[str] = None,
                s3_bucket: Optional[str] = None,
                cloudwatch_log_group: Optional[str] = None):
        """Initialize the DAG monitor"""
        self.region = region_name or os.getenv('AWS_REGION', 'us-west-2')
        self.aws_access_key_id = aws_access_key_id or os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = aws_secret_access_key or os.getenv('AWS_SECRET_ACCESS_KEY')
        self.aws_session_token = aws_session_token or os.getenv('AWS_SESSION_TOKEN')
        self.s3_bucket = s3_bucket or os.getenv('S3_DAG_BUCKET', 'marathon-agent-dags')
        self.log_group = cloudwatch_log_group or os.getenv('CLOUDWATCH_LOG_GROUP', '/aws/lambda/marathon-agent-dags')
        
        # Initialize AWS clients
        self._init_clients()
        
        # DAG files we're monitoring
        self.monitored_dags = ['extraction_dag.py', 'transformation_dag.py', 'loading_dag.py']
    
    def _init_clients(self):
        """Initialize AWS clients"""
        try:
            # Create a session with credentials
            session_kwargs = {
                'region_name': self.region
            }
            
            if self.aws_access_key_id and self.aws_secret_access_key:
                session_kwargs.update({
                    'aws_access_key_id': self.aws_access_key_id,
                    'aws_secret_access_key': self.aws_secret_access_key
                })
                
                if self.aws_session_token:
                    session_kwargs['aws_session_token'] = self.aws_session_token
            
            session = boto3.Session(**session_kwargs)
            
            # Create service clients
            self.s3_client = session.client('s3')
            self.logs_client = session.client('logs')
            self.lambda_client = session.client('lambda')
            
            logger.info("AWS clients initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize AWS clients: {str(e)}")
            # Set clients to None to indicate initialization failed
            self.s3_client = None
            self.logs_client = None
            self.lambda_client = None
    
    def check_s3_for_dags(self) -> List[Dict[str, Any]]:
        """Check S3 bucket for DAG files"""
        if not self.s3_client:
            logger.error("S3 client not initialized")
            return []
        
        try:
            # List objects in the bucket
            response = self.s3_client.list_objects_v2(Bucket=self.s3_bucket)
            
            # Filter for DAG files
            dag_files = []
            for obj in response.get('Contents', []):
                key = obj['Key']
                if key in self.monitored_dags:
                    dag_files.append({
                        'dag_id': os.path.splitext(key)[0],
                        's3_file_path': f"s3://{self.s3_bucket}/{key}",
                        'last_modified': obj['LastModified'].isoformat(),
                        'size': obj['Size']
                    })
            
            logger.info(f"Found {len(dag_files)} DAG files in S3 bucket")
            return dag_files
            
        except ClientError as e:
            logger.error(f"S3 error: {str(e)}")
            return []
    
    def get_cloudwatch_logs(self, 
                          dag_id: str, 
                          hours_back: int = 1) -> List[Dict[str, Any]]:
        """Get CloudWatch logs for a DAG"""
        if not self.logs_client:
            logger.error("CloudWatch Logs client not initialized")
            return []
        
        try:
            # Calculate time range
            end_time = int(datetime.now().timestamp() * 1000)
            start_time = int((datetime.now() - timedelta(hours=hours_back)).timestamp() * 1000)
            
            # Construct log stream name (may need adjustment based on actual naming convention)
            date_prefix = datetime.now().strftime("%Y/%m/%d")
            log_stream_name = f"{dag_id}/{date_prefix}"
            
            # Try to get the log events
            try:
                response = self.logs_client.get_log_events(
                    logGroupName=self.log_group,
                    logStreamName=log_stream_name,
                    startTime=start_time,
                    endTime=end_time,
                    limit=100  # Adjust as needed
                )
                
                # Extract events
                events = [{
                    'timestamp': event['timestamp'],
                    'message': event['message'].strip()
                } for event in response.get('events', [])]
                
                logger.info(f"Retrieved {len(events)} log events for {dag_id}")
                return events
                
            except ClientError as e:
                if 'ResourceNotFoundException' in str(e):
                    logger.warning(f"Log stream {log_stream_name} not found")
                    # Try with just the DAG ID as stream name
                    try:
                        response = self.logs_client.get_log_events(
                            logGroupName=self.log_group,
                            logStreamName=dag_id,
                            startTime=start_time,
                            endTime=end_time,
                            limit=100
                        )
                        
                        events = [{
                            'timestamp': event['timestamp'],
                            'message': event['message'].strip()
                        } for event in response.get('events', [])]
                        
                        logger.info(f"Retrieved {len(events)} log events for {dag_id} (fallback)")
                        return events
                        
                    except ClientError:
                        logger.warning(f"No logs found for {dag_id}")
                        return []
                else:
                    raise
            
        except Exception as e:
            logger.error(f"Error retrieving CloudWatch logs: {str(e)}")
            return []
    
    def detect_errors_in_logs(self, logs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Detect errors in CloudWatch logs"""
        if not logs:
            return []
        
        # Error patterns to look for
        error_patterns = [
            (r'error', 'general_error'),
            (r'exception', 'exception'),
            (r'traceback', 'traceback'),
            (r'failed', 'failure'),
            (r'api rate limit exceeded', 'rate_limit'),
            (r'authentication failed', 'auth_error'),
            (r'timeout', 'timeout'),
            (r'database connection', 'db_connection'),
            (r'constraint violation', 'constraint_violation'),
            (r'validation error', 'validation_error')
        ]
        
        # Look for errors in the logs
        errors_found = []
        
        for i, event in enumerate(logs):
            message = event['message'].lower()
            
            for pattern, error_type in error_patterns:
                if re.search(pattern, message):
                    # Found an error, collect context
                    context_start = max(0, i - 5)
                    context_end = min(len(logs), i + 5)
                    context_logs = logs[context_start:context_end]
                    
                    # Extract full error message (current and next few lines)
                    error_message = event['message']
                    for j in range(i+1, min(i+5, len(logs))):
                        if 'traceback' in logs[j]['message'].lower() or 'error:' in logs[j]['message'].lower():
                            error_message += "\n" + logs[j]['message']
                    
                    errors_found.append({
                        'error_type': error_type,
                        'error_message': error_message,
                        'timestamp': event['timestamp'],
                        'context_logs': context_logs
                    })
                    
                    # Skip to after this error to avoid duplicates
                    break
        
        logger.info(f"Found {len(errors_found)} errors in the logs")
        return errors_found
    
    def monitor_dags(self) -> List[Dict[str, Any]]:
        """Main monitoring function that checks all DAGs for errors"""
        # Get DAG files from S3
        dag_files = self.check_s3_for_dags()
        
        all_errors = []
        
        # Check each DAG for errors
        for dag_file in dag_files:
            dag_id = dag_file['dag_id']
            s3_path = dag_file['s3_file_path']
            
            logger.info(f"Monitoring {dag_id} from {s3_path}")
            
            # Get logs from CloudWatch
            logs = self.get_cloudwatch_logs(dag_id)
            
            # Detect errors
            errors = self.detect_errors_in_logs(logs)
            
            # Process each error
            for error in errors:
                # Create error entry
                log_data = json.dumps([log['message'] for log in error['context_logs']])
                
                error_entry = {
                    'dag_id': dag_id,
                    'error_type': error['error_type'],
                    'error_message': error['error_message'],
                    'log_data': log_data,
                    's3_file_path': s3_path,
                    'cloudwatch_log_group': self.log_group,
                    'cloudwatch_log_stream': f"{dag_id}/{datetime.now().strftime('%Y/%m/%d')}"
                }
                
                # Add to database
                error_id = dag_db.add_dag_error(**error_entry)
                error_entry['id'] = error_id
                all_errors.append(error_entry)
                
                logger.info(f"Registered error in {dag_id}: {error['error_type']}")
        
        return all_errors

# Create singleton instance
dag_monitor = DAGMonitor()
