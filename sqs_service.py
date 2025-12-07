"""
AWS SQS integration for Marathon Agent
Handles sending/receiving messages for error detection and approval events
"""
import os
import json
import logging
import boto3
from typing import Dict, Any, Optional, List

# Configure logging
logger = logging.getLogger("marathon-agent-sqs")
logger.setLevel(logging.INFO)

class SQSService:
    """Service to integrate with AWS SQS"""
    
    def __init__(self):
        """Initialize SQS client and queues"""
        self.aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
        self.aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
        self.aws_session_token = os.environ.get('AWS_SESSION_TOKEN')
        self.aws_region = os.environ.get('AWS_REGION', 'us-east-1')
        
        # Queue names
        self.error_queue_name = os.environ.get('ERROR_QUEUE_NAME', 'marathon-agent-errors')
        self.approval_queue_name = os.environ.get('APPROVAL_QUEUE_NAME', 'marathon-agent-approvals')
        
        self.sqs_client = None
        self.error_queue_url = None
        self.approval_queue_url = None
        
        # Initialize client
        self._init_sqs()
    
    def _init_sqs(self) -> None:
        """Initialize SQS client and get/create queues"""
        try:
            # Initialize AWS SQS client
            kwargs = {
                'region_name': self.aws_region
            }
            
            # Add credentials if available
            if self.aws_access_key and self.aws_secret_key:
                kwargs.update({
                    'aws_access_key_id': self.aws_access_key,
                    'aws_secret_access_key': self.aws_secret_key
                })
                
                if self.aws_session_token:
                    kwargs['aws_session_token'] = self.aws_session_token
            
            self.sqs_client = boto3.client('sqs', **kwargs)
            
            # Create or get error queue
            self.error_queue_url = self._get_or_create_queue(self.error_queue_name)
            
            # Create or get approval queue
            self.approval_queue_url = self._get_or_create_queue(self.approval_queue_name)
            
            logger.info(f"SQS service initialized with error queue: {self.error_queue_name}")
            logger.info(f"SQS service initialized with approval queue: {self.approval_queue_name}")
            
        except Exception as e:
            logger.error(f"Error initializing SQS client: {str(e)}")
            self.sqs_client = None
    
    def _get_or_create_queue(self, queue_name: str) -> Optional[str]:
        """Get queue URL or create if it doesn't exist"""
        if not self.sqs_client:
            logger.error("SQS client not initialized")
            return None
            
        try:
            # Try to get the queue URL
            response = self.sqs_client.get_queue_url(QueueName=queue_name)
            return response['QueueUrl']
        except self.sqs_client.exceptions.QueueDoesNotExist:
            # Create the queue if it doesn't exist
            logger.info(f"Creating SQS queue: {queue_name}")
            response = self.sqs_client.create_queue(
                QueueName=queue_name,
                Attributes={
                    'DelaySeconds': '0',
                    'MessageRetentionPeriod': '86400'  # 24 hours
                }
            )
            return response['QueueUrl']
        except Exception as e:
            logger.error(f"Error getting/creating queue {queue_name}: {str(e)}")
            return None
    
    def send_error_message(self, error_data: Dict[str, Any]) -> bool:
        """Send a message to the error queue"""
        return self._send_message(self.error_queue_url, error_data, "error")
    
    def send_approval_message(self, approval_data: Dict[str, Any]) -> bool:
        """Send a message to the approval queue"""
        return self._send_message(self.approval_queue_url, approval_data, "approval")
    
    def _send_message(self, queue_url: str, message_data: Dict[str, Any], message_type: str) -> bool:
        """Send a message to the specified queue"""
        if not self.sqs_client or not queue_url:
            logger.error(f"Cannot send {message_type} message: SQS client or queue URL not initialized")
            return False
            
        try:
            # Add message type to the data
            message_data['message_type'] = message_type
            
            # Convert to JSON
            message_body = json.dumps(message_data)
            
            # Send message
            response = self.sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=message_body,
                MessageAttributes={
                    'Type': {
                        'StringValue': message_type,
                        'DataType': 'String'
                    }
                }
            )
            
            logger.info(f"Sent {message_type} message to SQS: {response['MessageId']}")
            return True
        except Exception as e:
            logger.error(f"Error sending {message_type} message to SQS: {str(e)}")
            return False
    
    def receive_messages(self, queue_type: str = "error", max_messages: int = 10) -> List[Dict[str, Any]]:
        """Receive messages from the specified queue"""
        queue_url = self.error_queue_url if queue_type == "error" else self.approval_queue_url
        
        if not self.sqs_client or not queue_url:
            logger.error(f"Cannot receive messages: SQS client or queue URL not initialized")
            return []
            
        try:
            # Receive messages
            response = self.sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=1,
                MessageAttributeNames=['Type'],
                AttributeNames=['All']
            )
            
            messages = []
            if 'Messages' in response:
                for message in response['Messages']:
                    # Parse message body
                    try:
                        message_body = json.loads(message['Body'])
                        message_body['receipt_handle'] = message['ReceiptHandle']
                        messages.append(message_body)
                    except json.JSONDecodeError:
                        logger.error(f"Failed to parse message body as JSON: {message['Body']}")
                        
                logger.info(f"Received {len(messages)} messages from {queue_type} queue")
            
            return messages
        except Exception as e:
            logger.error(f"Error receiving messages from {queue_type} queue: {str(e)}")
            return []
    
    def delete_message(self, queue_type: str, receipt_handle: str) -> bool:
        """Delete a message from the queue"""
        queue_url = self.error_queue_url if queue_type == "error" else self.approval_queue_url
        
        if not self.sqs_client or not queue_url:
            logger.error(f"Cannot delete message: SQS client or queue URL not initialized")
            return False
            
        try:
            self.sqs_client.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
            logger.info(f"Deleted message from {queue_type} queue")
            return True
        except Exception as e:
            logger.error(f"Error deleting message from {queue_type} queue: {str(e)}")
            return False

# Create singleton instance
sqs_service = SQSService()
