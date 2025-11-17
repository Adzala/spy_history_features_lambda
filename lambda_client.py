"""Lambda client for invoking SPY History Features Lambda."""

import json
import logging
import boto3
from typing import Dict, List, Any

logger = logging.getLogger(__name__)


class LambdaClient:
    """Client for invoking Lambda functions with batch events."""
    
    def __init__(self, function_name: str, region: str = 'us-east-1'):
        """
        Initialize Lambda client.
        
        Args:
            function_name: Name of the Lambda function to invoke
            region: AWS region (default: us-east-1)
        """
        self.function_name = function_name
        self.region = region
        self.client = boto3.client('lambda', region_name=region)
        
        logger.info(f"LambdaClient initialized: {function_name} in {region}")
    
    def invoke_batch(self, s3_uris: List[str]) -> Dict[str, Any]:
        """
        Invoke Lambda function with batch event.
        
        Args:
            s3_uris: List of S3 URIs to process
        
        Returns:
            Lambda response with parsed payload
        
        Raises:
            Exception: If Lambda invocation fails or returns error
        """
        # Construct batch event
        event = {
            'mode': 'batch',
            's3_uris': s3_uris
        }
        
        logger.info(f"Invoking Lambda {self.function_name} with {len(s3_uris)} URIs")
        logger.debug(f"Event payload: {json.dumps(event, indent=2)}")
        
        try:
            # Invoke Lambda synchronously
            response = self.client.invoke(
                FunctionName=self.function_name,
                InvocationType='RequestResponse',
                Payload=json.dumps(event)
            )
            
            # Parse response
            status_code = response['StatusCode']
            payload_bytes = response['Payload'].read()
            
            logger.debug(f"Lambda response status: {status_code}")
            
            # Check for Lambda execution errors
            if 'FunctionError' in response:
                function_error = response['FunctionError']
                logger.error(f"Lambda function error: {function_error}")
                
                # Try to parse error details from payload
                try:
                    error_payload = json.loads(payload_bytes)
                    error_message = error_payload.get('errorMessage', 'Unknown error')
                    error_type = error_payload.get('errorType', 'Unknown')
                    
                    raise Exception(f"Lambda function error ({error_type}): {error_message}")
                except json.JSONDecodeError:
                    raise Exception(f"Lambda function error: {function_error}")
            
            # Parse successful response payload
            try:
                result = json.loads(payload_bytes)
                logger.info(f"Lambda invocation successful: "
                          f"{result.get('success_count', 0)} succeeded, "
                          f"{result.get('failure_count', 0)} failed")
                
                return result
                
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse Lambda response payload: {e}")
                raise Exception(f"Invalid JSON response from Lambda: {e}")
        
        except Exception as e:
            logger.error(f"Lambda invocation failed: {e}")
            raise
