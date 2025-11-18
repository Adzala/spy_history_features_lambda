"""Lambda client for invoking SPY History Features Lambda."""

import json
import logging
import time
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
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
        
        # Configure with larger connection pool and extended timeout for long-running Lambda
        config = Config(
            max_pool_connections=50,  # Increased from default 10
            retries={'max_attempts': 3, 'mode': 'adaptive'},
            read_timeout=1200,  # 20 minutes for long-running batches
            connect_timeout=10  # 10 seconds for connection
        )
        
        self.client = boto3.client('lambda', region_name=region, config=config)
        
        logger.info(f"LambdaClient initialized: {function_name} in {region} (max_pool_connections=50)")
    
    def invoke_batch(self, s3_uris: List[str], max_retries: int = 5) -> Dict[str, Any]:
        """
        Invoke Lambda function with batch event with retry logic for rate limits.
        
        Args:
            s3_uris: List of S3 URIs to process
            max_retries: Maximum number of retries for rate limit errors (default: 5)
        
        Returns:
            Lambda response with parsed payload
        
        Raises:
            Exception: If Lambda invocation fails or returns error after retries
        """
        # Construct batch event
        event = {
            'mode': 'batch',
            's3_uris': s3_uris
        }
        
        logger.info(f"Invoking Lambda {self.function_name} with {len(s3_uris)} URIs")
        logger.debug(f"Event payload: {json.dumps(event, indent=2)}")
        
        # Retry loop for rate limit errors
        for attempt in range(max_retries + 1):
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
            
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', '')
                
                # Check if it's a rate limit error
                if error_code == 'TooManyRequestsException':
                    if attempt < max_retries:
                        # Calculate exponential backoff: 2^attempt seconds
                        wait_time = min(2 ** attempt, 32)  # Cap at 32 seconds
                        logger.warning(f"Rate limit exceeded, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Rate limit exceeded after {max_retries} retries")
                        raise
                else:
                    # Non-retryable error
                    logger.error(f"Lambda invocation failed: {e}")
                    raise
            
            except Exception as e:
                logger.error(f"Lambda invocation failed: {e}")
                raise
        
        # Should never reach here, but just in case
        raise Exception(f"Failed to invoke Lambda after {max_retries} retries")
