"""Date thread for processing files for a single date."""

import logging
from typing import Dict, List, Any

from lambda_client import LambdaClient

logger = logging.getLogger(__name__)


class DateThread:
    """Thread for processing all files for a single date."""
    
    def __init__(self, date: str, s3_uris: List[str], lambda_client: LambdaClient):
        """
        Initialize DateThread.
        
        Args:
            date: Date string (YYYYMMDD)
            s3_uris: List of S3 URIs for this date (chronologically sorted)
            lambda_client: LambdaClient instance for invoking Lambda
        """
        self.date = date
        self.s3_uris = s3_uris
        self.lambda_client = lambda_client
        
        logger.debug(f"DateThread created for {date} with {len(s3_uris)} files")
    
    def process(self) -> Dict[str, Any]:
        """
        Process all files for this date by invoking Lambda.
        
        Returns:
            Result dictionary with:
                - date: Date string
                - completed: Number of successfully processed files
                - failed: Number of failed files
                - failures: List of failure details (if any)
        
        Raises:
            Exception: If Lambda invocation fails
        """
        logger.info(f"Processing date {self.date}: {len(self.s3_uris)} files")
        
        try:
            # Invoke Lambda with all URIs for this date
            response = self.lambda_client.invoke_batch(self.s3_uris)
            
            # Extract results from Lambda response
            success_count = response.get('success_count', 0)
            failure_count = response.get('failure_count', 0)
            failures = response.get('failures', [])
            unprocessed = response.get('unprocessed_files', [])
            
            # Log unprocessed files if any (due to timeout)
            if unprocessed:
                logger.warning(f"Date {self.date}: {len(unprocessed)} files unprocessed due to timeout")
            
            # Return result summary
            result = {
                'date': self.date,
                'completed': success_count,
                'failed': failure_count + len(unprocessed),  # Count unprocessed as failed
                'failures': failures
            }
            
            # Add unprocessed files to failures list
            if unprocessed:
                for uri in unprocessed:
                    result['failures'].append({
                        'uri': uri,
                        'error': 'Unprocessed due to Lambda timeout',
                        'error_type': 'TimeoutError'
                    })
            
            logger.info(f"Date {self.date} complete: {result['completed']} succeeded, "
                       f"{result['failed']} failed")
            
            return result
            
        except Exception as e:
            # Lambda invocation failed - return error result
            logger.error(f"Date {self.date} failed: {e}")
            
            return {
                'date': self.date,
                'completed': 0,
                'failed': len(self.s3_uris),
                'failures': [{
                    'uri': 'all',
                    'error': str(e),
                    'error_type': type(e).__name__
                }]
            }
