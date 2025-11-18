"""Date thread for processing files for a single date."""

import logging
import time
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
    
    def process(self, batch_size: int = 45, max_retries: int = 3) -> Dict[str, Any]:
        """
        Process all files for this date by invoking Lambda in batches.
        
        Args:
            batch_size: Number of files per Lambda invocation (default: 45)
            max_retries: Maximum retry attempts for failed batches (default: 3)
        
        Returns:
            Result dictionary with:
                - date: Date string
                - completed: Number of successfully processed files
                - failed: Number of failed files
                - failures: List of failure details (if any)
        
        Raises:
            Exception: If Lambda invocation fails after retries
        """
        logger.info(f"Processing date {self.date}: {len(self.s3_uris)} files in batches of {batch_size}")
        
        # Split files into batches
        batches = [self.s3_uris[i:i + batch_size] for i in range(0, len(self.s3_uris), batch_size)]
        logger.info(f"Date {self.date}: Split into {len(batches)} batches")
        
        # Track results
        total_success = 0
        total_failed = 0
        all_failures = []
        pending_files = []
        
        # Process each batch with retry logic
        for batch_idx, batch in enumerate(batches):
            logger.info(f"Date {self.date}: Processing batch {batch_idx + 1}/{len(batches)} ({len(batch)} files)")
            
            # Add delay between batches to avoid rate limiting (except for first batch)
            if batch_idx > 0:
                time.sleep(0.5)  # 500ms delay between Lambda invocations
            
            # Try to process this batch with retries
            batch_success = False
            batch_attempt = 0
            
            while not batch_success and batch_attempt < max_retries:
                try:
                    if batch_attempt > 0:
                        logger.info(f"Date {self.date} batch {batch_idx + 1}: Retry attempt {batch_attempt}/{max_retries - 1}")
                        time.sleep(2 ** batch_attempt)  # Exponential backoff: 2s, 4s, 8s
                    
                    # Invoke Lambda with this batch
                    response = self.lambda_client.invoke_batch(batch)
                    
                    # Extract results
                    success_count = response.get('success_count', 0)
                    failure_count = response.get('failure_count', 0)
                    failures = response.get('failures', [])
                    unprocessed = response.get('unprocessed_files', [])
                    
                    # Update totals
                    total_success += success_count
                    total_failed += failure_count
                    all_failures.extend(failures)
                    
                    # Add unprocessed files to pending for retry
                    if unprocessed:
                        logger.warning(f"Date {self.date} batch {batch_idx + 1}: {len(unprocessed)} files unprocessed, will retry later")
                        pending_files.extend(unprocessed)
                    
                    # Batch succeeded (even if some files failed)
                    batch_success = True
                    
                except Exception as e:
                    batch_attempt += 1
                    logger.error(f"Date {self.date} batch {batch_idx + 1} attempt {batch_attempt} failed: {e}")
                    
                    if batch_attempt >= max_retries:
                        # Max retries reached, mark all files in this batch as failed
                        logger.error(f"Date {self.date} batch {batch_idx + 1}: Failed after {max_retries} attempts")
                        total_failed += len(batch)
                        for uri in batch:
                            all_failures.append({
                                'uri': uri,
                                'error': str(e),
                                'error_type': type(e).__name__
                            })
                        break
        
        # Retry unprocessed files (files that Lambda returned as unprocessed)
        retry_count = 0
        while pending_files and retry_count < max_retries:
            retry_count += 1
            logger.info(f"Date {self.date}: Retry {retry_count}/{max_retries} for {len(pending_files)} unprocessed files")
            
            # Split pending files into batches
            retry_batches = [pending_files[i:i + batch_size] for i in range(0, len(pending_files), batch_size)]
            pending_files = []  # Clear pending list
            
            for retry_batch_idx, retry_batch in enumerate(retry_batches):
                # Add delay between retry batches
                if retry_batch_idx > 0:
                    time.sleep(0.5)
                
                try:
                    response = self.lambda_client.invoke_batch(retry_batch)
                    
                    total_success += response.get('success_count', 0)
                    total_failed += response.get('failure_count', 0)
                    all_failures.extend(response.get('failures', []))
                    
                    # Add any still-unprocessed files back to pending
                    unprocessed = response.get('unprocessed_files', [])
                    if unprocessed:
                        pending_files.extend(unprocessed)
                        
                except Exception as e:
                    # Retry batch failed, mark all files as failed
                    logger.error(f"Date {self.date} retry batch {retry_batch_idx + 1} failed: {e}")
                    total_failed += len(retry_batch)
                    for uri in retry_batch:
                        all_failures.append({
                            'uri': uri,
                            'error': str(e),
                            'error_type': type(e).__name__
                        })
        
        # Mark any remaining unprocessed files as failed
        if pending_files:
            logger.error(f"Date {self.date}: {len(pending_files)} files still unprocessed after {max_retries} retries")
            total_failed += len(pending_files)
            for uri in pending_files:
                all_failures.append({
                    'uri': uri,
                    'error': f'Unprocessed after {max_retries} retries',
                    'error_type': 'MaxRetriesExceeded'
                })
        
        # Return result summary
        result = {
            'date': self.date,
            'completed': total_success,
            'failed': total_failed,
            'failures': all_failures
        }
        
        logger.info(f"Date {self.date} complete: {result['completed']} succeeded, "
                   f"{result['failed']} failed")
        
        return result
