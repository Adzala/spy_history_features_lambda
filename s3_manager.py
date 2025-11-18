"""S3 operations manager for SPY History Features Lambda."""

import logging
import boto3
from botocore.config import Config as BotocoreConfig
from botocore.exceptions import ClientError
import pandas as pd
import io
import time
import random
from typing import Dict

from config import Config

logger = logging.getLogger(__name__)


class S3Manager:
    """Handles S3 read/write operations for parquet files."""
    
    def __init__(self):
        """Initialize S3Manager with boto3 client."""
        # Configure with larger connection pool for concurrent operations
        config = BotocoreConfig(
            max_pool_connections=50,  # Increased from default 10
            retries={'max_attempts': 3, 'mode': 'adaptive'}
        )
        
        self.s3_client = boto3.client('s3', config=config)
        logger.info("S3Manager initialized (max_pool_connections=50)")
    
    def read_parquet(self, s3_uri: str) -> pd.DataFrame:
        """
        Read parquet file from S3 with retry logic.
        
        Args:
            s3_uri: S3 URI (e.g., s3://bucket/key)
        
        Returns:
            DataFrame with parquet contents
        
        Raises:
            ValueError: If S3 URI format is invalid
            Exception: If S3 read fails after retries
        """
        bucket, key = self._parse_s3_uri(s3_uri)
        
        for attempt in range(Config.get_max_retries()):
            try:
                logger.debug(f"Reading parquet from s3://{bucket}/{key} (attempt {attempt + 1})")
                
                # Get object from S3
                response = self.s3_client.get_object(Bucket=bucket, Key=key)
                
                # Read parquet from bytes
                parquet_bytes = response['Body'].read()
                df = pd.read_parquet(io.BytesIO(parquet_bytes))
                
                logger.info(f"Successfully read parquet: {len(df)} rows, {len(df.columns)} columns")
                return df
                
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', '')
                
                if error_code in ['RequestLimitExceeded', 'SlowDown', 'ServiceUnavailable']:
                    # Transient error - retry with exponential backoff
                    if attempt < Config.get_max_retries() - 1:
                        wait_time = self._calculate_backoff(attempt)
                        logger.warning(f"S3 throttling/error ({error_code}), retrying in {wait_time:.2f}s")
                        time.sleep(wait_time)
                        continue
                
                # Non-retryable error or max retries reached
                logger.error(f"Failed to read from S3: {e}")
                raise
                
            except Exception as e:
                logger.error(f"Unexpected error reading parquet: {e}")
                raise
        
        raise Exception(f"Failed to read parquet after {Config.get_max_retries()} attempts")
    
    def write_parquet(self, df: pd.DataFrame, s3_uri: str, tags: Dict[str, str]) -> None:
        """
        Write parquet file to S3 with metadata tags and retry logic.
        
        Args:
            df: DataFrame to write
            s3_uri: Destination S3 URI
            tags: Object tags to apply (e.g., feature_version_hash, processing_timestamp)
        
        Raises:
            ValueError: If S3 URI format is invalid
            Exception: If S3 write fails after retries
        """
        bucket, key = self._parse_s3_uri(s3_uri)
        
        for attempt in range(Config.get_max_retries()):
            try:
                logger.debug(f"Writing parquet to s3://{bucket}/{key} (attempt {attempt + 1})")
                
                # Convert DataFrame to parquet bytes
                parquet_buffer = io.BytesIO()
                df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
                parquet_bytes = parquet_buffer.getvalue()
                
                # Upload to S3
                self.s3_client.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=parquet_bytes,
                    ContentType='application/octet-stream'
                )
                
                # Apply tags
                if tags:
                    tag_set = [{'Key': k, 'Value': v} for k, v in tags.items()]
                    self.s3_client.put_object_tagging(
                        Bucket=bucket,
                        Key=key,
                        Tagging={'TagSet': tag_set}
                    )
                
                logger.info(f"Successfully wrote parquet: {len(parquet_bytes)} bytes, {len(tags)} tags")
                return
                
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', '')
                
                if error_code in ['RequestLimitExceeded', 'SlowDown', 'ServiceUnavailable']:
                    # Transient error - retry with exponential backoff
                    if attempt < Config.get_max_retries() - 1:
                        wait_time = self._calculate_backoff(attempt)
                        logger.warning(f"S3 throttling/error ({error_code}), retrying in {wait_time:.2f}s")
                        time.sleep(wait_time)
                        continue
                
                # Non-retryable error or max retries reached
                logger.error(f"Failed to write to S3: {e}")
                raise
                
            except Exception as e:
                logger.error(f"Unexpected error writing parquet: {e}")
                raise
        
        raise Exception(f"Failed to write parquet after {Config.get_max_retries()} attempts")
    
    def compute_checksum(self, s3_uri: str) -> str:
        """
        Compute MD5 checksum of S3 object.
        
        Args:
            s3_uri: S3 URI
        
        Returns:
            MD5 checksum string (ETag without quotes)
        
        Raises:
            ValueError: If S3 URI format is invalid
            Exception: If checksum computation fails after retries
        """
        bucket, key = self._parse_s3_uri(s3_uri)
        
        for attempt in range(Config.get_max_retries()):
            try:
                logger.debug(f"Computing checksum for s3://{bucket}/{key} (attempt {attempt + 1})")
                
                # Get object metadata (ETag is MD5 for non-multipart uploads)
                response = self.s3_client.head_object(Bucket=bucket, Key=key)
                etag = response['ETag'].strip('"')
                
                logger.debug(f"Checksum computed: {etag}")
                return etag
                
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', '')
                
                if error_code in ['RequestLimitExceeded', 'SlowDown', 'ServiceUnavailable']:
                    # Transient error - retry with exponential backoff
                    if attempt < Config.get_max_retries() - 1:
                        wait_time = self._calculate_backoff(attempt)
                        logger.warning(f"S3 throttling/error ({error_code}), retrying in {wait_time:.2f}s")
                        time.sleep(wait_time)
                        continue
                
                # Non-retryable error or max retries reached
                logger.error(f"Failed to compute checksum: {e}")
                raise
                
            except Exception as e:
                logger.error(f"Unexpected error computing checksum: {e}")
                raise
        
        raise Exception(f"Failed to compute checksum after {Config.get_max_retries()} attempts")
    
    def _parse_s3_uri(self, s3_uri: str) -> tuple:
        """
        Parse S3 URI into bucket and key.
        
        Args:
            s3_uri: S3 URI (e.g., s3://bucket/key)
        
        Returns:
            Tuple of (bucket, key)
        
        Raises:
            ValueError: If URI format is invalid
        """
        if not s3_uri.startswith('s3://'):
            raise ValueError(f"Invalid S3 URI format: {s3_uri}")
        
        parts = s3_uri[5:].split('/', 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid S3 URI format: {s3_uri}")
        
        return parts[0], parts[1]
    
    def _calculate_backoff(self, attempt: int) -> float:
        """
        Calculate exponential backoff with jitter.
        
        Args:
            attempt: Retry attempt number (0-indexed)
        
        Returns:
            Wait time in seconds
        """
        base_delay = Config.get_retry_base_delay()
        max_delay = 60.0
        
        # Exponential backoff: base * 2^attempt
        delay = min(base_delay * (2 ** attempt), max_delay)
        
        # Add jitter (random 0-1 seconds)
        jitter = random.uniform(0, 1)
        
        return delay + jitter
