"""Configuration management for SPY History Features Lambda."""

import os
from typing import Optional


class Config:
    """Configuration loaded from environment variables."""
    
    # S3 Configuration
    SOURCE_BUCKET: str = os.environ.get('SOURCE_BUCKET', 'spy-no-history-features')
    DEST_BUCKET: str = os.environ.get('DEST_BUCKET', 'spy-with-history-features')
    
    # History Management Configuration
    HISTORY_WINDOW_SIZE: int = int(os.environ.get('HISTORY_WINDOW_SIZE', '300'))
    
    # Processing Configuration
    NUMERIC_PRECISION: int = int(os.environ.get('NUMERIC_PRECISION', '4'))
    TIMEOUT_BUFFER_SECONDS: int = int(os.environ.get('TIMEOUT_BUFFER_SECONDS', '5'))
    
    # Retry Configuration
    MAX_RETRIES: int = int(os.environ.get('MAX_RETRIES', '3'))
    RETRY_BASE_DELAY: float = float(os.environ.get('RETRY_BASE_DELAY', '1.0'))
    
    # Logging Configuration
    LOG_LEVEL: str = os.environ.get('LOG_LEVEL', 'INFO')
    
    @classmethod
    def validate(cls) -> None:
        """
        Validate required configuration is present.
        
        Raises:
            ValueError: If required configuration is missing or invalid
        """
        required_vars = {
            'SOURCE_BUCKET': cls.SOURCE_BUCKET,
            'DEST_BUCKET': cls.DEST_BUCKET,
        }
        
        missing = [name for name, value in required_vars.items() if not value]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
        
        # Validate numeric values
        if cls.HISTORY_WINDOW_SIZE <= 0:
            raise ValueError(f"HISTORY_WINDOW_SIZE must be positive, got {cls.HISTORY_WINDOW_SIZE}")
        
        if cls.NUMERIC_PRECISION < 0:
            raise ValueError(f"NUMERIC_PRECISION must be non-negative, got {cls.NUMERIC_PRECISION}")
        
        if cls.TIMEOUT_BUFFER_SECONDS < 0:
            raise ValueError(f"TIMEOUT_BUFFER_SECONDS must be non-negative, got {cls.TIMEOUT_BUFFER_SECONDS}")
    
    @classmethod
    def get_source_bucket(cls) -> str:
        """Get source S3 bucket name."""
        return cls.SOURCE_BUCKET
    
    @classmethod
    def get_dest_bucket(cls) -> str:
        """Get destination S3 bucket name."""
        return cls.DEST_BUCKET
    
    @classmethod
    def get_history_window_size(cls) -> int:
        """Get history window size in minutes."""
        return cls.HISTORY_WINDOW_SIZE
    
    @classmethod
    def get_numeric_precision(cls) -> int:
        """Get numeric rounding precision."""
        return cls.NUMERIC_PRECISION
    
    @classmethod
    def get_timeout_buffer(cls) -> int:
        """Get timeout buffer in seconds."""
        return cls.TIMEOUT_BUFFER_SECONDS
    
    @classmethod
    def get_max_retries(cls) -> int:
        """Get maximum retry attempts."""
        return cls.MAX_RETRIES
    
    @classmethod
    def get_retry_base_delay(cls) -> float:
        """Get base delay for exponential backoff."""
        return cls.RETRY_BASE_DELAY
    
    @classmethod
    def get_log_level(cls) -> str:
        """Get logging level."""
        return cls.LOG_LEVEL


# Validate configuration on module import
try:
    Config.validate()
except ValueError as e:
    # Log warning but don't fail - allows module to be imported for testing
    import logging
    logging.warning(f"Configuration validation failed: {e}")
