"""Utility functions for SPY History Features Lambda."""

import re
from datetime import datetime, time
from typing import Tuple, Dict


def parse_cache_key(filename: str) -> Tuple[str, str]:
    """
    Extract date and minute from filename.
    
    Args:
        filename: Filename in format strikes_YYYYMMDDHHMM.parquet
                 Can be full path or just filename
    
    Returns:
        Tuple of (date, minute) where:
            - date: YYYYMMDD string (e.g., "20250325")
            - minute: HHMM string (e.g., "0935")
    
    Raises:
        ValueError: If filename format is invalid
    
    Examples:
        >>> parse_cache_key("strikes_202503250935.parquet")
        ("20250325", "0935")
        >>> parse_cache_key("s3://bucket/path/strikes_202503250935.parquet")
        ("20250325", "0935")
    """
    # Extract just the filename if full path provided
    if '/' in filename:
        filename = filename.split('/')[-1]
    
    # Validate filename format
    pattern = r'^strikes_(\d{8})(\d{4})\.parquet$'
    match = re.match(pattern, filename)
    
    if not match:
        raise ValueError(
            f"Invalid filename format: '{filename}'. "
            f"Expected format: strikes_YYYYMMDDHHMM.parquet"
        )
    
    date = match.group(1)
    minute = match.group(2)
    
    # Validate date format
    try:
        datetime.strptime(date, '%Y%m%d')
    except ValueError as e:
        raise ValueError(f"Invalid date in filename '{filename}': {e}")
    
    # Validate time format (HHMM)
    try:
        hour = int(minute[:2])
        min_val = int(minute[2:])
        if not (0 <= hour <= 23 and 0 <= min_val <= 59):
            raise ValueError(f"Invalid time values: hour={hour}, minute={min_val}")
    except (ValueError, IndexError) as e:
        raise ValueError(f"Invalid time in filename '{filename}': {e}")
    
    return date, minute


def compute_sequence_info(filename: str) -> Dict[str, int]:
    """
    Calculate minutes since market open (9:30 AM).
    
    Args:
        filename: Filename in format strikes_YYYYMMDDHHMM.parquet
    
    Returns:
        Dictionary containing:
            - sequence_number: Minutes since market open (0-indexed)
            - hour: Hour component (0-23)
            - minute: Minute component (0-59)
    
    Raises:
        ValueError: If filename format is invalid
    
    Examples:
        >>> compute_sequence_info("strikes_202503250930.parquet")
        {'sequence_number': 0, 'hour': 9, 'minute': 30}
        >>> compute_sequence_info("strikes_202503251000.parquet")
        {'sequence_number': 30, 'hour': 10, 'minute': 0}
        >>> compute_sequence_info("strikes_202503251600.parquet")
        {'sequence_number': 390, 'hour': 16, 'minute': 0}
    """
    date, minute_str = parse_cache_key(filename)
    
    # Parse hour and minute
    hour = int(minute_str[:2])
    minute = int(minute_str[2:])
    
    # Market opens at 9:30 AM
    market_open = time(9, 30)
    market_open_minutes = market_open.hour * 60 + market_open.minute
    
    # Current time in minutes since midnight
    current_minutes = hour * 60 + minute
    
    # Calculate minutes since market open
    sequence_number = current_minutes - market_open_minutes
    
    return {
        'sequence_number': sequence_number,
        'hour': hour,
        'minute': minute
    }


def validate_filename_format(filename: str) -> bool:
    """
    Validate that filename matches expected format.
    
    Args:
        filename: Filename to validate
    
    Returns:
        True if valid, False otherwise
    
    Examples:
        >>> validate_filename_format("strikes_202503250935.parquet")
        True
        >>> validate_filename_format("invalid_file.parquet")
        False
    """
    try:
        parse_cache_key(filename)
        return True
    except ValueError:
        return False


def extract_timestamp_int(filename: str) -> int:
    """
    Extract timestamp as integer for chronological ordering.
    
    Args:
        filename: Filename in format strikes_YYYYMMDDHHMM.parquet
    
    Returns:
        Integer timestamp in format YYYYMMDDHHMM (e.g., 202503250935)
    
    Raises:
        ValueError: If filename format is invalid
    
    Examples:
        >>> extract_timestamp_int("strikes_202503250935.parquet")
        202503250935
    """
    date, minute = parse_cache_key(filename)
    return int(date + minute)
