"""
HistoryManager: Maintains a rolling window of minute-level DataFrames for computing lookback features.

This module provides temporal validation to prevent look-ahead bias by ensuring that
timestamps are strictly increasing when adding new data to the history queue.
"""

from collections import deque
from typing import Optional, List, Tuple
import pandas as pd


class HistoryManager:
    """
    Manages a rolling FIFO queue of minute-level DataFrames for computing lookback features.
    
    The queue maintains chronological ordering and validates that new data timestamps
    are strictly greater than existing timestamps to prevent look-ahead bias.
    """
    
    def __init__(self, window_size: int):
        """
        Initialize HistoryManager with a maximum window size.
        
        Args:
            window_size: Maximum number of minutes to maintain in the queue (e.g., 300)
        """
        self.window_size = window_size
        self.queue: deque[Tuple[int, str, str, pd.DataFrame]] = deque(maxlen=window_size)
    
    def add_minute(self, df: pd.DataFrame, date: str, minute: str) -> None:
        """
        Add a minute's data to the queue with temporal validation.
        
        Validates that the new timestamp is strictly greater than the most recent
        timestamp in the queue to prevent look-ahead bias. Automatically evicts
        the oldest minute if the queue is at capacity.
        
        Args:
            df: DataFrame containing the minute's data
            date: Date string in YYYYMMDD format (e.g., "20250325")
            minute: Minute string in HHMM format (e.g., "0935")
            
        Raises:
            ValueError: If new timestamp is not greater than the most recent timestamp
        """
        # Calculate timestamp_int for chronological ordering
        timestamp_int = int(date + minute)
        
        # Validate temporal ordering if queue is not empty
        if len(self.queue) > 0:
            most_recent_timestamp = self.queue[-1][0]
            if timestamp_int <= most_recent_timestamp:
                raise ValueError(
                    f"Temporal ordering violation: new timestamp {timestamp_int} "
                    f"is not greater than most recent timestamp {most_recent_timestamp}. "
                    f"This would introduce look-ahead bias."
                )
        
        # Add to queue (automatically evicts oldest if at capacity)
        self.queue.append((timestamp_int, date, minute, df))
    
    def get_history(self, lag_k: int) -> Optional[pd.DataFrame]:
        """
        Retrieve DataFrame from k minutes ago.
        
        Args:
            lag_k: Number of minutes to look back (e.g., 1 for previous minute)
            
        Returns:
            DataFrame from k minutes ago, or None if insufficient history
        """
        # Check if we have enough history
        if lag_k < 1 or lag_k > len(self.queue):
            return None
        
        # Get DataFrame from k minutes ago
        # queue[-1] is most recent, queue[-2] is 1 minute ago, etc.
        index = -lag_k - 1
        if abs(index) > len(self.queue):
            return None
        
        return self.queue[index][3]
    
    def get_window(self, N: int) -> List[pd.DataFrame]:
        """
        Retrieve the last N minutes as a list of DataFrames.
        
        Args:
            N: Number of minutes to retrieve
            
        Returns:
            List of DataFrames for the last N minutes (or fewer if insufficient history).
            Returns empty list if no history available.
        """
        if N < 1 or len(self.queue) == 0:
            return []
        
        # Get last N elements (or all if N > queue size)
        window_size = min(N, len(self.queue))
        
        # Extract DataFrames from the tuples
        return [item[3] for item in list(self.queue)[-window_size:]]
    
    def get_current_size(self) -> int:
        """
        Get the current number of minutes in the queue.
        
        Returns:
            Current queue size
        """
        return len(self.queue)
    
    def clear(self) -> None:
        """
        Clear all data from the queue.
        """
        self.queue.clear()
