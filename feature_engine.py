"""Feature computation orchestrator for history-based features."""

import logging
import time
import pandas as pd
import numpy as np
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from history_manager import HistoryManager
    from features.registry import FeatureRegistry

logger = logging.getLogger(__name__)


class FeatureEngine:
    """Orchestrates feature computation using the feature registry."""
    
    def __init__(self, registry: 'FeatureRegistry'):
        """
        Initialize FeatureEngine.
        
        Args:
            registry: FeatureRegistry instance
        """
        self.registry = registry
        logger.info("FeatureEngine initialized")
    
    def compute_features(self, df: pd.DataFrame, history_mgr: 'HistoryManager', 
                        filename: str) -> pd.DataFrame:
        """
        Compute all enabled features using historical data.
        
        Args:
            df: Input DataFrame for current minute
            history_mgr: HistoryManager instance providing access to historical data
            filename: Source filename for context
        
        Returns:
            DataFrame with computed features added
        """
        start_time = time.time()
        df_result = df.copy()
        
        initial_col_count = len(df_result.columns)
        
        # Apply all enabled sections
        for section_name in sorted(self.registry.enabled_sections):
            section = self.registry.sections[section_name]
            logger.info(f"Applying section: {section_name}")
            
            section_start = time.time()
            df_result = section.compute(df_result, history_mgr, filename=filename)
            section_elapsed = (time.time() - section_start) * 1000
            
            # Count features added by this section
            current_col_count = len(df_result.columns)
            features_added = current_col_count - initial_col_count
            initial_col_count = current_col_count
            
            logger.info(
                f"Section {section_name} completed in {section_elapsed:.2f}ms, "
                f"added {features_added} features"
            )
        
        elapsed_ms = (time.time() - start_time) * 1000
        total_features = len(df_result.columns) - len(df.columns)
        logger.info(
            f"Feature computation completed in {elapsed_ms:.2f}ms, "
            f"total features added: {total_features}"
        )
        
        return df_result
    
    def round_numerics(self, df: pd.DataFrame, decimals: int = 4) -> pd.DataFrame:
        """
        Round all numeric columns to configured precision.
        
        Args:
            df: Input DataFrame
            decimals: Number of decimal places (default: 4)
        
        Returns:
            DataFrame with rounded numeric values
        """
        df_result = df.copy()
        
        # Select numeric columns
        numeric_cols = df_result.select_dtypes(include=[np.number]).columns
        
        # Round numeric columns
        df_result[numeric_cols] = df_result[numeric_cols].round(decimals)
        
        logger.info(f"Rounded {len(numeric_cols)} numeric columns to {decimals} decimals")
        
        return df_result
