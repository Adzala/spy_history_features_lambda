"""Section 3: Relative moneyness node features (distance_to_atm offsets)."""

import logging
import pandas as pd
import numpy as np
from typing import TYPE_CHECKING, Optional
from .registry import FeatureSection

if TYPE_CHECKING:
    from ..history_manager import HistoryManager

logger = logging.getLogger(__name__)


class SectionOffsetFeatures(FeatureSection):
    """Implements Section 3 features (Relative moneyness node lookback)."""
    
    # Offsets to compute features for
    OFFSETS = [-2, -1, 0, 1, 2]
    
    @property
    def feature_names(self) -> list:
        """Return list of feature names in this section."""
        features = []
        
        # IV_offset features for each offset
        for offset in self.OFFSETS:
            # IV change features (lags: 1, 5)
            features.append(f'IV_offsetChange_{offset}_L1')
            features.append(f'IV_offsetChange_{offset}_L5')
            
            # IV z-score features (windows: 5, 15)
            features.append(f'IV_offsetZ_{offset}_5')
            features.append(f'IV_offsetZ_{offset}_15')
            
            # IV skew to ATM features
            features.append(f'IV_SkewToATM_{offset}')
            
            # SpreadPct_offset z-score features (windows: 5, 15)
            features.append(f'SpreadPct_offsetZ_{offset}_5')
            features.append(f'SpreadPct_offsetZ_{offset}_15')
        
        return features
    
    def compute(self, df: pd.DataFrame, history_mgr: 'HistoryManager', **kwargs) -> pd.DataFrame:
        """
        Compute all Section 3 features using historical data.
        
        Args:
            df: Input DataFrame for current minute
            history_mgr: HistoryManager instance providing access to historical data
            **kwargs: Additional context (filename, etc.)
        
        Returns:
            DataFrame with Section 3 features added
        """
        logger.info("Computing Section 3 features (Offset node lookback)")
        
        df_result = df.copy()
        
        # Initialize all features with NaN
        for feature in self.feature_names:
            df_result[feature] = np.nan
        
        # Group by expiry to compute offset features per expiry
        for expiry, expiry_group in df_result.groupby('expirDate'):
            # Get ATM IV for this expiry (offset 0)
            atm_iv = self._get_offset_value(expiry_group, 0, 'CallIVMid')
            
            # Process each offset
            for offset in self.OFFSETS:
                # Get current values for this offset
                current_iv = self._get_offset_value(expiry_group, offset, 'CallIVMid')
                current_spread = self._get_offset_value(expiry_group, offset, 'CallSpreadPct')
                
                # Get indices for rows with this offset
                offset_indices = expiry_group[expiry_group['distance_to_atm'] == offset].index
                
                if len(offset_indices) == 0:
                    continue
                
                # Compute IV change features
                df_result.loc[offset_indices, f'IV_offsetChange_{offset}_L1'] = self._compute_offset_change(
                    history_mgr, expiry, offset, current_iv, 'CallIVMid', lag=1
                )
                df_result.loc[offset_indices, f'IV_offsetChange_{offset}_L5'] = self._compute_offset_change(
                    history_mgr, expiry, offset, current_iv, 'CallIVMid', lag=5
                )
                
                # Compute IV z-score features
                df_result.loc[offset_indices, f'IV_offsetZ_{offset}_5'] = self._compute_offset_zscore(
                    history_mgr, expiry, offset, current_iv, 'CallIVMid', window=5
                )
                df_result.loc[offset_indices, f'IV_offsetZ_{offset}_15'] = self._compute_offset_zscore(
                    history_mgr, expiry, offset, current_iv, 'CallIVMid', window=15
                )
                
                # Compute IV skew to ATM
                if not pd.isna(current_iv) and not pd.isna(atm_iv):
                    df_result.loc[offset_indices, f'IV_SkewToATM_{offset}'] = current_iv - atm_iv
                
                # Compute SpreadPct z-score features
                df_result.loc[offset_indices, f'SpreadPct_offsetZ_{offset}_5'] = self._compute_offset_zscore(
                    history_mgr, expiry, offset, current_spread, 'CallSpreadPct', window=5
                )
                df_result.loc[offset_indices, f'SpreadPct_offsetZ_{offset}_15'] = self._compute_offset_zscore(
                    history_mgr, expiry, offset, current_spread, 'CallSpreadPct', window=15
                )
        
        # Round all computed features to 4 decimals
        computed_features = self.feature_names
        df_result[computed_features] = df_result[computed_features].round(4)
        
        logger.info(f"Section 3 features computed: {len(computed_features)} features")
        
        return df_result
    
    def _get_offset_value(self, expiry_group: pd.DataFrame, offset: int, column: str) -> Optional[float]:
        """
        Get value for a specific offset within an expiry group.
        
        Args:
            expiry_group: DataFrame subset for a single expiry
            offset: distance_to_atm offset value
            column: Column name to extract
        
        Returns:
            Value at offset or NaN if not found
        """
        try:
            offset_rows = expiry_group[expiry_group['distance_to_atm'] == offset]
            
            if offset_rows.empty:
                return np.nan
            
            # Take first row if multiple (shouldn't happen but be safe)
            value = offset_rows.iloc[0].get(column, np.nan)
            return value if not pd.isna(value) else np.nan
        except (KeyError, IndexError):
            return np.nan
    
    def _get_historical_offset_value(
        self,
        hist_df: pd.DataFrame,
        expiry: str,
        offset: int,
        column: str
    ) -> Optional[float]:
        """
        Extract offset value for a specific expiry from historical DataFrame.
        
        Args:
            hist_df: Historical DataFrame
            expiry: Expiry date to filter
            offset: distance_to_atm offset value
            column: Column name to extract
        
        Returns:
            Offset value or None if not found
        """
        try:
            # Filter for the specific expiry and offset
            offset_data = hist_df[(hist_df['expirDate'] == expiry) & (hist_df['distance_to_atm'] == offset)]
            
            if offset_data.empty:
                return None
            
            # Take first row if multiple
            offset_value = offset_data.iloc[0].get(column, np.nan)
            
            return offset_value if not pd.isna(offset_value) else None
        except (KeyError, IndexError):
            return None
    
    def _compute_offset_change(
        self,
        history_mgr: 'HistoryManager',
        expiry: str,
        offset: int,
        current_value: float,
        column: str,
        lag: int
    ) -> float:
        """
        Compute change in offset value over lag k: value_t - value_{t-k}.
        
        Args:
            history_mgr: HistoryManager instance
            expiry: Expiry date
            offset: distance_to_atm offset value
            current_value: Current offset value
            column: Column name to extract from history
            lag: Number of minutes to look back
        
        Returns:
            Change value or NaN if insufficient history
        """
        # Check if we have enough history
        if history_mgr.get_current_size() < lag:
            return np.nan
        
        # Check if current value is valid
        if pd.isna(current_value):
            return np.nan
        
        try:
            # Get historical DataFrame
            if lag > len(history_mgr.queue):
                return np.nan
            
            index = -lag
            hist_df = list(history_mgr.queue)[index][3]
            
            if hist_df is None or hist_df.empty:
                return np.nan
            
            # Extract historical offset value
            historical_value = self._get_historical_offset_value(hist_df, expiry, offset, column)
            
            if historical_value is None:
                return np.nan
            
            # Compute change
            return current_value - historical_value
        except (KeyError, IndexError):
            return np.nan
    
    def _compute_offset_zscore(
        self,
        history_mgr: 'HistoryManager',
        expiry: str,
        offset: int,
        current_value: float,
        column: str,
        window: int
    ) -> float:
        """
        Compute z-score of offset value over window N: (value - mean) / std.
        
        Args:
            history_mgr: HistoryManager instance
            expiry: Expiry date
            offset: distance_to_atm offset value
            current_value: Current offset value
            column: Column name to extract from history
            window: Window size in minutes
        
        Returns:
            Z-score or NaN if insufficient history
        """
        window_dfs = history_mgr.get_window(window)
        
        if len(window_dfs) < window:
            return np.nan
        
        # Check if current value is valid
        if pd.isna(current_value):
            return np.nan
        
        try:
            # Extract offset values from window
            values = []
            for hist_df in window_dfs:
                hist_value = self._get_historical_offset_value(hist_df, expiry, offset, column)
                if hist_value is not None:
                    values.append(hist_value)
            
            # Add current value
            values.append(current_value)
            
            if len(values) < 2:
                return np.nan
            
            # Compute z-score
            mean = np.mean(values)
            std = np.std(values, ddof=1)
            
            # Avoid division by zero
            if std < 1e-6:
                return np.nan
            
            return (current_value - mean) / std
        except (KeyError, IndexError):
            return np.nan
