"""Section 4: Cross-sectional dynamics within expiry."""

import logging
import pandas as pd
import numpy as np
from typing import TYPE_CHECKING
from .registry import FeatureSection

if TYPE_CHECKING:
    from ..history_manager import HistoryManager

logger = logging.getLogger(__name__)


class SectionCrossSectionalFeatures(FeatureSection):
    """Implements Section 4 features (Cross-sectional dynamics)."""
    
    @property
    def feature_names(self) -> list:
        """Return list of feature names in this section."""
        return [
            # Percentile change features (lags: 5, 15)
            'IVPercentile_Change_L5',
            'IVPercentile_Change_L15',
            'VolumePercentile_Change_L5',
            'VolumePercentile_Change_L15',
            'OIPercentile_Change_L5',
            'OIPercentile_Change_L15',
            # Volume share features
            'VolumeShare_Expiry',
            'VolumeShare_ExpirySMA_15',
            'VolumeShare_ExpirySMA_30'
        ]
    
    def compute(self, df: pd.DataFrame, history_mgr: 'HistoryManager', **kwargs) -> pd.DataFrame:
        """
        Compute all Section 4 features using historical data.
        
        Args:
            df: Input DataFrame for current minute
            history_mgr: HistoryManager instance providing access to historical data
            **kwargs: Additional context (filename, etc.)
        
        Returns:
            DataFrame with Section 4 features added
        """
        logger.info("Computing Section 4 features (Cross-sectional dynamics)")
        
        df_result = df.copy()
        
        # Initialize all features with NaN
        for feature in self.feature_names:
            df_result[feature] = np.nan
        
        # Compute volume share features first (current minute only)
        df_result = self._compute_volume_share(df_result)
        
        # Process each row for percentile change features
        for idx, row in df_result.iterrows():
            expiry = row['expirDate']
            strike = row['strike']
            
            # Get current percentile values
            current_iv_pct = row.get('IVPercentile_Expiry', np.nan)
            current_vol_pct = row.get('VolumePercentile_Expiry', np.nan)
            current_oi_pct = row.get('OIPercentile_Expiry', np.nan)
            current_vol_share = row.get('VolumeShare_Expiry', np.nan)
            
            # Compute percentile change features
            df_result.loc[idx, 'IVPercentile_Change_L5'] = self._compute_percentile_change(
                history_mgr, expiry, strike, current_iv_pct, 'IVPercentile_Expiry', lag=5
            )
            df_result.loc[idx, 'IVPercentile_Change_L15'] = self._compute_percentile_change(
                history_mgr, expiry, strike, current_iv_pct, 'IVPercentile_Expiry', lag=15
            )
            
            df_result.loc[idx, 'VolumePercentile_Change_L5'] = self._compute_percentile_change(
                history_mgr, expiry, strike, current_vol_pct, 'VolumePercentile_Expiry', lag=5
            )
            df_result.loc[idx, 'VolumePercentile_Change_L15'] = self._compute_percentile_change(
                history_mgr, expiry, strike, current_vol_pct, 'VolumePercentile_Expiry', lag=15
            )
            
            df_result.loc[idx, 'OIPercentile_Change_L5'] = self._compute_percentile_change(
                history_mgr, expiry, strike, current_oi_pct, 'OIPercentile_Expiry', lag=5
            )
            df_result.loc[idx, 'OIPercentile_Change_L15'] = self._compute_percentile_change(
                history_mgr, expiry, strike, current_oi_pct, 'OIPercentile_Expiry', lag=15
            )
            
            # Compute volume share SMA features
            df_result.loc[idx, 'VolumeShare_ExpirySMA_15'] = self._compute_volume_share_sma(
                history_mgr, expiry, strike, current_vol_share, window=15
            )
            df_result.loc[idx, 'VolumeShare_ExpirySMA_30'] = self._compute_volume_share_sma(
                history_mgr, expiry, strike, current_vol_share, window=30
            )
        
        # Round all computed features to 4 decimals
        computed_features = self.feature_names
        df_result[computed_features] = df_result[computed_features].round(4)
        
        logger.info(f"Section 4 features computed: {len(computed_features)} features")
        
        return df_result
    
    def _compute_volume_share(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute VolumeShare_Expiry for current minute.
        
        VolumeShare_Expiry = callVolume / sum(callVolume for all strikes in expiry)
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with VolumeShare_Expiry added
        """
        df_result = df.copy()
        
        # Group by expiry and compute volume share
        for expiry, expiry_group in df_result.groupby('expirDate'):
            # Get call volume for each row
            call_volumes = expiry_group.get('callVolume', pd.Series(dtype=float))
            
            # Compute total volume for expiry
            total_volume = call_volumes.sum()
            
            # Avoid division by zero
            if total_volume > 0:
                volume_shares = call_volumes / total_volume
            else:
                volume_shares = pd.Series(np.nan, index=expiry_group.index)
            
            # Assign to result
            df_result.loc[expiry_group.index, 'VolumeShare_Expiry'] = volume_shares
        
        return df_result
    
    def _get_historical_row_value(
        self,
        hist_df: pd.DataFrame,
        expiry: str,
        strike: float,
        column: str
    ) -> float:
        """
        Extract value for a specific (expiry, strike) from historical DataFrame.
        
        Args:
            hist_df: Historical DataFrame
            expiry: Expiry date to filter
            strike: Strike price to filter
            column: Column name to extract
        
        Returns:
            Value or NaN if not found
        """
        try:
            # Filter for the specific expiry and strike
            row_data = hist_df[(hist_df['expirDate'] == expiry) & (hist_df['strike'] == strike)]
            
            if row_data.empty:
                return np.nan
            
            # Take first row if multiple (shouldn't happen)
            value = row_data.iloc[0].get(column, np.nan)
            
            return value if not pd.isna(value) else np.nan
        except (KeyError, IndexError):
            return np.nan
    
    def _compute_percentile_change(
        self,
        history_mgr: 'HistoryManager',
        expiry: str,
        strike: float,
        current_value: float,
        column: str,
        lag: int
    ) -> float:
        """
        Compute change in percentile over lag k: percentile_t - percentile_{t-k}.
        
        Args:
            history_mgr: HistoryManager instance
            expiry: Expiry date
            strike: Strike price
            current_value: Current percentile value
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
            
            # Extract historical value
            historical_value = self._get_historical_row_value(hist_df, expiry, strike, column)
            
            if pd.isna(historical_value):
                return np.nan
            
            # Compute change
            return current_value - historical_value
        except (KeyError, IndexError):
            return np.nan
    
    def _compute_volume_share_sma(
        self,
        history_mgr: 'HistoryManager',
        expiry: str,
        strike: float,
        current_value: float,
        window: int
    ) -> float:
        """
        Compute SMA of VolumeShare_Expiry over window N.
        
        Args:
            history_mgr: HistoryManager instance
            expiry: Expiry date
            strike: Strike price
            current_value: Current volume share value
            window: Window size in minutes
        
        Returns:
            SMA or NaN if insufficient history
        """
        window_dfs = history_mgr.get_window(window)
        
        if len(window_dfs) < window:
            return np.nan
        
        # Check if current value is valid
        if pd.isna(current_value):
            return np.nan
        
        try:
            # Extract volume share values from window
            # Note: VolumeShare_Expiry needs to be computed for historical data
            # We'll compute it on the fly for each historical DataFrame
            values = []
            
            for hist_df in window_dfs:
                # Compute volume share for this historical minute
                hist_df_with_share = self._compute_volume_share(hist_df)
                
                # Extract value for this specific (expiry, strike)
                hist_value = self._get_historical_row_value(hist_df_with_share, expiry, strike, 'VolumeShare_Expiry')
                
                if not pd.isna(hist_value):
                    values.append(hist_value)
            
            # Add current value
            values.append(current_value)
            
            if len(values) < 2:
                return np.nan
            
            # Compute SMA
            return np.mean(values)
        except (KeyError, IndexError):
            return np.nan
