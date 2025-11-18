"""Section 4: Cross-sectional dynamics within expiry."""

import logging
import pandas as pd
import numpy as np
from typing import TYPE_CHECKING, Optional, List
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
        
        # Build historical lookup indices for vectorized operations
        # This ensures we only look at historical data (no look-ahead)
        hist_lookup_l5 = self._build_historical_lookup(history_mgr, lag=5)
        hist_lookup_l15 = self._build_historical_lookup(history_mgr, lag=15)
        hist_window_15 = self._build_window_lookup(history_mgr, window=15)
        hist_window_30 = self._build_window_lookup(history_mgr, window=30)
        
        # Vectorized percentile change features (lag 5)
        if hist_lookup_l5 is not None:
            df_result = self._compute_percentile_changes_vectorized(
                df_result, hist_lookup_l5, lag=5
            )
        
        # Vectorized percentile change features (lag 15)
        if hist_lookup_l15 is not None:
            df_result = self._compute_percentile_changes_vectorized(
                df_result, hist_lookup_l15, lag=15
            )
        
        # Vectorized volume share SMA features
        if hist_window_15 is not None:
            df_result['VolumeShare_ExpirySMA_15'] = self._compute_volume_share_sma_vectorized(
                df_result, hist_window_15, window=15
            )
        
        if hist_window_30 is not None:
            df_result['VolumeShare_ExpirySMA_30'] = self._compute_volume_share_sma_vectorized(
                df_result, hist_window_30, window=30
            )
        
        # Round all computed features to 4 decimals
        computed_features = self.feature_names
        df_result[computed_features] = df_result[computed_features].round(4)
        
        logger.info(f"Section 4 features computed: {len(computed_features)} features")
        
        return df_result
    
    def _build_historical_lookup(self, history_mgr: 'HistoryManager', lag: int) -> Optional[pd.DataFrame]:
        """
        Build historical lookup DataFrame for a specific lag.
        Returns None if insufficient history.
        """
        if history_mgr.get_current_size() < lag:
            return None
        
        try:
            index = -lag
            hist_df = list(history_mgr.queue)[index][3]
            
            if hist_df is None or hist_df.empty:
                return None
            
            # Return only the columns we need with (expirDate, strike) as index
            return hist_df[['expirDate', 'strike', 'IVPercentile_Expiry', 
                           'VolumePercentile_Expiry', 'OIPercentile_Expiry']].copy()
        except (KeyError, IndexError):
            return None
    
    def _build_window_lookup(self, history_mgr: 'HistoryManager', window: int) -> Optional[List[pd.DataFrame]]:
        """
        Build list of historical DataFrames for a window.
        Returns None if insufficient history.
        """
        window_dfs = history_mgr.get_window(window)
        
        if len(window_dfs) < window:
            return None
        
        # Pre-compute volume share for each historical DataFrame
        result = []
        for hist_df in window_dfs:
            if hist_df is not None and not hist_df.empty:
                hist_with_share = self._compute_volume_share(hist_df)
                result.append(hist_with_share[['expirDate', 'strike', 'VolumeShare_Expiry']].copy())
        
        return result if result else None
    
    def _compute_percentile_changes_vectorized(
        self, 
        df: pd.DataFrame, 
        hist_df: pd.DataFrame, 
        lag: int
    ) -> pd.DataFrame:
        """
        Vectorized computation of percentile changes using merge.
        """
        # Create merge key
        df_with_key = df.copy()
        hist_with_key = hist_df.copy()
        
        # Merge on (expirDate, strike) to get historical values
        merged = df_with_key.merge(
            hist_with_key,
            on=['expirDate', 'strike'],
            how='left',
            suffixes=('', '_hist')
        )
        
        # Compute changes vectorized
        df[f'IVPercentile_Change_L{lag}'] = (
            merged['IVPercentile_Expiry'] - merged['IVPercentile_Expiry_hist']
        )
        df[f'VolumePercentile_Change_L{lag}'] = (
            merged['VolumePercentile_Expiry'] - merged['VolumePercentile_Expiry_hist']
        )
        df[f'OIPercentile_Change_L{lag}'] = (
            merged['OIPercentile_Expiry'] - merged['OIPercentile_Expiry_hist']
        )
        
        return df
    
    def _compute_volume_share_sma_vectorized(
        self,
        df: pd.DataFrame,
        hist_window: List[pd.DataFrame],
        window: int
    ) -> pd.Series:
        """
        Vectorized computation of volume share SMA using concat and groupby.
        """
        # Concatenate all historical DataFrames with current
        all_dfs = hist_window + [df[['expirDate', 'strike', 'VolumeShare_Expiry']].copy()]
        combined = pd.concat(all_dfs, ignore_index=True)
        
        # Group by (expirDate, strike) and compute mean
        sma_result = combined.groupby(['expirDate', 'strike'])['VolumeShare_Expiry'].mean()
        
        # Map back to original DataFrame
        df_with_key = df.set_index(['expirDate', 'strike'])
        result = df_with_key.index.map(lambda x: sma_result.get(x, np.nan))
        
        return pd.Series(result, index=df.index)
    
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
    

