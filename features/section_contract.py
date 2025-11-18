"""Section 5: Per-contract short history features."""

import logging
import pandas as pd
import numpy as np
from typing import TYPE_CHECKING, Optional, List
from .registry import FeatureSection

if TYPE_CHECKING:
    from ..history_manager import HistoryManager

logger = logging.getLogger(__name__)


class SectionContractFeatures(FeatureSection):
    """Implements Section 5 features (Per-contract short history)."""
    
    @property
    def feature_names(self) -> list:
        """Return list of feature names in this section."""
        return [
            # Call features
            'CallMidReturn_L1',
            'CallMidReturn_L2',
            'CallMidReturn_L3',
            'CallMidZ_3',
            'CallMidZ_5',
            'CallIVChange_L1',
            'CallIVZ_3',
            'CallIVZ_5',
            'CallVolumeZ_3',
            'CallVolumeZ_5',
            'CallSpreadPctChange_L1',
            # Put features (symmetric)
            'PutMidReturn_L1',
            'PutMidReturn_L2',
            'PutMidReturn_L3',
            'PutMidZ_3',
            'PutMidZ_5',
            'PutIVChange_L1',
            'PutIVZ_3',
            'PutIVZ_5',
            'PutVolumeZ_3',
            'PutVolumeZ_5',
            'PutSpreadPctChange_L1'
        ]
    
    def compute(self, df: pd.DataFrame, history_mgr: 'HistoryManager', **kwargs) -> pd.DataFrame:
        """
        Compute all Section 5 features using historical data.
        
        Args:
            df: Input DataFrame for current minute
            history_mgr: HistoryManager instance providing access to historical data
            **kwargs: Additional context (filename, etc.)
        
        Returns:
            DataFrame with Section 5 features added
        """
        logger.info("Computing Section 5 features (Per-contract short history)")
        
        df_result = df.copy()
        
        # Initialize all features with NaN
        for feature in self.feature_names:
            df_result[feature] = np.nan
        
        # Build historical lookups for vectorized operations
        hist_lag_1 = self._build_lag_lookup(history_mgr, lag=1)
        hist_lag_2 = self._build_lag_lookup(history_mgr, lag=2)
        hist_lag_3 = self._build_lag_lookup(history_mgr, lag=3)
        hist_window_3 = self._build_window_data(history_mgr, window=3)
        hist_window_5 = self._build_window_data(history_mgr, window=5)
        
        # Vectorized Call features
        if hist_lag_1 is not None:
            df_result = self._compute_log_returns_vectorized(df_result, hist_lag_1, 'Call', lag=1)
            df_result = self._compute_log_returns_vectorized(df_result, hist_lag_1, 'Put', lag=1)
            df_result = self._compute_changes_vectorized(df_result, hist_lag_1, 'Call', lag=1)
            df_result = self._compute_changes_vectorized(df_result, hist_lag_1, 'Put', lag=1)
        
        if hist_lag_2 is not None:
            df_result = self._compute_log_returns_vectorized(df_result, hist_lag_2, 'Call', lag=2)
            df_result = self._compute_log_returns_vectorized(df_result, hist_lag_2, 'Put', lag=2)
        
        if hist_lag_3 is not None:
            df_result = self._compute_log_returns_vectorized(df_result, hist_lag_3, 'Call', lag=3)
            df_result = self._compute_log_returns_vectorized(df_result, hist_lag_3, 'Put', lag=3)
        
        if hist_window_3 is not None:
            df_result = self._compute_zscores_vectorized(df_result, hist_window_3, 'Call', window=3)
            df_result = self._compute_zscores_vectorized(df_result, hist_window_3, 'Put', window=3)
        
        if hist_window_5 is not None:
            df_result = self._compute_zscores_vectorized(df_result, hist_window_5, 'Call', window=5)
            df_result = self._compute_zscores_vectorized(df_result, hist_window_5, 'Put', window=5)
        
        # Round all computed features to 4 decimals
        computed_features = self.feature_names
        df_result[computed_features] = df_result[computed_features].round(4)
        
        logger.info(f"Section 5 features computed: {len(computed_features)} features")
        
        return df_result
    
    def _build_lag_lookup(self, history_mgr: 'HistoryManager', lag: int) -> Optional[pd.DataFrame]:
        """Build historical lookup DataFrame for a specific lag."""
        if history_mgr.get_current_size() < lag:
            return None
        
        try:
            index = -lag
            hist_df = list(history_mgr.queue)[index][3]
            
            if hist_df is None or hist_df.empty:
                return None
            
            # Return relevant columns
            cols = ['expirDate', 'strike', 'CallMid', 'CallIVMid', 'CallSpreadPct', 'callVolume',
                   'PutMid', 'PutIVMid', 'PutSpreadPct', 'putVolume']
            return hist_df[cols].copy()
        except (KeyError, IndexError):
            return None
    
    def _build_window_data(self, history_mgr: 'HistoryManager', window: int) -> Optional[pd.DataFrame]:
        """Build concatenated historical data for a window."""
        window_dfs = history_mgr.get_window(window)
        
        if len(window_dfs) < window:
            return None
        
        try:
            cols = ['expirDate', 'strike', 'CallMid', 'CallIVMid', 'callVolume',
                   'PutMid', 'PutIVMid', 'putVolume']
            dfs_to_concat = [df[cols].copy() for df in window_dfs if df is not None and not df.empty]
            
            if not dfs_to_concat:
                return None
            
            return pd.concat(dfs_to_concat, ignore_index=True)
        except (KeyError, IndexError):
            return None
    
    def _compute_log_returns_vectorized(
        self, 
        df: pd.DataFrame, 
        hist_df: pd.DataFrame, 
        leg: str,
        lag: int
    ) -> pd.DataFrame:
        """Vectorized log return computation."""
        col = f'{leg}Mid'
        
        # Merge to get historical values
        merged = df.merge(
            hist_df[['expirDate', 'strike', col]],
            on=['expirDate', 'strike'],
            how='left',
            suffixes=('', '_hist')
        )
        
        # Compute log returns vectorized
        current = merged[col]
        historical = merged[f'{col}_hist']
        
        # Only compute where both values are positive
        mask = (current > 0) & (historical > 0)
        df[f'{leg}MidReturn_L{lag}'] = np.where(mask, np.log(current / historical), np.nan)
        
        return df
    
    def _compute_changes_vectorized(
        self,
        df: pd.DataFrame,
        hist_df: pd.DataFrame,
        leg: str,
        lag: int
    ) -> pd.DataFrame:
        """Vectorized change computation."""
        # IV change
        iv_col = f'{leg}IVMid'
        merged_iv = df.merge(
            hist_df[['expirDate', 'strike', iv_col]],
            on=['expirDate', 'strike'],
            how='left',
            suffixes=('', '_hist')
        )
        df[f'{leg}IVChange_L{lag}'] = merged_iv[iv_col] - merged_iv[f'{iv_col}_hist']
        
        # Spread change
        spread_col = f'{leg}SpreadPct'
        merged_spread = df.merge(
            hist_df[['expirDate', 'strike', spread_col]],
            on=['expirDate', 'strike'],
            how='left',
            suffixes=('', '_hist')
        )
        df[f'{leg}SpreadPctChange_L{lag}'] = merged_spread[spread_col] - merged_spread[f'{spread_col}_hist']
        
        return df
    
    def _compute_zscores_vectorized(
        self,
        df: pd.DataFrame,
        hist_window: pd.DataFrame,
        leg: str,
        window: int
    ) -> pd.DataFrame:
        """Vectorized z-score computation."""
        # Compute stats for each contract
        mid_col = f'{leg}Mid'
        iv_col = f'{leg}IVMid'
        vol_col = f'{leg.lower()}Volume'
        
        # Group by contract and compute mean/std
        stats_mid = hist_window.groupby(['expirDate', 'strike'])[mid_col].agg(['mean', 'std']).reset_index()
        stats_iv = hist_window.groupby(['expirDate', 'strike'])[iv_col].agg(['mean', 'std']).reset_index()
        stats_vol = hist_window.groupby(['expirDate', 'strike'])[vol_col].agg(['mean', 'std']).reset_index()
        
        # Merge stats back to current data
        df_with_stats_mid = df.merge(stats_mid, on=['expirDate', 'strike'], how='left', suffixes=('', '_stats'))
        df_with_stats_iv = df.merge(stats_iv, on=['expirDate', 'strike'], how='left', suffixes=('', '_stats'))
        df_with_stats_vol = df.merge(stats_vol, on=['expirDate', 'strike'], how='left', suffixes=('', '_stats'))
        
        # Compute z-scores vectorized
        df[f'{leg}MidZ_{window}'] = np.where(
            df_with_stats_mid['std'] > 1e-6,
            (df[mid_col] - df_with_stats_mid['mean']) / df_with_stats_mid['std'],
            np.nan
        )
        
        df[f'{leg}IVZ_{window}'] = np.where(
            df_with_stats_iv['std'] > 1e-6,
            (df[iv_col] - df_with_stats_iv['mean']) / df_with_stats_iv['std'],
            np.nan
        )
        
        df[f'{leg}VolumeZ_{window}'] = np.where(
            df_with_stats_vol['std'] > 1e-6,
            (df[vol_col] - df_with_stats_vol['mean']) / df_with_stats_vol['std'],
            np.nan
        )
        
        return df
    

