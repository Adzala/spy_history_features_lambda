"""Section 5: Per-contract short history features."""

import logging
import pandas as pd
import numpy as np
from typing import TYPE_CHECKING
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
        
        # Process each row (each contract)
        for idx, row in df_result.iterrows():
            expiry = row['expirDate']
            strike = row['strike']
            
            # Get current values
            current_call_mid = row.get('CallMid', np.nan)
            current_call_iv = row.get('CallIVMid', np.nan)
            current_call_spread = row.get('CallSpreadPct', np.nan)
            current_call_volume = row.get('callVolume', np.nan)
            
            current_put_mid = row.get('PutMid', np.nan)
            current_put_iv = row.get('PutIVMid', np.nan)
            current_put_spread = row.get('PutSpreadPct', np.nan)
            current_put_volume = row.get('putVolume', np.nan)
            
            # Compute Call features
            # CallMidReturn features
            df_result.loc[idx, 'CallMidReturn_L1'] = self._compute_log_return(
                history_mgr, expiry, strike, current_call_mid, 'CallMid', lag=1
            )
            df_result.loc[idx, 'CallMidReturn_L2'] = self._compute_log_return(
                history_mgr, expiry, strike, current_call_mid, 'CallMid', lag=2
            )
            df_result.loc[idx, 'CallMidReturn_L3'] = self._compute_log_return(
                history_mgr, expiry, strike, current_call_mid, 'CallMid', lag=3
            )
            
            # CallMidZ features
            df_result.loc[idx, 'CallMidZ_3'] = self._compute_zscore(
                history_mgr, expiry, strike, current_call_mid, 'CallMid', window=3
            )
            df_result.loc[idx, 'CallMidZ_5'] = self._compute_zscore(
                history_mgr, expiry, strike, current_call_mid, 'CallMid', window=5
            )
            
            # CallIVChange features
            df_result.loc[idx, 'CallIVChange_L1'] = self._compute_change(
                history_mgr, expiry, strike, current_call_iv, 'CallIVMid', lag=1
            )
            
            # CallIVZ features
            df_result.loc[idx, 'CallIVZ_3'] = self._compute_zscore(
                history_mgr, expiry, strike, current_call_iv, 'CallIVMid', window=3
            )
            df_result.loc[idx, 'CallIVZ_5'] = self._compute_zscore(
                history_mgr, expiry, strike, current_call_iv, 'CallIVMid', window=5
            )
            
            # CallVolumeZ features
            df_result.loc[idx, 'CallVolumeZ_3'] = self._compute_zscore(
                history_mgr, expiry, strike, current_call_volume, 'callVolume', window=3
            )
            df_result.loc[idx, 'CallVolumeZ_5'] = self._compute_zscore(
                history_mgr, expiry, strike, current_call_volume, 'callVolume', window=5
            )
            
            # CallSpreadPctChange features
            df_result.loc[idx, 'CallSpreadPctChange_L1'] = self._compute_change(
                history_mgr, expiry, strike, current_call_spread, 'CallSpreadPct', lag=1
            )
            
            # Compute Put features (symmetric)
            # PutMidReturn features
            df_result.loc[idx, 'PutMidReturn_L1'] = self._compute_log_return(
                history_mgr, expiry, strike, current_put_mid, 'PutMid', lag=1
            )
            df_result.loc[idx, 'PutMidReturn_L2'] = self._compute_log_return(
                history_mgr, expiry, strike, current_put_mid, 'PutMid', lag=2
            )
            df_result.loc[idx, 'PutMidReturn_L3'] = self._compute_log_return(
                history_mgr, expiry, strike, current_put_mid, 'PutMid', lag=3
            )
            
            # PutMidZ features
            df_result.loc[idx, 'PutMidZ_3'] = self._compute_zscore(
                history_mgr, expiry, strike, current_put_mid, 'PutMid', window=3
            )
            df_result.loc[idx, 'PutMidZ_5'] = self._compute_zscore(
                history_mgr, expiry, strike, current_put_mid, 'PutMid', window=5
            )
            
            # PutIVChange features
            df_result.loc[idx, 'PutIVChange_L1'] = self._compute_change(
                history_mgr, expiry, strike, current_put_iv, 'PutIVMid', lag=1
            )
            
            # PutIVZ features
            df_result.loc[idx, 'PutIVZ_3'] = self._compute_zscore(
                history_mgr, expiry, strike, current_put_iv, 'PutIVMid', window=3
            )
            df_result.loc[idx, 'PutIVZ_5'] = self._compute_zscore(
                history_mgr, expiry, strike, current_put_iv, 'PutIVMid', window=5
            )
            
            # PutVolumeZ features
            df_result.loc[idx, 'PutVolumeZ_3'] = self._compute_zscore(
                history_mgr, expiry, strike, current_put_volume, 'putVolume', window=3
            )
            df_result.loc[idx, 'PutVolumeZ_5'] = self._compute_zscore(
                history_mgr, expiry, strike, current_put_volume, 'putVolume', window=5
            )
            
            # PutSpreadPctChange features
            df_result.loc[idx, 'PutSpreadPctChange_L1'] = self._compute_change(
                history_mgr, expiry, strike, current_put_spread, 'PutSpreadPct', lag=1
            )
        
        # Round all computed features to 4 decimals
        computed_features = self.feature_names
        df_result[computed_features] = df_result[computed_features].round(4)
        
        logger.info(f"Section 5 features computed: {len(computed_features)} features")
        
        return df_result
    
    def _get_historical_contract_value(
        self,
        hist_df: pd.DataFrame,
        expiry: str,
        strike: float,
        column: str
    ) -> float:
        """
        Extract value for a specific contract (expiry, strike) from historical DataFrame.
        
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
            contract_data = hist_df[(hist_df['expirDate'] == expiry) & (hist_df['strike'] == strike)]
            
            if contract_data.empty:
                return np.nan
            
            # Take first row if multiple (shouldn't happen)
            value = contract_data.iloc[0].get(column, np.nan)
            
            return value if not pd.isna(value) else np.nan
        except (KeyError, IndexError):
            return np.nan
    
    def _compute_log_return(
        self,
        history_mgr: 'HistoryManager',
        expiry: str,
        strike: float,
        current_value: float,
        column: str,
        lag: int
    ) -> float:
        """
        Compute log return over lag k: ln(value_t / value_{t-k}).
        
        Args:
            history_mgr: HistoryManager instance
            expiry: Expiry date
            strike: Strike price
            current_value: Current value
            column: Column name to extract from history
            lag: Number of minutes to look back
        
        Returns:
            Log return or NaN if insufficient history
        """
        # Check if we have enough history
        if history_mgr.get_current_size() < lag:
            return np.nan
        
        # Check if current value is valid and positive
        if pd.isna(current_value) or current_value <= 0:
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
            historical_value = self._get_historical_contract_value(hist_df, expiry, strike, column)
            
            if pd.isna(historical_value) or historical_value <= 0:
                return np.nan
            
            # Compute log return
            return np.log(current_value / historical_value)
        except (KeyError, IndexError):
            return np.nan
    
    def _compute_change(
        self,
        history_mgr: 'HistoryManager',
        expiry: str,
        strike: float,
        current_value: float,
        column: str,
        lag: int
    ) -> float:
        """
        Compute change over lag k: value_t - value_{t-k}.
        
        Args:
            history_mgr: HistoryManager instance
            expiry: Expiry date
            strike: Strike price
            current_value: Current value
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
            historical_value = self._get_historical_contract_value(hist_df, expiry, strike, column)
            
            if pd.isna(historical_value):
                return np.nan
            
            # Compute change
            return current_value - historical_value
        except (KeyError, IndexError):
            return np.nan
    
    def _compute_zscore(
        self,
        history_mgr: 'HistoryManager',
        expiry: str,
        strike: float,
        current_value: float,
        column: str,
        window: int
    ) -> float:
        """
        Compute z-score over window N: (value - mean) / std.
        
        Args:
            history_mgr: HistoryManager instance
            expiry: Expiry date
            strike: Strike price
            current_value: Current value
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
            # Extract values from window
            values = []
            for hist_df in window_dfs:
                hist_value = self._get_historical_contract_value(hist_df, expiry, strike, column)
                if not pd.isna(hist_value):
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
