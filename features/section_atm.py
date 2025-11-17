"""Section 2.2: ATM node lookback features."""

import logging
import pandas as pd
import numpy as np
from typing import TYPE_CHECKING, Optional, Dict
from .registry import FeatureSection

if TYPE_CHECKING:
    from ..history_manager import HistoryManager

logger = logging.getLogger(__name__)


class SectionATMFeatures(FeatureSection):
    """Implements Section 2.2 features (ATM node lookback)."""
    
    @property
    def feature_names(self) -> list:
        """Return list of feature names in this section."""
        return [
            # ATM Call IV change features
            'ATM_CallIVChange_L1', 'ATM_CallIVChange_L5', 'ATM_CallIVChange_L15',
            # ATM Call IV z-score features
            'ATM_CallIVZ_5', 'ATM_CallIVZ_15', 'ATM_CallIVZ_30',
            # ATM Call Spread change features
            'ATM_CallSpreadPctChange_L1', 'ATM_CallSpreadPctChange_L5', 'ATM_CallSpreadPctChange_L15',
            # ATM Call Gamma change features
            'ATM_CallGammaChange_L1', 'ATM_CallGammaChange_L5', 'ATM_CallGammaChange_L15'
        ]
    
    def compute(self, df: pd.DataFrame, history_mgr: 'HistoryManager', **kwargs) -> pd.DataFrame:
        """
        Compute all Section 2.2 features using historical data.
        
        Args:
            df: Input DataFrame for current minute
            history_mgr: HistoryManager instance providing access to historical data
            **kwargs: Additional context (filename, etc.)
        
        Returns:
            DataFrame with Section 2.2 features added
        """
        logger.info("Computing Section 2.2 features (ATM node lookback)")
        
        df_result = df.copy()
        
        # Initialize all features with NaN
        for feature in self.feature_names:
            df_result[feature] = np.nan
        
        # Group by expiry to compute ATM features per expiry
        for expiry, expiry_group in df_result.groupby('expirDate'):
            # Get ATM values for current minute
            atm_values = self._get_atm_values(expiry_group)
            
            if atm_values is None:
                logger.debug(f"Could not identify ATM row for expiry {expiry}")
                continue
            
            # Compute lag change features
            df_result.loc[expiry_group.index, 'ATM_CallIVChange_L1'] = self._compute_atm_change(
                history_mgr, expiry, atm_values['CallIVMid'], 'CallIVMid', lag=1
            )
            df_result.loc[expiry_group.index, 'ATM_CallIVChange_L5'] = self._compute_atm_change(
                history_mgr, expiry, atm_values['CallIVMid'], 'CallIVMid', lag=5
            )
            df_result.loc[expiry_group.index, 'ATM_CallIVChange_L15'] = self._compute_atm_change(
                history_mgr, expiry, atm_values['CallIVMid'], 'CallIVMid', lag=15
            )
            
            # Compute z-score features
            df_result.loc[expiry_group.index, 'ATM_CallIVZ_5'] = self._compute_atm_zscore(
                history_mgr, expiry, atm_values['CallIVMid'], 'CallIVMid', window=5
            )
            df_result.loc[expiry_group.index, 'ATM_CallIVZ_15'] = self._compute_atm_zscore(
                history_mgr, expiry, atm_values['CallIVMid'], 'CallIVMid', window=15
            )
            df_result.loc[expiry_group.index, 'ATM_CallIVZ_30'] = self._compute_atm_zscore(
                history_mgr, expiry, atm_values['CallIVMid'], 'CallIVMid', window=30
            )
            
            # Compute spread change features
            df_result.loc[expiry_group.index, 'ATM_CallSpreadPctChange_L1'] = self._compute_atm_change(
                history_mgr, expiry, atm_values['CallSpreadPct'], 'CallSpreadPct', lag=1
            )
            df_result.loc[expiry_group.index, 'ATM_CallSpreadPctChange_L5'] = self._compute_atm_change(
                history_mgr, expiry, atm_values['CallSpreadPct'], 'CallSpreadPct', lag=5
            )
            df_result.loc[expiry_group.index, 'ATM_CallSpreadPctChange_L15'] = self._compute_atm_change(
                history_mgr, expiry, atm_values['CallSpreadPct'], 'CallSpreadPct', lag=15
            )
            
            # Compute gamma change features
            df_result.loc[expiry_group.index, 'ATM_CallGammaChange_L1'] = self._compute_atm_change(
                history_mgr, expiry, atm_values['CallGamma'], 'CallGamma', lag=1
            )
            df_result.loc[expiry_group.index, 'ATM_CallGammaChange_L5'] = self._compute_atm_change(
                history_mgr, expiry, atm_values['CallGamma'], 'CallGamma', lag=5
            )
            df_result.loc[expiry_group.index, 'ATM_CallGammaChange_L15'] = self._compute_atm_change(
                history_mgr, expiry, atm_values['CallGamma'], 'CallGamma', lag=15
            )
        
        # Round all computed features to 4 decimals
        computed_features = self.feature_names
        df_result[computed_features] = df_result[computed_features].round(4)
        
        logger.info(f"Section 2.2 features computed: {len(computed_features)} features")
        
        return df_result
    
    def _get_atm_values(self, expiry_group: pd.DataFrame) -> Optional[Dict[str, float]]:
        """
        Identify ATM row and extract relevant values.
        
        Args:
            expiry_group: DataFrame subset for a single expiry
        
        Returns:
            Dictionary with ATM values or None if not found
        """
        try:
            # Find row with minimum absolute distance_to_atm
            min_distance = expiry_group['distance_to_atm'].abs().min()
            atm_rows = expiry_group[expiry_group['distance_to_atm'].abs() == min_distance]
            
            # Take first row if multiple have same distance
            atm_row = atm_rows.iloc[0]
            
            return {
                'CallIVMid': atm_row.get('CallIVMid', np.nan),
                'CallSpreadPct': atm_row.get('CallSpreadPct', np.nan),
                'CallGamma': atm_row.get('CallGamma', np.nan)
            }
        except (KeyError, IndexError) as e:
            logger.debug(f"Error extracting ATM values: {e}")
            return None
    
    def _get_historical_atm_value(
        self, 
        hist_df: pd.DataFrame, 
        expiry: str, 
        column: str
    ) -> Optional[float]:
        """
        Extract ATM value for a specific expiry from historical DataFrame.
        
        Args:
            hist_df: Historical DataFrame
            expiry: Expiry date to filter
            column: Column name to extract
        
        Returns:
            ATM value or None if not found
        """
        try:
            # Filter for the specific expiry
            expiry_data = hist_df[hist_df['expirDate'] == expiry]
            
            if expiry_data.empty:
                return None
            
            # Find ATM row
            min_distance = expiry_data['distance_to_atm'].abs().min()
            atm_rows = expiry_data[expiry_data['distance_to_atm'].abs() == min_distance]
            
            if atm_rows.empty:
                return None
            
            # Take first row if multiple
            atm_value = atm_rows.iloc[0].get(column, np.nan)
            
            return atm_value if not pd.isna(atm_value) else None
        except (KeyError, IndexError) as e:
            logger.debug(f"Error extracting historical ATM value: {e}")
            return None
    
    def _compute_atm_change(
        self,
        history_mgr: 'HistoryManager',
        expiry: str,
        current_value: float,
        column: str,
        lag: int
    ) -> float:
        """
        Compute change in ATM value over lag k: value_t - value_{t-k}.
        
        Args:
            history_mgr: HistoryManager instance
            expiry: Expiry date
            current_value: Current ATM value
            column: Column name to extract from history
            lag: Number of minutes to look back
        
        Returns:
            Change value or NaN if insufficient history
        """
        # Check if we have enough history
        if history_mgr.get_current_size() < lag:
            logger.debug(f"Insufficient history for lag {lag}, returning NaN")
            return np.nan
        
        # Check if current value is valid
        if pd.isna(current_value):
            return np.nan
        
        try:
            # Get historical DataFrame
            # For lag=1, we want the most recent in history (queue[-1])
            # For lag=5, we want 5 minutes ago (queue[-5])
            if lag > len(history_mgr.queue):
                logger.debug(f"Insufficient history for lag {lag}, returning NaN")
                return np.nan
            
            index = -lag
            hist_df = list(history_mgr.queue)[index][3]
            
            if hist_df is None or hist_df.empty:
                logger.debug(f"Empty history for lag {lag}, returning NaN")
                return np.nan
            
            # Extract historical ATM value for this expiry
            historical_value = self._get_historical_atm_value(hist_df, expiry, column)
            
            if historical_value is None:
                logger.debug(f"Could not find historical ATM value for expiry {expiry}, lag {lag}")
                return np.nan
            
            # Compute change
            return current_value - historical_value
        except (KeyError, IndexError) as e:
            logger.debug(f"Error computing ATM change for lag {lag}: {e}")
            return np.nan
    
    def _compute_atm_zscore(
        self,
        history_mgr: 'HistoryManager',
        expiry: str,
        current_value: float,
        column: str,
        window: int
    ) -> float:
        """
        Compute z-score of ATM value over window N: (value - mean) / std.
        
        Args:
            history_mgr: HistoryManager instance
            expiry: Expiry date
            current_value: Current ATM value
            column: Column name to extract from history
            window: Window size in minutes
        
        Returns:
            Z-score or NaN if insufficient history
        """
        window_dfs = history_mgr.get_window(window)
        
        if len(window_dfs) < window:
            logger.debug(f"Insufficient history for z-score window {window}, returning NaN")
            return np.nan
        
        # Check if current value is valid
        if pd.isna(current_value):
            return np.nan
        
        try:
            # Extract ATM values from window
            values = []
            for hist_df in window_dfs:
                hist_value = self._get_historical_atm_value(hist_df, expiry, column)
                if hist_value is not None:
                    values.append(hist_value)
            
            # Add current value
            values.append(current_value)
            
            if len(values) < 2:
                logger.debug(f"Insufficient valid values for z-score window {window}")
                return np.nan
            
            # Compute z-score
            mean = np.mean(values)
            std = np.std(values, ddof=1)  # Use sample std
            
            # Avoid division by zero
            if std < 1e-6:
                logger.debug(f"Standard deviation too small for z-score calculation")
                return np.nan
            
            return (current_value - mean) / std
        except (KeyError, IndexError) as e:
            logger.debug(f"Error computing ATM z-score for window {window}: {e}")
            return np.nan
