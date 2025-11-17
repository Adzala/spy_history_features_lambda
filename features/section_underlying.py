"""Section 2.1: Underlying stock price lookback features."""

import logging
import pandas as pd
import numpy as np
from typing import TYPE_CHECKING
from .registry import FeatureSection

if TYPE_CHECKING:
    from ..history_manager import HistoryManager

logger = logging.getLogger(__name__)


class SectionUnderlyingFeatures(FeatureSection):
    """Implements Section 2.1 features (Underlying stock price lookback)."""
    
    @property
    def feature_names(self) -> list:
        """Return list of feature names in this section."""
        return [
            # Lag return features
            'UnderlyingReturn_L1', 'UnderlyingReturn_L5', 'UnderlyingReturn_L15',
            # Cumulative return features
            'UnderlyingCumReturn_5', 'UnderlyingCumReturn_15', 'UnderlyingCumReturn_30',
            # Simple moving average features
            'UnderlyingSMA_5', 'UnderlyingSMA_15', 'UnderlyingSMA_30',
            # Exponential moving average features
            'UnderlyingEMA_5', 'UnderlyingEMA_15', 'UnderlyingEMA_30',
            # Volatility features
            'UnderlyingVol_5', 'UnderlyingVol_15', 'UnderlyingVol_30'
        ]
    
    def compute(self, df: pd.DataFrame, history_mgr: 'HistoryManager', **kwargs) -> pd.DataFrame:
        """
        Compute all Section 2.1 features using historical data.
        
        Args:
            df: Input DataFrame for current minute
            history_mgr: HistoryManager instance providing access to historical data
            **kwargs: Additional context (filename, etc.)
        
        Returns:
            DataFrame with Section 2.1 features added
        """
        logger.info("Computing Section 2.1 features (Underlying lookback)")
        
        df_result = df.copy()
        
        # Extract current stock price (same for all rows in the minute)
        current_price = df_result['stockPrice'].iloc[0]
        
        # Compute lag return features
        df_result['UnderlyingReturn_L1'] = self._compute_lag_return(history_mgr, current_price, lag=1)
        df_result['UnderlyingReturn_L5'] = self._compute_lag_return(history_mgr, current_price, lag=5)
        df_result['UnderlyingReturn_L15'] = self._compute_lag_return(history_mgr, current_price, lag=15)
        
        # Compute cumulative return features (same as lag returns for these windows)
        df_result['UnderlyingCumReturn_5'] = self._compute_lag_return(history_mgr, current_price, lag=5)
        df_result['UnderlyingCumReturn_15'] = self._compute_lag_return(history_mgr, current_price, lag=15)
        df_result['UnderlyingCumReturn_30'] = self._compute_lag_return(history_mgr, current_price, lag=30)
        
        # Compute simple moving average features
        df_result['UnderlyingSMA_5'] = self._compute_sma(history_mgr, current_price, window=5)
        df_result['UnderlyingSMA_15'] = self._compute_sma(history_mgr, current_price, window=15)
        df_result['UnderlyingSMA_30'] = self._compute_sma(history_mgr, current_price, window=30)
        
        # Compute exponential moving average features
        df_result['UnderlyingEMA_5'] = self._compute_ema(history_mgr, current_price, window=5)
        df_result['UnderlyingEMA_15'] = self._compute_ema(history_mgr, current_price, window=15)
        df_result['UnderlyingEMA_30'] = self._compute_ema(history_mgr, current_price, window=30)
        
        # Compute volatility features
        df_result['UnderlyingVol_5'] = self._compute_volatility(history_mgr, current_price, window=5)
        df_result['UnderlyingVol_15'] = self._compute_volatility(history_mgr, current_price, window=15)
        df_result['UnderlyingVol_30'] = self._compute_volatility(history_mgr, current_price, window=30)
        
        # Round all computed features to 4 decimals
        computed_features = self.feature_names
        df_result[computed_features] = df_result[computed_features].round(4)
        
        logger.info(f"Section 2.1 features computed: {len(computed_features)} features")
        
        return df_result
    
    def _compute_lag_return(self, history_mgr: 'HistoryManager', current_price: float, lag: int) -> float:
        """
        Compute log return over lag k: ln(price_t / price_{t-k}).
        
        Args:
            history_mgr: HistoryManager instance
            current_price: Current stock price
            lag: Number of minutes to look back
        
        Returns:
            Log return or NaN if insufficient history
        """
        # Check if we have enough history
        if history_mgr.get_current_size() < lag:
            logger.debug(f"Insufficient history for lag {lag}, returning NaN")
            return np.nan
        
        # Get the DataFrame from lag minutes ago
        # queue[-1] is most recent, queue[-2] is 1 minute ago, etc.
        # For lag=1, we want queue[-1] (most recent in history)
        # For lag=k, we want queue[-k]
        try:
            index = -lag
            hist_df = list(history_mgr.queue)[index][3]
            
            if hist_df is None or hist_df.empty:
                logger.debug(f"Empty history for lag {lag}, returning NaN")
                return np.nan
            
            historical_price = hist_df['stockPrice'].iloc[0]
            if historical_price <= 0 or current_price <= 0:
                return np.nan
            return np.log(current_price / historical_price)
        except (KeyError, IndexError):
            logger.debug(f"Error extracting historical price for lag {lag}")
            return np.nan
    
    def _compute_sma(self, history_mgr: 'HistoryManager', current_price: float, window: int) -> float:
        """
        Compute simple moving average over window N.
        
        Args:
            history_mgr: HistoryManager instance
            current_price: Current stock price
            window: Window size in minutes
        
        Returns:
            Simple moving average or NaN if insufficient history
        """
        window_dfs = history_mgr.get_window(window)
        
        if len(window_dfs) < window:
            logger.debug(f"Insufficient history for SMA window {window}, returning NaN")
            return np.nan
        
        try:
            # Extract prices from window (including current)
            prices = [df['stockPrice'].iloc[0] for df in window_dfs]
            prices.append(current_price)
            return np.mean(prices)
        except (KeyError, IndexError):
            logger.debug(f"Error computing SMA for window {window}")
            return np.nan
    
    def _compute_ema(self, history_mgr: 'HistoryManager', current_price: float, window: int) -> float:
        """
        Compute exponential moving average over window N using pandas EMA.
        
        Args:
            history_mgr: HistoryManager instance
            current_price: Current stock price
            window: Window size in minutes (span parameter)
        
        Returns:
            Exponential moving average or NaN if insufficient history
        """
        window_dfs = history_mgr.get_window(window)
        
        if len(window_dfs) < window:
            logger.debug(f"Insufficient history for EMA window {window}, returning NaN")
            return np.nan
        
        try:
            # Extract prices from window (including current)
            prices = [df['stockPrice'].iloc[0] for df in window_dfs]
            prices.append(current_price)
            
            # Compute EMA using pandas
            price_series = pd.Series(prices)
            ema_series = price_series.ewm(span=window, adjust=False).mean()
            return ema_series.iloc[-1]
        except (KeyError, IndexError):
            logger.debug(f"Error computing EMA for window {window}")
            return np.nan
    
    def _compute_volatility(self, history_mgr: 'HistoryManager', current_price: float, window: int) -> float:
        """
        Compute realized volatility: sqrt(sum((ln(price_i / price_{i-1}))^2)).
        
        Args:
            history_mgr: HistoryManager instance
            current_price: Current stock price
            window: Window size in minutes
        
        Returns:
            Realized volatility or NaN if insufficient history
        """
        window_dfs = history_mgr.get_window(window)
        
        if len(window_dfs) < window:
            logger.debug(f"Insufficient history for volatility window {window}, returning NaN")
            return np.nan
        
        try:
            # Extract prices from window (including current)
            prices = [df['stockPrice'].iloc[0] for df in window_dfs]
            prices.append(current_price)
            
            # Compute log returns
            returns = []
            for i in range(1, len(prices)):
                if prices[i-1] <= 0 or prices[i] <= 0:
                    continue
                log_return = np.log(prices[i] / prices[i-1])
                returns.append(log_return ** 2)
            
            if len(returns) == 0:
                return np.nan
            
            # Compute volatility as sqrt(sum(returns^2))
            return np.sqrt(np.sum(returns))
        except (KeyError, IndexError):
            logger.debug(f"Error computing volatility for window {window}")
            return np.nan
