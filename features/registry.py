"""Feature registry and base classes for history-based features."""

import logging
import hashlib
from abc import ABC, abstractmethod
from typing import List, Dict, TYPE_CHECKING
import pandas as pd

if TYPE_CHECKING:
    from history_manager import HistoryManager

logger = logging.getLogger(__name__)


class FeatureSection(ABC):
    """Abstract base class for feature sections that compute history-based features."""
    
    @property
    @abstractmethod
    def feature_names(self) -> List[str]:
        """Return list of feature names in this section."""
        pass
    
    @abstractmethod
    def compute(self, df: pd.DataFrame, history_mgr: 'HistoryManager', **kwargs) -> pd.DataFrame:
        """
        Compute all features in this section using historical data.
        
        Args:
            df: Input DataFrame for current minute
            history_mgr: HistoryManager instance providing access to historical data
            **kwargs: Additional context (filename, etc.)
        
        Returns:
            DataFrame with computed features added
        """
        pass


class FeatureRegistry:
    """Manages feature definitions and enables/disables sections."""
    
    def __init__(self):
        """Initialize FeatureRegistry."""
        self.sections: Dict[str, FeatureSection] = {}
        self.enabled_sections: set = set()
        logger.info("FeatureRegistry initialized")
    
    def register_section(self, name: str, section: FeatureSection) -> None:
        """
        Register a feature section.
        
        Args:
            name: Section name (e.g., "section_underlying")
            section: FeatureSection instance
        """
        self.sections[name] = section
        logger.info(f"Registered section: {name}")
    
    def enable_section(self, name: str) -> None:
        """
        Enable a feature section.
        
        Args:
            name: Section name
        
        Raises:
            ValueError: If section is not registered
        """
        if name not in self.sections:
            raise ValueError(f"Section '{name}' is not registered")
        self.enabled_sections.add(name)
        logger.info(f"Enabled section: {name}")
    
    def disable_section(self, name: str) -> None:
        """
        Disable a feature section.
        
        Args:
            name: Section name
        """
        self.enabled_sections.discard(name)
        logger.info(f"Disabled section: {name}")
    
    def get_active_features(self) -> List[str]:
        """
        Get list of all active feature names.
        
        Returns:
            List of feature names from enabled sections
        """
        active_features = []
        for section_name in sorted(self.enabled_sections):
            section = self.sections[section_name]
            active_features.extend(section.feature_names)
        return active_features
    
    def compute_version_hash(self) -> str:
        """
        Generate SHA256 hash of enabled features.
        
        Returns:
            SHA256 hash string (first 16 characters)
        """
        active_features = self.get_active_features()
        # Sort features for consistent hashing
        sorted_features = sorted(active_features)
        # Create hash from concatenated feature names
        feature_string = ','.join(sorted_features)
        hash_obj = hashlib.sha256(feature_string.encode('utf-8'))
        return hash_obj.hexdigest()[:16]
