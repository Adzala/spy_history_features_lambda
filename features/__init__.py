"""Feature computation modules for history-based features."""

from .registry import FeatureRegistry, FeatureSection


def create_default_registry() -> FeatureRegistry:
    """
    Create and configure a FeatureRegistry with all sections registered.
    
    Initially, no sections are registered. Sections will be added as they
    are implemented (Section 2.1: Underlying, Section 2.2: ATM, etc.).
    
    Returns:
        FeatureRegistry with sections registered and enabled
    """
    registry = FeatureRegistry()
    
    # Register Section 2.1: Underlying stock price lookback features
    from .section_underlying import SectionUnderlyingFeatures
    registry.register_section('section_underlying', SectionUnderlyingFeatures())
    registry.enable_section('section_underlying')
    
    # Register Section 2.2: ATM node lookback features
    from .section_atm import SectionATMFeatures
    registry.register_section('section_atm', SectionATMFeatures())
    registry.enable_section('section_atm')
    
    # Register Section 3: Relative moneyness node features (distance_to_atm offsets)
    from .section_offset import SectionOffsetFeatures
    registry.register_section('section_offset', SectionOffsetFeatures())
    registry.enable_section('section_offset')
    
    # Register Section 4: Cross-sectional dynamics within expiry
    from .section_cross_sectional import SectionCrossSectionalFeatures
    registry.register_section('section_cross_sectional', SectionCrossSectionalFeatures())
    registry.enable_section('section_cross_sectional')
    
    # Register Section 5: Per-contract short history
    from .section_contract import SectionContractFeatures
    registry.register_section('section_contract', SectionContractFeatures())
    registry.enable_section('section_contract')
    
    return registry


__all__ = [
    'FeatureRegistry', 
    'FeatureSection', 
    'create_default_registry'
]
