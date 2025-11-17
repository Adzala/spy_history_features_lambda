"""Verification script to check all feature sections are properly registered."""

from features import create_default_registry


def main():
    """Verify all feature sections and count features."""
    registry = create_default_registry()
    
    print("=" * 80)
    print("SPY History Features Lambda - Feature Verification")
    print("=" * 80)
    print()
    
    # List all registered sections
    print(f"Registered sections: {len(registry.sections)}")
    for section_name in sorted(registry.sections.keys()):
        section = registry.sections[section_name]
        feature_count = len(section.feature_names)
        enabled = "✓" if section_name in registry.enabled_sections else "✗"
        print(f"  [{enabled}] {section_name}: {feature_count} features")
    
    print()
    
    # List all active features
    active_features = registry.get_active_features()
    print(f"Total active features: {len(active_features)}")
    print()
    
    # Group features by section
    print("Features by section:")
    print("-" * 80)
    
    for section_name in sorted(registry.enabled_sections):
        section = registry.sections[section_name]
        print(f"\n{section_name} ({len(section.feature_names)} features):")
        for feature in section.feature_names:
            print(f"  - {feature}")
    
    print()
    print("=" * 80)
    
    # Compute version hash
    version_hash = registry.compute_version_hash()
    print(f"Feature version hash: {version_hash}")
    print("=" * 80)


if __name__ == "__main__":
    main()
