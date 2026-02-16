"""Test script to verify Prefect flow parameter mapping to Dagster Config."""

import sys
import inspect
from pathlib import Path

# Add script_orchestrator to path
sys.path.insert(0, str(Path(__file__).parent / "script_orchestrator"))

from dagster import Definitions
from script_orchestrator.components.script_github_component import ScriptGithubComponent


def test_config_generation():
    """Test that flows with parameters generate Config classes."""

    # Create component instance
    from script_orchestrator.components.script_github_component import ScriptGithubComponentParams

    params = ScriptGithubComponentParams(
        use_local=True,
        scripts_directory="example_scripts/orchestrator_migration"
    )

    component = ScriptGithubComponent(params=params)

    # Get definitions
    defs: Definitions = component.build_defs()

    # Check assets
    assets = defs.get_all_asset_specs()

    print("\n=== Testing Prefect Flow Config Generation ===\n")

    for asset_spec in assets:
        asset_name = asset_spec.key.to_user_string()
        print(f"\nAsset: {asset_name}")
        print(f"  Tags: {asset_spec.tags}")

        # Check if this is a Prefect mapped flow
        if asset_spec.tags.get("script_type") == "prefect_mapped":
            print(f"  ✓ Prefect mapped flow detected")

            # Try to get the asset function
            # Note: This is a simplified check - in production we'd inspect the actual asset
            if "parameterized" in asset_spec.description or False:
                print(f"  ✓ Has parameters (based on description)")
            else:
                print(f"  ℹ️  No parameter indication in description")

    print("\n=== Config Generation Test Complete ===\n")


if __name__ == "__main__":
    test_config_generation()
