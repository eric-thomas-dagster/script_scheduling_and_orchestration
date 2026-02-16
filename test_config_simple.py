"""Simple test to verify parameter extraction from Prefect flows."""

import ast
import sys
from pathlib import Path

# Add script_orchestrator to path
sys.path.insert(0, str(Path(__file__).parent / "script_orchestrator"))

from script_orchestrator.components.script_github_component import ScriptGithubComponent


def test_parameter_extraction():
    """Test parameter extraction from 02_simple_web_scraper.py"""

    script_path = Path("script_orchestrator/example_scripts/orchestrator_migration/02_simple_web_scraper.py")

    # Create a component instance just to access the methods
    from script_orchestrator.components.script_github_component import ScriptGithubComponentParams
    params = ScriptGithubComponentParams()
    component = ScriptGithubComponent(params=params)

    print("\n=== Testing Parameter Extraction ===\n")

    # Parse the Prefect flow
    tasks, flows = component._parse_prefect_flow(script_path)

    print(f"Found {len(tasks)} tasks and {len(flows)} flows\n")

    for flow in flows:
        print(f"Flow: {flow['name']}")
        print(f"  Parameters: {flow.get('parameters', [])}")

        # Test Config generation
        from script_orchestrator.components.script_github_component import ScriptInfo
        script_info = ScriptInfo(name="test", script_path=str(script_path), metadata=None)

        config_class = component._generate_flow_config_class(flow, script_info)

        if config_class:
            print(f"  ✓ Config class generated: {config_class.__name__}")
            print(f"  ✓ Config annotations: {config_class.__annotations__}")

            # Try to instantiate the config
            try:
                config_instance = config_class()
                print(f"  ✓ Config instantiated successfully")

                # Check attributes
                for param in flow.get('parameters', []):
                    param_name = param['name']
                    if hasattr(config_instance, param_name):
                        value = getattr(config_instance, param_name)
                        print(f"    - {param_name}: {value} (type: {type(value).__name__})")
            except Exception as e:
                print(f"  ✗ Failed to instantiate config: {e}")
        else:
            print(f"  ℹ️  No config class generated (no parameters)")

        print()

    print("=== Test Complete ===\n")


if __name__ == "__main__":
    test_parameter_extraction()
