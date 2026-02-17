"""Script orchestrator definitions.

Loads a StateBackedComponent that discovers and orchestrates Python scripts.
"""

from dagster import Definitions
from pathlib import Path
import yaml

# Load component configuration
config_path = Path(__file__).parent / "defs.yaml"
with open(config_path) as f:
    config = yaml.safe_load(f)

# Import and instantiate the StateBackedComponent
from script_orchestrator.components import ScriptGithubComponent

component = ScriptGithubComponent(**config.get("attributes", {}))

# Determine state file path
# StateBackedComponent uses defs_state config to determine where state is stored
state_dir = Path("/tmp/dagster_clean/scripts")
state_file = state_dir / "scripts_state.json"

# Create a minimal context for build_defs_from_state
class ComponentContext:
    """Minimal context for component loading."""
    def __init__(self, path: Path):
        self.path = path

context = ComponentContext(state_dir)

# Build definitions from cached state
# If state doesn't exist, returns empty Definitions
defs = component.build_defs_from_state(
    context,
    state_file if state_file.exists() else None
)
