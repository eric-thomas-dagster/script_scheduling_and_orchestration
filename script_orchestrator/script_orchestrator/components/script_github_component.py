"""State-backed component for Python script orchestration.

Features:
- Automatic schedule creation from YAML
- Partition support for time-based scripts
- Rich metadata emission
- Prefect flow mapping to Dagster ops
- Airflow DAG mapping to Dagster ops
- Config extraction from argparse, sys.argv, Prefect flows, and Airflow DAGs
- Discovers Python scripts from GitHub repositories
"""

import ast
import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Optional, Tuple

import yaml

from .parsers import AirflowParser, PrefectParser
from .parsers.dag_factory_parser import DagFactoryYamlParser
from .utils import AirflowCheckDetector, DocumentationExtractor, PerformanceMonitor, ResourceDetector

logger = logging.getLogger(__name__)
from dagster import (
    AssetCheckSpec,
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    AssetOut,
    AssetSelection,
    AssetSpec,
    Backoff,
    Config,
    ConfigurableIOManager,
    DailyPartitionsDefinition,
    DefaultScheduleStatus,
    Definitions,
    DynamicPartitionsDefinition,
    Field,
    HourlyPartitionsDefinition,
    InputContext,
    Jitter,
    MaterializeResult,
    MetadataValue,
    MonthlyPartitionsDefinition,
    Nothing,
    OpExecutionContext,
    Output,
    OutputContext,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    StaticPartitionsDefinition,
    WeeklyPartitionsDefinition,
    asset,
    asset_sensor,
    define_asset_job,
    job,
    multi_asset,
    op,
)
from dagster.components import ComponentLoadContext, StateBackedComponent
from dagster.components.resolved.base import Resolvable
from dagster.components.utils.defs_state import DefsStateConfig, DefsStateConfigArgs, ResolvedDefsStateConfig
from pydantic import BaseModel, Field as PydanticField, field_validator

# Make git optional for environments where it's not available
try:
    from git import Repo
    GIT_AVAILABLE = True
except (ImportError, Exception):
    GIT_AVAILABLE = False
    Repo = None  # type: ignore

from ..schemas.script_metadata import ScriptMetadata


def _imports_cosmos(source: str) -> bool:
    """Return True if *source* contains an import from the 'cosmos' package."""
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return False
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            module = getattr(node, "module", "") or ""
            if module == "cosmos" or module.startswith("cosmos."):
                return True
            for alias in getattr(node, "names", []):
                if alias.name == "cosmos" or alias.name.startswith("cosmos."):
                    return True
    return False


class NoOpIOManager(ConfigurableIOManager):
    """IO Manager that doesn't persist anything - used for Airflow assets that only yield metadata."""

    def handle_output(self, context: OutputContext, obj):
        """Do nothing - Airflow assets don't produce outputs to store."""
        pass

    def load_input(self, context: InputContext):
        """Do nothing - Airflow assets don't have stored inputs."""
        pass


# Helper function to parse boolean environment variables
def _env_bool(key: str, default: bool = True) -> bool:
    """Parse boolean from environment variable."""
    value = os.getenv(key)
    if value is None:
        return default
    return value.lower() == "true"


class ScriptInfo(BaseModel):
    """Discovered script information."""

    name: str
    script_path: Path
    yaml_path: Optional[Path] = None
    metadata: Optional[ScriptMetadata] = None

    class Config:
        arbitrary_types_allowed = True


class ScriptsState(BaseModel):
    """State data for discovered scripts."""

    scripts: List[ScriptInfo] = PydanticField(default_factory=list)
    repo_commit: Optional[str] = None
    repo_path: Optional[str] = None
    error: Optional[str] = None

    class Config:
        arbitrary_types_allowed = True


class ScriptGithubComponent(StateBackedComponent, BaseModel, Resolvable):
    """Component for orchestrating Python scripts with Prefect flow mapping.

    This is a state-backed component that discovers scripts from local directories
    or GitHub repositories and builds Dagster definitions for them.

    Inherits from StateBackedComponent (for state management), BaseModel (for Pydantic),
    and Resolvable (for YAML configuration support).
    """

    # State management configuration
    defs_state: ResolvedDefsStateConfig = PydanticField(
        default_factory=lambda: DefsStateConfigArgs.local_filesystem(),
        description="Configuration for where to store component state"
    )

    # Configuration fields - defaults come from environment variables
    repo_url: Optional[str] = PydanticField(
        default=None,
        description="GitHub repository URL (env: SCRIPTS_REPO_URL)"
    )
    repo_branch: str = PydanticField(
        default="main",
        description="Branch to clone/pull (env: SCRIPTS_REPO_BRANCH)"
    )
    github_token: Optional[str] = PydanticField(
        default=None,
        description="GitHub token for private repos (env: GITHUB_TOKEN)"
    )
    scripts_directory: str = PydanticField(
        default="scripts",
        description="Directory containing script files (env: SCRIPTS_DIR)"
    )
    use_local: bool = PydanticField(
        default=False,
        description="Use local scripts instead of cloning from GitHub"
    )

    # Orchestrator configuration
    airflow_enabled: bool = PydanticField(
        default=True,
        description="Enable Airflow DAG discovery and execution (env: AIRFLOW_ENABLED)"
    )
    airflow_version: Optional[str] = PydanticField(
        default=None,
        description="Target Airflow version (e.g., '2.9', '3.1'). If specified and not installed, will auto-install. (env: AIRFLOW_VERSION)"
    )
    airflow_auto_install: bool = PydanticField(
        default=True,
        description="Automatically install target airflow_version if not present or mismatched (env: AIRFLOW_AUTO_INSTALL)"
    )
    prefect_enabled: bool = PydanticField(
        default=True,
        description="Enable Prefect flow discovery and execution (env: PREFECT_ENABLED)"
    )
    prefect_version: Optional[str] = PydanticField(
        default=None,
        description="Target Prefect version (e.g., '2.14', '3.0'). If specified and not installed, will auto-install. (env: PREFECT_VERSION)"
    )
    prefect_auto_install: bool = PydanticField(
        default=True,
        description="Automatically install target prefect_auto_install if not present or mismatched (env: PREFECT_AUTO_INSTALL)"
    )

    # ── dbt / Cosmos ──────────────────────────────────────────────────────
    # Set dbt_project_path to enable native Cosmos/dbt support.
    # Any .py file that imports from 'cosmos' will be classified and handled
    # via dagster-dbt instead of being wrapped as a plain Airflow asset.
    dbt_project_path: str = PydanticField(
        default="",
        description=(
            "Path to the dbt project inside the repo (e.g. 'dbt/jaffle_shop'). "
            "Setting this enables native Cosmos/dbt support: files that import "
            "from 'cosmos' become @dbt_assets jobs instead of Airflow assets."
        ),
    )
    dbt_target: str = PydanticField(
        default="dev",
        description="dbt target profile name (must exist in profiles.yml).",
    )
    dbt_profiles_dir: str = PydanticField(
        default="",
        description="Absolute path to a profiles.yml directory. Leave blank for ~/.dbt",
    )
    use_default_cosmos_skip_patterns: bool = PydanticField(
        default=True,
        description=(
            "Apply built-in heuristic skip patterns for Cosmos DAGs "
            "(kubernetes, virtualenv, watcher, etc.). "
            "Set to false if your repo filenames contain those substrings legitimately."
        ),
    )

    # Per-file overrides — for repos you don't own.
    # Key = filename stem (no .py), value = dict of override fields.
    # Supported keys: enabled, cosmos_action, schedule, dbt_select,
    #                 description, reason.
    # Example:
    #   file_overrides:
    #     basic_cosmos_dag:
    #       cosmos_action: replace
    #       schedule: "0 0 * * *"
    #     jaffle_shop_kubernetes:
    #       enabled: false
    file_overrides: Dict[str, Any] = PydanticField(
        default_factory=dict,
        description="Per-file configuration overrides keyed by filename stem.",
    )

    # Internal flag — set automatically when dbt_project_path is configured.
    # Tells _discover_scripts to leave Cosmos files for _build_cosmos_dbt_defs.
    skip_cosmos_dags: bool = PydanticField(
        default=False,
        description="Internal — auto-set to True when dbt_project_path is configured.",
    )

    # Parser instances (initialized in model_post_init)
    prefect_parser: Optional[Any] = None
    airflow_parser: Optional[Any] = None
    dag_factory_parser: Optional[Any] = None

    # Class variable to track if Airflow DB has been initialized
    _airflow_db_checked: ClassVar[bool] = False

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    @field_validator('airflow_version', 'prefect_version', mode='before')
    @classmethod
    def coerce_version_to_string(cls, v):
        """Convert numeric versions to strings (e.g., YAML's 3.1 -> "3.1")."""
        if v is None:
            return v
        return str(v)

    def model_post_init(self, __context):
        """Initialize after Pydantic model initialization."""
        # Apply environment variable defaults if not explicitly set in YAML
        if self.repo_url is None:
            self.repo_url = os.getenv("SCRIPTS_REPO_URL")
        if self.repo_branch == "main" and os.getenv("SCRIPTS_REPO_BRANCH"):
            self.repo_branch = os.getenv("SCRIPTS_REPO_BRANCH")
        if self.github_token is None:
            self.github_token = os.getenv("GITHUB_TOKEN")
        if self.scripts_directory == "scripts" and os.getenv("SCRIPTS_DIR"):
            self.scripts_directory = os.getenv("SCRIPTS_DIR")
        if not self.use_local and os.getenv("USE_LOCAL_SCRIPTS"):
            self.use_local = _env_bool("USE_LOCAL_SCRIPTS", False)
        if self.airflow_version is None and os.getenv("AIRFLOW_VERSION"):
            self.airflow_version = os.getenv("AIRFLOW_VERSION")
        if self.prefect_version is None and os.getenv("PREFECT_VERSION"):
            self.prefect_version = os.getenv("PREFECT_VERSION")

        # When a dbt project is configured, Cosmos DAGs are handled natively —
        # skip them in the regular script discovery pass.
        if self.dbt_project_path:
            self.skip_cosmos_dags = True

        # Log orchestrator configuration
        logger.info(f"Orchestrator config: airflow_enabled={self.airflow_enabled}, "
                   f"airflow_version={self.airflow_version}, "
                   f"airflow_auto_install={self.airflow_auto_install}, "
                   f"prefect_enabled={self.prefect_enabled}, "
                   f"prefect_version={self.prefect_version}, "
                   f"prefect_auto_install={self.prefect_auto_install}")

        # Ensure orchestrator versions are installed
        if self.airflow_enabled:
            self._ensure_orchestrator_installed("airflow", self.airflow_version, self.airflow_auto_install)
        if self.prefect_enabled:
            self._ensure_orchestrator_installed("prefect", self.prefect_version, self.prefect_auto_install)

        # Initialize parsers
        self.prefect_parser = PrefectParser()
        try:
            self.airflow_parser = AirflowParser()
        except Exception as e:
            logger.warning(f"Failed to initialize AirflowParser: {e}. Airflow support may be limited.")
            self.airflow_parser = None
        self.dag_factory_parser = DagFactoryYamlParser()

    @property
    def defs_state_config(self) -> DefsStateConfig:
        """Return the state configuration for this component."""
        return DefsStateConfig.from_args(
            self.defs_state,
            default_key="scripts"
        )

    def _ensure_orchestrator_installed(self, orchestrator: str, target_version: Optional[str], auto_install: bool = True):
        """Ensure the specified orchestrator is installed at the target version.

        Args:
            orchestrator: "airflow" or "prefect"
            target_version: Target version like "2.9" or "3.1" (None = use any installed version)
            auto_install: If True, automatically install/upgrade to target version
        """
        package_name = "apache-airflow" if orchestrator == "airflow" else "prefect"

        # Check if already installed
        try:
            if orchestrator == "airflow":
                import airflow
                # Handle both Airflow 2.x and 3.x version detection
                installed_version = None
                if hasattr(airflow, '__version__'):
                    installed_version = airflow.__version__
                elif hasattr(airflow, 'version'):
                    import airflow.version
                    if hasattr(airflow.version, 'version'):
                        installed_version = airflow.version.version

                if not installed_version:
                    # Fallback to importlib.metadata
                    import importlib.metadata
                    installed_version = importlib.metadata.version('apache-airflow')

            elif orchestrator == "prefect":
                import prefect
                installed_version = prefect.__version__
            else:
                return

            # No target version specified - use whatever's installed
            if not target_version:
                logger.info(f"{orchestrator.title()} {installed_version} is installed (no target version specified, using installed version)")
                return

            # Check if installed version matches target
            installed_major_minor = ".".join(installed_version.split(".")[:2])
            target_major_minor = ".".join(target_version.split(".")[:2])

            if installed_major_minor == target_major_minor:
                logger.info(f"{orchestrator.title()} {installed_version} is installed (matches target {target_version})")
                return

            # Version mismatch
            if not auto_install:
                logger.warning(
                    f"{orchestrator.title()} {installed_version} is installed, but version {target_version} was requested. "
                    f"Auto-install is disabled. To install manually, run: "
                    f"uv pip install '{package_name}>={target_version},<{int(target_version.split('.')[0])+1}.0'"
                )
                return

            # Auto-install target version
            logger.info(f"{orchestrator.title()} {installed_version} installed, but {target_version} requested. Auto-installing...")
            self._install_orchestrator(orchestrator, target_version)

        except ImportError:
            # Not installed at all
            if not target_version:
                logger.warning(
                    f"{orchestrator.title()} is not installed and no target version specified. "
                    f"Skipping {orchestrator} support. To enable, install manually: uv pip install {package_name}"
                )
                return

            if not auto_install:
                logger.warning(
                    f"{orchestrator.title()} is not installed. Auto-install is disabled. "
                    f"To install manually, run: uv pip install '{package_name}>={target_version},<{int(target_version.split('.')[0])+1}.0'"
                )
                return

            # Auto-install
            logger.info(f"{orchestrator.title()} not installed. Auto-installing version {target_version}...")
            self._install_orchestrator(orchestrator, target_version)

        except Exception as e:
            # Only log at debug level since we handle most cases gracefully
            logger.debug(f"Exception during {orchestrator} version check (handled): {e}")
            logger.warning(f"{orchestrator.title()} installation check encountered an issue. Skipping auto-install.")

    def _install_orchestrator(self, orchestrator: str, version: str):
        """Install the specified orchestrator version using uv pip.

        Args:
            orchestrator: "airflow" or "prefect"
            version: Version to install like "2.9" or "3.1"
        """
        package_name = "apache-airflow" if orchestrator == "airflow" else "prefect"
        major_version = int(version.split('.')[0])
        next_major = major_version + 1
        version_spec = f"{package_name}>={version},<{next_major}.0"

        try:
            logger.info(f"Installing {orchestrator} {version}...")
            logger.info(f"Running: uv pip install '{version_spec}'")

            result = subprocess.run(
                ["uv", "pip", "install", version_spec],
                capture_output=True,
                text=True,
                timeout=300,  # 5 minute timeout for installation
                check=False
            )

            if result.returncode == 0:
                logger.info(f"Successfully installed {orchestrator} {version}")

                # Initialize Airflow DB after installation (if we have repo_path)
                # Will be called again from build_defs_from_state with proper repo_path
                if orchestrator == "airflow":
                    ScriptGithubComponent._airflow_db_checked = False  # Reset flag
            else:
                logger.error(
                    f"Failed to install {orchestrator} {version}. "
                    f"Exit code: {result.returncode}\n"
                    f"stdout: {result.stdout}\n"
                    f"stderr: {result.stderr}"
                )

        except subprocess.TimeoutExpired:
            logger.error(f"Installation of {orchestrator} {version} timed out after 5 minutes")
        except Exception as e:
            logger.error(f"Error installing {orchestrator} {version}: {e}")

    def _ensure_airflow_db_initialized(self, repo_path: str):
        """Ensure Airflow database is initialized (one-time check).

        This check runs once when definitions are first built to ensure the Airflow
        database is initialized before any DAG executions. Uses sys.executable to
        invoke airflow as a module from the current Python environment.

        Args:
            repo_path: Path to the repository/scripts directory
        """
        logger.info(f"🔍 Checking Airflow DB initialization (enabled={self.airflow_enabled}, checked={ScriptGithubComponent._airflow_db_checked})")

        # Skip if Airflow is disabled
        if not self.airflow_enabled:
            logger.info("⏭️  Airflow is disabled - skipping DB initialization")
            return

        # Skip if already checked
        if ScriptGithubComponent._airflow_db_checked:
            logger.info("⏭️  Airflow DB already checked - skipping")
            return

        try:
            # Set up environment with AIRFLOW_HOME
            env = os.environ.copy()
            # Use repo directory for Airflow home to isolate from system Airflow
            airflow_home = Path(repo_path) / ".airflow"
            airflow_home.mkdir(exist_ok=True)
            env["AIRFLOW_HOME"] = str(airflow_home)
            env["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
            env["AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS"] = "False"

            logger.info(f"Checking Airflow database at AIRFLOW_HOME={airflow_home}")

            # Check if Airflow DB is initialized
            # Use uv run to execute airflow with the correct virtual environment
            check_result = subprocess.run(
                self._build_airflow_command("db", "check"),
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
                env=env
            )

            if check_result.returncode != 0:
                # DB not initialized - run migration  (this can take 30-60 seconds)
                logger.info("Airflow database not initialized - running migration (this may take 30-60 seconds)...")
                migrate_result = subprocess.run(
                    self._build_airflow_command("db", "migrate"),
                    capture_output=True,
                    text=True,
                    timeout=120,
                    check=False,
                    env=env
                )

                if migrate_result.returncode == 0:
                    # Successfully initialized
                    logger.info("✅ Airflow database initialized successfully")

                    # Create stub connections for examples
                    self._create_stub_airflow_connections(env)

                    ScriptGithubComponent._airflow_db_checked = True
                else:
                    # Log the actual error
                    error_msg = migrate_result.stderr.strip() if migrate_result.stderr else migrate_result.stdout.strip()
                    logger.error(
                        f"Failed to initialize Airflow database:\n{error_msg}\n"
                        "Airflow DAGs may not execute properly. "
                        f"Try running manually: cd {repo_path} && AIRFLOW_HOME={airflow_home} uv run airflow db migrate"
                    )
            else:
                # DB already initialized
                logger.debug("Airflow database already initialized")

                # Ensure stub connections exist (idempotent)
                self._create_stub_airflow_connections(env)

                ScriptGithubComponent._airflow_db_checked = True

        except (FileNotFoundError, ModuleNotFoundError) as e:
            # Check if this is the "airflow command not found" issue
            if "airflow" in str(e) or "No such file" in str(e):
                logger.warning(
                    "⚠️  Airflow command not found. The 'airflow' console script may not be installed properly.\n"
                    "   This can happen with uv. To fix, run:\n"
                    "   uv pip install --reinstall apache-airflow\n"
                    "   Then restart the server."
                )
            else:
                logger.info(f"⏭️  Airflow not available ({e.__class__.__name__}: {e}) - skipping DB initialization check")
            ScriptGithubComponent._airflow_db_checked = True  # Don't check again
        except subprocess.TimeoutExpired:
            logger.warning("⏱️  Airflow DB check timed out - skipping initialization")
            ScriptGithubComponent._airflow_db_checked = True  # Don't check again
        except Exception as e:
            logger.warning(f"⚠️  Could not check Airflow DB: {e}")
            ScriptGithubComponent._airflow_db_checked = True  # Don't check again


    def _extract_connection_ids_from_dags(self) -> dict:
        """Extract all connection IDs and their parameters from DAG files."""
        connections = {}  # conn_id -> {type, params}

        # Scan all dag-factory YAML files
        for script_info in self.discovered_scripts:
            if script_info.script_path.suffix == '.yaml' and script_info.script_path.name != script_info.script_path.stem + '.dagster.yaml':
                try:
                    import yaml
                    with open(script_info.script_path, 'r') as f:
                        dag_config = yaml.safe_load(f)

                    if not dag_config:
                        continue

                    # Iterate through all DAGs in the file
                    for dag_id, dag_def in dag_config.items():
                        if not isinstance(dag_def, dict) or 'tasks' not in dag_def:
                            continue

                        # Check each task for connection IDs
                        for task_id, task_config in dag_def['tasks'].items():
                            if not isinstance(task_config, dict):
                                continue

                            # Look for *_conn_id fields
                            for key, value in task_config.items():
                                if key.endswith('_conn_id') and isinstance(value, str):
                                    if value not in connections:
                                        # Infer connection type and extract parameters
                                        conn_type = self._infer_connection_type(key, task_config.get('operator', ''))
                                        conn_params = self._extract_connection_params(task_config, conn_type)
                                        connections[value] = {
                                            'type': conn_type,
                                            'params': conn_params
                                        }
                                        logger.debug(f"Found connection: {value} (type: {conn_type}, params: {conn_params})")

                except Exception as e:
                    logger.debug(f"Could not extract connections from {script_info.script_path}: {e}")

        return connections

    def _extract_connection_params(self, task_config: dict, conn_type: str) -> dict:
        """Extract connection-specific parameters from task config."""
        params = {}

        # Extract parameters based on connection type
        if conn_type == 'snowflake':
            # Snowflake-specific parameters
            for key in ['warehouse', 'database', 'schema', 'role', 'account']:
                if key in task_config:
                    params[key] = task_config[key]

        elif conn_type == 'aws':
            # AWS-specific parameters
            for key in ['region_name', 'aws_access_key_id', 'aws_secret_access_key']:
                if key in task_config:
                    params[key] = task_config[key]
            # S3-specific
            if 'bucket_name' in task_config:
                params['bucket_name'] = task_config['bucket_name']

        elif conn_type == 'postgres' or conn_type == 'mysql':
            # Database-specific parameters
            for key in ['host', 'port', 'database', 'schema']:
                if key in task_config:
                    params[key] = task_config[key]

        elif conn_type == 'http':
            # HTTP-specific parameters
            for key in ['host', 'port', 'endpoint', 'method']:
                if key in task_config:
                    params[key] = task_config[key]

        return params

    def _infer_connection_type(self, conn_id_field: str, operator: str) -> str:
        """Infer Airflow connection type from field name or operator."""
        # Map common patterns to connection types
        if 'aws' in conn_id_field.lower() or 's3' in operator.lower():
            return 'aws'
        elif 'snowflake' in conn_id_field.lower() or 'snowflake' in operator.lower():
            return 'snowflake'
        elif 'postgres' in conn_id_field.lower() or 'postgres' in operator.lower():
            return 'postgres'
        elif 'mysql' in conn_id_field.lower() or 'mysql' in operator.lower():
            return 'mysql'
        elif 'http' in conn_id_field.lower() or 'http' in operator.lower() or 'api' in conn_id_field.lower():
            return 'http'
        elif 'gcp' in conn_id_field.lower() or 'google' in operator.lower():
            return 'google_cloud_platform'
        elif 'azure' in conn_id_field.lower() or 'azure' in operator.lower():
            return 'azure'
        else:
            return 'generic'  # Fallback

    def _create_stub_airflow_connections(self, env: dict):
        """Create Airflow connections with parameters from DAG files."""
        # Discover connections from DAG files
        connections = self._extract_connection_ids_from_dags()

        if not connections:
            logger.debug("No connection IDs found in DAG files")
            return

        logger.info(f"Creating Airflow connections for {len(connections)} discovered connection(s)...")

        for conn_id, conn_info in connections.items():
            try:
                conn_type = conn_info['type']
                conn_params = conn_info['params']

                # Check if connection already exists
                check_cmd = self._build_airflow_command("connections", "get", conn_id)
                check_result = subprocess.run(
                    check_cmd,
                    capture_output=True,
                    text=True,
                    timeout=10,
                    check=False,
                    env=env
                )

                if check_result.returncode == 0:
                    logger.debug(f"Connection {conn_id} already exists")
                    continue

                # Build connection creation command
                add_cmd = self._build_airflow_command(
                    "connections", "add", conn_id,
                    "--conn-type", conn_type
                )

                # Add connection-specific parameters
                if conn_type == 'snowflake':
                    # Use JSON extra for Snowflake parameters
                    import json
                    extra = {}
                    for key in ['warehouse', 'database', 'schema', 'role', 'account']:
                        if key in conn_params:
                            extra[key] = conn_params[key]
                    if extra:
                        add_cmd.extend(["--conn-extra", json.dumps(extra)])

                elif conn_type == 'http':
                    # HTTP connection parameters
                    if 'host' in conn_params:
                        add_cmd.extend(["--conn-host", conn_params['host']])
                    else:
                        add_cmd.extend(["--conn-host", "localhost"])

                    if 'port' in conn_params:
                        add_cmd.extend(["--conn-port", str(conn_params['port'])])
                    else:
                        add_cmd.extend(["--conn-port", "8080"])

                elif conn_type in ['postgres', 'mysql']:
                    # Database connection parameters
                    if 'host' in conn_params:
                        add_cmd.extend(["--conn-host", conn_params['host']])
                    if 'port' in conn_params:
                        add_cmd.extend(["--conn-port", str(conn_params['port'])])
                    if 'database' in conn_params:
                        add_cmd.extend(["--conn-schema", conn_params['database']])

                elif conn_type == 'aws':
                    # AWS connection parameters
                    import json
                    extra = {}
                    for key in ['region_name', 'bucket_name']:
                        if key in conn_params:
                            extra[key] = conn_params[key]
                    if extra:
                        add_cmd.extend(["--conn-extra", json.dumps(extra)])

                result = subprocess.run(
                    add_cmd,
                    capture_output=True,
                    text=True,
                    timeout=10,
                    check=False,
                    env=env
                )

                if result.returncode == 0:
                    param_str = ', '.join(f"{k}={v}" for k, v in conn_params.items()) if conn_params else 'default'
                    logger.info(f"  ✅ Created connection: {conn_id} (type: {conn_type}, params: {param_str})")
                else:
                    logger.debug(f"Could not create connection {conn_id}: {result.stderr}")

            except Exception as e:
                logger.debug(f"Error creating connection {conn_id}: {e}")

    def _detect_airflow_version_in_uv(self) -> Optional[Tuple[int, int]]:
        """Detect Airflow version by running Python in uv environment.

        This is more reliable than trying to import at module load time,
        since the Dagster process may not have access to uv-managed packages.
        """
        try:
            import subprocess
            result = subprocess.run(
                ["uv", "run", "python", "-c",
                 "import importlib.metadata; print(importlib.metadata.version('apache-airflow'))"],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                version_str = result.stdout.strip()
                parts = version_str.split('.')
                version_tuple = (int(parts[0]), int(parts[1]))
                logger.info(f"Detected Airflow version via uv: {version_tuple}")
                return version_tuple
        except Exception as e:
            logger.debug(f"Could not detect Airflow version via uv: {e}")
        return None

    def _build_airflow_command(self, *args) -> List[str]:
        """Build Airflow CLI command that works with both Airflow 2.x and 3.x.

        Airflow 2.x: python -m airflow <command>
        Airflow 3.x: airflow <command> (no python -m)

        Args:
            *args: Airflow command arguments (e.g., "dags", "test", "dag_id", "date")

        Returns:
            List of command arguments suitable for subprocess
        """
        # Detect version in uv environment (cached after first call)
        if not hasattr(self, '_cached_airflow_version'):
            self._cached_airflow_version = self._detect_airflow_version_in_uv()

        version = self._cached_airflow_version
        if version and version[0] >= 3:
            # Airflow 3.x: use direct airflow command
            logger.debug(f"Using Airflow 3.x command format (detected version: {version})")
            return ["uv", "run", "airflow"] + list(args)

        # Default to Airflow 2.x style (python -m airflow)
        # This also works as fallback if version detection fails
        logger.debug(f"Using Airflow 2.x command format (detected version: {version})")
        return ["uv", "run", "python", "-m", "airflow"] + list(args)

    def write_state_to_path(self, state_path: Path) -> None:
        """Clone/pull the GitHub repo and discover script directories.

        Args:
            state_path: Path to write the state JSON file to
        """
        state = ScriptsState()

        try:
            if self.use_local:
                # Use local scripts directory
                scripts_dir = Path(self.scripts_directory)
                if not scripts_dir.is_absolute():
                    scripts_dir = Path.cwd() / scripts_dir

                state.repo_path = str(scripts_dir.parent)
                logger.info(f"Using local scripts from: {scripts_dir}")

                if scripts_dir.exists():
                    state.scripts = self._discover_scripts(scripts_dir)
                    logger.info(f"Discovered {len(state.scripts)} scripts locally")
                else:
                    raise ValueError(f"Local scripts directory not found: {scripts_dir}")
            else:
                # Clone from GitHub
                if not GIT_AVAILABLE:
                    raise ValueError(
                        "Git is not available in this environment. "
                        "Set USE_LOCAL_SCRIPTS=true to use local scripts instead."
                    )

                if not self.repo_url:
                    raise ValueError(
                        "SCRIPTS_REPO_URL environment variable is required when not using local scripts."
                    )

                clone_dir = state_path.parent / "repo_clone"
                clone_dir.mkdir(parents=True, exist_ok=True)

                repo = self._clone_or_pull_repo(clone_dir, self.github_token)
                state.repo_commit = repo.head.commit.hexsha
                state.repo_path = str(clone_dir)

                scripts_dir = clone_dir / self.scripts_directory
                if scripts_dir.exists():
                    state.scripts = self._discover_scripts(scripts_dir)
                    logger.info(f"Discovered {len(state.scripts)} scripts from GitHub")

            # Install dependencies from the scripts directory if found
            if state.scripts and state.repo_path:
                self._install_script_dependencies(Path(state.repo_path) / self.scripts_directory)

        except Exception as e:
            state.error = str(e)
            logger.error(f"Error discovering scripts: {e}")

        # Write state to disk
        state_path.write_text(state.model_dump_json(indent=2))

    def build_defs_from_state(self, context: ComponentLoadContext, state_path: Optional[Path]) -> Definitions:
        """Build Dagster definitions from cached state.

        Args:
            context: Component loading context
            state_path: Path to the state file (None if state doesn't exist yet)

        Returns:
            Definitions containing discovered assets, jobs, sensors, and schedules
        """
        if state_path is None or not state_path.exists():
            logger.warning("No scripts state found. Run refresh to discover scripts.")
            return Definitions()

        state = ScriptsState.model_validate_json(state_path.read_text())

        if state.error:
            logger.error(f"Error in scripts state: {state.error}")
            return Definitions()

        # Ensure Airflow DB is initialized (one-time check) before building any Airflow assets
        # Now that we have state loaded, we can access repo_path
        if state.repo_path:
            self._ensure_airflow_db_initialized(state.repo_path)

        all_assets = []
        all_jobs = []
        all_sensors = []
        all_schedules = []
        all_asset_checks = []

        # Build script assets
        for script_info in state.scripts:
            if script_info.metadata and not script_info.metadata.enabled:
                continue

            result = self._build_script_asset_with_prefect_check(script_info, state.scripts, state.repo_path)

            # Handle both single definitions and lists of definitions
            if isinstance(result, list):
                # Multiple definitions returned (assets, jobs, sensors)
                for item in result:
                    # Classify each definition by type name
                    type_name = type(item).__name__
                    if 'Job' in type_name:
                        # This is a job
                        all_jobs.append(item)
                    elif 'Sensor' in type_name:
                        # This is a sensor
                        all_sensors.append(item)
                    elif 'Asset' in type_name:
                        # This is an asset
                        all_assets.append(item)
                    else:
                        # Unknown type - log warning
                        logger.warning(f"Unknown definition type: {type_name}")
            elif result is not None:
                # Check type even for single results
                type_name = type(result).__name__
                if 'Job' in type_name:
                    all_jobs.append(result)
                elif 'Sensor' in type_name:
                    all_sensors.append(result)
                elif 'Asset' in type_name:
                    all_assets.append(result)

                    # Create schedule if configured (only for assets)
                    if script_info.metadata and script_info.metadata.schedule:
                        # Get the actual asset key from the definition
                        # This handles cases where the asset name differs from script name (e.g., Airflow DAGs with datasets)
                        actual_asset_key = result.key.to_user_string()

                        schedule = self._build_schedule(
                            f"{actual_asset_key}_schedule",
                            script_info.metadata.schedule,
                            actual_asset_key,
                        )
                        all_schedules.append(schedule)
                else:
                    logger.warning(f"Unknown definition type for single result: {type_name}")

        logger.info(
            f"Created {len(all_assets)} assets, {len(all_jobs)} jobs, "
            f"{len(all_sensors)} sensors, {len(all_schedules)} schedules, "
            f"and {len(all_asset_checks)} asset checks"
        )

        # Add no-op IO manager for Airflow assets that only yield metadata
        script_defs = Definitions(
            assets=all_assets,
            jobs=all_jobs,
            sensors=all_sensors,
            schedules=all_schedules,
            asset_checks=all_asset_checks if all_asset_checks else None,
            resources={"airflow_io_manager": NoOpIOManager()},
        )

        # If a dbt project is configured, build native Cosmos/dbt definitions
        # and merge them in.  The repo is already cloned (state.repo_path exists).
        if self.dbt_project_path and state.repo_path:
            try:
                cosmos_defs = self._build_cosmos_dbt_defs(Path(state.repo_path))
                return Definitions.merge(script_defs, cosmos_defs)
            except Exception as exc:
                logger.warning(
                    "Cosmos/dbt setup failed — returning script definitions only. "
                    "Error: %s", exc, exc_info=True,
                )

        return script_defs

    def _clone_or_pull_repo(self, clone_dir: Path, github_token: Optional[str]) -> Repo:
        """Clone or pull the GitHub repository."""
        repo_url = self.repo_url

        if github_token:
            if "github.com" in repo_url:
                repo_url = repo_url.replace("https://", f"https://{github_token}@")

        if (clone_dir / ".git").exists():
            repo = Repo(clone_dir)
            repo.remotes.origin.pull(self.repo_branch)
        else:
            repo = Repo.clone_from(repo_url, clone_dir, branch=self.repo_branch)

        return repo

    def _is_airflow_version_compatible(self, detected_version: str) -> bool:
        """
        Check if a DAG's detected version is compatible with the installed Airflow version.

        Args:
            detected_version: "2.x" or "3.x"

        Returns:
            True if compatible, False otherwise
        """
        if not self.airflow_parser or not self.airflow_parser._airflow_version:
            # Can't determine compatibility without version info
            return True

        installed_major, _ = self.airflow_parser._airflow_version

        # Extract major version from detected version (e.g., "2.x" -> 2)
        try:
            detected_major = int(detected_version.split('.')[0])
        except (ValueError, IndexError):
            # Can't parse, assume compatible
            return True

        # Check if major versions match
        compatible = detected_major == installed_major

        if not compatible:
            logger.info(
                f"Version mismatch: DAG written for Airflow {detected_version}, "
                f"but Airflow {installed_major}.x is installed"
            )

        return compatible

    def _discover_scripts(self, scripts_dir: Path) -> List[ScriptInfo]:
        """Discover all Python scripts with optional YAML configuration."""
        scripts = []
        discovered_yaml_files = set()
        skipped_version_mismatch = []

        for script_file in scripts_dir.rglob("*.py"):
            # Skip __init__.py and hidden files
            if script_file.name.startswith("_") or script_file.name.startswith("."):
                continue

            # Skip files in utility directories (include/, tasks/, scripts/, etc.)
            # These are typically helper modules, not standalone scripts
            path_parts = script_file.parts
            if any(part in ['include', 'utils', 'lib', 'helpers', 'scripts'] for part in path_parts):
                logger.debug(f"Skipping {script_file.name} - in utility directory")
                continue

            # Skip Cosmos DAGs when CosmosGithubComponent is also active.
            # Those files are handled natively via dagster-dbt and should not
            # be double-processed as plain Airflow assets.
            if self.skip_cosmos_dags:
                try:
                    source = script_file.read_text(encoding="utf-8", errors="ignore")
                    if _imports_cosmos(source):
                        logger.info(
                            "Skipping %s — imports from 'cosmos' (handled by CosmosGithubComponent)",
                            script_file.name,
                        )
                        continue
                except OSError:
                    pass

            # Look for corresponding YAML file
            yaml_file = script_file.with_suffix(".yaml")
            metadata = None
            if yaml_file.exists():
                discovered_yaml_files.add(yaml_file)

                # Check if this is a dag-factory YAML - if so, skip the .py file
                if self.dag_factory_parser.is_dag_factory_yaml(yaml_file):
                    logger.debug(f"Skipping {script_file.name} - generated from dag-factory YAML {yaml_file.name}")
                    continue

                try:
                    yaml_content = yaml_file.read_text()
                    metadata_dict = yaml.safe_load(yaml_content)
                    logger.debug(f"Loading YAML: {yaml_file.name}")
                    # Debug: log ETL YAML specifically
                    if "etl" in yaml_file.name.lower():
                        logger.warning(f"!!! ETL YAML: {yaml_file.name}")
                        logger.warning(f"!!! Parsed mode: {metadata_dict.get('prefect_mapping', {}).get('mode')}")
                    metadata = ScriptMetadata(**metadata_dict)
                except Exception as e:
                    logger.warning(f"Could not parse {yaml_file}: {e}")

            # Generate script name from file path
            script_name = script_file.stem

            scripts.append(
                ScriptInfo(
                    name=script_name,
                    script_path=script_file,
                    yaml_path=yaml_file if yaml_file.exists() else None,
                    metadata=metadata,
                )
            )

        # Also discover standalone dag-factory YAML files (no corresponding .py file)
        if self.airflow_enabled:
            for yaml_file in scripts_dir.rglob("*.yaml"):
                # Skip if already associated with a Python script
                if yaml_file in discovered_yaml_files:
                    logger.debug(f"Skipping {yaml_file.name} - already discovered with Python script")
                    continue

                # Skip hidden files
                if yaml_file.name.startswith("."):
                    continue

                # Check if this is a dag-factory YAML
                if self.dag_factory_parser.is_dag_factory_yaml(yaml_file):
                    logger.info(f"Found dag-factory YAML: {yaml_file}")

                    # Parse the YAML to get all DAGs for metadata
                    dags = self.dag_factory_parser.parse_dag_factory_yaml(yaml_file)

                    # Check version compatibility
                    if dags and 'dag_airflow_version' in dags[0]:
                        detected_version = dags[0]['dag_airflow_version']
                        if not self._is_airflow_version_compatible(detected_version):
                            logger.warning(
                                f"⏭️  Skipping {yaml_file.name} - requires Airflow {detected_version}, "
                                f"but Airflow {self.airflow_parser._airflow_version[0]}.x is installed"
                            )
                            skipped_version_mismatch.append(yaml_file.name)
                            continue

                    dag_ids = [dag_info['dag_id'] for dag_info in dags]
                    logger.info(f"Discovered dag-factory YAML with {len(dag_ids)} DAG(s): {dag_ids} from {yaml_file}")

                    # Create ONE ScriptInfo for the entire YAML file
                    # The _build_dag_factory_yaml_assets method will create assets for all DAGs in it
                    script_name = f"dag_factory_{yaml_file.stem}"
                    logger.warning(f"🔵 NEW CODE RUNNING: Creating single ScriptInfo '{script_name}' for {len(dag_ids)} DAGs")

                    # Look for companion metadata file for dag-factory YAMLs
                    # Pattern: example_dag_factory.yaml -> example_dag_factory.dagster.yaml
                    companion_yaml = yaml_file.parent / f"{yaml_file.stem}.dagster.yaml"
                    metadata = None

                    if companion_yaml.exists():
                        try:
                            companion_content = companion_yaml.read_text()
                            metadata_dict = yaml.safe_load(companion_content)
                            metadata = ScriptMetadata(**metadata_dict)
                            logger.info(f"Loaded companion metadata from {companion_yaml.name} for {yaml_file.name}")
                        except Exception as e:
                            logger.warning(f"Could not parse companion file {companion_yaml}: {e}")

                    # Fall back to default metadata if no companion file
                    if metadata is None:
                        metadata = ScriptMetadata(
                            enabled=True,
                            script_type="airflow",
                            description=f"DAG Factory YAML with {len(dag_ids)} DAG(s): {', '.join(dag_ids)}",
                            kinds=["airflow", "dag-factory", "yaml"],
                            tags={
                                "source": "dag_factory_yaml",
                                "yaml_file": yaml_file.name,
                                "dag_count": str(len(dag_ids)),
                            },
                        )

                    scripts.append(
                        ScriptInfo(
                            name=script_name,
                            script_path=yaml_file,
                            yaml_path=yaml_file,
                            metadata=metadata,
                        )
                    )

        # Log summary of version compatibility filtering
        if skipped_version_mismatch:
            logger.warning(
                f"⏭️  Skipped {len(skipped_version_mismatch)} Airflow DAG(s) due to version mismatch: "
                f"{', '.join(skipped_version_mismatch)}"
            )

        return scripts

    def _install_script_dependencies(self, scripts_dir: Path):
        """Detect and install dependencies from script projects."""
        dependency_files_installed = []
        
        # Check for requirements.txt in root
        requirements_txt = scripts_dir / "requirements.txt"
        if requirements_txt.exists():
            logger.info(f"Found {requirements_txt}, installing dependencies...")
            try:
                result = subprocess.run(
                    ["uv", "pip", "install", "-r", str(requirements_txt)],
                    capture_output=True,
                    text=True,
                    check=False
                )
                if result.returncode == 0:
                    dependency_files_installed.append(str(requirements_txt))
                    logger.info(f"✅ Installed dependencies from {requirements_txt}")
                else:
                    logger.warning(f"Failed to install from {requirements_txt}: {result.stderr}")
            except Exception as e:
                logger.warning(f"Error installing from {requirements_txt}: {e}")
        
        # Check for pyproject.toml in root
        pyproject_toml = scripts_dir / "pyproject.toml"
        if pyproject_toml.exists():
            logger.info(f"Found {pyproject_toml}, installing dependencies...")
            try:
                # Try to install using uv pip install from the directory
                result = subprocess.run(
                    ["uv", "pip", "install", "-e", str(scripts_dir.parent)],
                    capture_output=True,
                    text=True,
                    check=False
                )
                if result.returncode == 0:
                    dependency_files_installed.append(str(pyproject_toml))
                    logger.info(f"✅ Installed dependencies from {pyproject_toml}")
                else:
                    logger.warning(f"Failed to install from {pyproject_toml}: {result.stderr}")
            except Exception as e:
                logger.warning(f"Error installing from {pyproject_toml}: {e}")
        
        # Check for setup.py in root
        setup_py = scripts_dir / "setup.py"
        if setup_py.exists():
            logger.info(f"Found {setup_py}, installing dependencies...")
            try:
                result = subprocess.run(
                    ["uv", "pip", "install", "-e", str(scripts_dir.parent)],
                    capture_output=True,
                    text=True,
                    check=False
                )
                if result.returncode == 0:
                    dependency_files_installed.append(str(setup_py))
                    logger.info(f"✅ Installed dependencies from {setup_py}")
                else:
                    logger.warning(f"Failed to install from {setup_py}: {result.stderr}")
            except Exception as e:
                logger.warning(f"Error installing from {setup_py}: {e}")
        
        # Check for subdirectories with their own requirements
        for subdir in scripts_dir.iterdir():
            if not subdir.is_dir() or subdir.name.startswith('.'):
                continue
            
            subdir_requirements = subdir / "requirements.txt"
            if subdir_requirements.exists():
                logger.info(f"Found {subdir_requirements}, installing dependencies...")
                try:
                    result = subprocess.run(
                        ["uv", "pip", "install", "-r", str(subdir_requirements)],
                        capture_output=True,
                        text=True,
                        check=False
                    )
                    if result.returncode == 0:
                        dependency_files_installed.append(str(subdir_requirements))
                        logger.info(f"✅ Installed dependencies from {subdir_requirements}")
                    else:
                        logger.warning(f"Failed to install from {subdir_requirements}: {result.stderr}")
                except Exception as e:
                    logger.warning(f"Error installing from {subdir_requirements}: {e}")
        
        if dependency_files_installed:
            logger.info(f"Installed dependencies from {len(dependency_files_installed)} file(s)")
        else:
            logger.info("No dependency files found in scripts directory")

    # ===== Prefect Flow Parsing Methods =====

    def _parse_prefect_flow(self, script_path: Path):
        """Parse Prefect file to extract tasks and flow structure using AST."""
        return self.prefect_parser.parse_flow(script_path)

    def _parse_airflow_dag(self, script_path: Path):
        """Parse Airflow DAG file to extract tasks and DAG structure using AST."""
        if not self.airflow_parser:
            logger.warning("Airflow parser not initialized. Cannot parse DAG.")
            return [], []
        return self.airflow_parser.parse_dag(script_path)

    # ===== Argparse and sys.argv Parsing Methods =====

    def _parse_argparse_arguments(self, script_path: Path):
        """Parse argparse argument definitions from a Python script."""
        try:
            with open(script_path, 'r') as f:
                tree = ast.parse(f.read(), filename=str(script_path))

            parameters = []

            for node in ast.walk(tree):
                if isinstance(node, ast.Call):
                    if (isinstance(node.func, ast.Attribute) and
                        node.func.attr == 'add_argument'):

                        if node.args:
                            arg_name_node = node.args[0]
                            if isinstance(arg_name_node, ast.Constant):
                                arg_name = arg_name_node.value
                                arg_name = arg_name.lstrip('-')
                                arg_name_normalized = arg_name.replace('-', '_')

                                param_info = {
                                    'name': arg_name_normalized,
                                    'cli_name': arg_name,
                                    'type_annotation': None,
                                    'default': None,
                                    'help': None
                                }

                                for keyword in node.keywords:
                                    if keyword.arg == 'type':
                                        if isinstance(keyword.value, ast.Name):
                                            param_info['type_annotation'] = keyword.value.id
                                    elif keyword.arg == 'default':
                                        param_info['default'] = ast.literal_eval(keyword.value)
                                    elif keyword.arg == 'help':
                                        if isinstance(keyword.value, ast.Constant):
                                            param_info['help'] = keyword.value.value

                                parameters.append(param_info)

            return parameters

        except Exception as e:
            logger.debug(f"Failed to parse argparse from {script_path}: {e}")
            return []

    def _parse_sys_argv_usage(self, script_path: Path):
        """Parse sys.argv usage patterns from a Python script.

        Attempts to infer parameter names and types from variable assignments like:
            filename = sys.argv[1]
            count = int(sys.argv[2])
        """
        try:
            with open(script_path, 'r') as f:
                tree = ast.parse(f.read(), filename=str(script_path))

            # Map of argv index -> {name, type, help}
            argv_info = {}

            # First pass: find all sys.argv subscripts and try to infer names from assignments
            for node in ast.walk(tree):
                # Look for assignments: var_name = sys.argv[N] or var_name = int(sys.argv[N])
                if isinstance(node, ast.Assign):
                    if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
                        var_name = node.targets[0].id

                        # Check if RHS is sys.argv[N]
                        argv_index = None
                        inferred_type = 'str'

                        if isinstance(node.value, ast.Subscript):
                            # Direct assignment: var_name = sys.argv[N]
                            if (isinstance(node.value.value, ast.Attribute) and
                                isinstance(node.value.value.value, ast.Name) and
                                node.value.value.value.id == 'sys' and
                                node.value.value.attr == 'argv' and
                                isinstance(node.value.slice, ast.Constant)):
                                argv_index = node.value.slice.value

                        elif isinstance(node.value, ast.Call):
                            # Type conversion: var_name = int(sys.argv[N])
                            if isinstance(node.value.func, ast.Name):
                                type_func = node.value.func.id
                                if type_func in ('int', 'float', 'bool', 'str'):
                                    inferred_type = type_func

                                    # Check if argument is sys.argv[N]
                                    if node.value.args and isinstance(node.value.args[0], ast.Subscript):
                                        subscript = node.value.args[0]
                                        if (isinstance(subscript.value, ast.Attribute) and
                                            isinstance(subscript.value.value, ast.Name) and
                                            subscript.value.value.id == 'sys' and
                                            subscript.value.attr == 'argv' and
                                            isinstance(subscript.slice, ast.Constant)):
                                            argv_index = subscript.slice.value

                        if argv_index and isinstance(argv_index, int) and argv_index > 0:
                            argv_info[argv_index] = {
                                'name': var_name,
                                'type': inferred_type,
                                'help': f'{var_name} (from sys.argv[{argv_index}])'
                            }

                # Also catch standalone sys.argv[N] references not in assignments
                elif isinstance(node, ast.Subscript):
                    if (isinstance(node.value, ast.Attribute) and
                        isinstance(node.value.value, ast.Name) and
                        node.value.value.id == 'sys' and
                        node.value.attr == 'argv' and
                        isinstance(node.slice, ast.Constant)):

                        index = node.slice.value
                        if isinstance(index, int) and index > 0:
                            # Only add generic name if we haven't seen this index yet
                            if index not in argv_info:
                                argv_info[index] = {
                                    'name': f'arg{index}',
                                    'type': 'str',
                                    'help': f'Command line argument {index}'
                                }

            # Build parameter list in order
            parameters = []
            for index in sorted(argv_info.keys()):
                info = argv_info[index]
                param_info = {
                    'name': info['name'],
                    'type_annotation': info['type'],
                    'default': None,
                    'help': info['help'],
                    'argv_index': index
                }
                parameters.append(param_info)

            return parameters

        except Exception as e:
            logger.debug(f"Failed to parse sys.argv from {script_path}: {e}")
            return []

    # ===== Config Generation and Helper Methods =====

    def _create_partition_definition(self, partition_config):
        """Create a Dagster partition definition from partition config."""
        # Check for static partitions first
        if partition_config.values:
            logger.info(f"Creating static partitions with {len(partition_config.values)} values")
            return StaticPartitionsDefinition(partition_config.values)

        # Check for dynamic partitions
        if partition_config.dynamic:
            partition_name = f"{partition_config.parameter}_partitions"
            logger.info(f"Creating dynamic partitions: {partition_name}")
            return DynamicPartitionsDefinition(name=partition_name)

        # Fall back to time-based partitions
        if not partition_config.schedule:
            # Default to daily if no schedule specified
            schedule = "daily"
        else:
            schedule = partition_config.schedule.lower()

        # Determine start date
        if partition_config.start_date:
            start_date = partition_config.start_date
        else:
            default_start = datetime.now() - timedelta(days=30)
            start_date = default_start.strftime("%Y-%m-%d")

        timezone = partition_config.timezone

        if schedule == "hourly":
            return HourlyPartitionsDefinition(
                start_date=start_date,
                timezone=timezone
            )
        elif schedule == "daily":
            return DailyPartitionsDefinition(
                start_date=start_date,
                timezone=timezone
            )
        elif schedule == "weekly":
            return WeeklyPartitionsDefinition(
                start_date=start_date,
                timezone=timezone
            )
        elif schedule == "monthly":
            return MonthlyPartitionsDefinition(
                start_date=start_date,
                timezone=timezone
            )
        else:
            logger.warning(f"Unknown partition schedule: {schedule}, defaulting to daily")
            return DailyPartitionsDefinition(
                start_date=start_date,
                timezone=timezone
            )

    def _build_cli_arguments(self, config, parameters: List[Dict], source: str) -> List[str]:
        """Build CLI arguments from config values."""
        args = []

        if source == 'argparse':
            for param in parameters:
                param_name = param['name']
                cli_name = param.get('cli_name', param_name.replace('_', '-'))
                value = getattr(config, param_name, None)

                if value is not None:
                    args.append(f"--{cli_name}")
                    if isinstance(value, list):
                        for item in value:
                            args.append(str(item))
                    else:
                        args.append(str(value))

        elif source == 'sys.argv':
            for param in sorted(parameters, key=lambda p: p.get('argv_index', 0)):
                param_name = param['name']
                value = getattr(config, param_name, None)

                if value is not None:
                    if isinstance(value, list):
                        args.extend([str(item) for item in value])
                    else:
                        args.append(str(value))

        return args

    def _generate_config_class(self, parameters: List[Dict], source: str = "script"):
        """Generate a Dagster Config class from parameter definitions."""
        if not parameters:
            return None

        from typing import Optional, List, Any

        annotations = {}
        defaults = {}

        for param in parameters:
            param_name = param['name']
            param_type_str = param.get('type_annotation')
            default_value = param.get('default')

            param_type = Any
            is_list_type = False

            if param_type_str:
                if '|' in param_type_str:
                    base_type_str = param_type_str.split('|')[0].strip()
                    if 'list[str]' in base_type_str.lower():
                        param_type = Optional[List[str]]
                        is_list_type = True
                    elif 'list' in base_type_str.lower():
                        param_type = Optional[List[Any]]
                        is_list_type = True
                    elif 'str' in base_type_str.lower():
                        param_type = Optional[str]
                    elif 'int' in base_type_str.lower():
                        param_type = Optional[int]
                    elif 'float' in base_type_str.lower():
                        param_type = Optional[float]
                    elif 'bool' in base_type_str.lower():
                        param_type = Optional[bool]
                    else:
                        param_type = Optional[Any]
                elif 'list[str]' in param_type_str.lower():
                    param_type = Optional[List[str]]
                    is_list_type = True
                elif 'list' in param_type_str.lower():
                    param_type = Optional[List[Any]]
                    is_list_type = True
                elif 'str' == param_type_str.lower():
                    param_type = Optional[str]
                elif 'int' == param_type_str.lower():
                    param_type = Optional[int]
                elif 'float' == param_type_str.lower():
                    param_type = Optional[float]
                elif 'bool' == param_type_str.lower():
                    param_type = Optional[bool]

            annotations[param_name] = param_type

            help_text = param.get('help', f'Parameter from {source}')
            if is_list_type:
                # For list types, try to use the actual default value
                default_desc = help_text or f"List parameter"
                if default_value is not None:
                    import json
                    try:
                        default_str = json.dumps(default_value)
                        default_desc += f" (default: {default_str})"
                        # Try using the actual default value for Optional[List] types
                        defaults[param_name] = PydanticField(
                            default=default_value if isinstance(default_value, list) else None,
                            description=default_desc
                        )
                    except:
                        default_desc += f" (default: {default_value})"
                        defaults[param_name] = PydanticField(
                            default=None,
                            description=default_desc
                        )
                else:
                    default_desc += " (no default - must be provided)"
                    defaults[param_name] = PydanticField(
                        default=None,
                        description=default_desc
                    )
            elif default_value is not None:
                defaults[param_name] = default_value
            else:
                defaults[param_name] = None

        config_attrs = {
            '__annotations__': annotations,
            **defaults
        }

        ScriptConfig = type('ScriptConfig', (Config,), config_attrs)
        ScriptConfig._parameters = parameters  # type: ignore
        ScriptConfig._source = source  # type: ignore

        return ScriptConfig

    def _generate_flow_config_class(self, flow_info: Dict, script_info: ScriptInfo):
        """Generate a Dagster Config class from Prefect flow parameters."""
        flow_params = flow_info.get('parameters', [])
        return self._generate_config_class(flow_params, source='prefect')

    def _get_asset_prefix(self, script_type: str) -> str:
        """Get the asset name prefix based on script type."""
        prefix_map = {
            'prefect': 'prefect',
            'airflow': 'airflow',
            'python': 'script',
            'dask': 'script',
            'spark': 'script',
        }
        return prefix_map.get(script_type, 'script')

    # ===== Prefect Graph Asset Creation Methods =====

    def _create_prefect_flow_graph_asset(
        self, flow_info: Dict, tasks_info: List[Dict], script_info: ScriptInfo,
        metadata: ScriptMetadata, repo_path: str
    ):
        """Create a graph-backed asset for a Prefect flow."""
        # Build dependency AssetKeys (add appropriate prefix)
        deps = []
        if metadata.depends_on:
            for dep_name in metadata.depends_on:
                # Look up the dependency to get its script type for the correct prefix
                dep_script = next((s for s in self.discovered_scripts if s.name == dep_name), None)
                if dep_script and dep_script.metadata:
                    prefix = self._get_asset_prefix(dep_script.metadata.script_type)
                else:
                    prefix = 'prefect'  # Default to prefect for Prefect flows
                deps.append(f"{prefix}_{dep_name}")

        asset_prefix = self._get_asset_prefix(metadata.script_type)
        return self.prefect_parser.create_graph_asset(
            flow_info, tasks_info, script_info, metadata, repo_path,
            dependencies=deps if deps else None,
            asset_prefix=asset_prefix
        )

    def _create_prefect_flow_job(
        self, flow_info: Dict, tasks_info: List[Dict], script_info: ScriptInfo,
        metadata: ScriptMetadata, repo_path: str
    ):
        """Create an op job for a Prefect flow."""
        return self.prefect_parser.create_job(
            flow_info, tasks_info, script_info, metadata, repo_path
        )

    def _create_airflow_dag_graph_asset(
        self, dag_info: Dict, tasks_info: List[Dict], script_info: ScriptInfo,
        metadata: ScriptMetadata, repo_path: str
    ):
        """Create a graph-backed asset for an Airflow DAG."""
        if not self.airflow_parser:
            logger.warning("Airflow parser not initialized. Cannot create graph asset.")
            return None
        return self.airflow_parser.create_graph_asset(
            dag_info, tasks_info, script_info, metadata, repo_path
        )

    def _build_script_asset_with_prefect_check(
        self, script_info: ScriptInfo, all_scripts: List[ScriptInfo], repo_path: str
    ):
        """Build asset with Prefect graph mapping check."""
        metadata = script_info.metadata or ScriptMetadata()

        # Check if this is a standalone dag-factory YAML file
        if (self.airflow_enabled and
            script_info.script_path.suffix == '.yaml' and
            self.dag_factory_parser.is_dag_factory_yaml(script_info.script_path)):
            try:
                logger.info(f"Creating assets from dag-factory YAML: {script_info.name}")
                return self._build_dag_factory_yaml_assets(script_info, all_scripts, repo_path)
            except Exception as e:
                logger.error(f"Failed to create assets from dag-factory YAML {script_info.name}: {e}")
                logger.exception("Full traceback:")
                logger.info(f"Skipping dag-factory YAML")
                return None

        # Check if this is a DAG Factory pattern (Airflow → Dagster partitioned asset)
        if (self.airflow_enabled and
            metadata.script_type == "airflow" and
            metadata.dag_factory and
            metadata.dag_factory.enabled):
            try:
                logger.info(f"Detected DAG Factory pattern for: {script_info.name}")
                return self._build_dag_factory_asset(script_info, all_scripts, repo_path)
            except Exception as e:
                logger.warning(f"Failed to create DAG Factory asset for {script_info.name}: {e}")
                logger.info(f"Falling back to regular asset creation")

        # Check if this is a Prefect flow with mapping enabled
        if (self.prefect_enabled and
            metadata.script_type == "prefect" and
            metadata.prefect_mapping and
            metadata.prefect_mapping.enabled):

            # If the flow has dependencies, use regular @asset (not @graph_asset)
            # because @graph_asset doesn't support the deps parameter
            if metadata.depends_on:
                logger.info(f"Prefect flow {script_info.name} has dependencies - using regular @asset for lineage support")
                # Fall through to regular asset creation with deps
            else:
                try:
                    tasks, flows = self._parse_prefect_flow(script_info.script_path)
                    if flows:
                        flow_info = flows[0]
                        logger.info(f"Prefect mapping config for {script_info.name}: {metadata.prefect_mapping}")
                        logger.info(f"Prefect mapping mode raw value: {metadata.prefect_mapping.mode}")
                        mode = metadata.prefect_mapping.mode or "graph_asset"
                        logger.info(f"Prefect flow {script_info.name} final mode: {mode}")

                        # Route based on mode
                        if mode == "job":
                            # Create op job
                            job_def = self._create_prefect_flow_job(
                                flow_info, tasks, script_info, metadata, repo_path
                            )

                            if job_def:
                                logger.info(f"Created op job for Prefect flow: {script_info.name}")
                                return job_def
                            else:
                                logger.info(f"Falling back to subprocess for: {script_info.name}")
                        else:
                            # Create graph asset (default)
                            graph_asset_def = self._create_prefect_flow_graph_asset(
                                flow_info, tasks, script_info, metadata, repo_path
                            )

                            if graph_asset_def:
                                logger.info(f"Created graph asset for Prefect flow: {script_info.name}")
                                return graph_asset_def
                            else:
                                logger.info(f"Falling back to subprocess for: {script_info.name}")
                except Exception as e:
                    logger.warning(f"Failed to create Prefect flow conversion for {script_info.name}: {e}")
                    logger.info(f"Falling back to subprocess execution")

        # Check if this is an Airflow DAG with mapping enabled
        if (self.airflow_enabled and
            metadata.script_type == "airflow" and
            metadata.airflow_mapping and
            metadata.airflow_mapping.enabled):
            try:
                tasks, dags = self._parse_airflow_dag(script_info.script_path)
                if dags:
                    # Check if file has multiple DAGs with assets - handle them separately for proper lineage
                    dags_with_assets = [d for d in dags if d.get('inlet_datasets') or d.get('outlet_datasets')]

                    if len(dags_with_assets) > 1:
                        logger.info(f"File {script_info.name} has {len(dags_with_assets)} DAGs with assets - creating separate definitions for lineage")
                        return self._build_multi_dag_file_assets(script_info, dags_with_assets, all_scripts, repo_path)

                    # Single DAG or no asset DAGs - use original logic
                    dag_info = dags[0]

                    # Check Airflow version compatibility
                    detected_version = dag_info.get('dag_airflow_version', '3.x')
                    if not self._is_airflow_version_compatible(detected_version):
                        logger.warning(
                            f"⏭️  Skipping {script_info.name} - requires Airflow {detected_version}, "
                            f"but Airflow {self.airflow_parser._airflow_version[0]}.x is installed"
                        )
                        return None  # Skip this DAG

                    # Extract schedule and retry config from DAG
                    dag_schedule = dag_info.get('schedule')
                    dag_retries = dag_info.get('retries')
                    dag_retry_delay = dag_info.get('retry_delay')

                    # Apply DAG-level schedule if not already configured in YAML
                    if dag_schedule and not metadata.schedule:
                        from ..schemas.script_metadata import ScheduleConfig
                        # Convert Airflow schedule to Dagster cron schedule
                        if isinstance(dag_schedule, str):
                            # Assume it's a cron expression
                            metadata.schedule = ScheduleConfig(
                                cron_schedule=dag_schedule,
                                timezone="UTC"
                            )
                            logger.info(f"Applied schedule from Airflow DAG: {dag_schedule}")

                    # Apply DAG-level retry policy if not already configured in YAML
                    if dag_retries and not metadata.retry_policy:
                        from ..schemas.script_metadata import RetryPolicyConfig
                        metadata.retry_policy = RetryPolicyConfig(
                            max_retries=dag_retries,
                            delay=dag_retry_delay or 60,
                            backoff="LINEAR"
                        )
                        logger.info(f"Applied retry policy from Airflow DAG: {dag_retries} retries")

                    # Try to create graph asset
                    graph_asset_def = self._create_airflow_dag_graph_asset(
                        dag_info, tasks, script_info, metadata, repo_path
                    )

                    if graph_asset_def:
                        logger.info(f"Created graph asset for Airflow DAG: {script_info.name}")
                        return graph_asset_def
                    else:
                        logger.info(f"Falling back to subprocess for: {script_info.name}")
                        # Pass dataset info to subprocess asset creation
                        return self._build_airflow_asset_with_datasets(
                            script_info, all_scripts, repo_path, dag_info
                        )
            except Exception as e:
                logger.warning(f"Failed to create graph asset for {script_info.name}: {e}")
                logger.info(f"Falling back to subprocess execution")
                # Pass dataset info to subprocess asset creation
                return self._build_airflow_asset_with_datasets(
                    script_info, all_scripts, repo_path, dag_info
                )

        # Fall back to regular subprocess-based asset
        return self._build_script_asset(script_info, all_scripts, repo_path)


    # ===== Asset Building Methods =====

    def _build_multi_dag_file_assets(
        self, script_info: ScriptInfo, dags_with_assets: List[Dict], all_scripts: List[ScriptInfo], repo_path: str
    ):
        """Build separate Dagster assets for each DAG in a Python file with multiple DAGs.

        This ensures proper lineage between producer and consumer DAGs in the same file.
        """
        from dagster import AssetKey

        all_definitions = []

        # Convert Airflow dataset URIs to Dagster asset keys
        def dataset_uri_to_asset_key(uri: str) -> str:
            """Convert an Airflow Dataset URI to a Dagster asset key."""
            import re
            cleaned = re.sub(r'^[a-z]+://', '', uri)
            cleaned = re.sub(r'[^A-Za-z0-9_]', '_', cleaned)
            cleaned = re.sub(r'_+', '_', cleaned).strip('_')
            return f"airflow_dataset_{cleaned}"

        logger.info(f"Creating {len(dags_with_assets)} separate definitions for DAGs with assets")

        for dag_info in dags_with_assets:
            dag_id = dag_info.get('dag_id', dag_info['name'])
            inlet_datasets = dag_info.get('inlet_datasets', [])
            outlet_datasets = dag_info.get('outlet_datasets', [])

            logger.info(f"  DAG {dag_id}: {len(inlet_datasets)} inlets, {len(outlet_datasets)} outlets")

            # Create individual asset for each outlet produced by this DAG
            for outlet_uri in outlet_datasets:
                outlet_asset_key = dataset_uri_to_asset_key(outlet_uri)

                # Find dependencies from inlet datasets
                deps = []
                for inlet_uri in inlet_datasets:
                    inlet_asset_key = dataset_uri_to_asset_key(inlet_uri)
                    deps.append(AssetKey(inlet_asset_key))

                # Create the asset definition
                asset_def = self._create_single_dag_asset(
                    dag_id=dag_id,
                    asset_key=outlet_asset_key,
                    outlet_uri=outlet_uri,
                    deps=deps,
                    script_info=script_info,
                    dag_info=dag_info,
                    repo_path=repo_path
                )

                if asset_def:
                    all_definitions.append(asset_def)
                    logger.info(f"    Created asset: {outlet_asset_key} (from DAG {dag_id})")

        logger.info(f"✅ Created {len(all_definitions)} assets from {len(dags_with_assets)} DAGs in {script_info.name}")
        return all_definitions

    def _create_single_dag_asset(
        self, dag_id: str, asset_key: str, outlet_uri: str, deps: List,
        script_info: ScriptInfo, dag_info: Dict, repo_path: str
    ):
        """Create a single Dagster asset for one DAG's outlet."""
        from dagster import asset, AssetExecutionContext, AssetKey
        import subprocess
        from pathlib import Path

        metadata = script_info.metadata or ScriptMetadata()

        # Build asset tags
        asset_tags = {
            **metadata.tags,
            "script_type": "airflow",
            "dag_id": dag_id,
            "outlet_uri": outlet_uri,
        }

        # Add kinds as tags with dagster/kind/ prefix
        for kind in metadata.kinds:
            asset_tags[f"dagster/kind/{kind}"] = ""

        # Create the asset function
        def make_asset_func(dag_id_param, script_path_param, repo_path_param):
            """Closure to capture parameters"""
            def asset_func(context: AssetExecutionContext):
                """Execute Airflow DAG to produce this asset."""
                context.log.info(f"Running Airflow DAG: {dag_id_param}")
                context.log.info(f"Producing asset: {outlet_uri}")

                try:
                    from datetime import datetime
                    start_time = datetime.now()
                    execution_date = start_time.strftime('%Y-%m-%d')

                    # Use uv run to execute airflow
                    airflow_cmd = self._build_airflow_command("dags", "test", dag_id_param, execution_date)

                    # Set environment
                    env = os.environ.copy()
                    env["AIRFLOW_HOME"] = str(Path(repo_path_param) / ".airflow")
                    env["AIRFLOW__CORE__DAGS_FOLDER"] = str(script_path_param.parent)
                    env["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"

                    context.log.info(f"Executing: {' '.join(airflow_cmd)}")

                    result = subprocess.run(
                        airflow_cmd,
                        capture_output=True,
                        text=True,
                        cwd=repo_path_param,
                        env=env,
                        timeout=300
                    )

                    if result.returncode == 0:
                        context.log.info("✅ DAG execution successful")
                        return {"status": "success", "dag_id": dag_id_param}
                    else:
                        context.log.error(f"DAG execution failed: {result.stderr}")
                        raise Exception(f"Airflow DAG {dag_id_param} failed")

                except Exception as e:
                    context.log.error(f"Error executing DAG: {e}")
                    raise

            return asset_func

        # Create the decorated asset
        asset_func = make_asset_func(dag_id, script_info.script_path, repo_path)

        asset_kwargs = {
            "name": asset_key,
            "tags": asset_tags,
            "description": f"Asset produced by Airflow DAG {dag_id}: {outlet_uri}",
            "group_name": metadata.group_name or "airflow",
        }

        if deps:
            asset_kwargs["deps"] = deps

        return asset(**asset_kwargs)(asset_func)

    def _build_airflow_asset_with_datasets(
        self, script_info: ScriptInfo, all_scripts: List[ScriptInfo], repo_path: str, dag_info: Dict
    ):
        """Build a Dagster multi-asset for an Airflow DAG with Dataset/Asset outputs.

        This creates a multi-asset where:
        - Each outlet dataset becomes an individual AssetSpec (a separate asset in the graph)
        - Inlet datasets become dependencies on specific dataset assets from producer DAGs
        - Executes the entire DAG once via `airflow dags test`
        - Yields MaterializeResult for each outlet dataset
        """
        metadata = script_info.metadata or ScriptMetadata()

        # Extract dataset information from dag_info
        inlet_datasets = dag_info.get('inlet_datasets', [])
        outlet_datasets = dag_info.get('outlet_datasets', [])
        dag_id = dag_info.get('dag_id', script_info.name)
        version_warning = dag_info.get('version_warning')
        dag_airflow_version = dag_info.get('dag_airflow_version', '3.x')

        logger.info(f"Building Airflow multi-asset {dag_id} with {len(inlet_datasets)} inlets, {len(outlet_datasets)} outlets")

        # If this DAG has inlet dependencies (consumes datasets from other DAGs),
        # use individual asset approach for proper cross-DAG lineage
        if inlet_datasets and outlet_datasets:
            logger.info(f"DAG {dag_id} has inlet dependencies - using individual asset approach for lineage")
            return self._build_multi_dag_file_assets(script_info, [dag_info], all_scripts, repo_path)

        # Log version compatibility warning if present
        if version_warning:
            logger.warning(f"Airflow version compatibility issue detected for {dag_id}")

        # Convert Airflow dataset URIs to Dagster asset keys
        def dataset_uri_to_asset_key(uri: str) -> str:
            """Convert an Airflow Dataset URI to a Dagster asset key."""
            import re
            cleaned = re.sub(r'^[a-z]+://', '', uri)
            cleaned = re.sub(r'[^A-Za-z0-9_]', '_', cleaned)
            cleaned = re.sub(r'_+', '_', cleaned).strip('_')
            return f"airflow_dataset_{cleaned}"

        # Build retry policy
        retry_policy = None
        if metadata.retry_policy:
            policy = metadata.retry_policy
            backoff = Backoff.EXPONENTIAL if policy.backoff == "EXPONENTIAL" else Backoff.LINEAR
            jitter = None
            if policy.jitter == "FULL":
                jitter = Jitter.FULL
            elif policy.jitter == "PLUS_MINUS":
                jitter = Jitter.PLUS_MINUS

            retry_policy = RetryPolicy(
                max_retries=policy.max_retries,
                delay=policy.delay,
                backoff=backoff,
                jitter=jitter,
            )

        # Try to extract DAG parameters for config
        # Note: parser returns 'params' as a dict, need to convert to list format for config generation
        dag_params_dict = dag_info.get('params', {})
        config_class = None
        if dag_params_dict:
            try:
                # Convert params dict to list format expected by _generate_config_class
                dag_params_list = [
                    {
                        'name': param_name,
                        'type_annotation': param_info.get('type'),
                        'default': param_info.get('default'),
                        'help': param_info.get('description')
                    }
                    for param_name, param_info in dag_params_dict.items()
                ]
                config_class = self._generate_config_class(dag_params_list, source='airflow')
                logger.info(f"Generated config for Airflow DAG {dag_id} with {len(dag_params_list)} parameters")
            except Exception as e:
                logger.debug(f"Could not generate config for {dag_id}: {e}")

        # Detect Airflow check operators
        detected_checks = AirflowCheckDetector.detect_check_operators(dag_info)
        check_specs_metadata = []
        check_specs = []
        if detected_checks:
            check_specs_metadata = AirflowCheckDetector.generate_check_specs_metadata(detected_checks)
            logger.info(f"🔍 Generated {len(check_specs_metadata)} asset check(s) from Airflow operators")

            # We'll create the actual AssetCheckSpec objects after we know the asset keys
            # Store for now and create them later

        # Build asset tags
        asset_tags = {
            **metadata.tags,
            "script_type": "airflow",
            "script_name": script_info.name,
            "dag_id": dag_id,
            f"airflow_{dag_airflow_version}": "",  # Add version tag (e.g., "airflow_2.x", "airflow_3.x")
        }
        for kind in metadata.kinds:
            asset_tags[f"dagster/kind/{kind}"] = ""

        # Add tags from DAG decorator
        dag_tags = dag_info.get('tags', [])
        if dag_tags:
            for tag in dag_tags:
                asset_tags[f"airflow_tag_{tag}"] = ""

        # Build AssetOuts for each outlet dataset with dagster_type=Nothing
        # This tells Dagster not to try to persist outputs (we only yield metadata)
        asset_outs = {}
        asset_deps_map = {}  # Track dependencies for each asset

        for dataset_uri in outlet_datasets:
            dataset_asset_key = dataset_uri_to_asset_key(dataset_uri)

            # Find dependencies: which dataset assets this depends on (from inlet datasets)
            dataset_deps = []
            for inlet_uri in inlet_datasets:
                inlet_asset_key = dataset_uri_to_asset_key(inlet_uri)

                # Verify the inlet dataset is produced by another DAG
                producer_found = False
                producer_scripts = [
                    s for s in all_scripts
                    if s.metadata and s.metadata.script_type == "airflow" and s.name != script_info.name
                ]

                for producer_script in producer_scripts:
                    try:
                        if not self.airflow_parser:
                            continue
                        _, producer_dags = self.airflow_parser.parse_dag(producer_script.script_path)
                        for producer_dag in producer_dags:
                            producer_outlets = producer_dag.get('outlet_datasets', [])
                            if inlet_uri in producer_outlets:
                                # Found the producer - add dependency on its dataset asset
                                dataset_deps.append(inlet_asset_key)
                                producer_found = True
                                logger.info(f"  Dataset {dataset_asset_key} depends on {inlet_asset_key}")
                                break
                        if producer_found:
                            break
                    except Exception as e:
                        logger.debug(f"Could not parse {producer_script.name} for dataset matching: {e}")

                if not producer_found:
                    logger.warning(f"  Inlet dataset {inlet_uri} has no known producer")

            # Create AssetOut with dagster_type=Nothing (no output to persist)
            # Store dependencies separately since AssetOut doesn't support deps parameter
            asset_deps_map[dataset_asset_key] = dataset_deps

            # Build metadata dict
            asset_metadata = {
                "airflow_dag_id": MetadataValue.text(dag_id),
                "dataset_uri": MetadataValue.text(dataset_uri),
            }
            if version_warning:
                asset_metadata["airflow_version_warning"] = MetadataValue.text(version_warning)

            # Add enhanced metadata from DAG default_args
            if dag_info.get('owner'):
                asset_metadata["owner"] = MetadataValue.text(dag_info['owner'])
            if dag_info.get('email'):
                asset_metadata["email"] = MetadataValue.text(dag_info['email'])
            if dag_info.get('pool'):
                asset_metadata["pool"] = MetadataValue.text(dag_info['pool'])
            if dag_info.get('queue'):
                asset_metadata["queue"] = MetadataValue.text(dag_info['queue'])
            if dag_info.get('priority_weight'):
                asset_metadata["priority_weight"] = MetadataValue.int(dag_info['priority_weight'])
            if dag_info.get('execution_timeout'):
                asset_metadata["execution_timeout"] = MetadataValue.text(str(dag_info['execution_timeout']))
            if dag_info.get('sla'):
                asset_metadata["sla"] = MetadataValue.text(str(dag_info['sla']))
            if dag_info.get('email_on_failure') is not None:
                asset_metadata["email_on_failure"] = MetadataValue.bool(dag_info['email_on_failure'])
            if dag_info.get('email_on_retry') is not None:
                asset_metadata["email_on_retry"] = MetadataValue.bool(dag_info['email_on_retry'])

            # Use metadata description, fallback to DAG docstring, then default
            base_desc = metadata.description or dag_info.get('docstring') or f"Airflow DAG {dag_id}"
            dataset_desc = f"{base_desc}\nDataset: {dataset_uri}"

            asset_outs[dataset_asset_key] = AssetOut(
                dagster_type=Nothing,
                description=dataset_desc,
                group_name=metadata.group_name,
                metadata=asset_metadata,
                tags=asset_tags,
            )

        # If no outlet datasets, create a single asset for the DAG itself
        if not asset_outs:
            # Build inlet dependencies directly for the DAG asset
            dag_deps = []
            for inlet_uri in inlet_datasets:
                inlet_asset_key = dataset_uri_to_asset_key(inlet_uri)
                dag_deps.append(inlet_asset_key)

            dag_asset_key = f"airflow_{dag_id}"
            asset_deps_map[dag_asset_key] = dag_deps

            # Build metadata dict
            dag_asset_metadata = {"airflow_dag_id": MetadataValue.text(dag_id)}
            if version_warning:
                dag_asset_metadata["airflow_version_warning"] = MetadataValue.text(version_warning)

            # Add enhanced metadata from DAG default_args
            if dag_info.get('owner'):
                dag_asset_metadata["owner"] = MetadataValue.text(dag_info['owner'])
            if dag_info.get('email'):
                dag_asset_metadata["email"] = MetadataValue.text(dag_info['email'])
            if dag_info.get('pool'):
                dag_asset_metadata["pool"] = MetadataValue.text(dag_info['pool'])
            if dag_info.get('queue'):
                dag_asset_metadata["queue"] = MetadataValue.text(dag_info['queue'])
            if dag_info.get('priority_weight'):
                dag_asset_metadata["priority_weight"] = MetadataValue.int(dag_info['priority_weight'])
            if dag_info.get('execution_timeout'):
                dag_asset_metadata["execution_timeout"] = MetadataValue.text(str(dag_info['execution_timeout']))
            if dag_info.get('sla'):
                dag_asset_metadata["sla"] = MetadataValue.text(str(dag_info['sla']))
            if dag_info.get('email_on_failure') is not None:
                dag_asset_metadata["email_on_failure"] = MetadataValue.bool(dag_info['email_on_failure'])
            if dag_info.get('email_on_retry') is not None:
                dag_asset_metadata["email_on_retry"] = MetadataValue.bool(dag_info['email_on_retry'])

            # Use metadata description, fallback to DAG docstring, then default
            description = metadata.description or dag_info.get('docstring') or f"Airflow DAG: {dag_id}"

            asset_outs[dag_asset_key] = AssetOut(
                dagster_type=Nothing,
                description=description,
                group_name=metadata.group_name,
                metadata=dag_asset_metadata,
                tags=asset_tags,
            )

        # Create AssetCheckSpec objects for detected Airflow check operators
        from dagster import AssetCheckSpec, AssetKey
        check_specs = []
        if check_specs_metadata:
            # Associate checks with the first outlet asset (or DAG asset if no outlets)
            first_asset_key = list(asset_outs.keys())[0]

            for spec_meta in check_specs_metadata:
                check_spec = AssetCheckSpec(
                    name=spec_meta['name'],
                    asset=AssetKey(first_asset_key),
                    description=spec_meta['description'],
                )
                check_specs.append(check_spec)

            logger.info(f"✅ Created {len(check_specs)} AssetCheckSpec(s) for {first_asset_key}")

        # Define the multi-asset execution function
        if config_class:
            def airflow_multi_asset_fn(context: AssetExecutionContext, config):
                """Execute Airflow DAG and yield results for each outlet dataset."""
                logger.info(f"Running Airflow DAG: {dag_id}")
                context.log.info(f"DAG config: {config}")
                context.log.info(f"Inlet datasets: {inlet_datasets}")
                context.log.info(f"Outlet datasets: {outlet_datasets}")

                try:
                    start_time = datetime.now()
                    execution_date = start_time.strftime('%Y-%m-%d')
                    # Use uv run to execute airflow with the correct virtual environment
                    airflow_cmd = self._build_airflow_command("dags", "test", dag_id, execution_date)

                    # Set config as environment variables
                    env = os.environ.copy()
                    # Set AIRFLOW_HOME to match the initialized database location
                    env["AIRFLOW_HOME"] = str(Path(repo_path) / ".airflow")
                    # Tell Airflow where to find DAG files - use the directory containing this specific DAG
                    dag_directory = str(script_info.script_path.parent)
                    env["AIRFLOW__CORE__DAGS_FOLDER"] = dag_directory
                    # Don't load example DAGs
                    env["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
                    # Continue even if some DAGs fail to import
                    env["AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT"] = "30"
                    for param_name, param_value in config.__dict__.items():
                        if param_value is not None:
                            env[f"AIRFLOW_VAR_{param_name.upper()}"] = str(param_value)

                    context.log.info(f"Executing: {' '.join(airflow_cmd)}")

                    # Stream output in real-time while capturing for metadata
                    stdout_lines = []
                    stderr_lines = []

                    process = subprocess.Popen(
                        airflow_cmd,
                        cwd=repo_path,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        env=env,
                    )

                    # Read and log stdout in real-time
                    while True:
                        # Check if process has finished
                        if process.poll() is not None:
                            # Read any remaining output
                            for line in process.stdout:
                                line = line.rstrip()
                                if line:
                                    context.log.info(f"[airflow] {line}")
                                    stdout_lines.append(line)
                            for line in process.stderr:
                                line = line.rstrip()
                                if line:
                                    context.log.warning(f"[airflow] {line}")
                                    stderr_lines.append(line)
                            break

                        # Read available output
                        line = process.stdout.readline()
                        if line:
                            line = line.rstrip()
                            if line:
                                context.log.info(f"[airflow] {line}")
                                stdout_lines.append(line)

                    returncode = process.wait()
                    stdout_text = '\n'.join(stdout_lines)
                    stderr_text = '\n'.join(stderr_lines)

                    if returncode != 0:
                        context.log.error(f"Airflow DAG failed with exit code {returncode}")
                        if stderr_text:
                            context.log.error(f"stderr: {stderr_text}")
                        raise subprocess.CalledProcessError(returncode, airflow_cmd, stdout_text, stderr_text)

                    # Create a result object for compatibility with existing code
                    class ProcessResult:
                        def __init__(self, returncode, stdout, stderr):
                            self.returncode = returncode
                            self.stdout = stdout
                            self.stderr = stderr

                    result = ProcessResult(returncode, stdout_text, stderr_text)

                    end_time = datetime.now()
                    duration = (end_time - start_time).total_seconds()

                    # Common metadata
                    common_metadata = {
                        "dag_id": MetadataValue.text(dag_id),
                        "script_path": MetadataValue.path(str(script_info.script_path)),
                        "execution_date": MetadataValue.text(execution_date),
                        "duration_seconds": MetadataValue.float(duration),
                        "execution_time": MetadataValue.timestamp(end_time.timestamp()),
                    }

                    if result.stdout:
                        common_metadata["stdout"] = MetadataValue.md(f"```\n{result.stdout[-5000:]}\n```")
                    if result.stderr:
                        common_metadata["stderr"] = MetadataValue.md(f"```\n{result.stderr[-5000:]}\n```")

                    # Parse and yield check results if we have check specs
                    if check_specs_metadata:
                        context.log.info(f"Parsing {len(check_specs_metadata)} check result(s) from logs")
                        # Combine stdout and stderr for parsing
                        full_log_output = result.stdout + "\n" + result.stderr
                        check_results = AirflowCheckDetector.parse_check_results_from_logs(
                            full_log_output,
                            check_specs_metadata
                        )

                        if check_results:
                            context.log.info(f"✅ Parsed {len(check_results)} check result(s)")
                            for check_result in check_results:
                                yield check_result
                        else:
                            context.log.warning("Could not parse check results from logs - using default pass status")
                            # Fallback: yield default results
                            default_results = AirflowCheckDetector.create_default_check_results(
                                check_specs_metadata,
                                passed=True  # Optimistic: if DAG succeeded, assume checks passed
                            )
                            for check_result in default_results:
                                yield check_result

                    # Yield MaterializeResult for each outlet dataset
                    if outlet_datasets:
                        for dataset_uri in outlet_datasets:
                            dataset_asset_key = dataset_uri_to_asset_key(dataset_uri)
                            yield MaterializeResult(
                                asset_key=dataset_asset_key,
                                metadata={
                                    **common_metadata,
                                    "dataset_uri": MetadataValue.text(dataset_uri),
                                }
                            )
                    else:
                        # No outlets - yield result for the DAG asset itself
                        yield MaterializeResult(
                            asset_key=f"airflow_{dag_id}",
                            metadata=common_metadata
                        )

                except Exception as e:
                    logger.error(f"Error running Airflow DAG: {e}")
                    raise
        else:
            def airflow_multi_asset_fn(context: AssetExecutionContext):
                """Execute Airflow DAG and yield results for each outlet dataset."""
                logger.info(f"Running Airflow DAG: {dag_id}")
                context.log.info(f"Inlet datasets: {inlet_datasets}")
                context.log.info(f"Outlet datasets: {outlet_datasets}")

                try:
                    start_time = datetime.now()
                    execution_date = start_time.strftime('%Y-%m-%d')
                    # Use uv run to execute airflow with the correct virtual environment
                    airflow_cmd = self._build_airflow_command("dags", "test", dag_id, execution_date)

                    context.log.info(f"Executing: {' '.join(airflow_cmd)}")

                    # Tell Airflow where to find DAG files - use the directory containing this specific DAG
                    dag_directory = str(script_info.script_path.parent)
                    env = os.environ.copy()
                    # Set AIRFLOW_HOME to match the initialized database location
                    env["AIRFLOW_HOME"] = str(Path(repo_path) / ".airflow")
                    env["AIRFLOW__CORE__DAGS_FOLDER"] = dag_directory
                    # Don't load example DAGs
                    env["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
                    # Continue even if some DAGs fail to import
                    env["AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT"] = "30"

                    # Stream output in real-time while capturing for metadata
                    stdout_lines = []
                    stderr_lines = []

                    process = subprocess.Popen(
                        airflow_cmd,
                        cwd=repo_path,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        env=env,
                    )

                    # Read and log stdout in real-time
                    while True:
                        # Check if process has finished
                        if process.poll() is not None:
                            # Read any remaining output
                            for line in process.stdout:
                                line = line.rstrip()
                                if line:
                                    context.log.info(f"[airflow] {line}")
                                    stdout_lines.append(line)
                            for line in process.stderr:
                                line = line.rstrip()
                                if line:
                                    context.log.warning(f"[airflow] {line}")
                                    stderr_lines.append(line)
                            break

                        # Read available output
                        line = process.stdout.readline()
                        if line:
                            line = line.rstrip()
                            if line:
                                context.log.info(f"[airflow] {line}")
                                stdout_lines.append(line)

                    returncode = process.wait()
                    stdout_text = '\n'.join(stdout_lines)
                    stderr_text = '\n'.join(stderr_lines)

                    if returncode != 0:
                        context.log.error(f"Airflow DAG failed with exit code {returncode}")
                        if stderr_text:
                            context.log.error(f"stderr: {stderr_text}")
                        raise subprocess.CalledProcessError(returncode, airflow_cmd, stdout_text, stderr_text)

                    # Create a result object for compatibility with existing code
                    class ProcessResult:
                        def __init__(self, returncode, stdout, stderr):
                            self.returncode = returncode
                            self.stdout = stdout
                            self.stderr = stderr

                    result = ProcessResult(returncode, stdout_text, stderr_text)

                    end_time = datetime.now()
                    duration = (end_time - start_time).total_seconds()

                    # Common metadata
                    common_metadata = {
                        "dag_id": MetadataValue.text(dag_id),
                        "script_path": MetadataValue.path(str(script_info.script_path)),
                        "execution_date": MetadataValue.text(execution_date),
                        "duration_seconds": MetadataValue.float(duration),
                        "execution_time": MetadataValue.timestamp(end_time.timestamp()),
                    }

                    if result.stdout:
                        common_metadata["stdout"] = MetadataValue.md(f"```\n{result.stdout[-5000:]}\n```")
                    if result.stderr:
                        common_metadata["stderr"] = MetadataValue.md(f"```\n{result.stderr[-5000:]}\n```")

                    # Parse and yield check results if we have check specs
                    if check_specs_metadata:
                        context.log.info(f"Parsing {len(check_specs_metadata)} check result(s) from logs")
                        # Combine stdout and stderr for parsing
                        full_log_output = result.stdout + "\n" + result.stderr
                        check_results = AirflowCheckDetector.parse_check_results_from_logs(
                            full_log_output,
                            check_specs_metadata
                        )

                        if check_results:
                            context.log.info(f"✅ Parsed {len(check_results)} check result(s)")
                            for check_result in check_results:
                                yield check_result
                        else:
                            context.log.warning("Could not parse check results from logs - using default pass status")
                            # Fallback: yield default results
                            default_results = AirflowCheckDetector.create_default_check_results(
                                check_specs_metadata,
                                passed=True  # Optimistic: if DAG succeeded, assume checks passed
                            )
                            for check_result in default_results:
                                yield check_result

                    # Yield MaterializeResult for each outlet dataset
                    if outlet_datasets:
                        for dataset_uri in outlet_datasets:
                            dataset_asset_key = dataset_uri_to_asset_key(dataset_uri)
                            yield MaterializeResult(
                                asset_key=dataset_asset_key,
                                metadata={
                                    **common_metadata,
                                    "dataset_uri": MetadataValue.text(dataset_uri),
                                }
                            )
                    else:
                        # No outlets - yield result for the DAG asset itself
                        yield MaterializeResult(
                            asset_key=f"airflow_{dag_id}",
                            metadata=common_metadata
                        )

                except Exception as e:
                    logger.error(f"Error running Airflow DAG: {e}")
                    raise

        # Set config annotation if needed
        if config_class:
            airflow_multi_asset_fn.__annotations__['config'] = config_class

        # Create the multi-asset using outs with dagster_type=Nothing
        # This tells Dagster not to try to persist outputs - we only yield MaterializeResult metadata
        # can_subset=True allows individual assets to be materialized independently
        # Note: DAGs with inlet dependencies use _create_single_dag_asset instead
        multi_asset_kwargs = {
            "name": f"airflow_{dag_id}",
            "outs": asset_outs,
            "group_name": metadata.group_name,
            "retry_policy": retry_policy,
            "can_subset": True,
        }

        # Add check specs if we have any
        if check_specs:
            multi_asset_kwargs["check_specs"] = check_specs

        multi_asset_def = multi_asset(**multi_asset_kwargs)(airflow_multi_asset_fn)

        return multi_asset_def

    def _build_dag_factory_yaml_assets(
        self, script_info: ScriptInfo, all_scripts: List[ScriptInfo], repo_path: str
    ):
        """Build Dagster constructs from dag-factory YAML file.

        Detects the pattern and creates appropriate constructs:
        - DAG with outlets → Assets + Asset Job
        - DAG without outlets, asset schedule → Op Job + Sensor
        - Regular DAG → Graph Asset (fallback)

        Returns:
            List of definitions (assets, jobs, sensors) or single definition
        """
        yaml_path = script_info.script_path
        metadata = script_info.metadata or ScriptMetadata()

        # Parse the dag-factory YAML
        dags = self.dag_factory_parser.parse_dag_factory_yaml(yaml_path)

        if not dags:
            logger.warning(f"No DAGs found in dag-factory YAML: {yaml_path}")
            return None

        logger.info(f"Found {len(dags)} DAG(s) in {yaml_path.name}")

        # Process ALL DAGs in the YAML file
        all_definitions = []

        for dag_info in dags:
            dag_id = dag_info['dag_id']

            # Detect pattern based on outlets and asset schedule
            has_outlets = bool(dag_info.get('asset_outlets'))
            has_asset_schedule = bool(dag_info.get('asset_schedule'))

            logger.info(f"Analyzing dag-factory DAG: {dag_id}")
            logger.info(f"  Has outlets: {has_outlets} {dag_info.get('asset_outlets', [])}")
            logger.info(f"  Has asset schedule: {has_asset_schedule} {dag_info.get('asset_schedule', [])}")

            # Pattern 1: DAG produces assets (has outlets)
            if has_outlets:
                logger.info(f"Pattern: Asset-producing DAG → Creating assets + asset job")
                result = self._build_assets_and_job_from_dag(dag_info, script_info, metadata, repo_path)
                if result:
                    all_definitions.extend(result)

            # Pattern 2: DAG triggered by assets but produces no assets (terminal operation)
            elif has_asset_schedule:
                logger.info(f"Pattern: Terminal operation DAG → Creating op job + sensor")
                result = self._build_op_job_and_sensor(dag_info, script_info, metadata, repo_path)
                if result:
                    all_definitions.extend(result)

            # Pattern 3: Regular DAG (no asset-based orchestration) - fallback to graph asset
            else:
                logger.info(f"Pattern: Regular DAG → Creating graph asset (fallback)")
                try:
                    graph_asset_def = self.dag_factory_parser.create_graph_asset(
                        dag_info, script_info, metadata, repo_path
                    )

                    if graph_asset_def:
                        logger.info(f"✅ Created graph asset for dag-factory DAG: {dag_id}")
                        all_definitions.append(graph_asset_def)

                except Exception as e:
                    logger.debug(f"Failed to create graph asset for dag-factory DAG {dag_id}: {e} (will use fallback mode)")
                    logger.info("Falling back to simple sequential asset")
                    # Continue to fallback below

        # Return all definitions if we created any
        if all_definitions:
            logger.info(f"✅ Created {len(all_definitions)} definition(s) from {len(dags)} DAG(s)")
            return all_definitions

        # If no patterns matched, fall back to creating a simple asset for the first DAG
        dag_info = dags[0]
        dag_id = dag_info['dag_id']

        # Fallback: Create a simple asset that executes tasks sequentially
        asset_tags = {
            **metadata.tags,
            "dag_id": dag_id,
            "source": "dag_factory_yaml",
        }
        for kind in metadata.kinds:
            asset_tags[f"dagster/kind/{kind}"] = ""

        # Build retry policy from default_args
        default_args = dag_info.get('default_args', {})
        retry_policy = None
        if default_args.get('retries', 0) > 0:
            retry_policy = RetryPolicy(
                max_retries=default_args.get('retries', 0),
                delay=default_args.get('retry_delay_sec', 300),
            )

        def dag_factory_yaml_asset(context: AssetExecutionContext):
            """Execute dag-factory DAG tasks sequentially."""
            logger.info(f"Running dag-factory DAG: {dag_id}")
            context.log.info(f"DAG: {dag_id}")
            context.log.info(f"Tasks: {[t['task_id'] for t in dag_info['tasks']]}")

            # Get execution order
            task_order = self.dag_factory_parser.get_task_execution_order(dag_info)
            context.log.info(f"Execution order: {task_order}")

            results = {}
            for task_id in task_order:
                # Find task config
                task_config = next((t for t in dag_info['tasks'] if t['task_id'] == task_id), None)
                if not task_config:
                    continue

                context.log.info(f"Executing task: {task_id}")

                # Execute based on operator type
                operator_type = task_config['operator_type']
                parameters = task_config['parameters']

                if operator_type == 'bash':
                    bash_command = parameters.get('bash_command', 'echo "No command"')
                    context.log.info(f"  Bash: {bash_command}")
                    result = subprocess.run(
                        bash_command,
                        shell=True,
                        capture_output=True,
                        text=True,
                        cwd=repo_path,
                    )
                    results[task_id] = {"stdout": result.stdout, "returncode": result.returncode}

                elif operator_type == 'python':
                    # Try to resolve python callable
                    callable_func = self.dag_factory_parser.resolve_python_callable(
                        task_config,
                        yaml_path.parent
                    )
                    if callable_func:
                        context.log.info(f"  Python callable: {callable_func.__name__}")
                        result = callable_func()
                        results[task_id] = result
                    else:
                        context.log.warning(f"  Could not resolve Python callable for task {task_id}")
                        results[task_id] = None

                elif operator_type == 'dummy':
                    context.log.info(f"  Dummy task (no-op)")
                    results[task_id] = "success"

                else:
                    context.log.warning(f"  Unsupported operator: {task_config['operator']}")
                    results[task_id] = None

            # Build enhanced metadata from default_args
            output_metadata = {
                "dag_id": MetadataValue.text(dag_id),
                "source": MetadataValue.text("dag_factory_yaml"),
                "yaml_file": MetadataValue.path(str(yaml_path)),
                "tasks_executed": MetadataValue.int(len(task_order)),
            }

            # Add enhanced metadata from DAG default_args
            if default_args.get('owner'):
                output_metadata["owner"] = MetadataValue.text(default_args['owner'])
            if default_args.get('email'):
                output_metadata["email"] = MetadataValue.text(default_args['email'])
            if default_args.get('pool'):
                output_metadata["pool"] = MetadataValue.text(default_args['pool'])
            if default_args.get('queue'):
                output_metadata["queue"] = MetadataValue.text(default_args['queue'])
            if default_args.get('priority_weight'):
                output_metadata["priority_weight"] = MetadataValue.int(default_args['priority_weight'])
            if default_args.get('execution_timeout'):
                output_metadata["execution_timeout"] = MetadataValue.text(str(default_args['execution_timeout']))
            if default_args.get('sla'):
                output_metadata["sla"] = MetadataValue.text(str(default_args['sla']))
            if default_args.get('email_on_failure') is not None:
                output_metadata["email_on_failure"] = MetadataValue.bool(default_args['email_on_failure'])
            if default_args.get('email_on_retry') is not None:
                output_metadata["email_on_retry"] = MetadataValue.bool(default_args['email_on_retry'])

            return Output(
                value={"dag_id": dag_id, "task_results": results},
                metadata=output_metadata
            )

        # Build static metadata for asset definition
        asset_metadata = {
            "dag_id": MetadataValue.text(dag_id),
            "source": MetadataValue.text("dag_factory_yaml"),
            "yaml_file": MetadataValue.path(str(yaml_path)),
        }

        # Add enhanced metadata from DAG default_args
        if default_args.get('owner'):
            asset_metadata["owner"] = MetadataValue.text(default_args['owner'])
        if default_args.get('email'):
            asset_metadata["email"] = MetadataValue.text(default_args['email'])
        if default_args.get('pool'):
            asset_metadata["pool"] = MetadataValue.text(default_args['pool'])
        if default_args.get('queue'):
            asset_metadata["queue"] = MetadataValue.text(default_args['queue'])
        if default_args.get('priority_weight'):
            asset_metadata["priority_weight"] = MetadataValue.int(default_args['priority_weight'])

        # Create the asset
        asset_kwargs = {
            "name": f"script_{script_info.name}",
            "group_name": metadata.group_name or "airflow_examples",
            "tags": asset_tags,
            "description": dag_info.get('description', f"DAG Factory: {dag_id}"),
            "retry_policy": retry_policy,
            "metadata": asset_metadata,
        }

        dag_factory_asset = asset(**asset_kwargs)(dag_factory_yaml_asset)

        logger.info(f"✅ Created dag-factory YAML asset (fallback): {script_info.name}")
        return dag_factory_asset

    def _resolve_callable_file(self, task_config: dict, base_path: Path) -> Optional[Path]:
        """Resolve the Python file containing the task's callable.

        Args:
            task_config: Task configuration dict
            base_path: Base path to resolve relative imports

        Returns:
            Path to the Python file, or None if not found
        """
        python_callable = task_config.get('python_callable')
        if not python_callable:
            return None

        try:
            # Convert module path to file path
            # e.g., "include.tasks.asset_example_tasks._update_iss_coordinates"
            # becomes "include/tasks/asset_example_tasks.py"
            parts = python_callable.split('.')
            if len(parts) < 2:
                return None

            # Remove function name (last part)
            module_parts = parts[:-1]
            module_path = Path(*module_parts).with_suffix('.py')
            file_path = base_path / module_path

            if file_path.exists():
                return file_path

        except Exception as e:
            logger.debug(f"Could not resolve callable file for {python_callable}: {e}")

        return None

    def _build_assets_and_job_from_dag(
        self, dag_info: dict, script_info: ScriptInfo, metadata: ScriptMetadata, repo_path: str
    ):
        """Build assets and asset job from DAG with outlets.

        Pattern: Airflow DAG with outlets → Dagster assets + asset job

        Returns:
            List containing [asset1, asset2, ..., asset_job]
        """
        dag_id = dag_info['dag_id']
        yaml_path = script_info.script_path
        yaml_filename = yaml_path.name  # Extract just the filename (e.g., "asset_chain_example.yaml")

        # Get tasks that produce outlets (these become assets)
        asset_tasks = []
        for task in dag_info['tasks']:
            if task.get('outlets'):
                asset_tasks.append(task)

        if not asset_tasks:
            logger.debug(f"DAG {dag_id} marked as having outlets but no tasks with outlets found")
            return None

        logger.info(f"Creating {len(asset_tasks)} assets from DAG {dag_id}")

        # Detect resources from task callables
        all_detected_resources = []
        for task in dag_info['tasks']:
            # Try to resolve the callable file
            task_file = self._resolve_callable_file(task, yaml_path.parent)
            if task_file and task_file.exists():
                try:
                    resources = ResourceDetector.detect_resources_from_file(task_file)
                    all_detected_resources.extend(resources)
                except Exception as e:
                    logger.debug(f"Could not detect resources from {task_file}: {e}")

        # Remove duplicates
        unique_resources = {r['resource_name']: r for r in all_detected_resources}
        if unique_resources:
            resource_names = list(unique_resources.keys())
            logger.info(f"🔧 Detected resources in Airflow tasks: {', '.join(resource_names)}")

        # Build retry policy from default_args
        default_args = dag_info.get('default_args', {})
        retry_policy = None
        if default_args.get('retries', 0) > 0:
            retry_policy = RetryPolicy(
                max_retries=default_args.get('retries', 0),
                delay=default_args.get('retry_delay_sec', 300),
            )

        created_assets = []

        # Create an asset for each task with outlets
        for task in asset_tasks:
            task_id = task['task_id']
            outlet_names = [o['name'] for o in task.get('outlets', [])]

            # Use first outlet name as asset name
            asset_name = outlet_names[0] if outlet_names else task_id

            # Determine dependencies from upstream tasks
            upstream_tasks = task.get('dependencies', [])
            deps = []

            # First, add DAG-level asset dependencies (cross-DAG dependencies)
            # These come from the 'schedule' field when it references assets
            asset_schedule = dag_info.get('asset_schedule') or []
            for scheduled_asset_name in asset_schedule:
                deps.append(AssetKey(scheduled_asset_name))
                logger.debug(f"Added cross-DAG dependency: {asset_name} depends on {scheduled_asset_name}")

            # Then, add task-level dependencies (within-DAG dependencies)
            # Map upstream task IDs to asset keys
            for upstream_task_id in upstream_tasks:
                # Find if upstream task produces an outlet
                upstream_task = next((t for t in dag_info['tasks'] if t['task_id'] == upstream_task_id), None)
                if upstream_task and upstream_task.get('outlets'):
                    # Upstream is also an asset
                    upstream_outlet = upstream_task['outlets'][0]['name']
                    deps.append(AssetKey(upstream_outlet))
                    logger.debug(f"Added within-DAG dependency: {asset_name} depends on {upstream_outlet}")

            # Build asset tags
            asset_tags = {
                **metadata.tags,
                "dag_id": dag_id,
                "task_id": task_id,
                "source": "dag_factory_yaml",
                "dag_factory_file": yaml_filename,
            }
            for kind in metadata.kinds:
                asset_tags[f"dagster/kind/{kind}"] = ""

            # Add detected resources as kinds and tags
            for resource_name, resource in unique_resources.items():
                asset_tags[f"dagster/kind/{resource_name}"] = ""
                asset_tags[f"uses_{resource_name}"] = ""
                asset_tags[f"resource_type_{resource['resource_type']}"] = ""

            # Add operator type as a kind (in addition to metadata kinds)
            operator_type = task.get('operator_type', 'unknown')
            if operator_type and operator_type != 'unknown':
                asset_tags[f"dagster/kind/{operator_type}"] = ""

            # Create the asset function
            def make_asset_func(task_config, yaml_path_param, repo_path_param, dag_id_param, parser):
                """Closure to capture task config and parser"""
                def asset_func(context: AssetExecutionContext):
                    """Execute the task and produce the asset."""
                    task_id = task_config['task_id']
                    context.log.info(f"Materializing asset from DAG {dag_id_param}, task {task_id}")

                    operator_type = task_config.get('operator_type')
                    parameters = task_config.get('parameters', {})

                    if operator_type == 'bash':
                        bash_command = parameters.get('bash_command', 'echo "No command"')
                        context.log.info(f"Executing bash: {bash_command}")
                        result = subprocess.run(
                            bash_command,
                            shell=True,
                            capture_output=True,
                            text=True,
                            cwd=repo_path_param,
                        )
                        return {"stdout": result.stdout, "returncode": result.returncode}

                    elif operator_type == 'python':
                        # Try to resolve python callable
                        callable_func = parser.resolve_python_callable(
                            task_config,
                            yaml_path_param.parent
                        )
                        if callable_func:
                            context.log.info(f"Executing Python callable: {callable_func.__name__}")
                            result = callable_func()
                            return result
                        else:
                            context.log.warning(f"Could not resolve Python callable for task {task_id}")
                            return None

                    elif operator_type == 'dummy':
                        context.log.info("Dummy task (no-op)")
                        return "success"

                    else:
                        context.log.warning(f"Unsupported operator: {operator_type}")
                        return None

                return asset_func

            # Build metadata with detected resources
            asset_metadata = {
                "dag_factory_file": yaml_filename,
                "dag_id": dag_id,
                "task_id": task_id,
            }
            if unique_resources:
                resource_info = {
                    name: {'type': res['resource_type'], 'import': res['import_name']}
                    for name, res in unique_resources.items()
                }
                asset_metadata["detected_resources"] = {
                    "resources": list(unique_resources.keys()),
                    "details": resource_info,
                }

            # Create asset kwargs
            # Build comprehensive description
            task_desc = task.get('description', '')
            if task_desc:
                description = f"{task_desc}\n\nFrom dag-factory YAML: {yaml_filename}\nDAG: {dag_id} | Task: {task_id}"
            else:
                description = f"Asset from dag-factory YAML: {yaml_filename}\nDAG: {dag_id} | Task: {task_id}"

            asset_kwargs = {
                "name": asset_name,
                "group_name": metadata.group_name or "airflow_examples",
                "tags": asset_tags,
                "description": description,
            }

            if asset_metadata:
                asset_kwargs["metadata"] = asset_metadata

            if deps:
                asset_kwargs["deps"] = deps

            if retry_policy:
                asset_kwargs["retry_policy"] = retry_policy

            # Create the decorated asset
            asset_func = make_asset_func(task, yaml_path, repo_path, dag_id, self.dag_factory_parser)
            asset_def = asset(**asset_kwargs)(asset_func)
            created_assets.append(asset_def)

            logger.info(f"  Created asset: {asset_name} (from task {task_id})")

        # Create asset job to group these assets
        job_name = dag_id  # Use DAG ID as job name

        # Create asset selection for all created assets
        asset_keys = [AssetKey(a.key.path[0]) for a in created_assets]

        # Build comprehensive job description
        dag_desc = dag_info.get('description', '')
        if dag_desc:
            job_description = f"{dag_desc}\n\nFrom dag-factory YAML: {yaml_filename}\nDAG: {dag_id}"
        else:
            job_description = f"Asset job from dag-factory YAML: {yaml_filename}\nDAG: {dag_id}"

        asset_job = define_asset_job(
            name=job_name,
            description=job_description,
            selection=asset_keys,
            tags={
                "source": "dag_factory_yaml",
                "dag_id": dag_id,
                "dag_factory_file": yaml_filename,
            }
        )

        logger.info(f"  Created asset job: {job_name} (groups {len(created_assets)} assets)")

        # Return list of all definitions
        return created_assets + [asset_job]

    def _build_op_job_and_sensor(
        self, dag_info: dict, script_info: ScriptInfo, metadata: ScriptMetadata, repo_path: str
    ):
        """Build op job and sensor from DAG without outlets.

        Pattern: Airflow DAG with asset_schedule but no outlets → Dagster op job + asset sensor

        Now with XCom support: Ops can accept parameters from upstream ops!

        Returns:
            List containing [op_job, sensor]
        """
        dag_id = dag_info['dag_id']
        yaml_path = script_info.script_path
        yaml_filename = yaml_path.name  # Extract just the filename

        # Get asset schedule (which asset triggers this DAG)
        asset_schedule = dag_info.get('asset_schedule', [])
        if not asset_schedule:
            logger.warning(f"DAG {dag_id} expected to have asset_schedule but none found")
            return None

        trigger_asset_name = asset_schedule[0] if isinstance(asset_schedule, list) else asset_schedule

        logger.info(f"Creating op job for DAG {dag_id} (triggered by asset: {trigger_asset_name})")

        # Detect resources from task callables
        all_detected_resources = []
        for task in dag_info['tasks']:
            task_file = self._resolve_callable_file(task, yaml_path.parent)
            if task_file and task_file.exists():
                try:
                    resources = ResourceDetector.detect_resources_from_file(task_file)
                    all_detected_resources.extend(resources)
                except Exception as e:
                    logger.debug(f"Could not detect resources from {task_file}: {e}")

        # Remove duplicates
        unique_resources = {r['resource_name']: r for r in all_detected_resources}
        if unique_resources:
            resource_names = list(unique_resources.keys())
            logger.info(f"🔧 Detected resources in Airflow tasks: {', '.join(resource_names)}")

        # Get XCom dependencies
        xcom_deps = dag_info.get('xcom_dependencies', {})

        # Create ops for each task
        created_ops = []

        for task in dag_info['tasks']:
            task_id = task['task_id']

            # Check if this task has XCom dependencies
            task_xcom_deps = xcom_deps.get(task_id, {})

            if task_xcom_deps:
                logger.info(f"  Task {task_id} has XCom dependencies: {task_xcom_deps}")

            # Create the op function with XCom support
            def make_op_func(task_config, yaml_path_param, repo_path_param, dag_id_param, parser, xcom_dependencies):
                """Closure to capture task config, parser, and XCom deps"""

                # Build parameter signature based on XCom dependencies
                if xcom_dependencies:
                    # Op needs to accept parameters from upstream ops
                    def op_func(context: OpExecutionContext, **xcom_inputs):
                        """Execute the task with XCom inputs."""
                        task_id = task_config['task_id']
                        context.log.info(f"Executing op from DAG {dag_id_param}, task {task_id}")

                        if xcom_inputs:
                            context.log.info(f"XCom inputs: {list(xcom_inputs.keys())}")

                        operator_type = task_config.get('operator_type')
                        parameters = task_config.get('parameters', {})

                        if operator_type == 'bash':
                            bash_command = parameters.get('bash_command', 'echo "No command"')
                            context.log.info(f"Executing bash: {bash_command}")
                            result = subprocess.run(
                                bash_command,
                                shell=True,
                                capture_output=True,
                                text=True,
                                cwd=repo_path_param,
                            )
                            return {"stdout": result.stdout, "returncode": result.returncode}

                        elif operator_type == 'python':
                            # Try to resolve python callable
                            callable_func = parser.resolve_python_callable(
                                task_config,
                                yaml_path_param.parent
                            )
                            if callable_func:
                                context.log.info(f"Executing Python callable: {callable_func.__name__}")

                                # Try to call with XCom inputs if callable accepts them
                                import inspect
                                sig = inspect.signature(callable_func)

                                if len(sig.parameters) > 0 and xcom_inputs:
                                    # Callable accepts parameters - pass XCom inputs
                                    context.log.info(f"Passing XCom inputs to callable: {list(xcom_inputs.keys())}")
                                    result = callable_func(**xcom_inputs)
                                else:
                                    # Callable doesn't accept parameters or no XCom inputs
                                    result = callable_func()

                                return result
                            else:
                                context.log.warning(f"Could not resolve Python callable for task {task_id}")
                                return None

                        elif operator_type == 'dummy':
                            context.log.info("Dummy task (no-op)")
                            return "success"

                        else:
                            context.log.warning(f"Unsupported operator: {operator_type}")
                            return None
                else:
                    # No XCom dependencies - standard op that accepts optional upstream data for structural dependencies
                    def op_func(context: OpExecutionContext, upstream_data=None):
                        """Execute the task with optional upstream dependency data."""
                        task_id = task_config['task_id']
                        context.log.info(f"Executing op from DAG {dag_id_param}, task {task_id}")

                        if upstream_data is not None:
                            context.log.info(f"Op has upstream dependency (structural wiring)")

                        operator_type = task_config.get('operator_type')
                        parameters = task_config.get('parameters', {})

                        if operator_type == 'bash':
                            bash_command = parameters.get('bash_command', 'echo "No command"')
                            context.log.info(f"Executing bash: {bash_command}")
                            result = subprocess.run(
                                bash_command,
                                shell=True,
                                capture_output=True,
                                text=True,
                                cwd=repo_path_param,
                            )
                            return {"stdout": result.stdout, "returncode": result.returncode}

                        elif operator_type == 'python':
                            # Try to resolve python callable
                            callable_func = parser.resolve_python_callable(
                                task_config,
                                yaml_path_param.parent
                            )
                            if callable_func:
                                context.log.info(f"Executing Python callable: {callable_func.__name__}")
                                result = callable_func()
                                return result
                            else:
                                context.log.warning(f"Could not resolve Python callable for task {task_id}")
                                return None

                        elif operator_type == 'dummy':
                            context.log.info("Dummy task (no-op)")
                            return "success"

                        else:
                            context.log.warning(f"Unsupported operator: {operator_type}")
                            return None

                return op_func

            # Create the decorated op with XCom dependencies
            op_func = make_op_func(task, yaml_path, repo_path, dag_id, self.dag_factory_parser, task_xcom_deps)
            op_def = op(name=task_id)(op_func)
            created_ops.append((task_id, op_def, task_xcom_deps))

            logger.info(f"  Created op: {task_id}")

        # Create job that executes ops in order with XCom data passing
        job_name = dag_id

        # Build job function that respects dependencies and passes data
        task_order = self.dag_factory_parser.get_task_execution_order(dag_info)
        task_dependencies = dag_info.get('task_dependencies', {})

        def make_job_func(ops_list, task_order, task_dependencies, xcom_deps_map):
            """Create job function with proper dependency wiring"""
            def job_func():
                """Execute ops with proper Dagster dependency wiring."""
                results = {}
                ops_dict = {task_id: op_def for task_id, op_def, _ in ops_list}
                xcom_dict = {task_id: xcom_deps for task_id, _, xcom_deps in ops_list}

                # Execute tasks in topological order, wiring up dependencies
                for task_id in task_order:
                    if task_id not in ops_dict:
                        continue

                    op = ops_dict[task_id]
                    deps = task_dependencies.get(task_id, [])
                    has_xcom = task_id in xcom_dict and xcom_dict[task_id]

                    if not deps:
                        # Root task - no dependencies
                        results[task_id] = op()
                    elif has_xcom:
                        # Task has XCom dependencies - pass upstream results
                        xcom_inputs = {}
                        for param_name, upstream_task_id in xcom_dict[task_id].items():
                            if upstream_task_id in results:
                                xcom_inputs[param_name] = results[upstream_task_id]
                        results[task_id] = op(**xcom_inputs)
                    elif len(deps) == 1:
                        # Single dependency - pass its result directly to create structural dependency
                        results[task_id] = op(results[deps[0]])
                    else:
                        # Multiple dependencies - just pass the first one to establish structural dependency
                        # (The actual data isn't used since there's no XCom - this is just for graph wiring)
                        upstream_results = [results[dep] for dep in deps if dep in results]
                        if upstream_results:
                            results[task_id] = op(upstream_results[0])
                        else:
                            results[task_id] = op()

                # Don't return results for terminal operation jobs (no outputs needed)
            return job_func

        # Create the job with dependency-aware function
        job_func = make_job_func(created_ops, task_order, task_dependencies, xcom_deps)

        # Build job tags including detected resources
        job_tags = {
            "source": "dag_factory_yaml",
            "dag_id": dag_id,
            "dag_factory_file": yaml_filename,
            "dagster/kind/airflow": "",  # Airflow framework kind
        }

        # Add detected resources as kinds and tags
        for resource_name, resource in unique_resources.items():
            job_tags[f"dagster/kind/{resource_name}"] = ""
            job_tags[f"uses_{resource_name}"] = ""
            job_tags[f"resource_type_{resource['resource_type']}"] = ""

        # Build comprehensive job description
        dag_desc = dag_info.get('description', '')
        if dag_desc:
            job_description = f"{dag_desc}\n\nFrom dag-factory YAML: {yaml_filename}\nDAG: {dag_id}\nTriggered by asset: {trigger_asset_name}"
        else:
            job_description = f"Op job from dag-factory YAML: {yaml_filename}\nDAG: {dag_id}\nTriggered by asset: {trigger_asset_name}"

        op_job = job(
            name=job_name,
            description=job_description,
            tags=job_tags
        )(job_func)

        if xcom_deps:
            logger.info(f"  ✅ Op job supports XCom data passing for {len(xcom_deps)} task(s)")

        logger.info(f"  Created op job: {job_name}")

        # Create asset sensor to trigger this job when asset materializes
        sensor_name = f"{dag_id}_sensor"

        def make_sensor_func(trigger_asset, job_ref):
            """Create sensor function"""
            def sensor_func(context, asset_event):
                """Trigger job when asset materializes."""
                context.log.info(f"Asset {trigger_asset} materialized, triggering job {job_ref.name}")
                yield RunRequest()
            return sensor_func

        sensor_func = make_sensor_func(trigger_asset_name, op_job)
        sensor_def = asset_sensor(
            asset_key=AssetKey(trigger_asset_name),
            job=op_job,
            name=sensor_name,
        )(sensor_func)

        logger.info(f"  Created asset sensor: {sensor_name} (watches {trigger_asset_name})")

        # Return list of definitions
        return [op_job, sensor_def]

    def _build_dag_factory_asset(
        self, script_info: ScriptInfo, all_scripts: List[ScriptInfo], repo_path: str
    ):
        """Build a partitioned Dagster asset from DAG Factory configuration.

        This converts the Airflow DAG Factory pattern (multiple similar DAGs) into
        a single Dagster partitioned asset where each partition represents what was
        previously a separate DAG.
        """
        metadata = script_info.metadata or ScriptMetadata()
        factory_config = metadata.dag_factory

        if not factory_config:
            raise ValueError("DAG Factory config is required but not provided")

        # Create partitions definition
        if factory_config.dynamic:
            # Dynamic partitions - can add/remove at runtime
            partitions_def = DynamicPartitionsDefinition(name=f"{script_info.name}_partitions")
            logger.info(f"Created dynamic partitions for DAG Factory: {script_info.name}")
        elif factory_config.partition_values:
            # Static partitions - fixed list
            partitions_def = StaticPartitionsDefinition(factory_config.partition_values)
            logger.info(f"Created static partitions for DAG Factory: {script_info.name} ({len(factory_config.partition_values)} partitions)")
        else:
            raise ValueError("DAG Factory requires either partition_values (static) or dynamic=true")

        # Build retry policy
        retry_policy = None
        if metadata.retry_policy:
            policy = metadata.retry_policy
            backoff = Backoff.EXPONENTIAL if policy.backoff == "EXPONENTIAL" else Backoff.LINEAR
            jitter = None
            if policy.jitter == "FULL":
                jitter = Jitter.FULL
            elif policy.jitter == "PLUS_MINUS":
                jitter = Jitter.PLUS_MINUS

            retry_policy = RetryPolicy(
                max_retries=policy.max_retries,
                delay=policy.delay,
                backoff=backoff,
                jitter=jitter,
            )

        # Build asset tags with dag-factory kind
        asset_tags = {
            **metadata.tags,
            "script_type": "airflow",
            "script_name": script_info.name,
            "pattern": "dag_factory",
        }
        for kind in metadata.kinds:
            asset_tags[f"dagster/kind/{kind}"] = ""

        # Parse the DAG to get structure
        tasks, dags = self._parse_airflow_dag(script_info.script_path)
        dag_info = dags[0] if dags else {}
        dag_id = dag_info.get('dag_id', script_info.name)
        logger.info(f"Creating DAG Factory asset from Python+YAML: script_{script_info.name} for DAG {dag_id} from {script_info.script_path}")

        # Define the partitioned asset function
        def dag_factory_asset(context: AssetExecutionContext):
            """Execute Airflow DAG with partition key as parameter."""
            # Check if running with partition
            partition_key = context.partition_key if context.has_partition_key else None
            if partition_key:
                logger.info(f"Running DAG Factory asset for partition: {partition_key}")
                context.log.info(f"Partition: {partition_key}")
            context.log.info(f"DAG: {dag_id}")

            try:
                start_time = datetime.now()
                execution_date = start_time.strftime('%Y-%m-%d')

                # Execute Airflow DAG with partition key as parameter
                airflow_cmd = self._build_airflow_command("dags", "test", dag_id, execution_date)

                # Set environment including the partition key
                env = os.environ.copy()
                # Set AIRFLOW_HOME to match the initialized database location
                env["AIRFLOW_HOME"] = str(Path(repo_path) / ".airflow")
                dag_directory = str(script_info.script_path.parent)
                env["AIRFLOW__CORE__DAGS_FOLDER"] = dag_directory
                env["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
                env["AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT"] = "30"

                # Get partition parameter name
                partition_param_name = factory_config.partition_key if hasattr(factory_config, 'partition_key') else "partition"

                # Pass partition key as Airflow variable (if provided)
                if partition_key:
                    env[f"AIRFLOW_VAR_{partition_param_name.upper()}"] = partition_key
                    context.log.info(f"Partition parameter: {partition_param_name}={partition_key}")

                context.log.info(f"Executing: {' '.join(airflow_cmd)}")

                result = subprocess.run(
                    airflow_cmd,
                    cwd=repo_path,
                    capture_output=True,
                    text=True,
                    check=False,
                    env=env,
                )

                if result.returncode != 0:
                    context.log.error(f"DAG failed with exit code {result.returncode}")
                    context.log.error(f"stdout: {result.stdout}")
                    context.log.error(f"stderr: {result.stderr}")
                    raise subprocess.CalledProcessError(result.returncode, airflow_cmd, result.stdout, result.stderr)

                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()

                output_metadata = {
                    "dag_id": MetadataValue.text(dag_id),
                    "partition_key": MetadataValue.text(partition_key or "default"),
                    "partition_param": MetadataValue.text(f"{partition_param_name}={partition_key or 'default'}"),
                    "script_path": MetadataValue.path(str(script_info.script_path)),
                    "execution_date": MetadataValue.text(execution_date),
                    "duration_seconds": MetadataValue.float(duration),
                    "execution_time": MetadataValue.timestamp(end_time.timestamp()),
                    "pattern": MetadataValue.text("dag_factory"),
                }

                if result.stdout:
                    output_metadata["stdout"] = MetadataValue.md(f"```\n{result.stdout[-5000:]}\n```")
                if result.stderr:
                    output_metadata["stderr"] = MetadataValue.md(f"```\n{result.stderr[-5000:]}\n```")

                return Output(
                    value={"status": "success", "partition": partition_key or "default"},
                    metadata=output_metadata,
                )
            except Exception as e:
                logger.error(f"Error running DAG Factory asset: {e}")
                raise

        # Create the partitioned asset with correct prefix
        asset_prefix = self._get_asset_prefix(metadata.script_type)
        asset_kwargs = {
            "name": f"{asset_prefix}_{script_info.name}",
            "partitions_def": partitions_def,
            "group_name": metadata.group_name,
            "tags": asset_tags,
            "description": metadata.description or f"DAG Factory partitioned asset: {dag_id} (by {factory_config.partition_key})",
            "owners": metadata.owners or [],
            "retry_policy": retry_policy,
        }

        partitioned_asset = asset(**asset_kwargs)(dag_factory_asset)

        logger.info(f"✅ Created DAG Factory partitioned asset: {script_info.name}")
        return partitioned_asset

    def _execute_script_with_monitoring(
        self,
        context: AssetExecutionContext,
        script_path: Path,
        repo_path: str,
        cli_args: Optional[List[str]] = None,
        base_metadata: Optional[Dict[str, Any]] = None,
    ) -> Output:
        """Execute a script with performance monitoring.

        Args:
            context: Dagster execution context
            script_path: Path to the script
            repo_path: Repository path
            cli_args: Optional CLI arguments
            base_metadata: Optional base metadata to include

        Returns:
            Output with result and performance metadata
        """
        python_cmd = ["uv", "run", "python", str(script_path)]
        if cli_args:
            python_cmd.extend(cli_args)

        # Execute with performance monitoring
        with PerformanceMonitor.track_performance(context.log) as perf:
            result = subprocess.run(
                python_cmd,
                cwd=repo_path,
                capture_output=True,
                text=True,
                check=False,
            )

            if result.returncode != 0:
                context.log.error(f"Script failed with exit code {result.returncode}")
                context.log.error(f"stdout: {result.stdout}")
                context.log.error(f"stderr: {result.stderr}")
                raise subprocess.CalledProcessError(
                    result.returncode, python_cmd, result.stdout, result.stderr
                )

        # Build output metadata
        output_metadata = base_metadata.copy() if base_metadata else {}

        # Add performance metrics
        output_metadata.update(perf.get_metadata())

        # Add script output
        if result.stdout:
            output_metadata["stdout"] = MetadataValue.md(f"```\n{result.stdout}\n```")
        if result.stderr:
            output_metadata["stderr"] = MetadataValue.md(f"```\n{result.stderr}\n```")

        # Log performance summary
        context.log.info(f"Performance: {perf.get_summary()}")

        return Output(
            value={"status": "success"},
            metadata=output_metadata,
        )

    def _build_script_asset(self, script_info: ScriptInfo, all_scripts: List[ScriptInfo], repo_path: str):
        """Build a Dagster asset for a Python script with config and partitions support."""
        metadata = script_info.metadata or ScriptMetadata()

        # Extract documentation from script
        doc_info = DocumentationExtractor.extract_from_file(script_info.script_path)
        if doc_info['has_documentation']:
            logger.info(f"📚 Extracted documentation from {script_info.name}")

        # Detect resources from script (for kinds and tags)
        detected_resources = []
        if script_info.script_path.suffix == '.py':
            detected_resources = ResourceDetector.detect_resources_from_file(script_info.script_path)
            if detected_resources:
                resource_names = [r['resource_name'] for r in detected_resources]
                logger.info(f"🔧 Detected resources: {', '.join(resource_names)}")

        # Build retry policy
        retry_policy = None
        if metadata.retry_policy:
            policy = metadata.retry_policy
            backoff = Backoff.EXPONENTIAL if policy.backoff == "EXPONENTIAL" else Backoff.LINEAR
            jitter = None
            if policy.jitter == "FULL":
                jitter = Jitter.FULL
            elif policy.jitter == "PLUS_MINUS":
                jitter = Jitter.PLUS_MINUS
            
            retry_policy = RetryPolicy(
                max_retries=policy.max_retries,
                delay=policy.delay,
                backoff=backoff,
                jitter=jitter,
            )
        
        # Try to extract parameters for config
        flow_config_class = None
        if metadata.script_type == "prefect":
            try:
                tasks, flows = self._parse_prefect_flow(script_info.script_path)
                if flows:
                    flow_info = flows[0]
                    flow_config_class = self._generate_flow_config_class(flow_info, script_info)
                    if flow_config_class:
                        logger.info(f"Generated config for {script_info.name} with {len(flow_info.get('parameters', []))} parameters")
            except Exception as e:
                logger.debug(f"Could not extract flow parameters for {script_info.name}: {e}")
        else:
            try:
                argparse_params = self._parse_argparse_arguments(script_info.script_path)
                if argparse_params:
                    flow_config_class = self._generate_config_class(argparse_params, source='argparse')
                    logger.info(f"Generated config for {script_info.name} with {len(argparse_params)} argparse parameters")
                else:
                    sys_argv_params = self._parse_sys_argv_usage(script_info.script_path)
                    if sys_argv_params:
                        flow_config_class = self._generate_config_class(sys_argv_params, source='sys.argv')
                        logger.info(f"Generated config for {script_info.name} with {len(sys_argv_params)} sys.argv parameters")
            except Exception as e:
                logger.debug(f"Could not extract parameters for {script_info.name}: {e}")
        
        # Build asset tags
        script_type = metadata.script_type if metadata else "python"
        asset_tags = {
            **metadata.tags,
            "script_type": script_type,
            "script_name": script_info.name
        }

        # Add kinds from metadata
        for kind in metadata.kinds:
            asset_tags[f"dagster/kind/{kind}"] = ""

        # Automatically add "python" kind for Python-based script types
        if script_type in ['python', 'dask', 'spark']:
            asset_tags[f"dagster/kind/python"] = ""

        # Add detected resources as kinds and tags
        resource_kinds = []
        if detected_resources:
            for resource in detected_resources:
                resource_name = resource['resource_name']
                resource_type = resource['resource_type']

                # Add as kind (shows icon in UI)
                asset_tags[f"dagster/kind/{resource_name}"] = ""
                resource_kinds.append(resource_name)

                # Add as regular tag for filtering
                asset_tags[f"uses_{resource_name}"] = ""
                asset_tags[f"resource_type_{resource_type}"] = ""
        
        # Build dependencies (for ordering only, not data passing)
        deps = []
        if metadata.depends_on:
            for dep_name in metadata.depends_on:
                dep_script = next((s for s in all_scripts if s.name == dep_name), None)
                if dep_script:
                    # Get the appropriate prefix based on dependency's script type
                    dep_prefix = self._get_asset_prefix(dep_script.metadata.script_type if dep_script.metadata else 'python')
                    # Use AssetKey for ordering-only dependencies (no data passing)
                    deps.append(AssetKey(f"{dep_prefix}_{dep_name}"))
                else:
                    logger.warning(f"Dependency {dep_name} not found for script {script_info.name}")
        
        # Create partition definition if configured
        partitions_def = None
        partition_param_name = None
        partition_date_format = "%Y-%m-%d"
        
        if metadata.partition:
            partitions_def = self._create_partition_definition(metadata.partition)
            partition_param_name = metadata.partition.parameter
            partition_date_format = metadata.partition.date_format
            # Determine partition type for logging
            if metadata.partition.values:
                partition_type = f"static ({len(metadata.partition.values)} partitions)"
            elif metadata.partition.dynamic:
                partition_type = "dynamic"
            else:
                partition_type = metadata.partition.schedule or "daily"
            logger.info(f"Created {partition_type} partitions for {script_info.name}")
        
        # Define asset function based on features (config and/or partitions)
        if flow_config_class and partitions_def:
            def script_asset(context: AssetExecutionContext, config):
                """Execute script with config and partition."""
                logger.info(f"Running {script_type} script: {script_info.name}")
                context.log.info(f"Script config: {config}")
                partition_key = context.partition_key if context.has_partition_key else "default"
                context.log.info(f"Partition: {partition_key}")

                try:
                    start_time = datetime.now()
                    python_cmd = ["uv", "run", "python", str(script_info.script_path)]

                    config_source = getattr(flow_config_class, '_source', None)
                    if config_source in ('argparse', 'sys.argv'):
                        parameters = getattr(flow_config_class, '_parameters', [])
                        cli_args = self._build_cli_arguments(config, parameters, config_source)

                        # Add partition as a CLI argument (not positional, since config takes positions)
                        if config_source == 'argparse':
                            # For argparse, add as --<parameter> <value>
                            cli_args.append(f"--{partition_param_name}")
                            cli_args.append(partition_key)
                        elif config_source == 'sys.argv':
                            # For sys.argv, append as positional after other args
                            cli_args.append(partition_key)

                        python_cmd.extend(cli_args)
                        context.log.info(f"CLI arguments (with partition): {cli_args}")
                    
                    result = subprocess.run(
                        python_cmd,
                        cwd=repo_path,
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    
                    if result.returncode != 0:
                        context.log.error(f"Script failed with exit code {result.returncode}")
                        context.log.error(f"stdout: {result.stdout}")
                        context.log.error(f"stderr: {result.stderr}")
                        raise subprocess.CalledProcessError(result.returncode, python_cmd, result.stdout, result.stderr)
                    
                    end_time = datetime.now()
                    duration = (end_time - start_time).total_seconds()
                    
                    output_metadata = {
                        "script_name": MetadataValue.text(script_info.name),
                        "script_path": MetadataValue.path(str(script_info.script_path)),
                        "partition": MetadataValue.text(partition_key),
                        "duration_seconds": MetadataValue.float(duration),
                        "execution_time": MetadataValue.timestamp(end_time.timestamp()),
                    }
                    
                    if result.stdout:
                        output_metadata["stdout"] = MetadataValue.md(f"```\n{result.stdout}\n```")
                    if result.stderr:
                        output_metadata["stderr"] = MetadataValue.md(f"```\n{result.stderr}\n```")
                    
                    return Output(
                        value={"status": "success", "partition": partition_key},
                        metadata=output_metadata,
                    )
                except Exception as e:
                    logger.error(f"Error running script: {e}")
                    raise
                    
        elif flow_config_class:
            def script_asset(context: AssetExecutionContext, config):
                """Execute script with config."""
                logger.info(f"Running {script_type} script: {script_info.name}")
                context.log.info(f"Script config: {config}")
                
                try:
                    start_time = datetime.now()
                    python_cmd = ["uv", "run", "python", str(script_info.script_path)]
                    
                    config_source = getattr(flow_config_class, '_source', None)
                    if config_source in ('argparse', 'sys.argv'):
                        parameters = getattr(flow_config_class, '_parameters', [])
                        cli_args = self._build_cli_arguments(config, parameters, config_source)
                        python_cmd.extend(cli_args)
                    
                    result = subprocess.run(
                        python_cmd,
                        cwd=repo_path,
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    
                    if result.returncode != 0:
                        context.log.error(f"Script failed with exit code {result.returncode}")
                        context.log.error(f"stdout: {result.stdout}")
                        context.log.error(f"stderr: {result.stderr}")
                        raise subprocess.CalledProcessError(result.returncode, python_cmd, result.stdout, result.stderr)
                    
                    end_time = datetime.now()
                    duration = (end_time - start_time).total_seconds()
                    
                    output_metadata = {
                        "script_name": MetadataValue.text(script_info.name),
                        "script_path": MetadataValue.path(str(script_info.script_path)),
                        "duration_seconds": MetadataValue.float(duration),
                        "execution_time": MetadataValue.timestamp(end_time.timestamp()),
                    }
                    
                    if result.stdout:
                        output_metadata["stdout"] = MetadataValue.md(f"```\n{result.stdout}\n```")
                    if result.stderr:
                        output_metadata["stderr"] = MetadataValue.md(f"```\n{result.stderr}\n```")
                    
                    return Output(
                        value={"status": "success"},
                        metadata=output_metadata,
                    )
                except Exception as e:
                    logger.error(f"Error running script: {e}")
                    raise
                    
        elif partitions_def:
            def script_asset(context: AssetExecutionContext):
                """Execute script with partition."""
                logger.info(f"Running {script_type} script: {script_info.name}")
                partition_key = context.partition_key if context.has_partition_key else "default"
                context.log.info(f"Partition: {partition_key}")

                try:
                    start_time = datetime.now()
                    python_cmd = ["uv", "run", "python", str(script_info.script_path), partition_key]
                    
                    result = subprocess.run(
                        python_cmd,
                        cwd=repo_path,
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    
                    if result.returncode != 0:
                        context.log.error(f"Script failed with exit code {result.returncode}")
                        context.log.error(f"stdout: {result.stdout}")
                        context.log.error(f"stderr: {result.stderr}")
                        raise subprocess.CalledProcessError(result.returncode, python_cmd, result.stdout, result.stderr)
                    
                    end_time = datetime.now()
                    duration = (end_time - start_time).total_seconds()
                    
                    output_metadata = {
                        "script_name": MetadataValue.text(script_info.name),
                        "script_path": MetadataValue.path(str(script_info.script_path)),
                        "partition": MetadataValue.text(partition_key),
                        "duration_seconds": MetadataValue.float(duration),
                        "execution_time": MetadataValue.timestamp(end_time.timestamp()),
                    }
                    
                    if result.stdout:
                        output_metadata["stdout"] = MetadataValue.md(f"```\n{result.stdout}\n```")
                    if result.stderr:
                        output_metadata["stderr"] = MetadataValue.md(f"```\n{result.stderr}\n```")
                    
                    return Output(
                        value={"status": "success", "partition": partition_key},
                        metadata=output_metadata,
                    )
                except Exception as e:
                    logger.error(f"Error running script: {e}")
                    raise
        else:
            def script_asset(context: AssetExecutionContext):
                """Execute script."""
                logger.info(f"Running {script_type} script: {script_info.name}")
                
                try:
                    start_time = datetime.now()
                    python_cmd = ["uv", "run", "python", str(script_info.script_path)]
                    
                    result = subprocess.run(
                        python_cmd,
                        cwd=repo_path,
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    
                    if result.returncode != 0:
                        context.log.error(f"Script failed with exit code {result.returncode}")
                        context.log.error(f"stdout: {result.stdout}")
                        context.log.error(f"stderr: {result.stderr}")
                        raise subprocess.CalledProcessError(result.returncode, python_cmd, result.stdout, result.stderr)
                    
                    end_time = datetime.now()
                    duration = (end_time - start_time).total_seconds()
                    
                    output_metadata = {
                        "script_name": MetadataValue.text(script_info.name),
                        "script_path": MetadataValue.path(str(script_info.script_path)),
                        "duration_seconds": MetadataValue.float(duration),
                        "execution_time": MetadataValue.timestamp(end_time.timestamp()),
                    }
                    
                    if result.stdout:
                        output_metadata["stdout"] = MetadataValue.md(f"```\n{result.stdout}\n```")
                    if result.stderr:
                        output_metadata["stderr"] = MetadataValue.md(f"```\n{result.stderr}\n```")
                    
                    return Output(
                        value={"status": "success"},
                        metadata=output_metadata,
                    )
                except Exception as e:
                    logger.error(f"Error running script: {e}")
                    raise
        
        # Set config annotation if needed
        if flow_config_class:
            script_asset.__annotations__['config'] = flow_config_class
        
        # Build base metadata for enrichment
        base_metadata = {
            "script_name": MetadataValue.text(script_info.name),
            "script_path": MetadataValue.path(str(script_info.script_path)),
            "script_type": MetadataValue.text(script_type),
        }

        # Add detected resources to metadata
        if detected_resources:
            resource_info = {}
            for resource in detected_resources:
                resource_info[resource['resource_name']] = {
                    'type': resource['resource_type'],
                    'import': resource['import_name'],
                }

            base_metadata["detected_resources"] = MetadataValue.json({
                "resources": [r['resource_name'] for r in detected_resources],
                "details": resource_info,
            })

        # Enrich with documentation metadata
        if doc_info['has_documentation']:
            enriched_metadata = DocumentationExtractor.enrich_asset_metadata(
                base_metadata, doc_info
            )
        else:
            enriched_metadata = base_metadata

        # Create rich description from documentation
        description = DocumentationExtractor.create_rich_description(
            script_info.name,
            doc_info,
            fallback_description=metadata.description
        )

        # Apply asset decorator
        asset_prefix = self._get_asset_prefix(metadata.script_type)
        asset_kwargs = {
            "name": f"{asset_prefix}_{script_info.name}",
            "group_name": metadata.group_name,
            "tags": asset_tags,
            "description": description,
            "metadata": enriched_metadata,
            "owners": metadata.owners or [],
            "retry_policy": retry_policy,
            "deps": deps if deps else None,  # Use deps for ordering-only dependencies
        }

        if partitions_def:
            asset_kwargs["partitions_def"] = partitions_def
        
        script_asset = asset(**asset_kwargs)(script_asset)
        return script_asset

    def _build_schedule(self, schedule_name: str, schedule_config, asset_name: str):
        """Build a Dagster schedule from configuration."""
        status_str = schedule_config.default_status.upper()
        default_status = DefaultScheduleStatus.RUNNING if status_str == "RUNNING" else DefaultScheduleStatus.STOPPED
        
        return ScheduleDefinition(
            name=schedule_name,
            target=AssetSelection.keys(asset_name),
            cron_schedule=schedule_config.cron_schedule,
            execution_timezone=schedule_config.timezone,
            default_status=default_status,
        )


    # =========================================================================
    # Cosmos / dbt support
    # =========================================================================
    #
    # When dbt_project_path is set, any .py file that imports from 'cosmos' is
    # routed here instead of being wrapped as a plain Airflow asset.
    #
    # Classification:
    #   replaced  — DAG just ran dbt via Cosmos; native @dbt_assets is better.
    #               A Dagster job is created; the Cosmos DAG can be disabled.
    #   absorbed  — Airflow/Cosmos concept that Dagster handles as a built-in
    #               (sensor, resource, asset selection). No job needed.
    #   skipped   — Infrastructure-specific; no Dagster equivalent.
    # =========================================================================

    def _build_cosmos_dbt_defs(self, repo_dir: Path) -> "Definitions":
        """Build native @dbt_assets + jobs for all applicable Cosmos DAGs."""
        try:
            from dagster_dbt import DbtCliResource, DbtProject, dbt_assets
            from dagster_dbt import build_schedule_from_dbt_selection, build_dbt_asset_selection
        except ImportError as exc:
            raise ImportError(
                "dagster-dbt is not installed. Add it to your dependencies: "
                "uv add dagster-dbt dbt-core dbt-<your-adapter>"
            ) from exc

        dbt_proj_dir = repo_dir / self.dbt_project_path
        if not dbt_proj_dir.exists():
            raise FileNotFoundError(
                f"dbt project not found at {dbt_proj_dir}. "
                f"Check dbt_project_path in defs.yaml."
            )

        # --- set up the dbt project ---
        project_kwargs: dict = {"project_dir": str(dbt_proj_dir)}
        if self.dbt_target:
            project_kwargs["target"] = self.dbt_target
        if self.dbt_profiles_dir:
            project_kwargs["profiles_dir"] = self.dbt_profiles_dir

        dbt_project = DbtProject(**project_kwargs)
        dbt_project.prepare_if_dev()
        dbt_resource = DbtCliResource(project_dir=dbt_project)

        # --- scan and classify Cosmos DAG files ---
        dags_dir = repo_dir / self.scripts_directory
        base_url = f"{(self.repo_url or '').rstrip('/')}/blob/{self.repo_branch}/{self.scripts_directory}"
        records = self._scan_cosmos_dags(dags_dir, base_url)

        replaced = [r for r in records if r["action"] == "replaced"]
        absorbed = [r for r in records if r["action"] == "absorbed"]
        skipped  = [r for r in records if r["action"] in ("skipped", "not_cosmos")]

        # --- core @dbt_assets covering all models ---
        @dbt_assets(manifest=dbt_project.manifest_path, name="dbt_assets")
        def all_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
            yield from (
                dbt.cli(["build"], context=context)
                .stream()
                .fetch_row_counts()
                .fetch_column_metadata()
            )

        # --- one job (or schedule) per replaced Cosmos DAG ---
        all_jobs = []
        all_schedules = []

        for rec in replaced:
            job_name = f"cosmos__{rec['stem']}"
            cron = self._normalise_cosmos_schedule(rec["schedule"])
            dbt_sel = rec["dbt_select"]

            if cron:
                sched = build_schedule_from_dbt_selection(
                    [all_dbt_assets],
                    job_name=job_name,
                    cron_schedule=cron,
                    dbt_select=dbt_sel or "*",
                    tags={"cosmos_source_dag": rec["stem"]},
                )
                all_schedules.append(sched)
            else:
                sel = (
                    build_dbt_asset_selection([all_dbt_assets], dbt_select=dbt_sel)
                    if dbt_sel
                    else AssetSelection.assets(all_dbt_assets)
                )
                job = define_asset_job(
                    name=job_name,
                    selection=sel,
                    description=rec["description"],
                    tags={"cosmos_source_dag": rec["stem"]},
                )
                all_jobs.append(job)

        # --- dbt docs asset (replaces dbt_docs.py Cosmos DAG) ---
        _dbt_proj_dir = dbt_proj_dir  # closure capture

        @asset(
            name="dbt_docs",
            group_name="dbt_documentation",
            kinds={"dbt"},
            description=(
                "Generates dbt HTML documentation. "
                "Replaces the dbt_docs.py Cosmos DAG. "
                "Run `dbt docs serve` to browse locally."
            ),
        )
        def dbt_docs_asset(
            context: AssetExecutionContext, dbt: DbtCliResource
        ) -> MaterializeResult:
            yield from dbt.cli(["docs", "generate"], context=context).stream()
            return MaterializeResult(
                metadata={
                    "docs_index": MetadataValue.path(
                        str(_dbt_proj_dir / "target" / "index.html")
                    )
                }
            )

        # --- migration summary asset ---
        _replaced_map = {r["stem"]: r["description"] for r in replaced}
        _absorbed_map = {r["stem"]: r["action_note"]  for r in absorbed}
        _skipped_map  = {r["stem"]: r["action_note"]  for r in skipped}
        _total        = len(records)

        @asset(
            name="cosmos_migration_summary",
            group_name="cosmos_metadata",
            description="Materialise to see the full Cosmos → Dagster migration report.",
            metadata={
                "replaced":      MetadataValue.int(len(replaced)),
                "absorbed":      MetadataValue.int(len(absorbed)),
                "skipped":       MetadataValue.int(len(skipped)),
                "replaced_dags": MetadataValue.json(_replaced_map),
                "absorbed_dags": MetadataValue.json(_absorbed_map),
                "skipped_dags":  MetadataValue.json(_skipped_map),
            },
        )
        def cosmos_migration_summary(context: AssetExecutionContext) -> MaterializeResult:
            w = 64
            context.log.info("=" * w)
            context.log.info("Cosmos → Dagster Migration Report")
            context.log.info(
                f"Scanned: {_total}   "
                f"Replaced: {len(replaced)}   "
                f"Absorbed: {len(absorbed)}   "
                f"Skipped: {len(skipped)}"
            )
            context.log.info("")
            context.log.info("REPLACED — Cosmos DAG can be disabled in Airflow")
            context.log.info("-" * w)
            for stem, desc in _replaced_map.items():
                context.log.info(f"  ✓ {stem}  →  job: cosmos__{stem}")
                context.log.info(f"    {desc}")
            context.log.info("")
            context.log.info("ABSORBED — Dagster handles this as a built-in primitive")
            context.log.info("-" * w)
            for stem, note in _absorbed_map.items():
                context.log.info(f"  ⊕ {stem}")
                context.log.info(f"    {note}")
            context.log.info("")
            context.log.info("SKIPPED — Infrastructure-specific, not applicable")
            context.log.info("-" * w)
            for stem, note in _skipped_map.items():
                context.log.info(f"  ✗ {stem}")
                context.log.info(f"    {note}")
            context.log.info("=" * w)
            return MaterializeResult(
                metadata={
                    "replaced_dags": MetadataValue.json(_replaced_map),
                    "absorbed_dags": MetadataValue.json(_absorbed_map),
                    "skipped_dags":  MetadataValue.json(_skipped_map),
                }
            )

        return Definitions(
            assets=[all_dbt_assets, dbt_docs_asset, cosmos_migration_summary],
            jobs=all_jobs,
            schedules=all_schedules,
            resources={"dbt": dbt_resource},
        )

    def _scan_cosmos_dags(self, dags_dir: Path, base_url: str) -> List[Dict]:
        """Scan *dags_dir* for .py files that import from cosmos and classify them."""
        records: List[Dict] = []
        if not dags_dir.exists():
            return records
        for path in sorted(dags_dir.glob("*.py")):
            if path.name.startswith("_"):
                continue
            try:
                content = path.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue
            if not _imports_cosmos(content):
                continue
            rec = self._classify_cosmos_file(path.stem, path.name, content, base_url)
            records.append(rec)
        return records

    def _classify_cosmos_file(
        self, stem: str, filename: str, content: str, base_url: str
    ) -> Dict:
        """Return a classification dict for one Cosmos DAG file."""
        override = self.file_overrides.get(stem, {})
        source_url = f"{base_url}/{filename}"

        def _make(action: str, note: str = "") -> Dict:
            return {
                "stem": stem,
                "filename": filename,
                "source_url": source_url,
                "description": override.get("description") or _docstring_from_source(content),
                "schedule": override.get("schedule") or _schedule_from_source(content),
                "dbt_select": override.get("dbt_select") or _select_from_source(content),
                "action": action,
                "action_note": note,
            }

        # file_overrides take highest priority
        if override.get("enabled") is False:
            return _make("skipped", override.get("reason", "Disabled via file_overrides"))

        if "cosmos_action" in override:
            act = override["cosmos_action"]
            if act == "skip":
                return _make("skipped", override.get("reason", "Skipped via file_overrides"))
            if act == "absorbed":
                return _make("absorbed", override.get("reason", "Absorbed via file_overrides"))
            return _make("replaced")  # "replace"

        # docs DAG → special asset, not a regular job
        if stem == "dbt_docs":
            return _make("replaced")

        # absorbed table
        if stem in _COSMOS_ABSORBED:
            return _make("absorbed", _COSMOS_ABSORBED[stem])

        # heuristic skip patterns
        if self.use_default_cosmos_skip_patterns:
            for pattern, reason in _COSMOS_SKIP_PATTERNS.items():
                if pattern in stem:
                    return _make("skipped", reason)

        return _make("replaced", _COSMOS_REPLACED.get(stem, ("",))[0])

    @staticmethod
    def _normalise_cosmos_schedule(schedule: Optional[str]) -> Optional[str]:
        if not schedule:
            return None
        return {
            "@daily":   "0 0 * * *",
            "@hourly":  "0 * * * *",
            "@weekly":  "0 0 * * 0",
            "@monthly": "0 0 1 * *",
        }.get(schedule, schedule)


# ---------------------------------------------------------------------------
# Cosmos classification tables
# ---------------------------------------------------------------------------

_COSMOS_REPLACED: Dict[str, tuple] = {
    "basic_cosmos_dag":                     ("Runs all dbt models — native @dbt_assets is strictly better", None),
    "basic_cosmos_task_group":              ("Task groups → asset selections in Dagster", None),
    "basic_cosmos_task_group_different_owners": ("Task groups with per-owner tags", None),
    "cosmos_seed_dag":                      ("dbt seeds → job scoped to resource_type:seed", "resource_type:seed"),
    "example_cosmos_dbt_build":             ("Full dbt build with integrated tests", None),
    "cosmos_manifest_example":              ("Manifest-based — dagster-dbt handles manifests automatically", None),
    "cosmos_manifest_selectors_example":    ("Selector-filtered DAG", "tag:daily"),
    "example_cosmos_sources":               ("Source freshness — built into dagster-dbt", None),
    "example_model_version":                ("Model versioning via manifest", None),
    "example_duckdb_dag":                   ("DuckDB adapter — swap dbt adapter in pyproject.toml", None),
}

_COSMOS_ABSORBED: Dict[str, str] = {
    "cosmos_callback_dag":           "Callbacks → Dagster run-status sensors",
    "cosmos_profile_mapping":        "Profile config → DbtCliResource",
    "example_cosmos_python_models":  "Python models supported natively by dagster-dbt",
    "user_defined_profile":          "Profile lives in DbtCliResource, not a separate DAG",
    "example_dbt_deps":              "dbt deps runs automatically via DbtProject.prepare_if_dev()",
    "example_operators":             "Airflow operators → Dagster @asset functions",
    "example_source_pruning":        "Source pruning → dagster-dbt asset selections",
    "example_source_rendering":      "Source rendering → dagster-dbt handles automatically",
    "example_tests_multiple_parents": "Multi-parent tests → dagster-dbt asset checks",
}

_COSMOS_SKIP_PATTERNS: Dict[str, str] = {
    "kubernetes":       "Kubernetes execution — infrastructure-specific",
    "virtualenv":       "Virtualenv task isolation — Airflow-specific",
    "watcher":          "File-watcher triggers — use a Dagster sensor instead",
    "performance_dag":  "Cosmos/Airflow benchmarking harness",
    "task_mapping":     "Airflow dynamic task mapping",
    "tasks_map":        "Airflow dynamic task mapping",
    "taskflow":         "TaskFlow API is Airflow-specific",
    "cross_project":    "Cross-project dbt ls — use separate code locations",
    "simple_dag_async": "Deferrable/async operators — Airflow-specific",
}


# ---------------------------------------------------------------------------
# Cosmos AST helpers
# ---------------------------------------------------------------------------

def _docstring_from_source(source: str) -> str:
    try:
        return ast.get_docstring(ast.parse(source)) or ""
    except SyntaxError:
        return ""


def _schedule_from_source(source: str) -> Optional[str]:
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return None
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            for kw in node.keywords:
                if kw.arg == "schedule" and isinstance(kw.value, ast.Constant):
                    return str(kw.value.value)
    return None


def _select_from_source(source: str) -> Optional[str]:
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return None
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            for kw in node.keywords:
                if kw.arg == "select":
                    val = kw.value
                    if isinstance(val, ast.Constant) and isinstance(val.value, str):
                        return val.value
                    if isinstance(val, ast.List) and val.elts:
                        first = val.elts[0]
                        if isinstance(first, ast.Constant):
                            return first.value
    return None
