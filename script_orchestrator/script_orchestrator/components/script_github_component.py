"""State-backed component for Python script orchestration.

Features:
- Automatic schedule creation from YAML
- Partition support for time-based scripts
- Rich metadata emission
- Prefect flow mapping to Dagster ops
- Config extraction from argparse, sys.argv, and Prefect flows
- Discovers Python scripts from GitHub repositories
"""

import ast
import importlib.util
import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)
from dagster import (
    Array,
    AssetExecutionContext,
    AssetIn,
    AssetSelection,
    Backoff,
    Config,
    ConfigMapping,
    DailyPartitionsDefinition,
    DefaultScheduleStatus,
    Definitions,
    DynamicOut,
    DynamicOutput,
    Field,
    HourlyPartitionsDefinition,
    In,
    Jitter,
    MetadataValue,
    MonthlyPartitionsDefinition,
    Noneable,
    OpExecutionContext,
    Output,
    RetryPolicy,
    ScheduleDefinition,
    Shape,
    String,
    WeeklyPartitionsDefinition,
    asset,
    graph_asset,
    op,
)
from dagster.components import Component, ComponentLoadContext
from pydantic import BaseModel
from pydantic import Field as PydanticField

# Make git optional for environments where it's not available
try:
    from git import Repo
    GIT_AVAILABLE = True
except (ImportError, Exception):
    GIT_AVAILABLE = False
    Repo = None  # type: ignore

from ..schemas.script_metadata import ScriptMetadata


class ScriptGithubComponentParams(BaseModel):
    """Parameters for the ScriptGithubComponent."""

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

    @classmethod
    def from_env(cls, **overrides):
        """Load parameters from environment variables with optional overrides."""
        params = {}

        repo_url = os.getenv("SCRIPTS_REPO_URL")
        if repo_url:
            params["repo_url"] = repo_url

        repo_branch = os.getenv("SCRIPTS_REPO_BRANCH")
        if repo_branch:
            params["repo_branch"] = repo_branch

        github_token = os.getenv("GITHUB_TOKEN")
        if github_token:
            params["github_token"] = github_token

        scripts_dir = os.getenv("SCRIPTS_DIR")
        if scripts_dir:
            params["scripts_directory"] = scripts_dir

        use_local_str = os.getenv("USE_LOCAL_SCRIPTS")
        if use_local_str:
            params["use_local"] = use_local_str.lower() == "true"

        # Apply overrides
        for key, value in overrides.items():
            if value is not None:
                params[key] = value

        return cls(**params)


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


class ScriptGithubComponent(Component):
    """Component for orchestrating Python scripts with Prefect flow mapping."""

    @classmethod
    def get_schema(cls):
        """Return the params schema for this component."""
        return ScriptGithubComponentParams

    def __init__(self, **params):
        super().__init__()
        if not params or (not params.get("repo_url") and not params.get("use_local")):
            self.params = ScriptGithubComponentParams.from_env(**params)
        else:
            self.params = ScriptGithubComponentParams(**params)

    @staticmethod
    def get_component_key_for_params(params: Dict[str, Any]) -> str:
        """Generate a unique key for this component instance."""
        repo_url = params.get("repo_url", "")
        safe_url = repo_url.replace("https://", "").replace("http://", "").replace("/", "_")
        return f"ScriptGithubComponent[{safe_url}]"

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Required abstract method - delegates to build_defs_from_state."""
        state_path = context.path
        return self.build_defs_from_state(context, state_path)

    def write_state_to_path(self, context: ComponentLoadContext, path: Path) -> None:
        """Clone/pull the GitHub repo and discover script directories."""
        state = ScriptsState()

        try:
            if self.params.use_local:
                # Use local scripts directory
                scripts_dir = Path(self.params.scripts_directory)
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

                if not self.params.repo_url:
                    raise ValueError(
                        "SCRIPTS_REPO_URL environment variable is required when not using local scripts."
                    )

                clone_dir = path / "repo_clone"
                clone_dir.mkdir(parents=True, exist_ok=True)

                repo = self._clone_or_pull_repo(clone_dir, self.params.github_token)
                state.repo_commit = repo.head.commit.hexsha
                state.repo_path = str(clone_dir)

                scripts_dir = clone_dir / self.params.scripts_directory
                if scripts_dir.exists():
                    state.scripts = self._discover_scripts(scripts_dir)
                    logger.info(f"Discovered {len(state.scripts)} scripts from GitHub")

            # Install dependencies from the scripts directory if found
            if state.scripts and state.repo_path:
                self._install_script_dependencies(Path(state.repo_path) / self.params.scripts_directory)

        except Exception as e:
            state.error = str(e)
            logger.error(f"Error discovering scripts: {e}")

        # Write state to disk
        state_file = path / "scripts_state.json"
        state_file.write_text(state.model_dump_json(indent=2))

    def build_defs_from_state(self, context: ComponentLoadContext, state_path: Path) -> Definitions:
        """Build Dagster definitions with schedules, partitions, and rich metadata."""
        state_file = state_path / "scripts_state.json"
        if not state_file.exists():
            logger.warning("No scripts state found. Run refresh to discover scripts.")
            return Definitions()

        state = ScriptsState.model_validate_json(state_file.read_text())

        if state.error:
            logger.error(f"Error in scripts state: {state.error}")
            return Definitions()

        all_assets = []
        all_schedules = []

        # Build script assets
        for script_info in state.scripts:
            if script_info.metadata and not script_info.metadata.enabled:
                continue

            asset_def = self._build_script_asset_with_prefect_check(script_info, state.scripts, state.repo_path)
            all_assets.append(asset_def)

            # Create schedule if configured
            if script_info.metadata and script_info.metadata.schedule:
                schedule = self._build_schedule(
                    f"script_{script_info.name}",
                    script_info.metadata.schedule,
                    f"script_{script_info.name}",
                )
                all_schedules.append(schedule)

        logger.info(
            f"Created {len(all_assets)} assets and {len(all_schedules)} schedules"
        )

        return Definitions(assets=all_assets, schedules=all_schedules)

    def _clone_or_pull_repo(self, clone_dir: Path, github_token: Optional[str]) -> Repo:
        """Clone or pull the GitHub repository."""
        repo_url = self.params.repo_url

        if github_token:
            if "github.com" in repo_url:
                repo_url = repo_url.replace("https://", f"https://{github_token}@")

        if (clone_dir / ".git").exists():
            repo = Repo(clone_dir)
            repo.remotes.origin.pull(self.params.repo_branch)
        else:
            repo = Repo.clone_from(repo_url, clone_dir, branch=self.params.repo_branch)

        return repo

    def _discover_scripts(self, scripts_dir: Path) -> List[ScriptInfo]:
        """Discover all Python scripts with optional YAML configuration."""
        scripts = []

        for script_file in scripts_dir.rglob("*.py"):
            # Skip __init__.py and hidden files
            if script_file.name.startswith("_") or script_file.name.startswith("."):
                continue

            # Look for corresponding YAML file
            yaml_file = script_file.with_suffix(".yaml")
            metadata = None
            if yaml_file.exists():
                try:
                    metadata_dict = yaml.safe_load(yaml_file.read_text())
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

    def _has_decorator(self, func_node: ast.FunctionDef, decorator_name: str) -> bool:
        """Check if function has a specific decorator."""
        for decorator in func_node.decorator_list:
            if isinstance(decorator, ast.Name) and decorator.id == decorator_name:
                return True
            elif isinstance(decorator, ast.Call):
                if isinstance(decorator.func, ast.Name) and decorator.func.id == decorator_name:
                    return True
        return False

    def _has_return_statement(self, func_node: ast.FunctionDef) -> bool:
        """Check if function has a return statement."""
        for node in ast.walk(func_node):
            if isinstance(node, ast.Return) and node.value is not None:
                return True
        return False

    def _extract_task_retry_config(self, task_node: ast.FunctionDef) -> dict:
        """Extract retry configuration from @task decorator."""
        for decorator in task_node.decorator_list:
            if isinstance(decorator, ast.Call):
                if isinstance(decorator.func, ast.Name) and decorator.func.id == 'task':
                    retry_config = {}
                    for keyword in decorator.keywords:
                        if keyword.arg == 'retries':
                            if isinstance(keyword.value, ast.Constant):
                                retry_config['retries'] = keyword.value.value
                        elif keyword.arg == 'retry_delay_seconds':
                            if isinstance(keyword.value, ast.Constant):
                                retry_config['retry_delay_seconds'] = keyword.value.value
                    return retry_config
        return {}

    def _extract_flow_parameters(self, flow_node: ast.FunctionDef) -> List[Dict]:
        """Extract parameters from a flow function."""
        parameters = []

        for arg in flow_node.args.args:
            if arg.arg == 'self':
                continue

            param_info = {
                'name': arg.arg,
                'type_annotation': None,
                'default': None
            }

            # Extract type annotation
            if arg.annotation:
                param_info['type_annotation'] = ast.unparse(arg.annotation)

            parameters.append(param_info)

        # Extract default values
        defaults = flow_node.args.defaults
        num_defaults = len(defaults)
        num_args = len(parameters)

        for i, default in enumerate(defaults):
            param_index = num_args - num_defaults + i
            if param_index >= 0 and param_index < len(parameters):
                try:
                    parameters[param_index]['default'] = ast.literal_eval(default)
                except (ValueError, SyntaxError):
                    parameters[param_index]['default'] = None

        return parameters

    def _extract_task_calls(self, flow_node: ast.FunctionDef, known_tasks: List[Dict] = None) -> tuple:
        """Extract task calls from flow function body."""
        # Pre-populate known task names
        known_task_names = set()
        if known_tasks:
            known_task_names = {task['name'] for task in known_tasks}

        seen_task_names = known_task_names.copy()
        task_calls = []
        has_complex_patterns = False

        for node in ast.walk(flow_node):
            if isinstance(node, ast.Call):
                # Direct task call
                if isinstance(node.func, ast.Name):
                    task_name = node.func.id
                    if task_name in seen_task_names:
                        task_calls.append({
                            'task_name': task_name,
                            'is_map_call': False
                        })
                # Method call (e.g., task.map())
                elif isinstance(node.func, ast.Attribute):
                    if node.func.attr == 'map':
                        if isinstance(node.func.value, ast.Name):
                            task_name = node.func.value.id
                            if task_name in seen_task_names:
                                task_calls.append({
                                    'task_name': task_name,
                                    'is_map_call': True
                                })
                                has_complex_patterns = True

        return task_calls, has_complex_patterns

    def _parse_prefect_flow(self, script_path: Path):
        """Parse Prefect file to extract tasks and flow structure using AST."""
        try:
            with open(script_path, 'r') as f:
                tree = ast.parse(f.read(), filename=str(script_path))

            tasks = []
            flows = []

            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    if self._has_decorator(node, 'task'):
                        retry_config = self._extract_task_retry_config(node)
                        task_info = {
                            'name': node.name,
                            'params': [arg.arg for arg in node.args.args],
                            'returns_value': self._has_return_statement(node),
                            'retry_config': retry_config
                        }
                        tasks.append(task_info)

                    elif self._has_decorator(node, 'flow'):
                        task_calls, has_complex_patterns = self._extract_task_calls(node, tasks)
                        flow_params = self._extract_flow_parameters(node)

                        flow_info = {
                            'name': node.name,
                            'task_calls': task_calls,
                            'has_complex_patterns': has_complex_patterns,
                            'parameters': flow_params
                        }
                        flows.append(flow_info)

            return tasks, flows

        except Exception as e:
            logger.warning(f"Failed to parse Prefect flow {script_path}: {e}")
            return [], []

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
        """Parse sys.argv usage patterns from a Python script."""
        try:
            with open(script_path, 'r') as f:
                tree = ast.parse(f.read(), filename=str(script_path))

            argv_indices = set()

            for node in ast.walk(tree):
                if isinstance(node, ast.Subscript):
                    if (isinstance(node.value, ast.Attribute) and
                        isinstance(node.value.value, ast.Name) and
                        node.value.value.id == 'sys' and
                        node.value.attr == 'argv'):

                        if isinstance(node.slice, ast.Constant):
                            index = node.slice.value
                            if isinstance(index, int) and index > 0:
                                argv_indices.add(index)

            parameters = []
            for index in sorted(argv_indices):
                param_info = {
                    'name': f'arg{index}',
                    'type_annotation': 'str',
                    'default': None,
                    'help': f'Command line argument {index}',
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
        # Determine start date
        if partition_config.start_date:
            start_date = partition_config.start_date
        else:
            default_start = datetime.now() - timedelta(days=30)
            start_date = default_start.strftime("%Y-%m-%d")

        timezone = partition_config.timezone
        schedule = partition_config.schedule.lower()

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


    # ===== Prefect Graph Asset Creation Methods =====

    def _create_prefect_flow_graph_asset(
        self, flow_info: Dict, tasks_info: List[Dict], script_info: ScriptInfo,
        metadata: ScriptMetadata, repo_path: str
    ):
        """Create a graph-backed asset for a Prefect flow."""
        import importlib.util

        flow_name = flow_info['name']
        task_calls = flow_info['task_calls']
        has_complex_patterns = flow_info['has_complex_patterns']
        flow_params = flow_info.get('parameters', [])

        # If flow has complex patterns, return None to fall back to subprocess
        if has_complex_patterns:
            logger.info(f"Flow {flow_name} has complex patterns (.map()), falling back to subprocess")
            return None

        # If flow has parameters, fall back to subprocess for Launchpad config support
        if flow_params:
            logger.info(f"Flow {flow_name} has {len(flow_params)} parameters, falling back to subprocess for config support")
            return None

        # Check if this is a simple sequential flow (each task called once, in order)
        task_call_counts = {}
        for task_call in task_calls:
            task_name = task_call['task_name']
            task_call_counts[task_name] = task_call_counts.get(task_name, 0) + 1

        # If any task is called more than once, fall back to subprocess
        if any(count > 1 for count in task_call_counts.values()):
            logger.info(f"Flow {flow_name} has tasks called multiple times, falling back to subprocess")
            return None

        # Try to import the script module to get actual task functions
        try:
            spec = importlib.util.spec_from_file_location("prefect_module", str(script_info.script_path))
            if spec is None or spec.loader is None:
                logger.warning(f"Could not load spec for {script_info.script_path}")
                return None

            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            # Get task functions from the module
            task_functions = {}
            for task_info in tasks_info:
                task_name = task_info['name']
                if hasattr(module, task_name):
                    task_functions[task_name] = getattr(module, task_name)
                else:
                    logger.warning(f"Task function {task_name} not found in module")
                    return None

        except Exception as e:
            logger.warning(f"Could not import Prefect module {script_info.script_path}: {e}")
            return None

        # Create ops for each task that actually call the Prefect task functions
        ops_dict = {}
        for task_info in tasks_info:
            task_name = task_info['name']
            task_func = task_functions[task_name]
            task_params = task_info.get('parameters', [])
            retry_config = task_info.get('retry_config', {})

            # Create retry policy for op
            retry_policy = None
            if retry_config.get('retries', 0) > 0:
                retry_policy = RetryPolicy(
                    max_retries=retry_config['retries'],
                    delay=retry_config.get('retry_delay_seconds', 0),
                )

            # Capture the task function in closure
            def make_task_op(tf, tn, params):
                @op(
                    name=f"{script_info.name}_{tn}",
                    retry_policy=retry_policy,
                )
                def task_op(context: OpExecutionContext, input_data=None):
                    """Execute Prefect task function."""
                    context.log.info(f"Executing Prefect task: {tn}")

                    # Call the task function with appropriate arguments
                    try:
                        if input_data is not None:
                            # Pass input data as positional argument
                            result = tf(input_data)
                        elif len(params) > 0 and params[0].get('default') is not None:
                            # Use default value for first parameter
                            result = tf(params[0]['default'])
                        else:
                            # No parameters or no input - call with no args
                            result = tf()
                    except TypeError as e:
                        # If parameter binding fails, try without arguments
                        context.log.warning(f"Parameter binding failed: {e}, trying without arguments")
                        result = tf()

                    context.log.info(f"Task {tn} completed with result type: {type(result).__name__}")
                    return result

                return task_op

            ops_dict[task_name] = make_task_op(task_func, task_name, task_params)

        # Build asset tags
        asset_tags = {
            **metadata.tags,
            "script_type": "prefect_mapped",
            "script_name": script_info.name,
            "prefect_flow": flow_name
        }
        for kind in metadata.kinds:
            asset_tags[f"dagster/kind/{kind}"] = ""

        # Create ops list in the order they should be called
        ops_list = [ops_dict[tc['task_name']] for tc in task_calls if tc['task_name'] in ops_dict]

        if not ops_list:
            logger.warning(f"No ops created for flow {flow_name}")
            return None

        # Create graph asset that explicitly calls ops in sequence
        # We need to build this dynamically so Dagster can statically analyze it
        @graph_asset(
            name=f"script_{script_info.name}",
            group_name=metadata.group_name,
            tags=asset_tags,
            description=metadata.description or f"Prefect flow: {flow_name}",
        )
        def flow_graph():
            """Execute Prefect flow as graph of ops."""
            # Build the op call chain based on number of ops
            # This explicit structure allows Dagster to see the ops
            if len(ops_list) == 1:
                return ops_list[0]()
            elif len(ops_list) == 2:
                result = ops_list[0]()
                return ops_list[1](result)
            elif len(ops_list) == 3:
                result = ops_list[0]()
                result = ops_list[1](result)
                return ops_list[2](result)
            elif len(ops_list) == 4:
                result = ops_list[0]()
                result = ops_list[1](result)
                result = ops_list[2](result)
                return ops_list[3](result)
            elif len(ops_list) == 5:
                result = ops_list[0]()
                result = ops_list[1](result)
                result = ops_list[2](result)
                result = ops_list[3](result)
                return ops_list[4](result)
            else:
                # For flows with more than 5 tasks, fall back to subprocess
                logger.info(f"Flow {flow_name} has {len(ops_list)} ops, which is too many for explicit wiring")
                return None

        return flow_graph

    def _build_script_asset_with_prefect_check(
        self, script_info: ScriptInfo, all_scripts: List[ScriptInfo], repo_path: str
    ):
        """Build asset with Prefect graph mapping check."""
        metadata = script_info.metadata or ScriptMetadata()
        
        # Check if this is a Prefect flow with mapping enabled
        if metadata.script_type == "prefect" and metadata.prefect_mapping and metadata.prefect_mapping.enabled:
            try:
                tasks, flows = self._parse_prefect_flow(script_info.script_path)
                if flows:
                    flow_info = flows[0]
                    
                    # Try to create graph asset
                    graph_asset_def = self._create_prefect_flow_graph_asset(
                        flow_info, tasks, script_info, metadata, repo_path
                    )
                    
                    if graph_asset_def:
                        logger.info(f"Created graph asset for Prefect flow: {script_info.name}")
                        return graph_asset_def
                    else:
                        logger.info(f"Falling back to subprocess for: {script_info.name}")
            except Exception as e:
                logger.warning(f"Failed to create graph asset for {script_info.name}: {e}")
                logger.info(f"Falling back to subprocess execution")
        
        # Fall back to regular subprocess-based asset
        return self._build_script_asset(script_info, all_scripts, repo_path)


    # ===== Asset Building Methods =====

    def _build_script_asset(self, script_info: ScriptInfo, all_scripts: List[ScriptInfo], repo_path: str):
        """Build a Dagster asset for a Python script with config and partitions support."""
        metadata = script_info.metadata or ScriptMetadata()
        
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
        for kind in metadata.kinds:
            asset_tags[f"dagster/kind/{kind}"] = ""
        
        # Build dependencies
        deps = {}
        if metadata.depends_on:
            for dep_name in metadata.depends_on:
                dep_script = next((s for s in all_scripts if s.name == dep_name), None)
                if dep_script:
                    deps[dep_name] = AssetIn(key=f"script_{dep_name}")
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
            logger.info(f"Created {metadata.partition.schedule} partitions for {script_info.name}")
        
        # Define asset function based on features (config and/or partitions)
        if flow_config_class and partitions_def:
            def script_asset(context: AssetExecutionContext, config, **dep_values):
                """Execute script with config and partition."""
                logger.info(f"Running {script_type} script: {script_info.name}")
                context.log.info(f"Script config: {config}")
                partition_key = context.partition_key
                context.log.info(f"Partition: {partition_key}")
                
                try:
                    start_time = datetime.now()
                    python_cmd = ["uv", "run", "python", str(script_info.script_path)]
                    
                    config_source = getattr(flow_config_class, '_source', None)
                    if config_source in ('argparse', 'sys.argv'):
                        parameters = getattr(flow_config_class, '_parameters', [])
                        cli_args = self._build_cli_arguments(config, parameters, config_source)
                        python_cmd.extend(cli_args)
                        context.log.info(f"CLI arguments: {cli_args}")
                    
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
            def script_asset(context: AssetExecutionContext, config, **dep_values):
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
            def script_asset(context: AssetExecutionContext, **dep_values):
                """Execute script with partition."""
                logger.info(f"Running {script_type} script: {script_info.name}")
                partition_key = context.partition_key
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
            def script_asset(context: AssetExecutionContext, **dep_values):
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
        
        # Apply asset decorator
        asset_kwargs = {
            "name": f"script_{script_info.name}",
            "group_name": metadata.group_name,
            "tags": asset_tags,
            "description": metadata.description or f"Python script: {script_info.name}",
            "owners": metadata.owners or [],
            "retry_policy": retry_policy,
            "ins": deps if deps else None,
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
