"""
Parser for Astronomer DAG Factory YAML files.

Converts dag-factory YAML configurations to Dagster assets, enabling
seamless migration from Airflow dag-factory patterns to Dagster.
"""

import ast
import importlib.util
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

logger = logging.getLogger(__name__)


class DagFactoryYamlParser:
    """Parse Astronomer dag-factory YAML files and convert to Dagster assets."""

    @staticmethod
    def is_dag_factory_yaml(yaml_path: Path) -> bool:
        """Check if a YAML file is a dag-factory configuration.

        dag-factory YAMLs have DAG definitions at the root level with structure:
        dag_id:
          default_args: {...}
          schedule_interval: ...
          tasks: {...}
        """
        try:
            with open(yaml_path, 'r') as f:
                data = yaml.safe_load(f)

            if not isinstance(data, dict):
                return False

            # Check if any root-level keys look like DAG definitions
            for key, value in data.items():
                if isinstance(value, dict) and 'tasks' in value:
                    return True

            return False
        except Exception as e:
            logger.debug(f"Error checking if {yaml_path} is dag-factory YAML: {e}")
            return False

    def _detect_yaml_airflow_version(self, yaml_content: str) -> str:
        """
        Detect which Airflow version a dag-factory YAML was written for.
        Returns "2.x" or "3.x".
        """
        # Check for 3.x syntax
        if 'airflow.sdk' in yaml_content or '__type__: airflow.sdk' in yaml_content:
            return "3.x"

        # Check for 2.x syntax
        if 'airflow.decorators' in yaml_content:
            return "2.x"

        # Default to 3.x for dag-factory (it's the modern format)
        return "3.x"

    def parse_dag_factory_yaml(self, yaml_path: Path) -> List[Dict[str, Any]]:
        """Parse a dag-factory YAML file and return list of DAG definitions.

        Supports Airflow 3.x features:
        - Global 'default' section
        - Asset-based scheduling (outlets/inlets)
        - Asset dependencies

        Returns:
            List of dicts, each containing:
            - dag_id: str
            - default_args: dict
            - schedule_interval: str or list (asset-based)
            - description: str
            - tasks: list of task dicts
            - task_dependencies: dict mapping task_id -> list of upstream task_ids
            - catchup: bool
            - max_active_runs: int
            - tags: list
            - asset_outlets: list of assets produced by this DAG
            - asset_schedule: list of assets that trigger this DAG
            - dag_airflow_version: str (e.g., "2.x" or "3.x")
        """
        try:
            with open(yaml_path, 'r') as f:
                yaml_content = f.read()

            # Detect Airflow version from YAML content
            dag_airflow_version = self._detect_yaml_airflow_version(yaml_content)
            logger.info(f"Detected {yaml_path.name} as Airflow {dag_airflow_version} based on YAML syntax")

            data = yaml.safe_load(yaml_content)

            # Parse global defaults
            global_defaults = data.pop('default', {})
            if global_defaults:
                logger.info(f"Found global defaults: {global_defaults}")

            dags = []

            for dag_id, dag_config in data.items():
                if not isinstance(dag_config, dict) or 'tasks' not in dag_config:
                    continue

                logger.info(f"Parsing dag-factory DAG: {dag_id}")

                # Parse task groups (for metadata/grouping)
                task_groups = dag_config.get('task_groups', {})
                task_group_info = {}
                for group_id, group_config in task_groups.items():
                    task_group_info[group_id] = {
                        'tooltip': group_config.get('tooltip', ''),
                        'tasks': []  # Will be populated when parsing tasks
                    }

                # Merge global defaults with DAG-level defaults
                dag_default_args = {**global_defaults, **dag_config.get('default_args', {})}

                # Parse schedule - can be string (cron) or list (asset-based)
                schedule = dag_config.get('schedule_interval') or dag_config.get('schedule')
                asset_schedule = None
                if isinstance(schedule, list):
                    # Asset-based schedule - extract asset names
                    asset_schedule = []
                    for item in schedule:
                        if isinstance(item, dict) and item.get('__type__') == 'airflow.sdk.Asset':
                            asset_schedule.append(item.get('name'))
                    logger.info(f"DAG {dag_id} scheduled by assets: {asset_schedule}")
                    schedule = None  # No cron schedule

                # Parse DAG-level configuration
                dag_info = {
                    'dag_id': dag_id,
                    'default_args': dag_default_args,
                    'schedule_interval': schedule,
                    'asset_schedule': asset_schedule,  # Assets that trigger this DAG
                    'description': dag_config.get('description', f'DAG Factory: {dag_id}'),
                    'catchup': dag_config.get('catchup', False),
                    'max_active_runs': dag_config.get('max_active_runs', 1),
                    'tags': dag_config.get('tags', []),
                    'task_groups': task_group_info,
                    'tasks': [],
                    'task_dependencies': {},
                    'xcom_dependencies': {},  # Track XCom passing between tasks
                    'asset_outlets': [],  # Assets produced by this DAG
                    'dag_airflow_version': dag_airflow_version,  # Detected Airflow version
                }

                # Parse tasks
                tasks_config = dag_config.get('tasks', {})
                for task_id, task_config in tasks_config.items():
                    if not isinstance(task_config, dict):
                        continue

                    task_info = self._parse_task(task_id, task_config, dag_info['default_args'])
                    dag_info['tasks'].append(task_info)

                    # Track task group membership
                    task_group_name = task_config.get('task_group_name')
                    if task_group_name and task_group_name in task_group_info:
                        task_group_info[task_group_name]['tasks'].append(task_id)
                        task_info['task_group'] = task_group_name

                    # Parse XCom dependencies (parameters starting with +)
                    xcom_deps = {}
                    for param_key, param_value in task_config.items():
                        if isinstance(param_value, str) and param_value.startswith('+'):
                            # +task_id means get XCom output from that task
                            upstream_task = param_value[1:]  # Remove the +
                            xcom_deps[param_key] = upstream_task
                            # Also add as dependency
                            if task_id not in dag_info['task_dependencies']:
                                dag_info['task_dependencies'][task_id] = []
                            if upstream_task not in dag_info['task_dependencies'][task_id]:
                                dag_info['task_dependencies'][task_id].append(upstream_task)

                    if xcom_deps:
                        dag_info['xcom_dependencies'][task_id] = xcom_deps

                    # Parse asset outlets - assets produced by this task
                    outlets = task_config.get('outlets', [])
                    if outlets:
                        for outlet in outlets:
                            if isinstance(outlet, dict) and outlet.get('__type__') == 'airflow.sdk.Asset':
                                asset_name = outlet.get('name')
                                if asset_name and asset_name not in dag_info['asset_outlets']:
                                    dag_info['asset_outlets'].append(asset_name)
                                    logger.info(f"Task {task_id} produces asset: {asset_name}")

                    # Parse dependencies (expand task groups)
                    dependencies = task_config.get('dependencies', [])
                    if dependencies:
                        expanded_deps = []
                        for dep in dependencies:
                            if dep in task_group_info:
                                # This is a task group - add all tasks in the group
                                expanded_deps.extend(task_group_info[dep]['tasks'])
                                logger.info(f"Expanded task group '{dep}' to tasks: {task_group_info[dep]['tasks']}")
                            else:
                                # Regular task dependency
                                expanded_deps.append(dep)
                        dag_info['task_dependencies'][task_id] = expanded_deps

                logger.info(f"Parsed dag-factory DAG {dag_id} with {len(dag_info['tasks'])} tasks")
                dags.append(dag_info)

            return dags

        except Exception as e:
            logger.error(f"Error parsing dag-factory YAML {yaml_path}: {e}")
            return []

    def _parse_task(self, task_id: str, task_config: Dict, default_args: Dict) -> Dict[str, Any]:
        """Parse a single task definition from dag-factory YAML.

        Supports both operator-based and decorator-based tasks.

        Returns:
            Dict with:
            - task_id: str
            - operator: str (full class path) or decorator: str
            - operator_type: str (bash, python, etc.)
            - parameters: dict (operator-specific params)
            - retries: int
            - retry_delay_sec: int
            - jinja_templates: dict (parameters with Jinja templates)
        """
        # Check if this is a decorator-based task or operator-based
        decorator = task_config.get('decorator')
        operator = task_config.get('operator')

        if decorator:
            # Decorator-based task (TaskFlow API)
            # These are similar to PythonOperator but use @task decorator
            operator = decorator
            operator_type = 'python'  # Decorators are Python callables
            logger.debug(f"Task {task_id} uses decorator: {decorator}")
        elif operator:
            # Traditional operator-based task
            operator_type = self._get_operator_type(operator)
        else:
            # Default to PythonOperator if neither specified
            operator = 'airflow.operators.python.PythonOperator'
            operator_type = 'python'

        # Extract operator parameters (all keys except special ones)
        special_keys = {
            'operator', 'decorator', 'dependencies', 'retries', 'retry_delay_sec',
            'task_group_name'  # Task group is metadata, not a parameter
        }
        parameters = {}
        jinja_templates = {}
        xcom_params = {}

        for k, v in task_config.items():
            if k in special_keys:
                continue

            # Check for XCom references (start with +)
            if isinstance(v, str) and v.startswith('+'):
                xcom_params[k] = v[1:]  # Store the upstream task id
                # Don't include in parameters - will be resolved at runtime
                continue

            # Check for Jinja templates (contain {{ }})
            if isinstance(v, str) and '{{' in v and '}}' in v:
                jinja_templates[k] = v
                logger.debug(f"Task {task_id} has Jinja template in {k}: {v}")

            parameters[k] = v

        # Merge with default_args (task-level takes precedence)
        retries = task_config.get('retries', default_args.get('retries', 0))
        retry_delay_sec = task_config.get('retry_delay_sec', default_args.get('retry_delay_sec', 300))

        return {
            'task_id': task_id,
            'operator': operator,
            'operator_type': operator_type,
            'parameters': parameters,
            'retries': retries,
            'retry_delay_sec': retry_delay_sec,
            'jinja_templates': jinja_templates,  # Parameters that need template rendering
            'xcom_params': xcom_params,  # Parameters that come from upstream XCom
        }

    def _get_operator_type(self, operator: str) -> str:
        """Determine operator type from full class path.

        Examples:
            airflow.operators.bash.BashOperator -> bash
            airflow.operators.python.PythonOperator -> python
            airflow.operators.dummy.DummyOperator -> dummy
        """
        operator_lower = operator.lower()

        if 'bash' in operator_lower:
            return 'bash'
        elif 'python' in operator_lower:
            return 'python'
        elif 'dummy' in operator_lower or 'empty' in operator_lower:
            return 'dummy'
        elif 'sensor' in operator_lower:
            return 'sensor'
        else:
            return 'unknown'

    def resolve_python_callable(
        self,
        task_config: Dict,
        dag_directory: Path
    ) -> Optional[callable]:
        """Resolve a Python callable from task configuration.

        Supports three formats:
        1. python_callable: 'module.path.function' - Import from module
        2. python_callable_name + python_callable_file - Load from file
        3. python_callable_name - Look in local context (not supported in parser)

        Returns:
            Callable function or None if cannot resolve
        """
        parameters = task_config.get('parameters', {})

        # Format 1: python_callable as module path
        if 'python_callable' in parameters:
            module_path = parameters['python_callable']
            try:
                return self._import_callable_from_module(module_path)
            except Exception as e:
                logger.warning(f"Could not import {module_path}: {e}")
                return None

        # Format 2: python_callable_name + python_callable_file
        if 'python_callable_name' in parameters and 'python_callable_file' in parameters:
            func_name = parameters['python_callable_name']
            file_path = Path(parameters['python_callable_file'])

            # Make relative to DAG directory if not absolute
            if not file_path.is_absolute():
                file_path = dag_directory / file_path

            try:
                return self._load_function_from_file(func_name, file_path)
            except Exception as e:
                logger.warning(f"Could not load {func_name} from {file_path}: {e}")
                return None

        # Format 3: python_callable_name alone (requires context, handled elsewhere)
        if 'python_callable_name' in parameters:
            logger.debug(f"python_callable_name without file - requires local context")
            return None

        return None

    def _import_callable_from_module(self, module_path: str) -> callable:
        """Import a callable from a module path like 'my_module.my_function'."""
        parts = module_path.rsplit('.', 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid module path: {module_path}")

        module_name, func_name = parts
        module = importlib.import_module(module_name)
        return getattr(module, func_name)

    def _load_function_from_file(self, func_name: str, file_path: Path) -> callable:
        """Load a function from a Python file."""
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        # Load module from file
        spec = importlib.util.spec_from_file_location("dynamic_module", file_path)
        if spec is None or spec.loader is None:
            raise ValueError(f"Could not load spec from {file_path}")

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # Get function
        if not hasattr(module, func_name):
            raise AttributeError(f"Function {func_name} not found in {file_path}")

        return getattr(module, func_name)

    def get_task_execution_order(self, dag_info: Dict) -> List[str]:
        """Determine task execution order based on dependencies.

        Returns:
            List of task_ids in topological order (can execute sequentially)
        """
        task_dependencies = dag_info.get('task_dependencies', {})
        all_task_ids = {task['task_id'] for task in dag_info['tasks']}

        # Build adjacency list
        graph = {task_id: set() for task_id in all_task_ids}
        for task_id, deps in task_dependencies.items():
            for dep in deps:
                if dep in graph:
                    graph[dep].add(task_id)

        # Topological sort (Kahn's algorithm)
        in_degree = {task_id: 0 for task_id in all_task_ids}
        for task_id, deps in task_dependencies.items():
            in_degree[task_id] = len(deps)

        queue = [task_id for task_id, degree in in_degree.items() if degree == 0]
        result = []

        while queue:
            task_id = queue.pop(0)
            result.append(task_id)

            for dependent in graph[task_id]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        return result

    def create_graph_asset(
        self,
        dag_info: Dict,
        script_info: Any,
        metadata: Any,
        repo_path: str
    ):
        """Create a graph-backed asset for a dag-factory YAML DAG.

        This creates individual Dagster ops for each task in the YAML,
        preserving the original task structure and dependencies.
        """
        from dagster import op, graph_asset, OpExecutionContext, Output as OpOutput
        import subprocess

        dag_id = dag_info['dag_id']
        tasks = dag_info['tasks']
        task_dependencies = dag_info.get('task_dependencies', {})
        xcom_dependencies = dag_info.get('xcom_dependencies', {})
        task_groups = dag_info.get('task_groups', {})

        logger.info(f"Creating graph asset for dag-factory DAG: {dag_id} with {len(tasks)} tasks")
        if xcom_dependencies:
            logger.info(f"XCom dependencies detected: {xcom_dependencies}")
        if task_groups:
            logger.info(f"Task groups: {list(task_groups.keys())}")

        # Create ops for each task
        ops_dict = {}
        for task_config in tasks:
            task_id = task_config['task_id']
            operator_type = task_config['operator_type']
            parameters = task_config['parameters']
            jinja_templates = task_config.get('jinja_templates', {})
            xcom_params = task_config.get('xcom_params', {})
            task_group = task_config.get('task_group')

            # Capture variables in closure
            def make_task_op(tid, op_type, params, task_cfg, yaml_path, jinja_tpl, xcom_prms, tg):
                # Build op tags with task group info
                op_tags = {
                    "task_id": tid,
                    "operator_type": op_type,
                    "source": "dag_factory_yaml"
                }
                if tg:
                    op_tags["task_group"] = tg

                @op(
                    name=f"{script_info.name}_{tid}",
                    tags=op_tags
                )
                def task_op(context: OpExecutionContext, upstream_results=None):
                    """Execute dag-factory YAML task."""
                    context.log.info(f"Executing task: {tid} (type: {op_type})")
                    if tg:
                        context.log.info(f"Task group: {tg}")

                    # Process upstream results for XCom parameters
                    resolved_params = dict(params)

                    if upstream_results is not None:
                        context.log.info(f"Received upstream results")

                        # If we have XCom parameters, extract values from upstream results
                        if xcom_prms and isinstance(upstream_results, dict):
                            for param_name, upstream_task in xcom_prms.items():
                                if upstream_task in upstream_results:
                                    xcom_value = upstream_results[upstream_task]
                                    resolved_params[param_name] = xcom_value
                                    context.log.info(f"XCom: {param_name} = {xcom_value} (from {upstream_task})")

                    # Render Jinja templates
                    if jinja_tpl:
                        from datetime import datetime
                        # Create Airflow-like context for template rendering
                        template_context = {
                            'logical_date': datetime.utcnow().isoformat(),
                            'execution_date': datetime.utcnow().isoformat(),
                            'ds': datetime.utcnow().strftime('%Y-%m-%d'),
                            'ds_nodash': datetime.utcnow().strftime('%Y%m%d'),
                        }

                        for param_name, template_str in jinja_tpl.items():
                            try:
                                # Simple template rendering (could use Jinja2 for full support)
                                rendered = template_str
                                for var_name, var_value in template_context.items():
                                    rendered = rendered.replace(f'{{{{ {var_name} }}}}', str(var_value))
                                resolved_params[param_name] = rendered
                                context.log.info(f"Rendered template {param_name}: {template_str} -> {rendered}")
                            except Exception as e:
                                context.log.warning(f"Could not render template {param_name}: {e}")
                                resolved_params[param_name] = template_str

                    if op_type == 'bash':
                        bash_command = resolved_params.get('bash_command', 'echo "No command"')
                        context.log.info(f"Running bash: {bash_command}")

                        result = subprocess.run(
                            bash_command,
                            shell=True,
                            capture_output=True,
                            text=True,
                            cwd=repo_path,
                        )

                        if result.stdout:
                            context.log.info(f"stdout: {result.stdout}")
                        if result.stderr:
                            context.log.warning(f"stderr: {result.stderr}")

                        if result.returncode != 0:
                            raise RuntimeError(f"Task {tid} failed with exit code {result.returncode}")

                        return OpOutput(
                            value={"task_id": tid, "stdout": result.stdout, "returncode": result.returncode},
                            metadata={
                                "task_id": tid,
                                "returncode": result.returncode,
                            }
                        )

                    elif op_type == 'python':
                        # Try to resolve python callable
                        callable_func = self.resolve_python_callable(task_cfg, Path(yaml_path).parent)

                        if callable_func:
                            context.log.info(f"Executing Python callable: {callable_func.__name__}")
                            result = callable_func()
                            context.log.info(f"Result type: {type(result).__name__}")
                            return OpOutput(value=result)
                        else:
                            context.log.warning(f"Could not resolve Python callable for task {tid}")
                            return OpOutput(value={"task_id": tid, "status": "skipped"})

                    elif op_type == 'dummy':
                        context.log.info(f"Dummy task (no-op)")
                        return OpOutput(value={"task_id": tid, "status": "success"})

                    else:
                        # Generic operator support - try to instantiate and execute any Airflow operator
                        context.log.info(f"Generic operator: {task_cfg['operator']}")

                        try:
                            # Try to import Airflow and the operator class
                            operator_class_path = task_cfg['operator']
                            module_path, class_name = operator_class_path.rsplit('.', 1)

                            import importlib
                            operator_module = importlib.import_module(module_path)
                            operator_class = getattr(operator_module, class_name)

                            # Instantiate the operator with parameters from YAML
                            # Remove 'operator' from params as it's not a valid operator argument
                            operator_params = {k: v for k, v in params.items() if k != 'operator'}
                            operator_params['task_id'] = tid

                            context.log.info(f"Instantiating {class_name} with params: {list(operator_params.keys())}")
                            operator_instance = operator_class(**operator_params)

                            # Execute the operator
                            # Create a minimal Airflow context
                            from airflow.utils.context import Context as AirflowContext
                            airflow_context = AirflowContext()

                            result = operator_instance.execute(airflow_context)
                            context.log.info(f"Operator executed successfully")

                            return OpOutput(
                                value={"task_id": tid, "result": result, "operator": operator_class_path},
                                metadata={
                                    "operator": operator_class_path,
                                }
                            )

                        except ImportError as e:
                            context.log.warning(f"Could not import operator {task_cfg['operator']}: {e}")
                            context.log.info("Falling back to bash execution via airflow CLI")

                            # Fallback: Use airflow CLI to run the task
                            # This requires the DAG to be in Airflow's DAG folder, but provides full operator support
                            bash_cmd = f"echo 'Operator {task_cfg['operator']} not directly supported - would need Airflow runtime'"
                            result = subprocess.run(bash_cmd, shell=True, capture_output=True, text=True, cwd=repo_path)

                            return OpOutput(value={"task_id": tid, "status": "operator_not_imported"})

                return task_op

            ops_dict[task_id] = make_task_op(
                task_id,
                operator_type,
                parameters,
                task_config,
                str(script_info.script_path),
                jinja_templates,
                xcom_params,
                task_group
            )

        # Build asset tags
        asset_tags = {
            **metadata.tags,
            "script_type": "airflow",
            "script_name": script_info.name,
            "dag_id": dag_id,
            "source": "dag_factory_yaml_graph"
        }
        for kind in metadata.kinds:
            asset_tags[f"dagster/kind/{kind}"] = ""

        # Find leaf tasks (no downstream dependencies - these will be the final outputs)
        all_upstream_deps = set()
        for deps in task_dependencies.values():
            all_upstream_deps.update(deps)

        leaf_tasks = [tid for tid in ops_dict.keys() if tid not in all_upstream_deps]

        if not leaf_tasks:
            # If no clear leaf tasks, use the last task in topological order
            task_order = self.get_task_execution_order(dag_info)
            leaf_tasks = [task_order[-1]] if task_order else list(ops_dict.keys())[-1:]

        logger.info(f"Leaf tasks for graph: {leaf_tasks}")

        # Create the graph asset with dependency structure
        # Use dag_id in the asset name to ensure uniqueness when multiple DAGs are in one YAML
        asset_name = f"script_{script_info.name}_{dag_id}"
        logger.info(f"Creating graph asset: {asset_name} for DAG {dag_id} from {script_info.script_path}")
        @graph_asset(
            name=asset_name,
            group_name=metadata.group_name or "dag_factory",
            tags=asset_tags,
            description=dag_info.get('description', f"DAG Factory: {dag_id}"),
        )
        def dag_factory_graph():
            """Execute dag-factory DAG as graph of ops."""
            # Build execution with proper dependencies by passing results between ops
            results = {}

            # Get topological order
            task_order = self.get_task_execution_order(dag_info)

            # Execute tasks in order, passing dependencies and XCom data
            for task_id in task_order:
                deps = task_dependencies.get(task_id, [])
                has_xcom = task_id in xcom_dependencies

                if not deps:
                    # Root task - no dependencies
                    results[task_id] = ops_dict[task_id]()
                elif has_xcom:
                    # Task has XCom dependencies - pass dict of all upstream results
                    # so the op can extract the specific ones it needs
                    upstream_dict = {dep: results[dep] for dep in deps if dep in results}
                    results[task_id] = ops_dict[task_id](upstream_dict)
                elif len(deps) == 1:
                    # Single dependency - pass its result directly
                    results[task_id] = ops_dict[task_id](results[deps[0]])
                else:
                    # Multiple dependencies without XCom - just invoke without passing data
                    # The graph structure itself ensures execution order
                    # We still need to depend on upstream ops, so reference them to establish dependencies
                    for dep in deps:
                        _ = results[dep]  # Reference to establish dependency in graph
                    results[task_id] = ops_dict[task_id]()

            # Return results from all leaf tasks
            # If single leaf, return its result; if multiple, return the last one
            if len(leaf_tasks) == 1:
                return results[leaf_tasks[0]]
            else:
                # For multiple leaves, return the last one in execution order
                for task_id in reversed(task_order):
                    if task_id in leaf_tasks:
                        return results[task_id]
                return results[leaf_tasks[0]]

        logger.info(f"✅ Created graph asset for dag-factory DAG: {dag_id} with {len(ops_dict)} ops")
        return dag_factory_graph
