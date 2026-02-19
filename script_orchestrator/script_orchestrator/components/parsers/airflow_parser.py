"""Airflow DAG parser for extracting tasks, DAGs, schedules, and dependencies."""

import ast
import importlib.util
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dagster import OpExecutionContext, RetryPolicy, graph_asset, op

from .base_parser import BaseParser

logger = logging.getLogger(__name__)


class AirflowParser(BaseParser):
    """Parser for Airflow DAGs and tasks with support for Airflow 2.x and 3.x features."""

    def __init__(self):
        super().__init__()
        self._airflow_version = self._get_airflow_version()

    def _get_airflow_version(self) -> Optional[Tuple[int, int]]:
        """Get installed Airflow version as (major, minor) tuple."""
        try:
            import airflow
            logger.debug("Successfully imported airflow module")

            # Try different ways to get version (varies by Airflow version)
            version_str = None
            if hasattr(airflow, '__version__'):
                version_str = airflow.__version__
                logger.debug(f"Got version from airflow.__version__: {version_str}")
            elif hasattr(airflow, 'version'):
                # Airflow 3.x stores version info differently
                import airflow.version
                if hasattr(airflow.version, 'version'):
                    version_str = airflow.version.version
                    logger.debug(f"Got version from airflow.version.version: {version_str}")

            if not version_str:
                # Fallback: try to get from package metadata
                try:
                    import importlib.metadata
                    version_str = importlib.metadata.version('apache-airflow')
                    logger.debug(f"Got version from importlib.metadata: {version_str}")
                except Exception as e:
                    logger.warning(f"Could not get Airflow version from metadata: {e}")
                    return None

            parts = version_str.split('.')
            version_tuple = (int(parts[0]), int(parts[1]))
            logger.info(f"Detected Airflow version: {version_tuple}")
            return version_tuple
        except ImportError as e:
            logger.warning(f"Could not import Airflow module: {e}")
            return None
        except (ValueError, IndexError, AttributeError) as e:
            logger.warning(f"Could not parse Airflow version: {e}")
            return None

    def _detect_dag_airflow_version_from_imports(self, script_path: Path) -> str:
        """
        Detect which Airflow version a DAG was written for based on import statements.
        Returns a version string like "2.x" or "3.x".
        """
        try:
            content = script_path.read_text()

            # Airflow 2.x imports
            airflow_2x_patterns = [
                'from airflow.decorators import',
                'from airflow.operators.python import',
                'from airflow.models.param import Param',
                'from airflow.operators.python import get_current_context',
            ]

            # Airflow 3.x imports
            airflow_3x_patterns = [
                'from airflow.sdk import',
                'from airflow.providers.standard.operators',
            ]

            has_2x_imports = any(pattern in content for pattern in airflow_2x_patterns)
            has_3x_imports = any(pattern in content for pattern in airflow_3x_patterns)

            if has_2x_imports and not has_3x_imports:
                return "2.x"
            elif has_3x_imports:
                return "3.x"

            # Default to 3.x if no clear indicators
            return "3.x"

        except Exception as e:
            logger.debug(f"Could not detect version from imports: {e}")
            return "3.x"

    def _detect_dag_airflow_version(self, dag_config: dict, script_path: Path) -> str:
        """
        Detect which Airflow version a DAG was written for based on syntax features and imports.
        Returns a version string like "2.x" or "3.x".
        """
        # First check imports (most reliable)
        version_from_imports = self._detect_dag_airflow_version_from_imports(script_path)

        # Feature: outlets in @dag decorator (Airflow 2.x only, removed in 3.x)
        if 'outlet_datasets' in dag_config:
            return "2.x"

        # Use import detection result
        return version_from_imports

    def _detect_airflow_version_requirements(self, dag_config: dict, script_path: Path) -> Optional[str]:
        """
        Detect which Airflow version a DAG was written for based on syntax features.
        Returns a warning message if there's a version mismatch, None otherwise.
        """
        if not self._airflow_version:
            return None  # Can't detect version, skip check

        installed_major, installed_minor = self._airflow_version
        issues = []

        # Feature: outlets in @dag decorator (Airflow 2.x only, removed in 3.x)
        if 'outlet_datasets' in dag_config:
            if installed_major >= 3:
                issues.append(
                    f"DAG uses 'outlets' parameter in @dag decorator (Airflow 2.x syntax), "
                    f"but Airflow {installed_major}.{installed_minor} is installed. "
                    f"In Airflow 3.x, use 'outlets' on individual @task decorators instead."
                )

        # Feature: Dataset-aware scheduling in 2.4+
        if 'inlet_datasets' in dag_config or 'outlet_datasets' in dag_config:
            if installed_major == 2 and installed_minor < 4:
                issues.append(
                    f"DAG uses Dataset-aware scheduling (requires Airflow 2.4+), "
                    f"but Airflow {installed_major}.{installed_minor} is installed."
                )

        # Note: Some DAGs may have other compatibility issues (e.g., task chaining patterns)
        # that are difficult to detect via static analysis. If execution fails, check Airflow logs.

        if issues:
            warning_msg = f"Version compatibility warning for {script_path.name}:\n" + "\n".join(f"  - {issue}" for issue in issues)
            return warning_msg

        return None

    def parse_dag(self, script_path: Path) -> Tuple[List[Dict], List[Dict]]:
        """Parse Airflow DAG file to extract tasks and DAG structure using AST."""
        try:
            with open(script_path, 'r') as f:
                tree = ast.parse(f.read(), filename=str(script_path))

            tasks = []
            dags = []
            dataset_definitions = {}  # Map variable names to URIs

            # First pass: Extract dataset/asset definitions at module level
            for node in tree.body:
                if isinstance(node, ast.Assign):
                    # Look for Dataset() or Asset() assignments
                    if isinstance(node.value, ast.Call):
                        if isinstance(node.value.func, ast.Name):
                            func_name = node.value.func.id
                            if func_name in ['Dataset', 'Asset', 'DatasetAlias', 'AssetAlias']:
                                if node.value.args and isinstance(node.value.args[0], ast.Constant):
                                    uri = node.value.args[0].value
                                    # Get variable name
                                    for target in node.targets:
                                        if isinstance(target, ast.Name):
                                            dataset_definitions[target.id] = uri
                                            logger.debug(f"Found {func_name} definition: {target.id} = {uri}")

            # Second pass: Extract operator tasks (from operator instantiations)
            operator_tasks = self._extract_operator_tasks(tree)

            # Third pass: Extract tasks and DAGs
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    # Check for @task decorator (Airflow TaskFlow API)
                    if self.has_decorator(node, 'task'):
                        # Extract task configuration including outlets
                        task_config = self._extract_task_config(node)

                        task_info = {
                            'name': node.name,
                            'params': [arg.arg for arg in node.args.args],
                            'parameters': self.extract_function_parameters(node),
                            'returns_value': self.has_return_statement(node),
                            'outlet_datasets': task_config.get('outlet_datasets', []),
                        }
                        tasks.append(task_info)

                    # Check for @dag decorator
                    elif self.has_decorator(node, 'dag'):
                        # Extract DAG parameters from decorator
                        dag_config = self._extract_dag_config(node)

                        # Detect which Airflow version this DAG was written for
                        dag_airflow_version = self._detect_dag_airflow_version(dag_config, script_path)
                        logger.info(f"Detected {script_path.name} as Airflow {dag_airflow_version} based on imports and syntax")

                        # Check for Airflow version compatibility issues
                        version_warning = self._detect_airflow_version_requirements(dag_config, script_path)
                        if version_warning:
                            logger.warning(version_warning)

                        # Extract tasks defined inside this DAG function (nested tasks)
                        nested_tasks = []
                        for inner_node in ast.walk(node):
                            if isinstance(inner_node, ast.FunctionDef) and inner_node != node:
                                if self.has_decorator(inner_node, 'task'):
                                    task_config = self._extract_task_config(inner_node)
                                    task_info = {
                                        'name': inner_node.name,
                                        'params': [arg.arg for arg in inner_node.args.args],
                                        'parameters': self.extract_function_parameters(inner_node),
                                        'returns_value': self.has_return_statement(inner_node),
                                        'outlet_datasets': task_config.get('outlet_datasets', []),
                                    }
                                    nested_tasks.append(task_info)
                                    logger.debug(f"Found nested task {inner_node.name} with {len(task_config.get('outlet_datasets', []))} outlet(s)")

                        # Combine module-level tasks with nested tasks for this DAG
                        all_tasks_for_dag = tasks + nested_tasks

                        # Extract task calls within the DAG function
                        task_calls = self._extract_task_calls(node, all_tasks_for_dag)

                        # Detect advanced Airflow features
                        advanced_features = self._detect_advanced_features(tree, node)

                        # Resolve dataset variable references
                        inlet_datasets = dag_config.get('inlet_datasets', [])
                        outlet_datasets = dag_config.get('outlet_datasets', [])

                        # Also collect outlets from individual tasks (both module-level and nested)
                        for task in all_tasks_for_dag:
                            task_outlets = task.get('outlet_datasets', [])
                            if task_outlets:
                                outlet_datasets.extend(task_outlets)

                        inlet_datasets = self._resolve_dataset_vars(inlet_datasets, dataset_definitions)
                        outlet_datasets = self._resolve_dataset_vars(outlet_datasets, dataset_definitions)

                        # Store task information with the DAG
                        dag_tasks_with_outlets = []
                        for task in all_tasks_for_dag:
                            task_outlets = task.get('outlet_datasets', [])
                            if task_outlets:
                                resolved_outlets = self._resolve_dataset_vars(task_outlets, dataset_definitions)
                                dag_tasks_with_outlets.append({
                                    'task_name': task['name'],
                                    'outlet_datasets': resolved_outlets,
                                })

                        dag_info = {
                            'name': node.name,
                            'dag_id': dag_config.get('dag_id', node.name),
                            'task_calls': task_calls,
                            'tasks': operator_tasks,  # Operator instantiations (for check operators, etc.)
                            'dag_tasks': dag_tasks_with_outlets,  # Tasks with their outlets
                            'params': dag_config.get('params', {}),
                            'schedule': dag_config.get('schedule'),
                            'start_date': dag_config.get('start_date'),
                            'retries': dag_config.get('retries'),
                            'retry_delay': dag_config.get('retry_delay'),
                            'tags': dag_config.get('tags', []),
                            'advanced_features': advanced_features,
                            'inlet_datasets': inlet_datasets,  # Datasets this DAG consumes
                            'outlet_datasets': outlet_datasets,  # Datasets this DAG produces (from DAG or tasks)
                            'version_warning': version_warning,  # Airflow version compatibility warning
                            'dag_airflow_version': dag_airflow_version,  # Detected Airflow version (e.g., "2.x", "3.x")
                        }
                        dags.append(dag_info)

            # If we found operator tasks but no @dag decorator, this might be a traditional DAG
            # Create a default DAG entry so check operators can be detected
            if operator_tasks and not dags:
                # Try to extract DAG ID from a 'with DAG(...)' statement
                dag_id = None
                for node in ast.walk(tree):
                    if isinstance(node, ast.With):
                        for item in node.items:
                            if isinstance(item.context_expr, ast.Call):
                                if isinstance(item.context_expr.func, ast.Name) and item.context_expr.func.id == 'DAG':
                                    # Found 'with DAG(...)'
                                    # Try to extract dag_id from first argument or keyword
                                    if item.context_expr.args and isinstance(item.context_expr.args[0], ast.Constant):
                                        dag_id = item.context_expr.args[0].value
                                    else:
                                        for kw in item.context_expr.keywords:
                                            if kw.arg == 'dag_id' and isinstance(kw.value, ast.Constant):
                                                dag_id = kw.value.value
                                    break
                        if dag_id:
                            break

                if dag_id:
                    logger.info(f"Detected traditional DAG style: {dag_id}")
                    dag_info = {
                        'name': dag_id,
                        'dag_id': dag_id,
                        'task_calls': [],
                        'tasks': operator_tasks,  # Operator instantiations
                        'params': {},
                        'schedule': None,
                        'start_date': None,
                        'retries': None,
                        'retry_delay': None,
                        'tags': [],
                        'advanced_features': {},
                        'inlet_datasets': [],
                        'outlet_datasets': [],
                        'version_warning': None,
                        'dag_airflow_version': '2.x',  # Traditional style is typically 2.x
                    }
                    dags.append(dag_info)

            return tasks, dags

        except Exception as e:
            logger.warning(f"Failed to parse Airflow DAG {script_path}: {e}")
            return [], []

    def _extract_dag_config(self, dag_node: ast.FunctionDef) -> dict:
        """Extract configuration from @dag decorator including schedule and retry settings."""
        dag_config = {}

        for decorator in dag_node.decorator_list:
            if isinstance(decorator, ast.Call):
                if isinstance(decorator.func, ast.Name) and decorator.func.id == 'dag':
                    for keyword in decorator.keywords:
                        if keyword.arg == 'dag_id':
                            if isinstance(keyword.value, ast.Constant):
                                dag_config['dag_id'] = keyword.value.value

                        elif keyword.arg == 'schedule' or keyword.arg == 'schedule_interval':
                            # Handle schedule - can be cron string, timedelta, or Dataset/Asset list
                            try:
                                schedule_value = ast.literal_eval(keyword.value)
                                dag_config['schedule'] = schedule_value
                            except (ValueError, SyntaxError):
                                # If it's not a literal, it might be a variable reference or Dataset list
                                if isinstance(keyword.value, ast.Constant):
                                    dag_config['schedule'] = keyword.value.value
                                elif isinstance(keyword.value, ast.List):
                                    # This is a list of Datasets/Assets for data-aware scheduling
                                    datasets = self._extract_dataset_references(keyword.value)
                                    if datasets:
                                        dag_config['inlet_datasets'] = datasets
                                        logger.info(f"Detected {len(datasets)} inlet datasets in schedule")

                        elif keyword.arg == 'outlets':
                            # Extract outlet datasets/assets (produced by this DAG)
                            if isinstance(keyword.value, ast.List):
                                datasets = self._extract_dataset_references(keyword.value)
                                if datasets:
                                    dag_config['outlet_datasets'] = datasets
                                    logger.info(f"Detected {len(datasets)} outlet datasets")

                        elif keyword.arg == 'start_date':
                            # Extract start_date - usually datetime(YYYY, MM, DD)
                            if isinstance(keyword.value, ast.Call):
                                if isinstance(keyword.value.func, ast.Name) and keyword.value.func.id == 'datetime':
                                    # Extract datetime args
                                    args = keyword.value.args
                                    if len(args) >= 3:
                                        try:
                                            year = ast.literal_eval(args[0])
                                            month = ast.literal_eval(args[1])
                                            day = ast.literal_eval(args[2])
                                            dag_config['start_date'] = f"{year:04d}-{month:02d}-{day:02d}"
                                        except (ValueError, SyntaxError):
                                            pass

                        elif keyword.arg == 'default_args':
                            # Extract default_args dict which may contain retries, retry_delay
                            if isinstance(keyword.value, ast.Dict):
                                default_args = self._extract_dict_literal(keyword.value)
                                if 'retries' in default_args:
                                    dag_config['retries'] = default_args['retries']
                                if 'retry_delay' in default_args:
                                    dag_config['retry_delay'] = default_args['retry_delay']

                        elif keyword.arg == 'retries':
                            try:
                                dag_config['retries'] = ast.literal_eval(keyword.value)
                            except (ValueError, SyntaxError):
                                pass

                        elif keyword.arg == 'retry_delay':
                            try:
                                # retry_delay is usually a timedelta, try to extract seconds
                                dag_config['retry_delay'] = self._extract_timedelta_seconds(keyword.value)
                            except (ValueError, SyntaxError):
                                pass

                        elif keyword.arg == 'tags':
                            try:
                                dag_config['tags'] = ast.literal_eval(keyword.value)
                            except (ValueError, SyntaxError):
                                pass

                        elif keyword.arg == 'params':
                            # Extract Param definitions from params dict
                            if isinstance(keyword.value, ast.Dict):
                                params_dict = {}
                                for key, value in zip(keyword.value.keys, keyword.value.values):
                                    if isinstance(key, ast.Constant):
                                        param_name = key.value
                                        param_info = self._extract_param_info(value)
                                        params_dict[param_name] = param_info
                                dag_config['params'] = params_dict

        return dag_config

    def _extract_dict_literal(self, dict_node: ast.Dict) -> dict:
        """Extract a dictionary literal from AST."""
        result = {}
        for key, value in zip(dict_node.keys, dict_node.values):
            if isinstance(key, ast.Constant):
                try:
                    result[key.value] = ast.literal_eval(value)
                except (ValueError, SyntaxError):
                    pass
        return result

    def _extract_timedelta_seconds(self, timedelta_node: ast.AST) -> Optional[int]:
        """Extract seconds from a timedelta() call."""
        if isinstance(timedelta_node, ast.Call):
            if isinstance(timedelta_node.func, ast.Name) and timedelta_node.func.id == 'timedelta':
                # Look for seconds, minutes, hours, days kwargs
                total_seconds = 0
                for keyword in timedelta_node.keywords:
                    if keyword.arg in ['seconds', 'minutes', 'hours', 'days']:
                        try:
                            value = ast.literal_eval(keyword.value)
                            if keyword.arg == 'seconds':
                                total_seconds += value
                            elif keyword.arg == 'minutes':
                                total_seconds += value * 60
                            elif keyword.arg == 'hours':
                                total_seconds += value * 3600
                            elif keyword.arg == 'days':
                                total_seconds += value * 86400
                        except (ValueError, SyntaxError):
                            pass
                return total_seconds if total_seconds > 0 else None
        return None

    def _extract_task_config(self, task_node: ast.FunctionDef) -> dict:
        """Extract configuration from @task decorator including outlets."""
        task_config = {}

        for decorator in task_node.decorator_list:
            if isinstance(decorator, ast.Call):
                if isinstance(decorator.func, ast.Name) and decorator.func.id == 'task':
                    for keyword in decorator.keywords:
                        if keyword.arg == 'outlets':
                            # Extract outlet datasets/assets (produced by this task)
                            if isinstance(keyword.value, ast.List):
                                datasets = self._extract_dataset_references(keyword.value)
                                if datasets:
                                    task_config['outlet_datasets'] = datasets
                                    logger.debug(f"Task {task_node.name} has {len(datasets)} outlet(s)")

        return task_config

    def _extract_dataset_references(self, list_node: ast.List) -> List[str]:
        """Extract Dataset or Asset URI references from a list.

        Handles patterns like:
        - schedule=[Dataset("s3://bucket/data"), Dataset("file:///path")]
        - outlets=[Asset("table://db.schema.table")]
        """
        dataset_uris = []

        for element in list_node.elts:
            # Handle Dataset("uri") or Asset("uri") calls
            if isinstance(element, ast.Call):
                if isinstance(element.func, ast.Name):
                    func_name = element.func.id
                    if func_name in ['Dataset', 'Asset', 'DatasetAlias', 'AssetAlias']:
                        # Extract the URI argument
                        if element.args and isinstance(element.args[0], ast.Constant):
                            uri = element.args[0].value
                            dataset_uris.append(uri)
                            logger.debug(f"Extracted {func_name} URI: {uri}")

            # Handle variable references to datasets
            elif isinstance(element, ast.Name):
                # This is a variable reference like `my_dataset`
                # We'll need to trace it back to its definition
                dataset_uris.append(f"var:{element.id}")
                logger.debug(f"Found dataset variable reference: {element.id}")

        return dataset_uris

    def _resolve_dataset_vars(self, dataset_refs: List[str], definitions: Dict[str, str]) -> List[str]:
        """Resolve dataset variable references to their URIs."""
        resolved = []
        for ref in dataset_refs:
            if ref.startswith('var:'):
                var_name = ref[4:]  # Remove 'var:' prefix
                if var_name in definitions:
                    resolved.append(definitions[var_name])
                else:
                    logger.warning(f"Could not resolve dataset variable: {var_name}")
                    resolved.append(ref)  # Keep the var reference
            else:
                resolved.append(ref)
        return resolved

    def _extract_param_info(self, param_node: ast.AST) -> dict:
        """Extract parameter information from Airflow Param() call."""
        param_info = {
            'default': None,
            'type': 'string',
            'description': None
        }

        if isinstance(param_node, ast.Call):
            # Check if it's a Param() call
            if isinstance(param_node.func, ast.Name) and param_node.func.id == 'Param':
                # Extract keyword arguments
                for keyword in param_node.keywords:
                    if keyword.arg == 'default':
                        try:
                            param_info['default'] = ast.literal_eval(keyword.value)
                        except (ValueError, SyntaxError):
                            pass
                    elif keyword.arg == 'type':
                        if isinstance(keyword.value, ast.Constant):
                            param_info['type'] = keyword.value.value
                        elif isinstance(keyword.value, ast.List):
                            # Handle type: ["array"]
                            if keyword.value.elts and isinstance(keyword.value.elts[0], ast.Constant):
                                param_info['type'] = keyword.value.elts[0].value
                    elif keyword.arg == 'description':
                        if isinstance(keyword.value, ast.Constant):
                            param_info['description'] = keyword.value.value

        return param_info

    def _extract_task_calls(self, dag_node: ast.FunctionDef, known_tasks: List[Dict]) -> List[Dict]:
        """Extract task calls from DAG function body."""
        known_task_names = {task['name'] for task in known_tasks}
        task_calls = []

        # Walk through the DAG function body
        for node in ast.walk(dag_node):
            if isinstance(node, ast.Call):
                # Direct task call
                if isinstance(node.func, ast.Name):
                    task_name = node.func.id
                    if task_name in known_task_names:
                        task_calls.append({
                            'task_name': task_name,
                            'order': len(task_calls)
                        })

        return task_calls

    def _detect_advanced_features(self, tree: ast.AST, dag_node: ast.FunctionDef) -> Dict[str, bool]:
        """Detect advanced Airflow features that require subprocess execution.

        Supports Airflow 2.x and 3.x features including:
        - XCom (xcom_push, xcom_pull, ti.xcom_pull)
        - Datasets (Airflow 2.4+)
        - Assets (Airflow 3.0+)
        - Sensors
        - Operators (non-@task decorators)
        - Branching operators
        - Dynamic task mapping
        """
        features = {
            'uses_xcom': False,
            'uses_datasets': False,
            'uses_assets': False,
            'uses_sensors': False,
            'uses_operators': False,
            'uses_branching': False,
            'uses_dynamic_mapping': False,
        }

        # Check imports for advanced features
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                module = node.module or ''

                # Detect dataset/asset imports (Airflow 2.4+ and 3.0+)
                if 'datasets' in module or 'Dataset' in [alias.name for alias in node.names]:
                    features['uses_datasets'] = True
                    logger.info("Detected Airflow Dataset usage (2.4+)")

                if 'assets' in module or 'Asset' in [alias.name for alias in node.names]:
                    features['uses_assets'] = True
                    logger.info("Detected Airflow Asset usage (3.0+)")

                # Detect sensor imports
                if 'sensors' in module or any('Sensor' in alias.name for alias in node.names):
                    features['uses_sensors'] = True
                    logger.info("Detected Airflow Sensor usage")

                # Detect operator imports (except @task decorators)
                if 'operators' in module:
                    # Check for specific operators
                    operator_names = [alias.name for alias in node.names]
                    if any(op for op in operator_names if 'Operator' in op and op != 'PythonOperator'):
                        features['uses_operators'] = True
                        logger.info(f"Detected Airflow Operators: {operator_names}")

                    # Detect branching operators
                    if any('Branch' in op for op in operator_names):
                        features['uses_branching'] = True
                        logger.info("Detected branching operators")

        # Check for XCom usage in DAG function body
        for node in ast.walk(dag_node):
            # Detect xcom_push/xcom_pull method calls
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Attribute):
                    method_name = node.func.attr
                    if method_name in ['xcom_push', 'xcom_pull']:
                        features['uses_xcom'] = True
                        logger.info(f"Detected XCom usage: {method_name}")

                    # Detect ti.xcom_pull pattern
                    if method_name == 'xcom_pull' and isinstance(node.func.value, ast.Name):
                        if node.func.value.id in ['ti', 'task_instance', 'context']:
                            features['uses_xcom'] = True
                            logger.info("Detected TaskInstance XCom usage")

                # Detect expand() for dynamic task mapping (Airflow 2.3+)
                if isinstance(node.func, ast.Attribute) and node.func.attr == 'expand':
                    features['uses_dynamic_mapping'] = True
                    logger.info("Detected dynamic task mapping with expand()")

            # Detect dataset/asset references in expressions
            if isinstance(node, ast.Name):
                if node.id in ['Dataset', 'Asset', 'DatasetAlias', 'AssetAlias']:
                    if node.id in ['Dataset', 'DatasetAlias']:
                        features['uses_datasets'] = True
                    if node.id in ['Asset', 'AssetAlias']:
                        features['uses_assets'] = True

        return features

    def create_graph_asset(
        self,
        dag_info: Dict,
        tasks_info: List[Dict],
        script_info: Any,
        metadata: Any,
        repo_path: str
    ):
        """Create a graph-backed asset for an Airflow DAG."""
        dag_name = dag_info['name']
        dag_id = dag_info['dag_id']
        task_calls = dag_info['task_calls']
        dag_params = dag_info.get('params', {})

        # If DAG has parameters, fall back to subprocess for Launchpad config support
        if dag_params:
            logger.info(f"DAG {dag_name} has {len(dag_params)} parameters, falling back to subprocess for config support")
            return None

        # Check for advanced Airflow features that require subprocess execution
        # EXCEPT for uses_assets - we handle those specially
        advanced_features = dag_info.get('advanced_features', {})
        blocking_features = {k: v for k, v in advanced_features.items()
                           if v and k not in ['uses_assets', 'uses_datasets']}

        if any(blocking_features.values()):
            enabled_features = [k for k, v in blocking_features.items() if v]
            logger.info(f"DAG {dag_name} uses advanced features {enabled_features}, falling back to subprocess")
            return None

        # Check if this is a simple sequential DAG (each task called once, in order)
        task_call_counts = {}
        for task_call in task_calls:
            task_name = task_call['task_name']
            task_call_counts[task_name] = task_call_counts.get(task_name, 0) + 1

        # If any task is called more than once, fall back to subprocess
        if any(count > 1 for count in task_call_counts.values()):
            logger.info(f"DAG {dag_name} has tasks called multiple times, falling back to subprocess")
            return None

        # Try to import the script module to get actual task functions
        try:
            spec = importlib.util.spec_from_file_location("airflow_module", str(script_info.script_path))
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
                    task_func = getattr(module, task_name)

                    # Check if this is an Airflow @task decorated function
                    # These return XComArg objects which contain Jinja templates and can't be pickled
                    # Detect by checking the type name or if it has Airflow-specific attributes
                    func_type = type(task_func).__name__
                    if func_type in ['XComArg', 'TaskDecorator'] or hasattr(task_func, 'operator_class'):
                        logger.info(f"Task {task_name} uses Airflow @task decorator, falling back to subprocess")
                        return None

                    task_functions[task_name] = task_func
                else:
                    logger.warning(f"Task function {task_name} not found in module")
                    return None

        except Exception as e:
            logger.warning(f"Could not import Airflow module {script_info.script_path}: {e}")
            return None

        # Create ops for each task that actually call the Airflow task functions
        ops_dict = {}
        for task_info in tasks_info:
            task_name = task_info['name']
            task_func = task_functions[task_name]
            task_params = task_info.get('params', [])

            # Capture the task function in closure
            def make_task_op(tf, tn, params):
                @op(
                    name=f"{script_info.name}_{tn}",
                )
                def task_op(context: OpExecutionContext, input_data=None):
                    """Execute Airflow task function."""
                    context.log.info(f"Executing Airflow task: {tn}")

                    # Call the task function with appropriate arguments
                    try:
                        if input_data is not None:
                            # Pass input data as positional argument
                            result = tf(input_data)
                        elif len(params) > 0 and params[0] != 'self':
                            # Try calling with first parameter if it has a default
                            result = tf()
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

        # Detect resources from Airflow DAG script (for kinds and tags)
        from ..utils.resource_detector import ResourceDetector
        detected_resources = []
        try:
            detected_resources = ResourceDetector.detect_resources_from_file(script_info.script_path)
            if detected_resources:
                resource_names = [r['resource_name'] for r in detected_resources]
                logger.info(f"🔧 Detected resources in Airflow DAG: {', '.join(resource_names)}")
        except Exception as e:
            logger.debug(f"Could not detect resources from {script_info.script_path}: {e}")

        # Build asset tags
        asset_tags = {
            **metadata.tags,
            "script_type": "airflow_mapped",
            "script_name": script_info.name,
            "airflow_dag": dag_id,
            "dagster/kind/airflow": "",  # Airflow framework kind
        }
        for kind in metadata.kinds:
            asset_tags[f"dagster/kind/{kind}"] = ""

        # Add detected resources as kinds and tags
        if detected_resources:
            for resource in detected_resources:
                resource_name = resource['resource_name']
                resource_type = resource['resource_type']
                # Add as kind (shows icon in UI)
                asset_tags[f"dagster/kind/{resource_name}"] = ""
                # Add as regular tag for filtering
                asset_tags[f"uses_{resource_name}"] = ""
                asset_tags[f"resource_type_{resource_type}"] = ""

        # Create ops list in the order they should be called
        ops_list = [ops_dict[tc['task_name']] for tc in task_calls if tc['task_name'] in ops_dict]

        if not ops_list:
            logger.warning(f"No ops created for DAG {dag_name}")
            return None

        # Create graph asset that explicitly calls ops in sequence
        # We need to build this dynamically so Dagster can statically analyze it
        # Ops use dagster_type=Nothing to avoid pickling errors with XCom data
        @graph_asset(
            name=f"script_{script_info.name}",
            group_name=metadata.group_name,
            tags=asset_tags,
            description=metadata.description or f"Airflow DAG: {dag_id}",
        )
        def dag_graph():
            """Execute Airflow DAG as graph of ops."""
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
                # For DAGs with more than 5 tasks, fall back to subprocess
                logger.info(f"DAG {dag_name} has {len(ops_list)} ops, which is too many for explicit wiring")
                return None

        return dag_graph

    def _extract_operator_tasks(self, tree: ast.AST) -> List[Dict]:
        """Extract tasks from operator instantiations (not @task decorators).

        Detects patterns like:
            SQLColumnCheckOperator(task_id='check', table='users', column_mapping={...})
            PythonOperator(task_id='run', python_callable=func)
            BashOperator(task_id='bash', bash_command='echo hi')

        Args:
            tree: AST tree of the Python file

        Returns:
            List of task dictionaries with:
            - task_id: Task identifier
            - operator_type: Type (sql_column_check, python, bash, etc.)
            - operator_class: Original class name
            - parameters: Extracted parameters dict
        """
        operator_tasks = []

        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                # Check if this is an operator instantiation
                operator_class = None
                if isinstance(node.func, ast.Name):
                    # Direct class call: SQLColumnCheckOperator(...)
                    operator_class = node.func.id
                elif isinstance(node.func, ast.Attribute):
                    # Module call: operators.SQLColumnCheckOperator(...)
                    operator_class = node.func.attr

                # Check if this looks like an Airflow operator
                if operator_class and 'Operator' in operator_class:
                    # Extract parameters
                    task_id = None
                    parameters = {}

                    for keyword in node.keywords:
                        if keyword.arg == 'task_id':
                            if isinstance(keyword.value, ast.Constant):
                                task_id = keyword.value.value
                        else:
                            # Try to extract parameter value
                            try:
                                param_value = ast.literal_eval(keyword.value)
                                parameters[keyword.arg] = param_value
                            except:
                                # Can't evaluate - skip complex expressions
                                pass

                    if task_id:
                        operator_type = self._convert_operator_class_to_type(operator_class)

                        operator_tasks.append({
                            'task_id': task_id,
                            'operator_type': operator_type,
                            'operator_class': operator_class,
                            'parameters': parameters,
                        })
                        logger.debug(f"Detected operator task: {task_id} ({operator_class})")

        return operator_tasks

    def _convert_operator_class_to_type(self, operator_class: str) -> str:
        """Convert operator class name to operator type.

        Examples:
            SQLColumnCheckOperator -> sql_column_check
            SQLTableCheckOperator -> sql_table_check
            SQLCheckOperator -> sql_check
            PythonOperator -> python
            BashOperator -> bash

        Args:
            operator_class: Class name (e.g., "SQLColumnCheckOperator")

        Returns:
            operator_type string (e.g., "sql_column_check")
        """
        import re

        # Remove 'Operator' suffix
        name = operator_class.replace('Operator', '')

        # Convert CamelCase to snake_case
        name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()

        return name
