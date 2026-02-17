"""Prefect flow parser for extracting tasks, flows, and dependencies."""

import ast
import importlib.util
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dagster import OpExecutionContext, RetryPolicy, graph_asset, op

from .base_parser import BaseParser

logger = logging.getLogger(__name__)


class PrefectParser(BaseParser):
    """Parser for Prefect flows and tasks."""

    def parse_flow(self, script_path: Path) -> Tuple[List[Dict], List[Dict]]:
        """Parse Prefect file to extract tasks and flow structure using AST."""
        try:
            with open(script_path, 'r') as f:
                tree = ast.parse(f.read(), filename=str(script_path))

            tasks = []
            flows = []

            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    if self.has_decorator(node, 'task'):
                        retry_config = self._extract_task_retry_config(node)
                        task_info = {
                            'name': node.name,
                            'params': [arg.arg for arg in node.args.args],
                            'parameters': self.extract_function_parameters(node),
                            'returns_value': self.has_return_statement(node),
                            'retry_config': retry_config
                        }
                        tasks.append(task_info)

                    elif self.has_decorator(node, 'flow'):
                        task_calls, has_complex_patterns = self._extract_task_calls(node, tasks)
                        flow_params = self.extract_function_parameters(node)

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

    def _extract_task_calls(self, flow_node: ast.FunctionDef, known_tasks: List[Dict] = None) -> Tuple[List[Dict], bool]:
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

    def create_graph_asset(
        self,
        flow_info: Dict,
        tasks_info: List[Dict],
        script_info: Any,
        metadata: Any,
        repo_path: str
    ):
        """Create a graph-backed asset for a Prefect flow."""
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
