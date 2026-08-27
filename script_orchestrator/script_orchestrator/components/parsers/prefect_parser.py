"""Prefect flow parser for extracting tasks, flows, and dependencies."""

import ast
import importlib.util
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dagster import OpExecutionContext, RetryPolicy, graph_asset, op

from .base_parser import BaseParser
from .prefect_asset_support import (
    create_materialize_multi_asset,
    parse_prefect_assets,
)

logger = logging.getLogger(__name__)


class PrefectParser(BaseParser):
    """Parser for Prefect flows and tasks."""

    @staticmethod
    def _create_fake_prefect_module(op_name_prefix=""):
        """Create a fake Prefect module that maps decorators to Dagster."""

        class FakePrefectModule:
            """Fake Prefect module that provides Dagster decorators."""

            @staticmethod
            def task(func=None, name=None, retries=None, retry_delay_seconds=None, **kwargs):
                """Map Prefect @task to Dagster @op.

                Supports both @task and @task() syntax.
                """
                def decorator(f):
                    op_kwargs = {}

                    # Make op name unique by prefixing with script/flow name
                    task_name = name if name else f.__name__
                    if op_name_prefix:
                        op_kwargs['name'] = f"{op_name_prefix}_{task_name}"
                    else:
                        op_kwargs['name'] = task_name

                    if retries and retries > 0:
                        # Handle retry_delay_seconds as either single value or list
                        delay = 0
                        if retry_delay_seconds:
                            if isinstance(retry_delay_seconds, (list, tuple)):
                                # Use first delay value (Dagster doesn't support progressive backoff)
                                delay = retry_delay_seconds[0] if retry_delay_seconds else 0
                            else:
                                delay = retry_delay_seconds

                        op_kwargs['retry_policy'] = RetryPolicy(
                            max_retries=retries,
                            delay=delay
                        )

                    # Just apply @op directly to the original function
                    # Keep the original Prefect signature (no context parameter)
                    # This allows the flow body to call it normally: greet(name)
                    return op(**op_kwargs)(f)

                # Handle @task (without parentheses)
                if func is not None:
                    return decorator(func)

                # Handle @task() (with parentheses)
                return decorator

            @staticmethod
            def flow(func=None, name=None, **kwargs):
                """Map Prefect @flow to a plain function with config support.

                Supports both @flow and @flow() syntax.
                """
                def decorator(f):
                    import inspect
                    from dagster import Config

                    # Get function signature to detect parameters
                    sig = inspect.signature(f)
                    params = [p for p in sig.parameters.values()
                             if p.name not in ('self', 'cls')]

                    # Just mark the flow - keep original signature!
                    # Parameters will become graph inputs
                    f._is_prefect_flow = True
                    f._flow_name = name or f.__name__
                    f._flow_kwargs = kwargs
                    f._has_params = bool(params)
                    return f

                # Handle @flow (without parentheses)
                if func is not None:
                    return decorator(func)

                # Handle @flow() (with parentheses)
                return decorator

            # Stub implementations for common Prefect imports
            @staticmethod
            def get_run_logger():
                """Stub for Prefect's get_run_logger."""
                import logging
                return logging.getLogger("prefect_stub")

            @staticmethod
            def tags(*tag_list, **tag_dict):
                """Stub for Prefect's tags context manager."""
                class TagsContext:
                    def __enter__(self):
                        return self
                    def __exit__(self, *args):
                        pass
                return TagsContext()

            @staticmethod
            def unmapped(value):
                """Stub for Prefect's unmapped - returns value as-is."""
                return value

            @staticmethod
            def allow_failure(task_run=None):
                """Stub for Prefect's allow_failure."""
                class AllowFailureContext:
                    def __enter__(self):
                        return self
                    def __exit__(self, *args):
                        pass
                return AllowFailureContext()

            @staticmethod
            def task_input_hash(*args, **kwargs):
                """Stub for Prefect's task_input_hash."""
                return None

            # Stub classes for Prefect types
            class State:
                """Stub for Prefect State."""
                pass

            class Task:
                """Stub for Prefect Task."""
                pass

            class Flow:
                """Stub for Prefect Flow."""
                pass

        return FakePrefectModule()

    def parse_assets(
        self, script_path: Path, repo_root: Optional[Path] = None
    ) -> Dict[str, Any]:
        """Parse Prefect script for @materialize-decorated asset declarations.

        Args:
          script_path: absolute path to the script.
          repo_root: absolute path to the cloned repo root. When set, the
            parser follows `from X import subflow` statements to sibling
            .py files under repo_root and treats their @materialize / @flow
            definitions as if declared in the current script — enabling
            cross-file lineage inference for @materialize called directly
            from an imported module.

        Returns {"materialized": [...], "external": [...]}. Empty lists when
        no @materialize decorators are found. See
        `prefect_asset_support.parse_prefect_assets` for entry shapes.
        """
        return parse_prefect_assets(script_path, repo_root=repo_root)

    def create_materialize_multi_asset(
        self,
        prefect_assets: Dict[str, Any],
        flow_info: Dict,
        script_info: Any,
        metadata: Any,
        dbt_project_path: Optional[str] = None,
        auto_freshness_policies: bool = False,
    ):
        """Build a Dagster @multi_asset (+ external upstream AssetSpecs +
        deployment ScheduleDefinitions).

        Args:
          dbt_project_path: for @materialize(materialized_by="dbt") assets,
            path to a dbt project (must contain target/catalog.json +
            target/manifest.json) — enables column-schema and column-lineage
            metadata on those assets at build time.
          auto_freshness_policies: when True, attach a FreshnessPolicy
            inferred from the asset's cron schedule (from prefect.yaml).
        """
        return create_materialize_multi_asset(
            prefect_assets=prefect_assets,
            flow_info=flow_info,
            script_info=script_info,
            metadata=metadata,
            fake_prefect_factory=self._create_fake_prefect_module,
            dbt_project_path=dbt_project_path,
            auto_freshness_policies=auto_freshness_policies,
        )

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

                        # Extract docstring if available
                        docstring = ast.get_docstring(node)
                        if docstring:
                            # Clean up the docstring (remove extra whitespace, take first line/paragraph)
                            docstring = docstring.strip()
                            # Take first line or first paragraph as description
                            first_line = docstring.split('\n\n')[0].replace('\n', ' ').strip()
                        else:
                            first_line = None

                        flow_info = {
                            'name': node.name,
                            'task_calls': task_calls,
                            'has_complex_patterns': has_complex_patterns,
                            'parameters': flow_params,
                            'docstring': first_line
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

    def try_monkey_patch_approach(
        self,
        flow_info: Dict,
        tasks_info: List[Dict],
        script_info: Any,
        metadata: Any,
        repo_path: str,
        dependencies: Optional[List[str]] = None,
        asset_prefix: str = "prefect"
    ):
        """Try to create graph asset using module monkey patching.

        Args:
            dependencies: List of asset names this flow depends on (for lineage)
            asset_prefix: Prefix for the asset name (default: "prefect")
        """
        flow_name = flow_info['name']
        flow_params = flow_info.get('parameters', [])

        try:
            logger.info(f"Attempting monkey patch approach for flow: {flow_name}")

            # Save original prefect module if it exists
            original_prefect = sys.modules.get('prefect')

            # Inject fake prefect module with unique op name prefix
            fake_prefect = self._create_fake_prefect_module(op_name_prefix=script_info.name)
            sys.modules['prefect'] = fake_prefect

            # Import the script module
            spec = importlib.util.spec_from_file_location(
                f"prefect_monkey_patch_{script_info.name}",
                str(script_info.script_path)
            )
            if spec is None or spec.loader is None:
                logger.warning(f"Could not load spec for {script_info.script_path}")
                return None

            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            # Find the flow function
            flow_func = None
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if callable(attr) and hasattr(attr, '_is_prefect_flow'):
                    if attr._flow_name == flow_name or attr.__name__ == flow_name:
                        flow_func = attr
                        break

            if flow_func is None:
                logger.warning(f"Could not find flow function {flow_name} in monkey patched module")
                return None

            logger.info(f"✅ Successfully monkey patched flow: {flow_name}")

            # Build asset tags
            asset_tags = {
                **metadata.tags,
                "script_type": "prefect_monkey_patched",
                "script_name": script_info.name,
                "prefect_flow": flow_name,
                "dagster/kind/prefect": "",
            }
            for kind in metadata.kinds:
                asset_tags[f"dagster/kind/{kind}"] = ""

            # Check if flow returns a value (required for graph assets)
            import inspect
            import ast
            import textwrap

            source = inspect.getsource(flow_func)
            has_return = 'return ' in source and not source.strip().endswith('return None')

            # We can add a return statement via AST rewriting if needed
            needs_return_injection = not has_return

            # Handle flows with parameters using the "config op" pattern
            if hasattr(flow_func, '_has_params') and flow_func._has_params and flow_params:
                logger.info(f"Flow {flow_name} has parameters - applying config op pattern")

                # Build config ops for flow parameters (one op per parameter)
                from dagster import op as dagster_op, Field
                from dagster import String, Int, Float, Bool

                config_ops = {}
                op_config = {}

                for param in flow_params:
                    param_name = param['name']
                    param_type_dagster = String  # Default to String

                    if param.get('type_annotation'):
                        type_str = param['type_annotation']
                        if type_str == 'int':
                            param_type_dagster = Int
                        elif type_str == 'float':
                            param_type_dagster = Float
                        elif type_str == 'bool':
                            param_type_dagster = Bool

                    # Create individual config op for this parameter
                    config_schema_single = {
                        param_name: Field(param_type_dagster, default_value=param.get('default'))
                        if param.get('default') is not None
                        else Field(param_type_dagster)
                    }

                    # Create the op with a unique name
                    op_name = f"{flow_name}_get_{param_name}"

                    # Create the op function dynamically
                    def make_config_op(p_name):
                        @dagster_op(
                            name=f"{flow_name}_get_{p_name}",
                            config_schema={p_name: config_schema_single[p_name]}
                        )
                        def config_op(context):
                            """Extract config value for parameter."""
                            return context.op_config[p_name]
                        return config_op

                    config_ops[param_name] = make_config_op(param_name)

                    # Add default config if available
                    if param.get('default') is not None:
                        op_config[f"{flow_name}_get_{param_name}"] = {
                            "config": {param_name: param['default']}
                        }

                # Now rewrite the flow function to use config ops
                # Parse the flow function's AST
                flow_source = textwrap.dedent(source)
                tree = ast.parse(flow_source)
                flow_def = tree.body[0]  # First statement should be the function def

                # Create a new function that calls individual config ops
                # For each parameter, inject: source = get_source_config()
                param_names = [p['name'] for p in flow_params]

                # Build new function body
                new_body = []

                # Call individual config op for each parameter
                for param_name in param_names:
                    new_body.append(
                        ast.Assign(
                            targets=[ast.Name(id=param_name, ctx=ast.Store())],
                            value=ast.Call(
                                func=ast.Name(id=f"get_{param_name}_config", ctx=ast.Load()),
                                args=[],
                                keywords=[]
                            )
                        )
                    )

                # Add original function body
                new_body.extend(flow_def.body)

                # If flow doesn't return, find the last assignment and return it
                if needs_return_injection:
                    # Find the last Assign node in the original body
                    last_assign_var = None
                    for stmt in reversed(flow_def.body):
                        if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1:
                            target = stmt.targets[0]
                            if isinstance(target, ast.Name):
                                last_assign_var = target.id
                                break

                    if last_assign_var:
                        # Add return statement
                        new_body.append(
                            ast.Return(
                                value=ast.Name(id=last_assign_var, ctx=ast.Load())
                            )
                        )
                        logger.info(f"Injected return statement for variable: {last_assign_var}")

                # Create new function with no parameters
                new_func_def = ast.FunctionDef(
                    name=flow_def.name,
                    args=ast.arguments(
                        posonlyargs=[],
                        args=[],  # No parameters!
                        kwonlyargs=[],
                        kw_defaults=[],
                        defaults=[]
                    ),
                    body=new_body,
                    decorator_list=[],
                    returns=flow_def.returns
                )

                # Compile and execute the new function
                new_module = ast.Module(body=[new_func_def], type_ignores=[])
                ast.fix_missing_locations(new_module)

                # Execute in the original module's namespace (includes task ops)
                # Add config ops to the namespace
                namespace = dict(module.__dict__)  # Copy module namespace
                for param_name in param_names:
                    namespace[f"get_{param_name}_config"] = config_ops[param_name]

                exec(compile(new_module, '<generated>', 'exec'), namespace)

                # Get the new flow function
                flow_func = namespace[flow_def.name]

                logger.info(f"✅ Rewrote flow {flow_name} with config op pattern")
            else:
                # No parameters, but might still need return injection
                if needs_return_injection:
                    import ast
                    import textwrap

                    # Parse and rewrite to add return
                    flow_source = textwrap.dedent(inspect.getsource(flow_func))
                    tree = ast.parse(flow_source)
                    flow_def = tree.body[0]

                    # Find last assignment
                    last_assign_var = None
                    for stmt in reversed(flow_def.body):
                        if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1:
                            target = stmt.targets[0]
                            if isinstance(target, ast.Name):
                                last_assign_var = target.id
                                break

                    if last_assign_var:
                        # Add return statement
                        flow_def.body.append(
                            ast.Return(
                                value=ast.Name(id=last_assign_var, ctx=ast.Load())
                            )
                        )

                        # Compile and execute
                        new_module = ast.Module(body=[flow_def], type_ignores=[])
                        ast.fix_missing_locations(new_module)

                        namespace = {}
                        exec(compile(new_module, '<generated>', 'exec'), namespace)
                        flow_func = namespace[flow_def.name]

                        logger.info(f"Injected return statement for variable: {last_assign_var}")

                op_config = {}

            # Use metadata description, fallback to flow docstring, then default
            description = metadata.description or flow_info.get('docstring') or f"Prefect flow (monkey patched): {flow_name}"

            # Build graph_asset kwargs
            graph_asset_kwargs = {
                'name': f"{asset_prefix}_{script_info.name}",
                'group_name': metadata.group_name,
                'tags': asset_tags,
                'description': description,
            }

            # Add config if we have it
            if op_config:
                graph_asset_kwargs['config'] = op_config
                logger.info(f"✅ Added config to graph_asset: {op_config}")

            # Add dependencies if provided (for lineage)
            if dependencies:
                from dagster import AssetKey
                graph_asset_kwargs['deps'] = [AssetKey(dep) for dep in dependencies]
                logger.info(f"✅ Added dependencies to graph_asset: {dependencies}")

            # Wrap the flow function with @graph_asset
            graph_asset_decorated = graph_asset(**graph_asset_kwargs)(flow_func)

            return graph_asset_decorated

        except Exception as e:
            logger.debug(f"Monkey patch approach failed for {flow_name}: {e} (falling back to subprocess mode)")
            return None

        finally:
            # Restore original prefect module
            if original_prefect is not None:
                sys.modules['prefect'] = original_prefect
            elif 'prefect' in sys.modules:
                del sys.modules['prefect']

    def try_monkey_patch_job_approach(
        self,
        flow_info: Dict,
        tasks_info: List[Dict],
        script_info: Any,
        metadata: Any,
        repo_path: str
    ):
        """Try to create job using module monkey patching.

        Similar to try_monkey_patch_approach but creates a @job instead of @graph_asset.
        Jobs don't need to return values, making them easier to convert.
        """
        from dagster import job as dagster_job

        flow_name = flow_info['name']
        flow_params = flow_info.get('parameters', [])

        try:
            logger.info(f"Attempting monkey patch job approach for flow: {flow_name}")

            # Save original prefect module if it exists
            original_prefect = sys.modules.get('prefect')

            # Inject fake prefect module with unique op name prefix
            fake_prefect = self._create_fake_prefect_module(op_name_prefix=script_info.name)
            sys.modules['prefect'] = fake_prefect

            # Import the script module
            spec = importlib.util.spec_from_file_location(
                f"prefect_script_{script_info.name}",
                str(script_info.script_path)
            )
            if spec is None or spec.loader is None:
                logger.warning(f"Could not load spec for {script_info.script_path}")
                return None

            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            # Find the flow function
            flow_func = None
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if callable(attr) and hasattr(attr, '_is_prefect_flow'):
                    if attr._flow_name == flow_name or attr.__name__ == flow_name:
                        flow_func = attr
                        break

            if flow_func is None:
                logger.warning(f"Could not find flow function {flow_name} in monkey patched module")
                return None

            logger.info(f"✅ Successfully monkey patched flow: {flow_name}")

            # Build job tags
            job_tags = {
                **metadata.tags,
                "script_type": "prefect_monkey_patched_job",
                "script_name": script_info.name,
                "prefect_flow": flow_name,
                "dagster/kind/prefect": "",
            }
            for kind in metadata.kinds:
                job_tags[f"dagster/kind/{kind}"] = ""

            # Handle flows with parameters using config op pattern
            import inspect
            import ast
            import textwrap

            op_config = {}

            if hasattr(flow_func, '_has_params') and flow_func._has_params and flow_params:
                logger.info(f"Flow {flow_name} has parameters - applying config op pattern for job")

                # Build config ops for flow parameters
                from dagster import op as dagster_op, Field
                from dagster import String, Int, Float, Bool

                config_ops = {}

                for param in flow_params:
                    param_name = param['name']
                    param_type_dagster = String

                    if param.get('type_annotation'):
                        type_str = param['type_annotation']
                        if type_str == 'int':
                            param_type_dagster = Int
                        elif type_str == 'float':
                            param_type_dagster = Float
                        elif type_str == 'bool':
                            param_type_dagster = Bool

                    config_schema_single = {
                        param_name: Field(param_type_dagster, default_value=param.get('default'))
                        if param.get('default') is not None
                        else Field(param_type_dagster)
                    }

                    def make_config_op(p_name):
                        @dagster_op(
                            name=f"{flow_name}_get_{p_name}",
                            config_schema={p_name: config_schema_single[p_name]}
                        )
                        def config_op(context):
                            return context.op_config[p_name]
                        return config_op

                    config_ops[param_name] = make_config_op(param_name)

                    if param.get('default') is not None:
                        op_config[f"{flow_name}_get_{param_name}"] = {
                            "config": {param_name: param['default']}
                        }

                # Rewrite flow to use config ops
                flow_source = textwrap.dedent(inspect.getsource(flow_func))
                tree = ast.parse(flow_source)
                flow_def = tree.body[0]

                param_names = [p['name'] for p in flow_params]

                # Build new function body with config op calls
                new_body = []
                for param_name in param_names:
                    new_body.append(
                        ast.Assign(
                            targets=[ast.Name(id=param_name, ctx=ast.Store())],
                            value=ast.Call(
                                func=ast.Name(id=f"get_{param_name}_config", ctx=ast.Load()),
                                args=[],
                                keywords=[]
                            )
                        )
                    )

                # Add original function body (but strip return statements for jobs)
                for stmt in flow_def.body:
                    # Skip return statements - jobs don't return values
                    if not isinstance(stmt, ast.Return):
                        new_body.append(stmt)

                # Jobs don't need to return a value (unlike graph assets)
                # We've stripped any existing return statements above

                # Create new function with no parameters
                new_func_def = ast.FunctionDef(
                    name=flow_def.name,
                    args=ast.arguments(
                        posonlyargs=[],
                        args=[],
                        kwonlyargs=[],
                        kw_defaults=[],
                        defaults=[]
                    ),
                    body=new_body,
                    decorator_list=[],
                    returns=flow_def.returns
                )

                # Compile and execute
                new_module = ast.Module(body=[new_func_def], type_ignores=[])
                ast.fix_missing_locations(new_module)

                namespace = dict(module.__dict__)
                for param_name in param_names:
                    namespace[f"get_{param_name}_config"] = config_ops[param_name]

                exec(compile(new_module, '<generated>', 'exec'), namespace)
                flow_func = namespace[flow_def.name]

                logger.info(f"✅ Rewrote flow {flow_name} with config op pattern for job")
            else:
                # Flow has no parameters, but we still need to strip return statements for jobs
                logger.info(f"Flow {flow_name} has no parameters - stripping return statements for job")

                flow_source = textwrap.dedent(inspect.getsource(flow_func))
                tree = ast.parse(flow_source)
                flow_def = tree.body[0]

                # Strip return statements from function body
                new_body = []
                for stmt in flow_def.body:
                    if not isinstance(stmt, ast.Return):
                        new_body.append(stmt)

                # Create new function without return statements
                new_func_def = ast.FunctionDef(
                    name=flow_def.name,
                    args=flow_def.args,
                    body=new_body if new_body else [ast.Pass()],  # Add pass if body is empty
                    decorator_list=[],
                    returns=None  # No return type for jobs
                )

                # Compile and execute
                new_module = ast.Module(body=[new_func_def], type_ignores=[])
                ast.fix_missing_locations(new_module)

                namespace = dict(module.__dict__)
                exec(compile(new_module, '<generated>', 'exec'), namespace)
                flow_func = namespace[flow_def.name]

                logger.info(f"✅ Stripped return statements from flow {flow_name} for job")

            # Use metadata description, fallback to flow docstring, then default
            job_description = metadata.description or flow_info.get('docstring') or f"Prefect flow (monkey patched job): {flow_name}"

            # Build job kwargs
            job_kwargs = {
                'name': f"script_{script_info.name}",
                'tags': job_tags,
                'description': job_description,
            }

            # Add config if we have it
            if op_config:
                job_kwargs['config'] = op_config
                logger.info(f"✅ Added config to job: {op_config}")

            # Wrap the flow function with @job
            job_decorated = dagster_job(**job_kwargs)(flow_func)

            return job_decorated

        except Exception as e:
            logger.debug(f"Monkey patch job approach failed for {flow_name}: {e} (falling back to subprocess mode)")
            return None

        finally:
            # Restore original prefect module
            if original_prefect is not None:
                sys.modules['prefect'] = original_prefect
            elif 'prefect' in sys.modules:
                del sys.modules['prefect']

    def create_graph_asset(
        self,
        flow_info: Dict,
        tasks_info: List[Dict],
        script_info: Any,
        metadata: Any,
        repo_path: str,
        dependencies: Optional[List[str]] = None,
        asset_prefix: str = "prefect"
    ):
        """Create a graph-backed asset for a Prefect flow.

        Args:
            dependencies: List of asset names this flow depends on (for lineage)
            asset_prefix: Prefix for the asset name (default: "prefect")
        """
        flow_name = flow_info['name']
        task_calls = flow_info['task_calls']
        has_complex_patterns = flow_info['has_complex_patterns']
        flow_params = flow_info.get('parameters', [])

        # Try monkey patch approach first - this works for all flows regardless of complexity!
        monkey_patched_asset = self.try_monkey_patch_approach(
            flow_info, tasks_info, script_info, metadata, repo_path, dependencies, asset_prefix
        )
        if monkey_patched_asset is not None:
            return monkey_patched_asset

        # If monkey patch failed, fall back to the manual approach with limitations
        logger.info(f"Falling back to manual op wrapping approach for flow: {flow_name}")

        # If flow has complex patterns, return None to fall back to subprocess
        if has_complex_patterns:
            logger.info(f"Flow {flow_name} has complex patterns (.map()), falling back to subprocess")
            return None

        # If flow has parameters, fall back to subprocess for now
        # TODO: Support config via graph_asset config parameter
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

        # Detect resources from Prefect flow script (for kinds and tags)
        from ..utils.resource_detector import ResourceDetector
        detected_resources = []
        try:
            detected_resources = ResourceDetector.detect_resources_from_file(script_info.script_path)
            if detected_resources:
                resource_names = [r['resource_name'] for r in detected_resources]
                logger.info(f"🔧 Detected resources in Prefect flow: {', '.join(resource_names)}")
        except Exception as e:
            logger.debug(f"Could not detect resources from {script_info.script_path}: {e}")

        # Build asset tags
        asset_tags = {
            **metadata.tags,
            "script_type": "prefect_mapped",
            "script_name": script_info.name,
            "prefect_flow": flow_name,
            "dagster/kind/prefect": "",  # Prefect framework kind
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
            logger.warning(f"No ops created for flow {flow_name}")
            return None

        # Create graph asset that explicitly calls ops in sequence
        # Generate the graph function dynamically to support any number of tasks

        # Build the function body code as a string
        graph_body_lines = []
        graph_body_lines.append("    \"\"\"Execute Prefect flow as graph of ops.\"\"\"")

        if len(ops_list) == 1:
            graph_body_lines.append("    return ops_list[0]()")
        else:
            graph_body_lines.append("    result = ops_list[0]()")
            for i in range(1, len(ops_list) - 1):
                graph_body_lines.append(f"    result = ops_list[{i}](result)")
            graph_body_lines.append(f"    return ops_list[{len(ops_list) - 1}](result)")

        graph_body = "\n".join(graph_body_lines)

        # Create the function using exec
        func_code = f"def flow_graph():\n{graph_body}"
        local_vars = {'ops_list': ops_list}
        exec(func_code, local_vars)
        flow_graph_func = local_vars['flow_graph']

        # Use metadata description, fallback to flow docstring, then default
        description = metadata.description or flow_info.get('docstring') or f"Prefect flow: {flow_name}"

        # Build graph_asset kwargs
        graph_asset_kwargs = {
            'name': f"{asset_prefix}_{script_info.name}",
            'group_name': metadata.group_name,
            'tags': asset_tags,
            'description': description,
        }

        # Add dependencies if provided (for lineage)
        if dependencies:
            from dagster import AssetKey
            graph_asset_kwargs['deps'] = [AssetKey(dep) for dep in dependencies]
            logger.info(f"✅ Added dependencies to graph_asset (manual approach): {dependencies}")

        # Apply the graph_asset decorator
        decorated_flow = graph_asset(**graph_asset_kwargs)(flow_graph_func)

        return decorated_flow

    def create_job(
        self,
        flow_info: Dict,
        tasks_info: List[Dict],
        script_info: Any,
        metadata: Any,
        repo_path: str
    ):
        """Create an op job for a Prefect flow.

        Similar to create_graph_asset but creates a @job instead of @graph_asset.
        Jobs are for workflows that do work but don't produce persistent assets.
        """
        flow_name = flow_info['name']

        # Try monkey patch approach - convert @flow to @job
        monkey_patched_job = self.try_monkey_patch_job_approach(
            flow_info, tasks_info, script_info, metadata, repo_path
        )
        if monkey_patched_job is not None:
            return monkey_patched_job

        # If monkey patch failed, fall back to subprocess
        logger.info(f"Falling back to subprocess for job: {flow_name}")
        return None
