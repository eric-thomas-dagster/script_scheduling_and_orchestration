# Prefect → Dagster Mapping Prototype

## Correct Approach: @graph_asset

You're absolutely right! To make Prefect tasks visible in the Dagster execution graph, we need:
- **@op** for each Prefect @task
- **@graph_asset** for the Prefect @flow (not @asset with deps)

### Why @graph_asset?

```python
# ❌ WRONG: @asset with deps
# Tasks are dependencies, but internal structure is hidden
@asset(deps=[task1_op, task2_op, task3_op])
def my_flow():
    # Dagster UI shows: single node
    pass

# ✅ CORRECT: @graph_asset
# Tasks are ops composed inside, graph shows all ops
@graph_asset
def my_flow():
    result1 = task1_op()
    result2 = task2_op(result1)
    return task3_op(result2)
    # Dagster UI shows: 3 op nodes with connections
```

## Prototype Design

### Input: Prefect Flow
```python
# prefect_flow_example.py
@task
def fetch_data():
    return {"records": [...]"}

@task
def process_data(data):
    return {"total": ...}

@task
def save_results(results):
    return results

@flow(name="data-processing-flow")
def data_processing_flow():
    data = fetch_data()
    results = process_data(data)
    final = save_results(results)
    return final
```

### Output: Generated Dagster Code
```python
import dagster as dg
from importlib import import_module

# Import the original Prefect module
prefect_module = import_module('prefect_flow_example')

# Generate @op for each @task
@dg.op(name="fetch_data")
def fetch_data_op(context: dg.OpExecutionContext):
    """Wrapper for Prefect task: fetch_data"""
    result = prefect_module.fetch_data()
    context.log.info(f"Prefect task 'fetch_data' completed")
    return result

@dg.op(name="process_data")
def process_data_op(context: dg.OpExecutionContext, data):
    """Wrapper for Prefect task: process_data"""
    result = prefect_module.process_data(data)
    context.log.info(f"Prefect task 'process_data' completed")
    return result

@dg.op(name="save_results")
def save_results_op(context: dg.OpExecutionContext, results):
    """Wrapper for Prefect task: save_results"""
    result = prefect_module.save_results(results)
    context.log.info(f"Prefect task 'save_results' completed")
    return result

# Generate @graph_asset for the @flow
@dg.graph_asset(name="data_processing_flow")
def data_processing_flow_graph():
    """
    Graph-backed asset representing Prefect flow: data-processing-flow

    This preserves the task structure from the original Prefect flow.
    Each task runs as a separate op, visible in the Dagster execution graph.
    """
    data = fetch_data_op()
    results = process_data_op(data)
    final = save_results_op(results)
    return final
```

### What User Sees in Dagster UI

**Execution Graph:**
```
┌─────────────────────────────────────┐
│ Asset: data_processing_flow         │
├─────────────────────────────────────┤
│                                     │
│  ┌──────────────┐                  │
│  │ fetch_data   │                  │
│  └──────┬───────┘                  │
│         │                           │
│         ▼                           │
│  ┌──────────────┐                  │
│  │process_data  │                  │
│  └──────┬───────┘                  │
│         │                           │
│         ▼                           │
│  ┌──────────────┐                  │
│  │save_results  │                  │
│  └──────────────┘                  │
│                                     │
└─────────────────────────────────────┘
```

**Benefits:**
- ✅ Click on each op to see logs
- ✅ Re-execute from any op boundary
- ✅ See which specific task failed
- ✅ Task-level execution times
- ✅ Retry individual tasks

## Implementation Steps

### Step 1: AST Parser
Parse Prefect file to extract task/flow structure:

```python
import ast
from typing import List, Dict, Any

class PrefectTaskInfo:
    def __init__(self, name: str, params: List[str], returns_value: bool):
        self.name = name
        self.params = params
        self.returns_value = returns_value

class PrefectFlowInfo:
    def __init__(self, name: str, task_calls: List[Dict]):
        self.name = name
        self.task_calls = task_calls  # [{task: 'fetch_data', args: [], result_var: 'data'}]

def parse_prefect_flow(file_path: str):
    """Parse Prefect file to extract tasks and flow structure."""
    with open(file_path, 'r') as f:
        tree = ast.parse(f.read())

    tasks = []
    flows = []

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            # Check for @task decorator
            if has_decorator(node, 'task'):
                task_info = PrefectTaskInfo(
                    name=node.name,
                    params=[arg.arg for arg in node.args.args],
                    returns_value=has_return_statement(node)
                )
                tasks.append(task_info)

            # Check for @flow decorator
            elif has_decorator(node, 'flow'):
                flow_info = PrefectFlowInfo(
                    name=node.name,
                    task_calls=extract_task_calls(node)
                )
                flows.append(flow_info)

    return tasks, flows

def has_decorator(func_node: ast.FunctionDef, decorator_name: str) -> bool:
    """Check if function has a specific decorator."""
    for decorator in func_node.decorator_list:
        if isinstance(decorator, ast.Name) and decorator.id == decorator_name:
            return True
        if isinstance(decorator, ast.Call):
            if isinstance(decorator.func, ast.Name) and decorator.func.id == decorator_name:
                return True
    return False

def extract_task_calls(flow_node: ast.FunctionDef) -> List[Dict]:
    """Extract task calls and their dependencies from flow body."""
    calls = []
    for stmt in ast.walk(flow_node):
        if isinstance(stmt, ast.Assign):
            # Look for: data = fetch_data()
            if isinstance(stmt.value, ast.Call):
                if isinstance(stmt.value.func, ast.Name):
                    task_name = stmt.value.func.id
                    result_var = stmt.targets[0].id if stmt.targets else None
                    args = [arg.id for arg in stmt.value.args if isinstance(arg, ast.Name)]

                    calls.append({
                        'task': task_name,
                        'args': args,
                        'result_var': result_var
                    })
    return calls
```

### Step 2: Dagster Op Generator
Generate @op for each Prefect @task:

```python
from textwrap import dedent

def generate_op_code(task_info: PrefectTaskInfo, module_name: str) -> str:
    """Generate Dagster @op code for a Prefect @task."""

    # Build parameter list
    params = ["context: dg.OpExecutionContext"]
    params.extend(task_info.params)
    params_str = ", ".join(params)

    # Build function call
    args_str = ", ".join(task_info.params)

    code = dedent(f'''
    @dg.op(name="{task_info.name}")
    def {task_info.name}_op({params_str}):
        """Wrapper for Prefect task: {task_info.name}"""
        result = {module_name}.{task_info.name}({args_str})
        context.log.info(f"Prefect task '{task_info.name}' completed")
        {"return result" if task_info.returns_value else ""}
    ''')

    return code.strip()
```

### Step 3: Graph Asset Generator
Generate @graph_asset for the Prefect @flow:

```python
def generate_graph_asset_code(flow_info: PrefectFlowInfo) -> str:
    """Generate Dagster @graph_asset code for a Prefect @flow."""

    # Build the op calls based on task_calls
    body_lines = []
    for call in flow_info.task_calls:
        op_name = f"{call['task']}_op"
        args = ", ".join(call['args'])

        if call['result_var']:
            body_lines.append(f"    {call['result_var']} = {op_name}({args})")
        else:
            body_lines.append(f"    {op_name}({args})")

    # Return the last variable
    last_var = flow_info.task_calls[-1]['result_var'] if flow_info.task_calls else None
    if last_var:
        body_lines.append(f"    return {last_var}")

    body = "\n".join(body_lines)

    code = dedent(f'''
    @dg.graph_asset(name="{flow_info.name}")
    def {flow_info.name}_graph():
        """
        Graph-backed asset representing Prefect flow: {flow_info.name}

        This preserves the task structure from the original Prefect flow.
        Each task runs as a separate op, visible in the Dagster execution graph.
        """
{body}
    ''')

    return code.strip()
```

### Step 4: Integration with Component

Add to `ScriptGithubComponent`:

```python
def _create_prefect_graph_asset(self, script_info: ScriptInfo, metadata: ScriptMetadata):
    """Create graph-backed asset from Prefect flow (if enabled)."""

    # Check if Prefect mapping is enabled
    if not metadata.prefect_mapping or not metadata.prefect_mapping.get('enabled'):
        # Fall back to regular subprocess asset
        return self._create_regular_script_asset(script_info, metadata)

    try:
        # Parse the Prefect flow
        tasks, flows = parse_prefect_flow(script_info.script_path)

        if not flows:
            # No flow found, fall back
            return self._create_regular_script_asset(script_info, metadata)

        # Generate ops dynamically
        ops = self._generate_prefect_ops(tasks, script_info)

        # Generate graph asset
        graph_asset = self._generate_prefect_graph_asset(flows[0], ops, script_info)

        return graph_asset

    except Exception as e:
        logger.warning(f"Failed to parse Prefect flow {script_info.name}: {e}")
        if metadata.prefect_mapping.get('fallback_on_error', True):
            return self._create_regular_script_asset(script_info, metadata)
        raise

def _generate_prefect_ops(self, tasks: List[PrefectTaskInfo], script_info: ScriptInfo):
    """Generate Dagster ops for Prefect tasks."""
    ops = {}

    # Import the Prefect module
    module_path = script_info.script_path
    module_name = script_info.name

    for task in tasks:
        @op(name=task.name)
        def task_op(context, **kwargs):
            # Dynamically import and execute
            import importlib.util
            spec = importlib.util.spec_from_file_location(module_name, module_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            task_func = getattr(module, task.name)
            result = task_func(**kwargs)

            context.log.info(f"Prefect task '{task.name}' completed")
            return result

        ops[task.name] = task_op

    return ops

def _generate_prefect_graph_asset(self, flow: PrefectFlowInfo, ops: Dict, script_info: ScriptInfo):
    """Generate graph-backed asset for Prefect flow."""

    @graph_asset(name=f"prefect_flow__{flow.name}")
    def flow_graph():
        # Build the execution graph based on parsed flow structure
        variables = {}
        for call in flow.task_calls:
            op = ops[call['task']]
            args = {arg: variables[arg] for arg in call['args'] if arg in variables}
            result = op(**args)
            if call['result_var']:
                variables[call['result_var']] = result

        # Return last result
        return result

    return flow_graph
```

## Configuration

```yaml
# prefect_flow_example.yaml
enabled: true
script_type: prefect
description: "Prefect flow with task-level visibility"

# Enable Prefect → Dagster mapping
prefect_mapping:
  enabled: true
  fallback_on_error: true  # If parsing fails, run as subprocess
  mode: "graph_asset"  # Use @graph_asset (recommended)
```

## Testing the Prototype

1. **Parse the flow**:
```python
tasks, flows = parse_prefect_flow('prefect_flow_example.py')
print(f"Found {len(tasks)} tasks: {[t.name for t in tasks]}")
print(f"Found {len(flows)} flows: {[f.name for f in flows]}")
```

2. **Generate ops**:
```python
for task in tasks:
    print(generate_op_code(task, 'prefect_flow_example'))
```

3. **Generate graph asset**:
```python
print(generate_graph_asset_code(flows[0]))
```

4. **Load in Dagster**:
```bash
uv run dg dev
# Navigate to assets
# See: prefect_flow__data_processing_flow with 3 ops inside
```

## Expected Output

**In Dagster UI:**
- Asset name: `prefect_flow__data_processing_flow`
- When you click "Materialize", you see the execution graph
- Graph shows 3 ops: fetch_data → process_data → save_results
- Each op has its own logs, timing, metadata
- Can re-execute from any op boundary

**Console output:**
```
2026-02-16 - dagster - INFO - Prefect task 'fetch_data' completed
2026-02-16 - dagster - INFO - Prefect task 'process_data' completed
2026-02-16 - dagster - INFO - Prefect task 'save_results' completed
```

## Next Step

Ready to implement this? I can:
1. Create the AST parser
2. Build the op/graph_asset generator
3. Integrate with ScriptGithubComponent
4. Test with prefect_flow_example.py

This will give you **true visibility** into your Prefect flows without touching the original code!
