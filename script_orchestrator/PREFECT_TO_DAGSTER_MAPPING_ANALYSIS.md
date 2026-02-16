# Prefect → Dagster Mapping Analysis

## Goal
Map existing Prefect flows (@task, @flow) to Dagster concepts (@op, @asset) **without modifying the original Prefect code**, to gain:
- ✅ Visibility into flow structure in Dagster UI
- ✅ Task-level observability
- ✅ Dagster features (retries, metadata, lineage)
- ✅ Better migration path

## Current State

### Prefect Flow (prefect_flow_example.py)
```python
@task
def fetch_data():
    # Fetch from API
    return data

@task
def process_data(data):
    # Process data
    return processed

@task
def save_results(results):
    # Save to storage
    return final

@flow(name="data-processing-flow")
def data_processing_flow():
    data = fetch_data()
    results = process_data(data)
    final = save_results(results)
    return final
```

### Current Dagster Integration
```python
@asset
def script_prefect_flow_example():
    # Black box - just runs subprocess
    subprocess.run(["python", "prefect_flow_example.py"])
    # Dagster sees: one asset, no internal visibility
```

**Problem:** Dagster has NO visibility into:
- Individual tasks (fetch_data, process_data, save_results)
- Task dependencies
- Task-level failures
- Individual task metadata

## Proposed Approaches

### Approach 1: Static AST Parsing (No Import)

**How it works:**
1. Parse the Python file with `ast` module
2. Find all @task and @flow decorated functions
3. Analyze function calls to infer dependencies
4. Generate Dagster @ops and @asset dynamically

**Pros:**
- ✅ No need to import Prefect
- ✅ Works without Prefect installed
- ✅ Fast - just parse the file

**Cons:**
- ❌ Limited - only sees static structure
- ❌ Can't handle dynamic task generation
- ❌ Hard to infer complex dependencies
- ❌ Doesn't capture Prefect-specific metadata

**Example Implementation:**
```python
import ast

def parse_prefect_flow(file_path):
    with open(file_path) as f:
        tree = ast.parse(f.read())

    tasks = []
    flows = []

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            # Check for @task decorator
            if any(d.id == 'task' for d in node.decorator_list if isinstance(d, ast.Name)):
                tasks.append({
                    'name': node.name,
                    'args': [arg.arg for arg in node.args.args]
                })
            # Check for @flow decorator
            if any(d.id == 'flow' for d in node.decorator_list if isinstance(d, ast.Name)):
                flows.append({
                    'name': node.name,
                    'calls': extract_function_calls(node)
                })

    return tasks, flows
```

### Approach 2: Runtime Introspection (Import & Inspect)

**How it works:**
1. Import the Prefect flow module
2. Introspect the flow object to get tasks
3. Use Prefect's internal APIs to get metadata
4. Dynamically create Dagster ops that wrap tasks

**Pros:**
- ✅ Full access to Prefect metadata
- ✅ Handles dynamic flows
- ✅ Can access task configs (retries, etc.)
- ✅ Most accurate representation

**Cons:**
- ❌ Requires Prefect installed
- ❌ Slower - imports and runs Prefect code
- ❌ Prefect 1.x vs 2.x compatibility issues
- ❌ May trigger Prefect side effects

**Example Implementation:**
```python
import importlib.util
from prefect import flow, task

def introspect_prefect_flow(file_path):
    # Import the module
    spec = importlib.util.spec_from_file_location("prefect_module", file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Find the flow function
    flow_func = None
    for name in dir(module):
        obj = getattr(module, name)
        if hasattr(obj, '__wrapped__') and isinstance(obj, flow):
            flow_func = obj
            break

    # Get tasks from flow
    if flow_func:
        # Prefect 2.x: flow_func.task_runners or similar
        # Prefect 1.x: flow_func.tasks
        tasks = get_tasks_from_flow(flow_func)
        return tasks
```

### Approach 3: Hybrid AST + Wrapper Execution

**How it works:**
1. Parse file with AST to get structure
2. Create Dagster ops that import & call original Prefect tasks
3. Ops execute the actual Prefect task functions
4. Preserve Prefect functionality while adding Dagster observability

**Pros:**
- ✅ Balance of static analysis and runtime execution
- ✅ Works with or without Prefect (graceful fallback)
- ✅ Dagster UI shows task breakdown
- ✅ Each task gets Dagster metadata

**Cons:**
- ❌ More complex implementation
- ❌ Requires careful handling of imports
- ❌ Need to manage state between ops

**Example Implementation:**
```python
def generate_dagster_ops_from_prefect_tasks(file_path):
    # Parse AST
    tasks = parse_prefect_tasks(file_path)

    # Generate Dagster ops dynamically
    ops = []
    for task_info in tasks:
        @op(name=f"prefect_task_{task_info['name']}")
        def task_op(context):
            # Import and execute the original Prefect task
            module = import_prefect_module(file_path)
            task_func = getattr(module, task_info['name'])
            result = task_func()
            return result

        ops.append(task_op)

    return ops
```

### Approach 4: Prefect-Dagster Integration Layer

**How it works:**
1. Create a bridge component that understands both Prefect and Dagster
2. At runtime, intercept Prefect task execution
3. Emit Dagster events for each task
4. Use Prefect's hooks or middleware

**Pros:**
- ✅ Most native integration
- ✅ Real-time task visibility
- ✅ Preserves all Prefect features
- ✅ Could be reusable for any Prefect flow

**Cons:**
- ❌ Most complex to implement
- ❌ Requires deep understanding of both systems
- ❌ May be fragile with Prefect updates
- ❌ Performance overhead

## Recommended Approach: Hybrid AST + Wrapper Execution

### Why This Works Best

1. **Static Discovery (AST)**:
   - Parse file to find @task and @flow decorators
   - Infer task dependencies from function calls
   - Extract task names and structure
   - No Prefect import needed for discovery

2. **Dynamic Execution (Wrapper Ops)**:
   - Create Dagster @op for each Prefect @task
   - Each op imports and calls the original task
   - Preserves Prefect functionality
   - Adds Dagster observability

3. **Graceful Fallback**:
   - If parsing fails → run as single asset (current behavior)
   - If Prefect not installed → mock decorators work
   - Progressive enhancement, not breaking change

### What User Sees in Dagster UI

**Before (Current):**
```
Assets:
  └── script_prefect_flow_example (black box)
```

**After (With Mapping):**
```
Assets:
  └── prefect_flow__data_processing_flow
       ├── prefect_task__fetch_data
       ├── prefect_task__process_data (depends on fetch_data)
       └── prefect_task__save_results (depends on process_data)
```

### Benefits

1. **Visibility**: See all tasks in Dagster UI
2. **Observability**: Metadata for each task
3. **Debugging**: Know which task failed
4. **Lineage**: Track data flow through tasks
5. **Migration**: Easy to convert task-by-task to native Dagster

### Implementation Strategy

```python
class PrefectFlowComponent:
    def discover_tasks(self, flow_file):
        """Parse AST to find tasks and flows."""
        tasks = parse_ast_for_tasks(flow_file)
        return tasks

    def create_dagster_ops(self, tasks):
        """Generate Dagster ops that wrap Prefect tasks."""
        ops = []
        for task_info in tasks:
            op = self.create_task_wrapper_op(task_info)
            ops.append(op)
        return ops

    def create_task_wrapper_op(self, task_info):
        """Create a Dagster op that executes a Prefect task."""
        @op(name=f"prefect_task__{task_info['name']}")
        def task_wrapper(context, **kwargs):
            # Import the original Prefect module
            module = import_module(task_info['module_path'])
            task_func = getattr(module, task_info['name'])

            # Execute the task
            result = task_func(**kwargs)

            # Emit Dagster metadata
            context.add_output_metadata({
                'task_name': task_info['name'],
                'prefect_task': True
            })

            return result

        return task_wrapper

    def create_flow_asset(self, flow_info, ops):
        """Create Dagster asset that represents the Prefect flow."""
        @asset(name=f"prefect_flow__{flow_info['name']}")
        def flow_asset(context, **task_results):
            # Compose the ops to match flow structure
            # Or just run the original flow
            return execute_flow(flow_info, task_results)

        return flow_asset
```

## Example: prefect_flow_example.py Mapping

### Original Prefect Flow
```python
@task
def fetch_data():
    return {"records": [...]}

@task
def process_data(data):
    return {"total": ...}

@task
def save_results(results):
    return results

@flow
def data_processing_flow():
    data = fetch_data()
    results = process_data(data)
    final = save_results(results)
    return final
```

### Generated Dagster Representation
```python
@op(name="prefect_task__fetch_data")
def fetch_data_op(context):
    from prefect_flow_example import fetch_data
    result = fetch_data()
    context.add_output_metadata({'prefect_task': 'fetch_data'})
    return result

@op(name="prefect_task__process_data")
def process_data_op(context, fetch_data_result):
    from prefect_flow_example import process_data
    result = process_data(fetch_data_result)
    context.add_output_metadata({'prefect_task': 'process_data'})
    return result

@op(name="prefect_task__save_results")
def save_results_op(context, process_data_result):
    from prefect_flow_example import save_results
    result = save_results(process_data_result)
    context.add_output_metadata({'prefect_task': 'save_results'})
    return result

@asset(
    name="prefect_flow__data_processing_flow",
    deps={
        'fetch_data': fetch_data_op,
        'process_data': process_data_op,
        'save_results': save_results_op
    }
)
def data_processing_flow_asset(context):
    # Execute the composed ops
    data = fetch_data_op()
    results = process_data_op(data)
    final = save_results_op(results)
    return final
```

## Challenges & Solutions

### Challenge 1: Task Dependencies
**Problem:** How to infer which tasks depend on which?

**Solution:**
- Parse function body AST to see function calls
- Match call args to task return values
- Use variable name tracking

### Challenge 2: State Management
**Problem:** Prefect tasks pass data directly; Dagster ops need explicit ins/outs

**Solution:**
- Parse task parameters to determine inputs
- Create OpExecutionContext for each task
- Use Dagster's IO manager for state

### Challenge 3: Prefect-Specific Features
**Problem:** Retries, caching, etc. are Prefect-specific

**Solution:**
- Extract from AST decorator arguments
- Map to equivalent Dagster features
- Document non-mappable features

### Challenge 4: Dynamic Flows
**Problem:** Prefect flows can generate tasks dynamically

**Solution:**
- Static analysis won't capture this
- Add runtime discovery option
- Fall back to single-asset for complex flows

## Configuration Options

```yaml
# prefect_flow_example.yaml
enabled: true
script_type: prefect
description: "Prefect flow with task-level visibility"

# NEW: Enable task-level mapping
prefect_mapping:
  enabled: true  # Map tasks to Dagster ops
  mode: "hybrid"  # ast, runtime, or hybrid
  fallback_on_error: true  # If mapping fails, run as single asset
  preserve_prefect_retries: true  # Map Prefect retries to Dagster
  emit_task_metadata: true  # Add metadata for each task
```

## Migration Path

### Phase 1: Visibility (This Analysis)
- See Prefect tasks in Dagster UI
- Observability without code changes
- Understand flow structure

### Phase 2: Hybrid Execution
- Some tasks as Prefect
- Some tasks as native Dagster
- Gradual migration

### Phase 3: Full Native
- All tasks converted to Dagster assets/ops
- Remove Prefect dependency
- Full Dagster features

## Next Steps

1. **Prototype AST Parser**
   - Parse prefect_flow_example.py
   - Extract tasks and flow structure
   - Generate dependency graph

2. **Build Wrapper Generator**
   - Create Dagster ops for each task
   - Handle task dependencies
   - Test with example flow

3. **Integrate with Component**
   - Add to ScriptGithubComponent
   - Make it opt-in via YAML config
   - Test with multiple Prefect flows

4. **Validate in UI**
   - Ensure tasks show up correctly
   - Verify metadata is captured
   - Test materialization

Would you like me to proceed with a prototype implementation?
