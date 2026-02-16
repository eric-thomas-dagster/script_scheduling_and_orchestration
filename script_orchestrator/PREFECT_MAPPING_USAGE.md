# Using Prefect → Dagster Mapping

## Quick Start

### 1. Enable Mapping in YAML

For any Prefect flow script, add this to the YAML metadata:

```yaml
# my_prefect_flow.yaml
enabled: true
script_type: prefect
description: "My Prefect flow with task-level visibility"

# Enable Prefect → Dagster mapping
prefect_mapping:
  enabled: true
  fallback_on_error: true  # If parsing fails, run as subprocess
  mode: "graph_asset"
```

### 2. View in Dagster UI

1. Start the dev server:
   ```bash
   uv run dg dev
   ```

2. Navigate to **Assets** in the UI

3. Find your flow asset (e.g., `script_my_prefect_flow`)

4. Click on the asset to see:
   - **Lineage** tab: Task dependencies
   - **Definition** tab: Code reference
   - **Materialize** button: Run the flow

### 3. See Task-Level Execution

When you materialize the asset:

1. **Execution Graph** shows all tasks as separate ops
2. Click on each op to see:
   - Individual logs
   - Execution time
   - Status (success/failure)
3. If a task fails:
   - See exactly which task failed
   - Re-execute from that task
   - View task-specific error messages

## Example: Before & After

### Before (Black Box)

```
Dagster UI:
┌─────────────────────────────┐
│ script_prefect_flow_example │
│                             │
│   [Single asset node]       │
│                             │
│   Logs show entire flow     │
│   output mixed together     │
└─────────────────────────────┘
```

### After (Task Visibility)

```
Dagster UI - Execution Graph:
┌─────────────────────────────────────────┐
│ script_prefect_flow_example             │
│                                         │
│  ┌──────────────┐                      │
│  │ fetch_data   │                      │
│  │              │                      │
│  │ ✅ 2.3s      │                      │
│  └──────┬───────┘                      │
│         │                               │
│         ▼                               │
│  ┌──────────────┐                      │
│  │process_data  │                      │
│  │              │                      │
│  │ ✅ 5.1s      │                      │
│  └──────┬───────┘                      │
│         │                               │
│         ▼                               │
│  ┌──────────────┐                      │
│  │save_results  │                      │
│  │              │                      │
│  │ ✅ 1.2s      │                      │
│  └──────────────┘                      │
│                                         │
│  Total: 8.6s                           │
└─────────────────────────────────────────┘
```

## Features

### ✅ What Works
- Parse @task and @flow decorators via AST
- Map tasks to Dagster ops
- Map flows to graph-backed assets
- Preserve task dependencies
- Show ops in execution graph
- Individual op logs and timing
- Re-execution from any op
- Compatible with schedules and retries

### ⚠️ Current Limitations
- Only supports static task definitions
- Doesn't handle dynamic task generation
- Parameters must be passed positionally
- Doesn't preserve Prefect-specific metadata (yet)

### 🔄 Fallback Behavior
If parsing fails (enabled via `fallback_on_error: true`):
- Falls back to subprocess execution
- Runs as regular asset (no op visibility)
- Logs warning message
- No breaking changes

## Supported Prefect Patterns

### ✅ Supported
```python
@task
def my_task(input_data):
    return process(input_data)

@flow
def my_flow():
    data = my_task(some_input)
    return data
```

### ⚠️ Not Yet Supported
```python
@task
def my_task():
    pass

@flow
def my_flow():
    # Dynamic task generation
    for i in range(n):
        my_task.submit()
```

## Troubleshooting

### No Ops Showing in UI
1. Check `prefect_mapping.enabled: true` in YAML
2. Verify script has `@task` and `@flow` decorators
3. Check logs for parsing errors
4. Ensure `fallback_on_error: true` if you want graceful fallback

### Op Execution Fails
1. Check op logs for error details
2. Verify Prefect task function can be imported
3. Check parameter mapping is correct
4. Test original Prefect flow works standalone

### Schedule Not Working
- Schedules should work automatically
- Asset name remains `script_{name}`
- Schedule targets correct asset key

## Migration Strategy

### Step 1: Enable Visibility
```yaml
prefect_mapping:
  enabled: true
  fallback_on_error: true
```
- **Goal**: See task structure
- **Risk**: Low (fallback enabled)
- **Benefit**: Understand flow complexity

### Step 2: Optimize Based on Insights
- Identify long-running tasks
- Find failure points
- Understand dependencies
- Plan optimization

### Step 3: Gradually Migrate
- Convert simple tasks to native Dagster first
- Keep complex tasks as Prefect
- Use hybrid approach
- Remove Prefect dependency when ready

## API Reference

### YAML Configuration

```yaml
prefect_mapping:
  enabled: bool              # Enable mapping (default: false)
  fallback_on_error: bool    # Fallback to subprocess on error (default: true)
  mode: string              # "graph_asset" (default and recommended)
```

### Component Methods

```python
# In script_github_component.py

_parse_prefect_flow(script_path: Path) -> Tuple[List[Dict], List[Dict]]
    # Parse Prefect file to extract tasks and flows

_create_prefect_task_op(task_info: Dict, script_info: ScriptInfo, repo_path: str) -> Op
    # Create Dagster op for Prefect task

_create_prefect_flow_graph_asset(...) -> GraphBackedAsset
    # Create graph-backed asset for Prefect flow

_create_prefect_graph_asset(script_info: ScriptInfo, metadata: ScriptMetadata, repo_path: str) -> Optional[GraphBackedAsset]
    # Main orchestration method
```

## Examples

### Simple ETL Flow
```python
# prefect_etl.py
@task
def extract():
    return fetch_from_api()

@task
def transform(data):
    return clean(data)

@task
def load(data):
    save_to_db(data)

@flow
def etl_flow():
    data = extract()
    cleaned = transform(data)
    load(cleaned)
```

```yaml
# prefect_etl.yaml
script_type: prefect
prefect_mapping:
  enabled: true
```

Result: 3 ops visible in Dagster UI!

### With Dependencies
```python
# prefect_pipeline.py
@task
def step1():
    return initial_data()

@task
def step2(data):
    return process(data)

@task
def step3(data):
    return finalize(data)

@flow
def pipeline():
    d1 = step1()
    d2 = step2(d1)
    d3 = step3(d2)
    return d3
```

Dagster execution graph shows linear pipeline: `step1 → step2 → step3`

## Next Steps

1. **Try it**: Enable for one Prefect flow
2. **Observe**: Look at execution graph
3. **Optimize**: Identify bottlenecks
4. **Migrate**: Gradually convert to native Dagster
5. **Share**: Help improve the prototype!

## Feedback

Found a bug? Have a feature request?
- Check implementation in `script_github_component.py`
- Review design docs: `PREFECT_MAPPING_PROTOTYPE.md`
- See results: `PREFECT_MAPPING_PROTOTYPE_RESULTS.md`
