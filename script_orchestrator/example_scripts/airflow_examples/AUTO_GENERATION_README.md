# Auto-Generation from dag-factory YAML

## What Was Implemented

The `script_github_component` now **automatically generates** Dagster constructs (assets, asset jobs, op jobs, and sensors) from Astronomer dag-factory YAML files.

No manual `.py` files needed! Just provide the YAML, and the component will create the correct Dagster patterns.

---

## How It Works

### Pattern Detection

The component analyzes each DAG in the YAML file and detects which pattern to use:

```python
# Pattern 1: DAG has outlets → Assets + Asset Job
if has_outlets:
    return self._build_assets_and_job_from_dag(...)

# Pattern 2: DAG has asset_schedule but no outlets → Op Job + Sensor
elif has_asset_schedule:
    return self._build_op_job_and_sensor(...)

# Pattern 3: Regular DAG → Graph Asset (fallback)
else:
    return self.dag_factory_parser.create_graph_asset(...)
```

### Pattern 1: Assets + Asset Job

**When:** DAG has `outlets` defined on tasks

**Creates:**
- `@asset` for each task with outlets
- `define_asset_job()` to group the assets
- Respects dependencies between assets

**Example:**
```yaml
update_iss_coordinates:
  tasks:
    update_coordinates:
      outlets:
        - __type__: airflow.sdk.Asset
          name: "iss_coordinates"
```

**Generates:**
```python
@asset(name="iss_coordinates", ...)
def iss_coordinates(context: AssetExecutionContext):
    # Executes the task logic
    ...

# Asset job groups the assets
update_iss_coordinates_job = define_asset_job(
    name="update_iss_coordinates",
    selection=[AssetKey("iss_coordinates")]
)
```

### Pattern 2: Op Job + Sensor

**When:** DAG has `schedule` with assets but NO `outlets`

**Creates:**
- `@op` for each task
- `@job` to group the ops
- `@asset_sensor` to trigger job when upstream asset materializes

**Example:**
```yaml
process_iss_coordinates:
  schedule:
    - __type__: airflow.sdk.Asset
      name: "iss_coordinates"
  tasks:
    read_coordinates:
      # NO outlets
```

**Generates:**
```python
@op(name="read_coordinates")
def read_coordinates(context: OpExecutionContext):
    # Executes the task logic
    ...

@job(name="process_iss_coordinates")
def process_iss_coordinates():
    read_coordinates()

@asset_sensor(
    asset_key=AssetKey("iss_coordinates"),
    job=process_iss_coordinates
)
def process_iss_coordinates_sensor(context, asset_event):
    yield RunRequest()
```

---

## Example: ISS Coordinates

### Input: `asset_example_dag.yaml`

```yaml
update_iss_coordinates:
  tasks:
    update_coordinates:
      outlets: [{name: "iss_coordinates"}]

process_iss_coordinates:
  schedule: [{name: "iss_coordinates"}]
  tasks:
    read_coordinates: {}  # NO outlets
```

### Output: Automatically Generated Definitions

The component creates:

1. **Asset** `iss_coordinates` (from first DAG with outlets)
2. **Asset Job** `update_iss_coordinates` (groups the asset)
3. **Op** `read_coordinates` (from second DAG without outlets)
4. **Op Job** `process_iss_coordinates` (groups the op)
5. **Sensor** `process_iss_coordinates_sensor` (connects asset → job)

### Flow

```
update_iss_coordinates_job runs
    ↓
materializes iss_coordinates asset
    ↓
sensor detects materialization
    ↓
process_iss_coordinates job runs
```

---

## Multi-DAG Support

**The component processes ALL DAGs in a single YAML file!**

For `asset_example_dag.yaml` with 2 DAGs:
- First DAG → Assets + Asset Job
- Second DAG → Op Job + Sensor

All definitions are returned together and added to Dagster.

---

## Task Execution

The generated assets and ops correctly execute different task types:

### Bash Tasks
```python
if operator_type == 'bash':
    bash_command = parameters.get('bash_command')
    result = subprocess.run(bash_command, shell=True, ...)
```

### Python Callable Tasks
```python
elif operator_type == 'python':
    callable_func = parser.resolve_python_callable(task_config, yaml_path.parent)
    result = callable_func()
```

### Dummy Tasks
```python
elif operator_type == 'dummy':
    return "success"
```

---

## Implementation Details

### New Methods

**`_build_assets_and_job_from_dag()`**
- Extracts tasks with outlets
- Creates `@asset` for each task
- Handles dependencies between assets
- Creates `define_asset_job()` to group them
- Returns list: `[asset1, asset2, ..., asset_job]`

**`_build_op_job_and_sensor()`**
- Creates `@op` for each task
- Respects task execution order
- Creates `@job` to group ops
- Creates `@asset_sensor` to watch upstream asset
- Returns list: `[op_job, sensor]`

### Updated `build_defs()`

Now handles multiple definitions per script:

```python
result = self._build_script_asset_with_prefect_check(...)

if isinstance(result, list):
    # Multiple definitions returned (assets, jobs, sensors)
    for item in result:
        if hasattr(item, 'node_def'):
            all_jobs.append(item)
        elif hasattr(item, 'asset_key'):
            all_sensors.append(item)
        else:
            all_assets.append(item)
```

Returns:
```python
Definitions(
    assets=all_assets,
    jobs=all_jobs,
    sensors=all_sensors,
    schedules=all_schedules,
    ...
)
```

---

## Testing

### 1. Check Component Imports

```bash
cd script_orchestrator
python -c "from script_orchestrator.components.script_github_component import ScriptGithubComponent; print('✅ OK')"
```

### 2. Test with Example YAML

Place `asset_example_dag.yaml` in your scripts directory and run:

```bash
dagster dev
```

You should see in the UI:
- **Assets**: `iss_coordinates`
- **Jobs**: `update_iss_coordinates`, `process_iss_coordinates`
- **Sensors**: `process_iss_coordinates_sensor`

### 3. Materialize the Flow

1. Run `update_iss_coordinates` job → materializes `iss_coordinates` asset
2. Sensor detects materialization
3. `process_iss_coordinates` job runs automatically

---

## Advanced Features Supported

The parser already detects these features (from prior work):

✅ **Task Groups** - organizational grouping
✅ **Decorator-based tasks** - `airflow.sdk.task`
✅ **XCom dependencies** - `+task_id` syntax
✅ **Jinja templates** - `{{ logical_date }}`
✅ **Task group dependencies** - depend on all tasks in a group
✅ **Global defaults** - `default` section

---

## Key Benefits

### ✅ No Manual Coding
- No need to manually write `iss_coordinates_simple.py`
- YAML is the single source of truth
- Component generates everything automatically

### ✅ Correct Patterns
- Outlets → Assets + Asset Jobs
- No outlets → Op Jobs + Sensors
- No manual `@graph_asset` needed in most cases

### ✅ Seamless Migration
- Drop in Airflow dag-factory YAMLs
- Get idiomatic Dagster constructs
- Asset-based orchestration works out of the box

### ✅ Multi-DAG Support
- Single YAML with multiple DAGs works correctly
- Each DAG gets appropriate pattern
- All definitions returned together

---

## Summary

**Before:** Manual `.py` files with Dagster code

**After:** Automatic generation from YAML

The component now truly **parses Airflow YAMLs and generates proper Dagster definitions** - no manual coding required! 🎉
