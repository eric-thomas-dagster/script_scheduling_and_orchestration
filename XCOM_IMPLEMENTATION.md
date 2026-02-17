# Enhanced XCom Implementation

## 🎉 What Was Implemented

**Full data passing between Dagster ops** when migrating from Airflow dag-factory YAML with XCom syntax.

### The Problem (Before)

```yaml
tasks:
  extract_data:
    python_callable: tasks.extract
    # Returns [1, 2, 3]

  process_data:
    python_callable: tasks.process
    dependencies: [extract_data]
    data: +extract_data  # XCom syntax
```

**Before:** We detected `+extract_data` but didn't pass the data
```python
# Generated (incorrect):
@op
def extract_data(): return [1,2,3]

@op
def process_data(): pass  # data parameter ignored!

@job
def my_job():
    extract_data()  # Result lost!
    process_data()  # No data passed!
```

### The Solution (After)

**After:** We now actually pass data between ops!
```python
# Generated (correct):
@op
def extract_data(context):
    return [1, 2, 3]  # Returns data

@op
def process_data(context, **xcom_inputs):
    data = xcom_inputs['data']  # Receives data!
    return process(data)

@job
def my_job():
    result = extract_data()  # Capture result
    process_data(data=result)  # Pass to next op!
```

---

## How It Works

### 1. Parser Detection

The `DagFactoryYamlParser` already detects XCom syntax:

```python
dag_info['xcom_dependencies'] = {
    'process_data': {
        'data': 'extract_data'  # param_name: upstream_task_id
    }
}
```

### 2. Op Creation with XCom Support

When creating ops, we check for XCom dependencies:

```python
task_xcom_deps = xcom_deps.get(task_id, {})

if task_xcom_deps:
    # Op accepts **xcom_inputs
    def op_func(context, **xcom_inputs):
        # XCom inputs available as kwargs
        callable_func(**xcom_inputs)  # Pass to Python callable
else:
    # Standard op
    def op_func(context):
        callable_func()
```

### 3. Job Function with Data Passing

The job function now passes data:

```python
@job
def my_job():
    results = {}

    for task_id in task_order:
        task_xcom_deps = xcom_dict.get(task_id, {})

        if task_xcom_deps:
            # Build xcom_inputs from upstream results
            xcom_inputs = {}
            for param_name, upstream_task_id in task_xcom_deps.items():
                xcom_inputs[param_name] = results[upstream_task_id]

            # Call with XCom inputs
            results[task_id] = op(**xcom_inputs)
        else:
            # Standard call
            results[task_id] = op()
```

---

## Example: Data Pipeline with XCom

### Airflow YAML

```yaml
xcom_data_pipeline:
  tasks:
    extract_data_a:
      python_callable: tasks.extract_a
      # Returns [1, 2, 3]

    extract_data_b:
      python_callable: tasks.extract_b
      # Returns [10, 20, 30]

    process_data:
      python_callable: tasks.process
      dependencies: [extract_data_a, extract_data_b]
      data_a: +extract_data_a  # XCom from first task
      data_b: +extract_data_b  # XCom from second task

    store_results:
      python_callable: tasks.store
      dependencies: [process_data]
      processed_data: +process_data  # XCom from process
```

### Python Callables

```python
# tasks.py
def extract_a() -> list:
    return [1, 2, 3, 4, 5]

def extract_b() -> list:
    return [10, 20, 30, 40, 50]

def process(data_a: list, data_b: list) -> dict:
    # Receives both inputs via XCom
    return {
        'total': sum(data_a + data_b),
        'count': len(data_a + data_b),
    }

def store(processed_data: dict):
    # Receives processed data via XCom
    print(f"Storing: {processed_data}")
```

### Generated Dagster Code

```python
# Automatically generated:

@op
def extract_data_a(context):
    result = tasks.extract_a()
    return result  # ← Returns for XCom

@op
def extract_data_b(context):
    result = tasks.extract_b()
    return result  # ← Returns for XCom

@op
def process_data(context, **xcom_inputs):
    # ← Accepts XCom inputs
    result = tasks.process(**xcom_inputs)
    return result  # ← Returns for next op

@op
def store_results(context, **xcom_inputs):
    # ← Accepts XCom inputs
    tasks.store(**xcom_inputs)

@job
def xcom_data_pipeline():
    results = {}

    # Execute in order, passing data
    results['extract_data_a'] = extract_data_a()
    results['extract_data_b'] = extract_data_b()

    # Pass data to process_data
    results['process_data'] = process_data(
        data_a=results['extract_data_a'],
        data_b=results['extract_data_b']
    )

    # Pass data to store_results
    store_results(processed_data=results['process_data'])
```

---

## Key Features

### ✅ Automatic Detection
- Parser detects `+task_id` syntax
- Stores in `xcom_dependencies` dict
- No manual configuration needed

### ✅ Proper Data Flow
- Upstream ops return values
- Downstream ops receive via `**xcom_inputs`
- Job function passes results between ops

### ✅ Type-Safe Passing
- Python callables with type hints work correctly
- Parameters match XCom dependency names
- `inspect.signature()` checks parameter compatibility

### ✅ Debugging Support
- Logs when XCom inputs are passed
- Logs XCom input parameter names
- Warns if XCom dependency not satisfied

---

## Testing

### 1. Create Test Files

```bash
cd script_orchestrator/example_scripts/airflow_examples

# YAML already created: xcom_example.yaml
# Python callables already created: include/tasks/xcom_tasks.py
```

### 2. Run Dagster

```bash
cd script_orchestrator
dagster dev
```

### 3. Check the UI

Navigate to the `xcom_data_pipeline` job:

- **Ops**: See 4 ops created
- **Dependencies**: Ops properly connected
- **Run it**: Data flows through ops
- **Logs**: See XCom inputs logged

### 4. Expected Output

```
Extracting data from source A...
Extracted 5 items from source A
Extracting data from source B...
Extracted 5 items from source B
XCom inputs: ['data_a', 'data_b']
Processing data - A: 5 items, B: 5 items
Processed results: {'count_a': 5, 'count_b': 5, 'total_count': 10, ...}
XCom inputs: ['processed_data']
Storing results...
Results: {'count_a': 5, 'count_b': 5, ...}
✅ Results stored successfully
```

---

## Limitations & Future Work

### Current Implementation

✅ **Op Jobs**: Full XCom support with data passing
⚠️ **Asset Jobs**: Limited support (assets use `deps=` not XCom)

### For Assets

Assets with outlets currently use `deps=[upstream_asset]` for dependencies. To support XCom-style data passing between assets, we would need:

```python
@asset(
    ins={
        "data_a": AssetIn(key=AssetKey("data_a")),
        "data_b": AssetIn(key=AssetKey("data_b"))
    }
)
def combined_asset(context, data_a, data_b):
    # Receive data from upstream assets
    return process(data_a, data_b)
```

This is **future work** - for now, XCom is fully supported for op jobs (terminal operations without outlets).

---

## Technical Details

### Parameter Inspection

We use `inspect.signature()` to check if callables accept parameters:

```python
import inspect
sig = inspect.signature(callable_func)

if len(sig.parameters) > 0 and xcom_inputs:
    # Callable accepts params - pass XCom inputs
    result = callable_func(**xcom_inputs)
else:
    # No params - call without inputs
    result = callable_func()
```

### Execution Order

Task execution order is determined by the parser:

```python
task_order = self.dag_factory_parser.get_task_execution_order(dag_info)
# Returns: ['extract_data_a', 'extract_data_b', 'process_data', 'store_results']
```

This ensures upstream tasks run before downstream tasks that need their data.

### XCom Dependency Resolution

```python
for param_name, upstream_task_id in task_xcom_deps.items():
    if upstream_task_id in results:
        xcom_inputs[param_name] = results[upstream_task_id]
    else:
        logger.warning(f"XCom dependency not satisfied: {task_id}")
```

---

## Benefits

### For Airflow Migrations

✅ **Preserves Logic**: Data flow works exactly like Airflow
✅ **No Code Changes**: Python callables work as-is
✅ **Type Safety**: Type hints are preserved
✅ **Clear Lineage**: Dagster graph shows data dependencies

### For Dagster Users

✅ **True Dependencies**: Ops depend on actual data
✅ **Better Visualization**: Dependency graph shows data flow
✅ **Testability**: Can test ops with mock inputs
✅ **Observability**: See what data is passed between ops

---

## Summary

**Before:** XCom syntax detected but data not passed

**After:** Full data passing between ops!

- ✅ Ops return values
- ✅ Downstream ops receive via parameters
- ✅ Job function passes data correctly
- ✅ Works with existing Python callables
- ✅ Zero configuration needed
- ✅ Logs XCom inputs for debugging

**Result:** Airflow pipelines with XCom now work correctly in Dagster! 🎉
