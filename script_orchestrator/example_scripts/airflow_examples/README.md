# Airflow DAG Support (2.x and 3.x)

This directory contains example Airflow DAGs that demonstrate comprehensive Airflow → Dagster integration with support for both Airflow 2.x and 3.x features.

## Supported Airflow Versions

- ✅ **Airflow 2.x** (2.0 - 2.8+)
- ✅ **Airflow 3.x** (3.0+)
- ✅ TaskFlow API (@task decorators)
- ✅ Traditional Operators
- ✅ XCom for inter-task communication
- ✅ Datasets (2.4+) and Assets (3.0+)
- ✅ Sensors
- ✅ Dynamic task mapping
- ✅ Branching operators

## Features

The script orchestrator provides intelligent Airflow → Dagster integration with automatic feature detection:

### 1. **Task-to-Op Mapping** (Graph Assets)
Simple sequential DAGs are mapped to Dagster graph assets where each Airflow task becomes a Dagster op:

- Individual ops are visible in the Dagster UI
- Task dependencies are preserved
- Each op executes the actual Airflow task function
- Full execution logs for each individual task

### 2. **Automatic Fallback** (Subprocess Mode)
Complex DAGs automatically fall back to subprocess execution for:

- DAGs with parameters (enables config in Launchpad)
- DAGs with branching logic
- DAGs with tasks called multiple times
- DAGs with more than 5 tasks

### 3. **Configuration Support**
For DAGs with parameters, the orchestrator:

- Extracts parameter definitions from `Param()` declarations
- Creates Dagster Config classes with type annotations
- Makes parameters configurable in the Dagster Launchpad UI
- Supports default values, types, and descriptions

## Example DAGs

### Simple Sequential (`simple_sequential.py`)
**Status**: ✅ Graph Asset (3 ops visible in UI)
**Features**: Schedule extraction, retry policy

A simple sequential pipeline with 3 tasks demonstrating automatic schedule and retry extraction:
- `fetch_data` - Fetches data from a source
- `process_data` - Processes the fetched data
- `save_results` - Saves the processed results
- **Schedule**: Daily at 9am (extracted from DAG)
- **Retry**: 3 retries with 5-minute delay

**Mapped to**: Dagster graph asset with individual ops

### Parameterized (`dag_with_params.py`)
**Status**: ⚙️ Subprocess Mode (Config in Launchpad)
**Features**: Parameter extraction, Launchpad config

A parameterized pipeline demonstrating config extraction:
- Parameters: `urls` (list), `min_length` (int), `output_format` (string)
- All parameters configurable in Dagster Launchpad
- Default values and descriptions extracted from Airflow `Param()` declarations

**Mapped to**: Subprocess mode for parameter support

### Branching (`dag_with_branching.py`)
**Status**: ⚙️ Subprocess Mode (Complex patterns)
**Features**: Non-linear dependencies

A pipeline with conditional branching:
- Demonstrates non-linear task dependencies
- Uses multiple paths based on data analysis

**Mapped to**: Subprocess mode due to complex patterns

### XCom Communication (`dag_with_xcom.py`) 🆕
**Status**: ⚙️ Subprocess Mode (XCom support)
**Features**: Inter-task communication, XCom push/pull
**Airflow Version**: 2.x and 3.x

ETL pipeline demonstrating XCom for data passing between tasks:
- Uses `ti.xcom_pull()` to retrieve data from previous tasks
- Automatic XCom push on task return values
- Preserves full Airflow XCom functionality
- Supports `dag.test()` for local testing

**Why Subprocess**: XCom requires Airflow's task instance context, so we run the entire DAG through Airflow natively.

### Data-Aware Scheduling (`dag_with_datasets.py`) 🆕
**Status**: ⚙️ Subprocess Mode (Datasets support)
**Features**: Datasets, data-aware scheduling
**Airflow Version**: 2.4+ (Datasets), 3.0+ (Assets)

Producer-consumer pipelines demonstrating Datasets:
- **Producer DAG**: Generates data and updates datasets
- **Consumer DAG**: Automatically triggered when dataset is updated
- Schedule based on data availability, not just time
- Supports Airflow 2.4+ Datasets and 3.0+ Assets

**Why Subprocess**: Datasets are deeply integrated with Airflow's scheduler and require native Airflow execution.

## YAML Configuration

Enable Airflow mapping in your `.yaml` file:

```yaml
enabled: true
description: "Your DAG description"
group: "airflow_examples"
owners:
  - "team:your_team"
tags:
  category: "airflow"
  type: "sequential"
kinds:
  - python
  - airflow
script_type: airflow
airflow_mapping:
  enabled: true
  dag_id: "your_dag_id"  # optional - auto-detected if not specified
```

## Implementation Details

### Parser
The Airflow parser (`_parse_airflow_dag`) uses AST analysis to extract:
- Task definitions from `@task` decorators
- DAG metadata from `@dag` decorators
- Parameter definitions from `Param()` calls in the `params` dict
- Task call sequences within the DAG function

### Graph Asset Creation
For simple DAGs, the orchestrator:
1. Imports the Airflow script module
2. Extracts actual task functions
3. Creates Dagster `@op` definitions that call the Airflow tasks
4. Wires ops together in a `@graph_asset` based on dependencies
5. Results in individual ops visible in Dagster UI

### Subprocess Fallback
For complex DAGs, the orchestrator:
1. Runs DAGs via `airflow dags test` or `airflow tasks test`
2. Extracts parameters for Dagster Config
3. Provides full configurability in Launchpad

## Testing

To test the Airflow examples:

```bash
# Start Dagster dev server
uv run dg dev

# View assets in UI at http://localhost:3000

# Materialize simple_sequential to see individual ops
# Materialize dag_with_params to see parameter configuration
```

## Results

After implementation:
- **Total assets**: 23
- **Airflow assets**: 3
  - 1 graph asset (simple_sequential - 3 ops)
  - 2 subprocess assets (dag_with_params, dag_with_branching)
- **Prefect assets**: 9
- **Other assets**: 11

## Advanced Features Support

The orchestrator automatically detects and handles advanced Airflow features:

### XCom (Inter-Task Communication)
**Detection**: `xcom_push`, `xcom_pull`, `ti.xcom_pull()`
**Support**: ✅ Full support via subprocess mode
**Usage**: XCom allows tasks to exchange data. Small metadata (< 1MB recommended).

```python
@task
def extract():
    return {"data": [1, 2, 3]}  # Pushed to XCom

@task
def process(ti=None):
    data = ti.xcom_pull(task_ids='extract')  # Pull from XCom
```

### Datasets (Airflow 2.4+)
**Detection**: Import of `Dataset`, `DatasetAlias`
**Support**: ✅ Full support via subprocess mode
**Usage**: Data-aware scheduling - trigger DAGs when data is ready.

```python
from airflow import Dataset

dataset = Dataset("s3://bucket/data/")

@dag(schedule=[dataset])  # Triggered when dataset updates
def consumer_dag():
    ...
```

### Assets (Airflow 3.0+)
**Detection**: Import of `Asset`, `AssetAlias`
**Support**: ✅ Full support via subprocess mode
**Usage**: Evolution of Datasets with enhanced metadata and lineage.

### Sensors
**Detection**: Import from `airflow.sensors` or class names ending in `Sensor`
**Support**: ✅ Full support via subprocess mode
**Usage**: Wait for external conditions (file existence, time, etc.)

### Dynamic Task Mapping (Airflow 2.3+)
**Detection**: `.expand()` method calls
**Support**: ✅ Full support via subprocess mode
**Usage**: Create tasks dynamically based on runtime data.

```python
@task
def process_item(item):
    return item * 2

process_item.expand(item=[1, 2, 3, 4, 5])
```

### Branching Operators
**Detection**: Import of `BranchPythonOperator` or similar
**Support**: ✅ Full support via subprocess mode
**Usage**: Conditional execution paths within a DAG.

### Traditional Operators
**Detection**: Import from `airflow.operators` (non-PythonOperator)
**Support**: ✅ Full support via subprocess mode
**Usage**: BashOperator, EmailOperator, S3Operator, etc.

## Execution Modes

### Graph Asset Mode (Simple DAGs Only)
- **When**: Simple sequential TaskFlow API DAGs without advanced features
- **Benefit**: Individual tasks visible as Dagster ops in UI
- **Limitation**: No XCom, Datasets, Sensors, or complex patterns

### Subprocess Mode (Full Airflow Support) ⭐ **Recommended**
- **When**: Any of the following detected:
  - XCom usage
  - Datasets or Assets
  - Sensors
  - Parameters
  - Branching
  - Dynamic mapping
  - Traditional operators
- **Benefit**: 100% Airflow compatibility, all features work natively
- **Execution**: `airflow dags test` for full DAG runs

## Testing Airflow DAGs

All examples support `dag.test()` for local testing:

```bash
# Test a single DAG
python example_scripts/airflow_examples/dag_with_xcom.py

# Test with Airflow CLI
airflow dags test xcom_pipeline 2024-01-01
airflow tasks test xcom_pipeline extract_data 2024-01-01
```

## Architecture Notes

The Airflow support provides comprehensive integration:
- **AST-based parsing** for static analysis and feature detection
- **Intelligent fallback** to subprocess for advanced features
- **Full Airflow compatibility** via native execution
- **Schedule extraction** from DAG decorators
- **Retry policy extraction** from DAG defaults
- **Airflow 2.x and 3.x support** with version-specific feature detection
