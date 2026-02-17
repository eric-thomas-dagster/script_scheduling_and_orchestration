# Airflow Dataset/Asset Integration for Dagster

## Overview

Successfully implemented Airflow Dataset and Asset support, mapping Airflow data lineage to Dagster's asset dependency graph. This allows Airflow DAGs with Datasets (2.4+) or Assets (3.0+) to show proper data lineage in Dagster's UI.

## Implementation Summary

### What Was Built

**1. Dataset/Asset Parsing** (`airflow_parser.py`)
- Enhanced `_extract_dag_config()` to detect `schedule=[dataset]` (inlet datasets) and `outlets=[dataset]` (outlet datasets)
- Added `_extract_dataset_references()` to parse Dataset/Asset URIs from AST nodes
- Added `_resolve_dataset_vars()` to resolve variable references to their URIs
- Scans module-level for Dataset/Asset variable definitions (e.g., `raw_data = Dataset("s3://bucket/")`)
- Returns `inlet_datasets` and `outlet_datasets` in DAG info

**2. Asset Creation with Dependencies** (`script_github_component.py`)
- Created `_build_airflow_asset_with_datasets()` method
- Converts Airflow dataset URIs to valid Dagster asset keys
- Finds producer DAGs for inlet datasets and creates AssetIn dependencies
- Executes DAGs via `airflow dags test` while preserving Dagster lineage
- Stores dataset information in asset metadata (shown at runtime)

**3. Example DAGs**
- `dag_with_datasets_producer.py`: Produces `processed_data_dataset` on @daily schedule
- `dag_with_datasets_consumer.py`: Consumes `processed_data_dataset`, produces `analytics_dataset`
- Demonstrates proper producer-consumer lineage chain

## How It Works

### Dataset URI to Asset Key Conversion

Airflow datasets use URIs like `s3://bucket/path/` which aren't valid Dagster asset keys. The conversion:

```python
# Input:  s3://bucket/processed_data/
# Output: airflow_dataset_s3_bucket_processed_data

def dataset_uri_to_asset_key(uri: str) -> str:
    # Strip protocol (s3://, file://, etc.)
    cleaned = re.sub(r'^[a-z]+://', '', uri)
    # Replace special chars with underscores (Dagster requires ^[A-Za-z0-9_]+$)
    cleaned = re.sub(r'[^A-Za-z0-9_]', '_', cleaned)
    # Collapse multiple underscores
    cleaned = re.sub(r'_+', '_', cleaned).strip('_')
    return f"airflow_dataset_{cleaned}"
```

### Dependency Resolution

When a consumer DAG specifies `schedule=[dataset]`:

1. Parser extracts inlet dataset URI: `s3://bucket/processed_data/`
2. Component searches all Airflow DAGs for producers of that dataset
3. Finds producer DAG with matching `outlets=[dataset]`
4. Creates `AssetIn` dependency on the **producer DAG asset**, not on a separate dataset entity
5. Result: `airflow_consumer_pipeline` depends on `airflow_producer_pipeline`

### Asset Execution

Assets execute via `airflow dags test`:

```python
airflow dags test producer_pipeline 2026-02-16
```

Output metadata includes:
- DAG ID
- Execution date
- Duration
- Inlet datasets consumed (as markdown list)
- Outlet datasets produced (as markdown list)
- Stdout/stderr (last 5000 chars)

## Verification

### Log Output (Successful)

```
2026-02-16T19:15:53.425120Z [info] Inlet dataset s3://bucket/processed_data/ produced by airflow_producer_pipeline
2026-02-16T19:15:53.433255Z [info] DAG producer_pipeline uses advanced features ['uses_datasets'], falling back to subprocess
2026-02-16T19:15:53.433316Z [info] Building Airflow asset producer_pipeline with 0 inlet datasets and 1 outlet datasets
2026-02-16T19:15:53.443090Z [info] Created 23 assets and 6 schedules
[INFO] Serving dagster-webserver on http://127.0.0.1:3001
```

✅ **No errors** - all assets loaded successfully

### Asset Graph

In Dagster UI:
- **airflow_producer_pipeline** (scheduled @daily)
  - Produces: `s3://bucket/processed_data/`
  - Shows as upstream dependency

- **airflow_consumer_pipeline** (triggered by data)
  - Depends on: `airflow_producer_pipeline`
  - Consumes: `s3://bucket/processed_data/`
  - Produces: `s3://bucket/analytics/`
  - Shows proper lineage connection

## Files Created/Modified

### New Files
- `example_scripts/airflow_examples/dag_with_datasets_producer.py` - Producer DAG
- `example_scripts/airflow_examples/dag_with_datasets_producer.yaml` - Producer config
- `example_scripts/airflow_examples/dag_with_datasets_consumer.py` - Consumer DAG
- `example_scripts/airflow_examples/dag_with_datasets_consumer.yaml` - Consumer config

### Enhanced Files
- `components/parsers/airflow_parser.py`:
  - `_extract_dag_config()` - detects inlet/outlet datasets
  - `_extract_dataset_references()` - extracts dataset URIs
  - `_resolve_dataset_vars()` - resolves variable refs
  - `parse_dag()` - scans for module-level Dataset definitions

- `components/script_github_component.py`:
  - `_build_airflow_asset_with_datasets()` - creates assets with dependencies
  - Dataset URI to asset key conversion
  - Producer DAG discovery and dependency creation
  - Schedule creation with correct asset keys

## Architecture Benefits

1. **True Lineage Visualization**: Airflow data dependencies visible in Dagster UI
2. **No Code Duplication**: Airflow DAGs run via native `airflow dags test`
3. **Version Support**: Works with Airflow 2.4+ (Datasets) and 3.0+ (Assets)
4. **Proper Dependency Modeling**: Consumer assets depend on producer assets, not phantom dataset entities
5. **Metadata Preservation**: Dataset URIs stored in asset metadata for visibility

## Limitations

1. **Data-Aware Scheduling**: Consumer DAGs don't get triggered automatically when data is ready in Dagster (Airflow's native scheduler would handle this)
2. **Multiple Producers**: If multiple DAGs produce the same dataset, only the first discovered becomes the dependency
3. **Cross-Repository**: Only works for DAGs within the same Dagster code location

## Example Usage

### Producer DAG

```python
from airflow import Dataset
from airflow.decorators import dag, task

processed_data = Dataset("s3://bucket/processed_data/")

@dag(
    schedule="@daily",
    outlets=[processed_data],  # Declares output
)
def producer_pipeline():
    @task
    def process():
        return {"status": "processed"}

    process()
```

### Consumer DAG

```python
from airflow import Dataset
from airflow.decorators import dag, task

processed_data = Dataset("s3://bucket/processed_data/")

@dag(
    schedule=[processed_data],  # Triggered by dataset
)
def consumer_pipeline():
    @task
    def analyze():
        return {"status": "analyzed"}

    analyze()
```

### In Dagster

```yaml
# dag_with_datasets_consumer.yaml
enabled: true
script_type: airflow
airflow_mapping:
  enabled: true
```

Result: Consumer asset depends on producer asset, proper lineage in UI!

## Summary

The Airflow Dataset/Asset integration provides:
- ✅ **Airflow 2.4+ Dataset support**
- ✅ **Airflow 3.0+ Asset support**
- ✅ **Automatic dependency resolution**
- ✅ **Proper Dagster lineage visualization**
- ✅ **Native Airflow execution**
- ✅ **Metadata-rich asset outputs**

This implementation allows teams using Airflow's data-aware scheduling to visualize their data lineage in Dagster while maintaining full Airflow compatibility.
