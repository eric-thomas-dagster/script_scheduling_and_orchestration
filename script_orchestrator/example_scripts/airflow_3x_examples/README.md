# Airflow 3.x Examples

These examples are designed for **Apache Airflow 3.x** (3.1+).

## Key Features & Syntax

Airflow 3.x introduces breaking changes:

- **Schedule**: Use `schedule` parameter (not `schedule_interval`)
- **Datasets/Assets**: `outlets` moved from `@dag` to individual `@task` decorators
- **Imports**: Recommended to use `from airflow.sdk import dag, task`
- **Operators**: New provider structure: `from airflow.providers.standard.operators.python import PythonOperator`

## SQLAlchemy Compatibility

✅ **Compatible**: Airflow 3.x uses SQLAlchemy 2.0, which is compatible with Prefect 3.x.

Both orchestrators can run simultaneously in the same environment!

## Configuration

Default configuration in `.env` and `defs.yaml`:
```bash
AIRFLOW_VERSION=3.1
AIRFLOW_ENABLED=true
PREFECT_ENABLED=true
SCRIPTS_DIR=airflow_3x_examples
```

## Example DAGs

### Python DAGs
- `simple_etl_3x.py` - Basic ETL pipeline with asset/dataset support
- `data_pipeline_3x.py` - Multi-stage data processing with datasets
- `report_generator_3x.py` - Parallel data fetching and report generation
- `report_from_processed_data_3x.py` - Consumer DAG reading from datasets
- `customer_etl_factory.py` - Programmatic DAG generation example

### YAML DAGs (dag-factory)
- `basic_example_dag.yaml` - Simple YAML-defined DAG
- `xcom_example.yaml` - XCom data passing
- `asset_example_dag.yaml` - Dataset/asset examples
- `asset_chain_example.yaml` - Chained asset dependencies
- `example_dag_factory.yaml` - Multiple tasks and dependencies
- `example_advanced_operators.yaml` - Various operator types
- `multi_task_job_example.yaml` - Job-based execution
- `quality_checks_example.yaml` - Data quality validation

## dag-factory Support

YAML-defined DAGs use [dag-factory](https://github.com/ajbosco/dag-factory) for declarative DAG configuration. The Script Orchestrator can parse these YAML files and create corresponding Dagster assets.

## Migrating from Airflow 2.x

Key changes when migrating:
1. Replace `schedule_interval` with `schedule`
2. Move `outlets` from `@dag()` to `@task()` decorators
3. Update imports to use `airflow.sdk.*` instead of `airflow.decorators.*`
4. Update operator imports to use provider packages

## Migrating to Dagster

These examples demonstrate Airflow 3.x patterns that can be migrated to Dagster using the Script Orchestrator component. The component handles:
- Asset/dataset mapping to Dagster assets
- Automatic dependency detection
- Version compatibility filtering
- DAG execution via `airflow dags test`
