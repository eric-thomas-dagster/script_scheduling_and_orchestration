# Airflow 2.x Examples

These examples are designed for **Apache Airflow 2.x** (2.8+).

## Key Features & Syntax

Airflow 2.x uses these patterns:

- **Schedule**: `schedule_interval` parameter
- **Datasets/Assets**: `outlets` parameter on `@dag` decorator
- **Imports**: `from airflow.decorators import dag, task`
- **Operators**: `from airflow.operators.python import PythonOperator`

## SQLAlchemy Compatibility

⚠️ **Important**: Airflow 2.x requires SQLAlchemy < 2.0, which conflicts with Prefect 3.x (requires SQLAlchemy >= 2.0).

If you need to run these examples:
1. Disable Prefect in `.env`: `PREFECT_ENABLED=false`
2. Set Airflow version in `.env`: `AIRFLOW_VERSION=2.10`
3. Update `defs.yaml`: `airflow_version: '2.10'`
4. Change scripts directory: `scripts_directory: airflow_2x_examples`

## Example DAGs

- `dag_with_xcom.py` - XCom data passing between tasks
- `dag_with_params.py` - Parameterized DAG execution
- `dag_with_datasets_*.py` - Dataset/outlet dependencies (2.x syntax)
- `dag_with_branching.py` - Conditional task branching
- `simple_sequential.py` - Basic sequential task pipeline
- `standard_dag_with_checks.py` - Traditional DAG with data quality checks

## Migrating to Dagster

These examples demonstrate common Airflow 2.x patterns that can be migrated to Dagster with minimal code changes using the Script Orchestrator component.
