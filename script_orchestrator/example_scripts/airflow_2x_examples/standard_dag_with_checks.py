"""
Example: Standard Airflow Python DAG with Check Operators

This demonstrates Airflow check operators in a standard Python DAG
(not dag-factory YAML). The check operators should be automatically
detected and converted to Dagster Asset Checks.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator, SQLTableCheckOperator

# Handle Airflow 2.x vs 3.x operator imports
try:
    from airflow.operators.python import PythonOperator  # Airflow 2.x
except ImportError:
    from airflow.providers.standard.operators.python import PythonOperator  # Airflow 3.x

# Define the DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Build DAG kwargs compatible with both Airflow 2.x and 3.x
dag_kwargs = {
    'dag_id': 'data_quality_standard_dag',
    'default_args': default_args,
    'description': 'Standard DAG with data quality checks',
    'catchup': False,
    'tags': ['airflow-version:2.x'],
}

# Use schedule_interval for Airflow 2.x, schedule for Airflow 3.x
try:
    dag = DAG(**dag_kwargs, schedule_interval='@daily')
except TypeError:
    # Airflow 3.x doesn't support schedule_interval
    dag_kwargs['schedule'] = '@daily'
    dag = DAG(**dag_kwargs)

with dag:

    # Extract data task
    def extract_data():
        print("Extracting data from database...")
        # Simulate data extraction
        return "Data extracted"

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    # Validate column-level data quality
    validate_columns = SQLColumnCheckOperator(
        task_id='validate_user_columns',
        table='users',
        column_mapping={
            'email': {
                'null_check': {'equals': 0},
                'unique_check': {'equals': 1},
            },
            'age': {
                'min': {'greater_than': 0},
                'max': {'less_than': 120},
            },
            'created_at': {
                'null_check': {'equals': 0},
            },
        },
    )

    # Validate table-level data quality
    validate_table = SQLTableCheckOperator(
        task_id='validate_user_table',
        table='users',
        checks={
            'row_count_check': {
                'greater_than': 0,
            },
            'row_count_range': {
                'greater_than': 100,
                'less_than': 10000000,
            },
        },
    )

    # Load validated data
    def load_data():
        print("Loading validated data...")
        # Simulate data loading
        return "Data loaded"

    load_task = PythonOperator(
        task_id='load_validated_data',
        python_callable=load_data,
    )

    # Set dependencies
    extract_task >> validate_columns >> validate_table >> load_task
