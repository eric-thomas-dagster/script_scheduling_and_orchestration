"""
Airflow DAG with branching logic.
Tests dependency parsing for non-linear task graphs.
"""
from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.python import BranchPythonOperator


@task
def fetch_data() -> dict:
    """Fetch data to analyze."""
    print("Fetching data...")
    return {
        "value": 150,
        "status": "active",
        "records": 1000
    }


@task
def analyze_data(data: dict) -> dict:
    """Analyze the data and determine path."""
    print(f"Analyzing data: value={data['value']}")
    analysis = {
        "data": data,
        "threshold_exceeded": data["value"] > 100,
        "recommendation": "process_large" if data["value"] > 100 else "process_small"
    }
    return analysis


@task
def process_large_dataset(analysis: dict) -> str:
    """Process large dataset."""
    print(f"Processing large dataset with value={analysis['data']['value']}")
    return f"Large processing complete: {analysis['data']['records']} records"


@task
def process_small_dataset(analysis: dict) -> str:
    """Process small dataset."""
    print(f"Processing small dataset with value={analysis['data']['value']}")
    return f"Small processing complete: {analysis['data']['records']} records"


@task
def generate_report(process_result: str) -> str:
    """Generate final report."""
    print(f"Generating report from: {process_result}")
    return f"Report: {process_result}"


@dag(
    dag_id="branching_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "branching"],
)
def branching_pipeline():
    """Pipeline with conditional branching based on data analysis."""
    data = fetch_data()
    analysis = analyze_data(data)

    # In a real branching scenario, we'd use BranchPythonOperator
    # For this example, we'll show both paths
    large_result = process_large_dataset(analysis)
    small_result = process_small_dataset(analysis)

    # Generate reports from both paths
    generate_report(large_result)
    generate_report(small_result)


# Instantiate the DAG
dag_instance = branching_pipeline()
