"""
Airflow DAG demonstrating XCom for task communication.
This will automatically fall back to subprocess mode to preserve XCom functionality.
"""
from datetime import datetime
from airflow.decorators import dag, task


@task
def extract_data(source: str = "api"):
    """Extract data and push to XCom."""
    print(f"Extracting data from {source}")
    data = {
        "records": [1, 2, 3, 4, 5],
        "source": source,
        "count": 5
    }
    # Return value is automatically pushed to XCom
    return data


@task
def transform_data(ti=None):
    """Pull data from XCom and transform it."""
    # Pull data from previous task using XCom
    data = ti.xcom_pull(task_ids='extract_data')

    print(f"Transforming {data['count']} records from {data['source']}")

    transformed = {
        "source": data["source"],
        "transformed_records": [x * 2 for x in data["records"]],
        "count": data["count"]
    }
    return transformed


@task
def load_data(ti=None):
    """Pull transformed data and load to destination."""
    # Pull from previous task
    data = ti.xcom_pull(task_ids='transform_data')

    print(f"Loading {data['count']} transformed records")
    print(f"Records: {data['transformed_records']}")

    return {
        "status": "success",
        "records_loaded": data["count"]
    }


@dag(
    dag_id="xcom_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "xcom", "etl"],
    description="Demonstrates XCom communication between tasks"
)
def xcom_pipeline():
    """ETL pipeline using XCom for data passing between tasks."""
    # XCom is automatically used for data flow
    data = extract_data()
    transformed = transform_data()
    loaded = load_data()

    # Set dependencies - use task instances, not function names
    data >> transformed >> loaded


# Instantiate the DAG
dag_instance = xcom_pipeline()


# For local testing (Airflow 2.x and 3.x)
if __name__ == "__main__":
    # Test using dag.test() - runs DAG locally with XCom
    dag_instance.test()
