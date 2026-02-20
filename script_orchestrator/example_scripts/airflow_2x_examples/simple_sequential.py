"""
Simple sequential Airflow DAG with 3 tasks.
Similar to test_simple_prefect.py but using Airflow decorators.
"""
from datetime import datetime, timedelta
from airflow.decorators import dag, task


@task
def fetch_data(source: str = "api") -> dict:
    """Fetch data from a source."""
    print(f"Fetching data from {source}")
    return {"source": source, "data": [1, 2, 3, 4, 5]}


@task
def process_data(data: dict) -> dict:
    """Process the fetched data."""
    print(f"Processing data from {data['source']}")
    processed = {
        "source": data["source"],
        "processed": [x * 2 for x in data["data"]]
    }
    return processed


@task
def save_results(processed: dict) -> str:
    """Save the processed results."""
    result = f"Saved {len(processed['processed'])} items from {processed['source']}"
    print(result)
    return result


@dag(
    dag_id="simple_sequential_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="0 9 * * *",  # Daily at 9am
    catchup=False,
    tags=["example", "sequential"],
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
)
def simple_sequential_pipeline():
    """A simple sequential pipeline with 3 tasks."""
    data = fetch_data()
    processed = process_data(data)
    save_results(processed)


# Instantiate the DAG
dag_instance = simple_sequential_pipeline()
