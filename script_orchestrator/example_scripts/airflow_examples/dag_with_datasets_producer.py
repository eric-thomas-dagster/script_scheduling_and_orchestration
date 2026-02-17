"""
Airflow DAG demonstrating Datasets - Producer (Airflow 2.4+).
This DAG produces data and updates a dataset, which can trigger consumer DAGs.
"""
from datetime import datetime
from airflow import Dataset
from airflow.decorators import dag, task

# Define datasets
raw_data_dataset = Dataset("s3://bucket/raw_data/")
processed_data_dataset = Dataset("s3://bucket/processed_data/")


@task
def fetch_raw_data():
    """Fetch raw data from source."""
    print("Fetching raw data from API")
    data = {"records": [1, 2, 3, 4, 5]}
    print(f"Fetched {len(data['records'])} records")
    return data


@task
def process_data(data):
    """Process raw data."""
    print("Processing data...")
    processed = [x * 2 for x in data["records"]]
    print(f"Processed {len(processed)} records")
    return {"processed": processed}


@task
def save_processed_data(data):
    """Save processed data - produces a dataset."""
    print(f"Saving {len(data['processed'])} records to processed dataset")
    # In reality, this would write to S3, database, etc.
    return {"status": "saved", "count": len(data['processed'])}


@dag(
    dag_id="producer_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["example", "datasets", "producer"],
    description="Produces data and updates datasets",
    outlets=[processed_data_dataset]  # Declares this DAG produces the dataset
)
def producer_pipeline():
    """
    Producer DAG that generates data and marks datasets as updated.
    This can trigger consumer DAGs that depend on these datasets.
    """
    raw = fetch_raw_data()
    processed = process_data(raw)
    save_processed_data(processed)


# Instantiate the DAG
producer_dag = producer_pipeline()

# For local testing
if __name__ == "__main__":
    print("Testing producer pipeline...")
    producer_dag.test()
