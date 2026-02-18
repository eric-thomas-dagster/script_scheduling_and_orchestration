"""
Simple ETL pipeline using Airflow 3.x syntax.
Demonstrates airflow.sdk imports and basic task flow.
"""
from datetime import datetime

from airflow.sdk import dag, task


@task
def extract() -> dict:
    """Extract data from source."""
    print("Extracting data from source...")
    return {
        "records": [
            {"id": 1, "value": 100},
            {"id": 2, "value": 200},
            {"id": 3, "value": 300},
        ]
    }


@task
def transform(data: dict) -> dict:
    """Transform the extracted data."""
    print("Transforming data...")
    records = data["records"]

    # Simple transformation: double the values
    transformed = [
        {"id": r["id"], "value": r["value"] * 2}
        for r in records
    ]

    return {"records": transformed}


@task
def load(data: dict) -> None:
    """Load transformed data to destination."""
    print("Loading data to destination...")
    records = data["records"]

    for record in records:
        print(f"  Loaded: {record}")

    print(f"✅ Successfully loaded {len(records)} records")


@dag(
    dag_id="simple_etl_3x",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "3.x", "simple"],
)
def simple_etl_pipeline():
    """Simple ETL pipeline with extract, transform, load."""
    extracted_data = extract()
    transformed_data = transform(extracted_data)
    load(transformed_data)


# Create the DAG instance
simple_etl_pipeline()
