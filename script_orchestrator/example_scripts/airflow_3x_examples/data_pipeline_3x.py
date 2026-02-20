"""
Data pipeline using Airflow 3.x Asset-based scheduling.
Demonstrates airflow.sdk.Asset for producer/consumer patterns.
"""
from datetime import datetime

from airflow.sdk import Asset, dag, task


# Define assets (datasets)
raw_data_asset = Asset("raw_customer_data")
processed_data_asset = Asset("processed_customer_data")


@dag(
    dag_id="extract_customer_data_3x",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["producer", "3.x", "assets"],
)
def extract_customer_data():
    """Producer DAG: Extract raw customer data and produce an asset."""

    @task(outlets=[raw_data_asset])
    def fetch_customers():
        """Fetch customer data from API."""
        print("Fetching customer data from API...")

        # Simulate API call
        customers = [
            {"id": 1, "name": "Alice", "email": "alice@example.com"},
            {"id": 2, "name": "Bob", "email": "bob@example.com"},
            {"id": 3, "name": "Charlie", "email": "charlie@example.com"},
        ]

        print(f"✅ Fetched {len(customers)} customers")
        return customers

    fetch_customers()


@dag(
    dag_id="process_customer_data_3x",
    start_date=datetime(2024, 1, 1),
    schedule=[raw_data_asset],  # Triggered by raw_data_asset updates
    catchup=False,
    tags=["consumer", "3.x", "assets"],
)
def process_customer_data():
    """Consumer DAG: Process customer data when raw data is available."""

    @task(outlets=[processed_data_asset])
    def process_customers():
        """Process raw customer data."""
        print("Processing customer data...")

        # Simulate processing
        print("  - Validating email addresses")
        print("  - Normalizing names")
        print("  - Calculating customer score")

        print("✅ Customer data processed successfully")

    process_customers()


# Create DAG instances
extract_customer_data()
process_customer_data()
