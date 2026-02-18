#!/usr/bin/env python3
"""
Example Prefect flow that can be executed by Dagster.
This shows how existing Prefect workflows can run in Dagster.
"""

from datetime import datetime
import time

# Mock Prefect decorators for demo (in production, use actual Prefect)
try:
    from prefect import flow, task
except ImportError:
    # Fallback for demo without Prefect installed
    def flow(name=None):
        def decorator(func):
            return func
        return decorator
    def task(func):
        return func

@task
def fetch_data():
    """Simulate fetching data from an API."""
    print(f"[{datetime.now()}] Fetching data from API...")
    time.sleep(1)
    return {"records": [{"id": i, "value": i * 10} for i in range(1, 6)]}

@task
def process_data(data):
    """Process the fetched data."""
    print(f"[{datetime.now()}] Processing {len(data['records'])} records...")
    time.sleep(1)

    processed = {
        "total": sum(r["value"] for r in data["records"]),
        "count": len(data["records"]),
        "processed_at": datetime.now().isoformat()
    }
    return processed

@task
def save_results(results):
    """Save results to storage."""
    print(f"[{datetime.now()}] Saving results...")
    print(f"  Total: {results['total']}")
    print(f"  Count: {results['count']}")
    print(f"  Processed at: {results['processed_at']}")
    time.sleep(0.5)
    return results

@flow(name="data-processing-flow")
def data_processing_flow():
    """
    Prefect flow that processes data.
    Can be executed directly by Prefect or via Dagster orchestration.
    """
    print(f"[{datetime.now()}] Starting Prefect flow...")

    # Prefect's task orchestration
    data = fetch_data()
    results = process_data(data)
    final = save_results(results)

    print(f"[{datetime.now()}] Flow completed successfully!")
    return final

if __name__ == "__main__":
    # When run directly or by Dagster
    data_processing_flow()
