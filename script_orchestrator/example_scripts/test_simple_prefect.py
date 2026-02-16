#!/usr/bin/env python
"""Simple Prefect flow for testing graph asset mapping."""

from prefect import flow, task


@task(retries=2)
def fetch_data(source: str = "api") -> dict:
    """Fetch data from source."""
    print(f"Fetching data from {source}")
    return {"source": source, "data": [1, 2, 3, 4, 5]}


@task
def process_data(data: dict) -> dict:
    """Process the data."""
    print(f"Processing data from {data['source']}")
    processed = {"source": data["source"], "processed": [x * 2 for x in data["data"]]}
    return processed


@task
def save_results(processed: dict) -> str:
    """Save the results."""
    print(f"Saving results: {processed}")
    return f"Saved {len(processed['processed'])} items"


@flow(log_prints=True)
def simple_pipeline(source: str = "api"):
    """Simple sequential pipeline."""
    data = fetch_data(source)
    processed = process_data(data)
    result = save_results(processed)
    print(f"Pipeline complete: {result}")


if __name__ == "__main__":
    simple_pipeline()
