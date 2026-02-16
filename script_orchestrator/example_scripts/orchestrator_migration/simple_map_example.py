#!/usr/bin/env python3
"""
Simple Prefect flow demonstrating .map() without runtime complexity.
This should successfully map to Dagster's dynamic ops.
"""

from prefect import flow, task


@task(retries=2, retry_delay_seconds=5)
def process_number(n: int) -> int:
    """Process a single number."""
    print(f"Processing {n}")
    return n * 2


@task
def generate_numbers() -> list[int]:
    """Generate a list of numbers to process."""
    return [1, 2, 3, 4, 5]


@task
def sum_results(results: list[int]) -> int:
    """Sum all results."""
    return sum(results)


@flow
def simple_map_flow():
    """
    Flow demonstrating simple .map() pattern.

    This maps to Dagster dynamic ops because:
    - Uses .map() for parallel execution
    - No as_completed() or other runtime constructs
    - Results are collected and used directly
    """
    numbers = generate_numbers()
    processed = process_number.map(numbers)
    total = sum_results(processed)
    return total


if __name__ == "__main__":
    result = simple_map_flow()
    print(f"Total: {result}")
