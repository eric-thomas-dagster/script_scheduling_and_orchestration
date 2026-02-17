#!/usr/bin/env python3
"""Test Prefect flow for job mode."""

from prefect import flow, task


@task
def say_hello():
    """Say hello."""
    print("Hello from job!")
    return "hello"


@task
def say_goodbye(greeting: str):
    """Say goodbye."""
    print(f"Goodbye after {greeting}!")
    return "goodbye"


@flow
def test_job_flow():
    """Simple flow to test job mode."""
    greeting = say_hello()
    result = say_goodbye(greeting)
    return result


if __name__ == "__main__":
    test_job_flow()
