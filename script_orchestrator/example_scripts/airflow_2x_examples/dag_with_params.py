"""
Airflow DAG with parameters using Params.
Tests parameter extraction and config generation.
"""
from datetime import datetime
from airflow.decorators import dag, task
from airflow.models.param import Param


@task
def fetch_urls(urls: list) -> list:
    """Fetch content from multiple URLs."""
    print(f"Fetching {len(urls)} URLs")
    results = []
    for url in urls:
        results.append({
            "url": url,
            "content": f"Content from {url}",
            "length": len(url) * 10
        })
    return results


@task
def process_results(results: list, min_length: int) -> list:
    """Filter results by minimum length."""
    print(f"Filtering results with min_length={min_length}")
    filtered = [r for r in results if r["length"] >= min_length]
    print(f"Kept {len(filtered)} of {len(results)} results")
    return filtered


@task
def save_output(results: list, output_format: str) -> str:
    """Save results in specified format."""
    print(f"Saving {len(results)} results in {output_format} format")
    return f"Saved {len(results)} results as {output_format}"


@dag(
    dag_id="parameterized_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "params"],
    params={
        "urls": Param(
            default=["https://example.com", "https://test.com"],
            type=["array"],
            description="List of URLs to fetch"
        ),
        "min_length": Param(
            default=100,
            type="integer",
            description="Minimum content length to keep"
        ),
        "output_format": Param(
            default="json",
            type="string",
            enum=["json", "csv", "xml"],
            description="Output format for results"
        ),
    },
)
def parameterized_pipeline():
    """Pipeline with configurable parameters."""
    # Access params from dag_run.conf
    from airflow.operators.python import get_current_context

    @task
    def get_params():
        context = get_current_context()
        return context["params"]

    params = get_params()

    # Note: In actual execution, we'd access params differently
    # For now, using defaults for static analysis
    urls = fetch_urls(["https://example.com", "https://test.com"])
    filtered = process_results(urls, 100)
    save_output(filtered, "json")


# Instantiate the DAG
dag_instance = parameterized_pipeline()
