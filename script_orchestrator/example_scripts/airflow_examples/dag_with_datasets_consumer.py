"""
Airflow DAG demonstrating Datasets - Consumer (Airflow 2.4+).
This DAG is triggered automatically when the processed_data_dataset is updated.
"""
from datetime import datetime
from airflow import Dataset
from airflow.decorators import dag, task

# Reference to the dataset this DAG consumes
processed_data_dataset = Dataset("s3://bucket/processed_data/")
analytics_dataset = Dataset("s3://bucket/analytics/")


@dag(
    dag_id="consumer_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=[processed_data_dataset],  # Triggered when dataset is updated
    catchup=False,
    tags=["example", "datasets", "consumer"],
    description="Consumes data from datasets - runs when data is ready",
    outlets=[analytics_dataset]  # Produces analytics dataset
)
def consumer_pipeline():
    """
    Consumer DAG that is triggered automatically when the processed_data_dataset
    is updated by the producer pipeline. This enables data-aware scheduling.
    """

    @task
    def load_and_analyze():
        """Load data from the processed dataset and run analytics."""
        print("Loading data from processed dataset")
        print("Running analytics...")
        return {"analytics_complete": True, "insights": ["pattern_1", "pattern_2"]}

    @task
    def publish_results(results):
        """Publish analytics results."""
        print(f"Publishing analytics results: {results}")
        print(f"Found {len(results.get('insights', []))} insights")
        return "Analytics published"

    results = load_and_analyze()
    publish_results(results)


# Instantiate the DAG
consumer_dag = consumer_pipeline()

# For local testing
if __name__ == "__main__":
    print("Testing consumer pipeline...")
    consumer_dag.test()
