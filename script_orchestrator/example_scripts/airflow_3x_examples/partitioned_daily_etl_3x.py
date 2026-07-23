"""Airflow 3.x DAG exercising date-parameter → Dagster partitions.

This DAG runs daily and uses Airflow's context date params (`{{ ds }}`,
`data_interval_start`) inside its tasks — exactly the pattern that in
Airflow requires custom scripts to backfill, but in Dagster becomes a
drag-select operation in the UI.

When the orchestrator sees:
  1. Cron schedule = daily
  2. Task uses `ds` / `execution_date` / `data_interval_start` / etc.
it attaches a DailyPartitionsDefinition to the emitted asset. At run
time, the partition key gets passed to Airflow as `execution_date`, so
`{{ ds }}` inside tasks resolves to the partition being materialized.
"""

from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="partitioned_daily_etl",
    start_date=datetime(2024, 1, 1),
    schedule="0 6 * * *",  # daily at 06:00 UTC
    catchup=False,
    tags=["etl", "daily", "partitioned"],
)
def partitioned_daily_etl():
    """Daily ETL — one partition per day. Backfill any range in Dagster."""

    @task
    def extract(ds: str, data_interval_start=None) -> dict:
        """Extract data for the given day. `ds` is Airflow's date string.

        In Dagster this task receives the partition_key as `ds` via the
        `execution_date` we pass to `airflow dags test`.
        """
        print(f"Extracting rows for ds={ds}, interval_start={data_interval_start}")
        return {
            "ds": ds,
            "row_count": 100,
            "source": f"s3://raw/events/date={ds}",
        }

    @task
    def transform(raw: dict) -> dict:
        """Transform rows for the extract's day."""
        print(f"Transforming ds={raw['ds']}: {raw['row_count']} rows")
        return {**raw, "transformed": True}

    @task
    def load(rec: dict) -> None:
        """Write partitioned output. `s3://curated/date=<ds>/rows.parquet`."""
        target = f"s3://curated/date={rec['ds']}/rows.parquet"
        print(f"Loading {rec['row_count']} rows → {target}")

    load(transform(extract()))


partitioned_daily_etl_dag = partitioned_daily_etl()
