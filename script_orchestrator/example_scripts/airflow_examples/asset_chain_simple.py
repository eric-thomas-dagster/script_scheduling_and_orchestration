"""
Simple correct Dagster implementation for asset_chain_example.yaml

Key insight:
- Multiple assets that depend on each other can be grouped in an asset job
- No graph asset needed!
- Op job at the end for terminal operations
"""

from dagster import (
    asset,
    op,
    job,
    asset_sensor,
    RunRequest,
    AssetKey,
    define_asset_job,
    AssetSelection,
)
from include.tasks.chain_example import (
    _fetch_raw_data,
    _clean_data,
    _aggregate_data,
    _send_alert
)


# ============================================================================
# Asset Chain: 4 Airflow DAGs with outlets → 4 Assets grouped in one job
# ============================================================================

@asset(name="raw_data", compute_kind="api")
def raw_data():
    """Airflow DAG: fetch_raw_data (has outlets: [raw_data])"""
    return _fetch_raw_data()


@asset(name="cleaned_data", deps=[raw_data], compute_kind="python")
def cleaned_data():
    """Airflow DAG: clean_data (has outlets: [cleaned_data])"""
    return _clean_data()


@asset(name="aggregated_data", deps=[cleaned_data], compute_kind="python")
def aggregated_data():
    """Airflow DAG: aggregate_data (has outlets: [aggregated_data])"""
    return _aggregate_data()


# Asset Job - represents all the Airflow DAGs that produce assets
# This groups the 3 assets together as one executable unit
data_pipeline_job = define_asset_job(
    name="data_pipeline",
    description="Full data pipeline (from Airflow DAGs: fetch, clean, aggregate)",
    selection=AssetSelection.all(),  # All 3 assets
    tags={"source": "airflow_dags", "pipeline": "etl"}
)

# Or you can define separate jobs for each:
# fetch_job = define_asset_job("fetch_raw_data", selection=[raw_data])
# clean_job = define_asset_job("clean_data", selection=[cleaned_data])
# aggregate_job = define_asset_job("aggregate_data", selection=[aggregated_data])


# ============================================================================
# Terminal Operation: Airflow DAG without outlets → Op Job
# ============================================================================

@op
def send_alert():
    """Airflow task: notify_team (from send_completion_alert DAG)"""
    _send_alert()


@job(
    name="send_completion_alert",
    description="Send alert when pipeline completes (from Airflow DAG)",
    tags={"source": "airflow_dag", "type": "notification"}
)
def send_completion_alert():
    """
    Airflow DAG: send_completion_alert
    - NO outlets
    - Scheduled by aggregated_data asset
    → Dagster: Op job
    """
    send_alert()


# Sensor connects asset → op job
@asset_sensor(
    asset_key=AssetKey("aggregated_data"),
    job=send_completion_alert,
)
def alert_sensor(context, asset_event):
    """
    When aggregated_data materializes → trigger alert job.

    Represents Airflow: schedule: [aggregated_data asset]
    """
    yield RunRequest()


# ============================================================================
# Summary
# ============================================================================
"""
Two types of jobs:

1. data_pipeline_job (asset job)
   - Materializes: raw_data → cleaned_data → aggregated_data
   - Represents the 3 Airflow DAGs with outlets
   - When you run this job, Dagster materializes the assets in order

2. send_completion_alert (op job)
   - Just performs operations (sends email)
   - Represents the 1 Airflow DAG without outlets
   - Triggered by sensor when aggregated_data materializes

Flow:
data_pipeline_job runs → materializes raw_data, cleaned_data, aggregated_data
                                   ↓
                          sensor detects aggregated_data
                                   ↓
                       send_completion_alert op job runs

No graph assets needed!
- Asset jobs group assets
- Op jobs group ops
- Sensors connect them
"""
