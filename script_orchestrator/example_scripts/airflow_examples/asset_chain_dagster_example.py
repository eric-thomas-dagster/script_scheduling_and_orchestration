"""
Correct Dagster implementation for asset_chain_example.yaml

Demonstrates the key principle:
- Tasks with outlets → Assets (even if they depend on other assets)
- Tasks without outlets → Op jobs (terminal operations)

The chain: Asset → Asset → Asset → Op Job
"""

from dagster import (
    asset,
    op,
    job,
    asset_sensor,
    RunRequest,
    AssetKey,
    AssetExecutionContext,
    OpExecutionContext,
)
from include.tasks.chain_example import (
    _fetch_raw_data,
    _clean_data,
    _aggregate_data,
    _send_alert
)


# ============================================================================
# Asset Chain: Each step has outlets, so each is an asset
# ============================================================================

@asset(
    name="raw_data",
    description="Raw data fetched from API",
    group_name="data_pipeline",
    compute_kind="api"
)
def raw_data(context: AssetExecutionContext):
    """
    Step 1: Fetch raw data.

    Airflow DAG: fetch_raw_data
    - Has outlets: [raw_data]
    → Dagster: @asset ✅
    """
    context.log.info("Fetching raw data from API...")
    result = _fetch_raw_data()
    return result


@asset(
    name="cleaned_data",
    deps=[raw_data],  # ← Depends on another asset
    description="Cleaned and validated data",
    group_name="data_pipeline",
    compute_kind="python"
)
def cleaned_data(context: AssetExecutionContext):
    """
    Step 2: Clean raw data.

    Airflow DAG: clean_data
    - Schedule: [raw_data asset]  ← Has asset input
    - Has outlets: [cleaned_data] ← But also has outlets!
    → Dagster: @asset ✅ (NOT an op job - it produces data!)

    This is STILL an asset because it has outlets.
    The fact that it depends on another asset doesn't make it an op job.
    """
    context.log.info("Cleaning raw data...")
    result = _clean_data()
    return result


@asset(
    name="aggregated_data",
    deps=[cleaned_data],  # ← Depends on another asset
    description="Aggregated statistics",
    group_name="data_pipeline",
    compute_kind="python"
)
def aggregated_data(context: AssetExecutionContext):
    """
    Step 3: Aggregate cleaned data.

    Airflow DAG: aggregate_data
    - Schedule: [cleaned_data asset]  ← Has asset input
    - Has outlets: [aggregated_data] ← But also has outlets!
    → Dagster: @asset ✅ (NOT an op job - it produces data!)

    Again, STILL an asset because it has outlets.
    We can chain assets: raw_data → cleaned_data → aggregated_data
    """
    context.log.info("Aggregating data...")
    result = _aggregate_data()
    return result


# ============================================================================
# Terminal Operation: No outlets, so it's a job (NOT an asset!)
# ============================================================================
# This represents the Airflow DAG "send_completion_alert" as a Dagster @job
# because the DAG has NO outlets (doesn't produce assets)

@op
def send_alert(context: OpExecutionContext):
    """
    Send alert email.

    Airflow task: notify_team (part of send_completion_alert DAG)
    - NO outlets → This is an operation, not an asset
    """
    context.log.info("Sending completion alert...")
    _send_alert()


@job(
    name="send_completion_alert",  # Named after the Airflow DAG
    description="Send alert when data pipeline completes (Airflow DAG: send_completion_alert)",
    tags={"source": "airflow_dag", "airflow_dag_id": "send_completion_alert"}
)
def send_completion_alert():
    """
    Represents the Airflow DAG: send_completion_alert

    In Airflow:
    - DAG with NO outlets (doesn't produce assets)
    - Scheduled by aggregated_data asset
    - Has one task: notify_team

    In Dagster:
    - @job (because no outlets = not an asset)
    - Triggered by @asset_sensor watching aggregated_data
    - If this DAG had multiple tasks (e.g., format_message, send_email, log_sent),
      they would all be @ops within this job

    Example with multiple tasks:
    @job
    def send_completion_alert():
        message = format_message()
        send_email(message)
        log_sent(message)
    """
    send_alert()


@asset_sensor(
    asset_key=AssetKey("aggregated_data"),
    job=send_completion_alert,  # Reference the @job
    description="Trigger alert job when aggregation completes"
)
def aggregation_complete_sensor(context, asset_event):
    """
    Sensor that watches for aggregated_data materialization.

    This implements the Airflow asset-based scheduling pattern:
    - Airflow: DAG scheduled by asset → schedule: [aggregated_data]
    - Dagster: @asset_sensor → triggers @job

    When aggregated_data updates, this triggers the send_completion_alert job.
    """
    context.log.info("Aggregated data materialized! Triggering alert job...")
    yield RunRequest()


# ============================================================================
# Summary of the Architecture
# ============================================================================

"""
The full chain:

raw_data (asset)
    ↓ depends on
cleaned_data (asset)  ← STILL an asset! Has outlets even though it has deps
    ↓ depends on
aggregated_data (asset)  ← STILL an asset! Has outlets even though it has deps
    ↓ triggers via sensor
send_alert_job (op job)  ← OP JOB! No outlets, just performs operation

In Airflow YAML terms:
- fetch_raw_data: outlets: [raw_data] → Asset
- clean_data: outlets: [cleaned_data], schedule: [raw_data] → Asset (not op job!)
- aggregate_data: outlets: [aggregated_data], schedule: [cleaned_data] → Asset (not op job!)
- send_completion_alert: NO outlets, schedule: [aggregated_data] → Op Job

The rule: outlets → asset, no outlets → op job
NOT: "depends on asset" → op job ❌
"""
