"""
Correct Dagster implementation for asset_example_dag.yaml

This demonstrates the proper mapping:
- Airflow DAG with outlets → Dagster asset
- Airflow DAG with asset schedule → Dagster asset sensor + op job

NOT everything needs to be an asset!
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
    AssetMaterialization,
    DagsterEventType
)
from include.tasks.asset_example_tasks import (
    _update_iss_coordinates,
    _read_iss_coordinates,
    _get_iss_coordinates_file_path
)


# ============================================================================
# Producer: DAG with outlets → Asset
# ============================================================================

@asset(
    name="iss_coordinates",
    description="Current ISS coordinates fetched from API",
    group_name="space_tracking",
    compute_kind="api",
    metadata={
        "source": "http://api.open-notify.org/iss-now.json",
        "update_frequency": "daily"
    }
)
def iss_coordinates(context: AssetExecutionContext):
    """
    Fetch ISS coordinates and save to file.

    Corresponds to Airflow DAG: update_iss_coordinates
    - Task: update_coordinates
    - Outlets: [iss_coordinates]

    This IS an asset because it produces a data artifact (the file).
    """
    context.log.info("Fetching ISS coordinates from API...")
    _update_iss_coordinates()

    file_path = _get_iss_coordinates_file_path()
    context.log.info(f"ISS coordinates saved to: {file_path}")

    # Optional: read and return for metadata
    import json
    with open(file_path) as f:
        coords = json.load(f)

    return coords  # Dagster tracks this in metadata


# ============================================================================
# Consumer: DAG with asset schedule → Job (NOT an asset!)
# ============================================================================
# Note: This represents the entire Airflow DAG "process_iss_coordinates"
# as a Dagster @job because it has NO outlets (doesn't produce assets)

@op
def read_iss_coordinates(context: OpExecutionContext):
    """
    Read and display ISS coordinates.

    Corresponds to Airflow task: read_coordinates
    (part of process_iss_coordinates DAG)

    This is NOT an asset - it just reads and prints (side effect only).
    """
    context.log.info("Reading ISS coordinates from file...")
    _read_iss_coordinates()


@job(
    name="process_iss_coordinates",  # Named after the Airflow DAG
    description="Process ISS coordinates DAG (triggered by asset updates)",
    tags={"source": "airflow_dag", "airflow_dag_id": "process_iss_coordinates"}
)
def process_iss_coordinates():
    """
    Represents the Airflow DAG: process_iss_coordinates

    In Airflow:
    - DAG with NO outlets (doesn't produce assets)
    - Scheduled by iss_coordinates asset

    In Dagster:
    - @job (because no outlets = not an asset)
    - Triggered by @asset_sensor

    If this DAG had multiple tasks, they'd all be @ops in this job.
    """
    read_iss_coordinates()


@asset_sensor(
    asset_key=AssetKey("iss_coordinates"),
    job=process_iss_coordinates,  # Reference the job
    description="Trigger processing job when ISS coordinates are updated"
)
def iss_coordinates_sensor(context, asset_event):
    """
    Sensor that triggers the processing job when iss_coordinates materializes.

    This implements the Airflow asset-based scheduling pattern:
    - Airflow: DAG scheduled by asset → schedule: [iss_coordinates asset]
    - Dagster: @asset_sensor watching iss_coordinates → triggers @job

    The flow: iss_coordinates (asset) updates → sensor detects → triggers process_iss_coordinates (job)
    """
    context.log.info("ISS coordinates asset materialized! Triggering processing job...")
    yield RunRequest()


# ============================================================================
# Alternative: Make consumer an asset too (if you want to track execution)
# ============================================================================

@asset(
    name="iss_coordinates_processed",
    deps=[iss_coordinates],
    description="Processed ISS coordinates (read and logged)",
    group_name="space_tracking",
    compute_kind="python"
)
def iss_coordinates_processed(context: AssetExecutionContext):
    """
    Alternative: Make this an asset if you want to track when processing happens.

    Use this if:
    - You want to see processing in the asset lineage
    - You want to track when processing last ran
    - You want to backfill processing runs

    Use the op job version if:
    - It's truly just a side effect
    - You don't need to track it as data
    """
    context.log.info("Processing ISS coordinates...")
    _read_iss_coordinates()
    # No return value needed - just tracking that we processed it
