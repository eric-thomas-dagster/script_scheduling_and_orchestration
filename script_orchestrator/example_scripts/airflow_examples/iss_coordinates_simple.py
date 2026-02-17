"""
Simplest correct Dagster implementation for asset_example_dag.yaml

Key insight:
- Use asset jobs (not graph assets) for DAGs with outlets
- Use op jobs for DAGs without outlets
- No need to mix them with graph assets!
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
from include.tasks.asset_example_tasks import (
    _update_iss_coordinates,
    _read_iss_coordinates,
)


# ============================================================================
# Producer: Airflow DAG with outlets → Asset
# ============================================================================

@asset(
    name="iss_coordinates",
    description="Current ISS coordinates from API",
    compute_kind="api"
)
def iss_coordinates():
    """
    Airflow DAG: update_iss_coordinates
    - Has outlets: [iss_coordinates]
    → Dagster: Simple @asset (no graph asset needed!)
    """
    _update_iss_coordinates()


# Asset Job - represents the Airflow DAG
update_iss_coordinates_job = define_asset_job(
    name="update_iss_coordinates",
    description="Update ISS coordinates (from Airflow DAG)",
    selection=[iss_coordinates],
    tags={"source": "airflow_dag"}
)


# ============================================================================
# Consumer: Airflow DAG without outlets → Op Job
# ============================================================================

@op
def read_coordinates():
    """
    Airflow task: read_coordinates (from process_iss_coordinates DAG)
    - NO outlets
    → Dagster: Simple @op
    """
    _read_iss_coordinates()


@job(
    name="process_iss_coordinates",
    description="Process ISS coordinates (from Airflow DAG)",
    tags={"source": "airflow_dag"}
)
def process_iss_coordinates():
    """
    Airflow DAG: process_iss_coordinates
    - NO outlets
    - Scheduled by iss_coordinates asset
    → Dagster: Op job
    """
    read_coordinates()


# Sensor connects asset job → op job
@asset_sensor(
    asset_key=AssetKey("iss_coordinates"),
    job=process_iss_coordinates,
)
def iss_coordinates_sensor(context, asset_event):
    """
    When iss_coordinates asset materializes → trigger op job.

    This represents the Airflow asset-based scheduling:
    schedule: [iss_coordinates asset]
    """
    yield RunRequest()


# ============================================================================
# Summary
# ============================================================================
"""
Two jobs:
1. update_iss_coordinates_job (asset job) - materializes iss_coordinates
2. process_iss_coordinates (op job) - reads and prints

Flow:
update_iss_coordinates_job runs → materializes iss_coordinates asset
                                 ↓
                        sensor detects materialization
                                 ↓
                  process_iss_coordinates op job runs

No graph assets needed! Just:
- Asset jobs for DAGs with outlets
- Op jobs for DAGs without outlets
"""
