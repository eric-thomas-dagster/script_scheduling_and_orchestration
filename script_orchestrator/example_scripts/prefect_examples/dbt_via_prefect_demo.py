# /// script
# dependencies = ["prefect>=3.4", "prefect-dbt>=0.6"]
# ///

"""Prefect flow that runs a dbt project via prefect_dbt.

In Prefect: the entire dbt project runs as one opaque task — no per-model
visibility, no lineage between models, no column info, no test-based checks.

In Dagster (with `dbt_project_path` set OR PrefectDbtRunner(project_dir=...)
detected inline): this whole file is skipped in the regular Prefect asset
build path, and the dbt project is expanded into one native `@dbt_assets`
Dagster asset per model — with lineage from `manifest.json`, column schemas
from `catalog.json`, and dbt tests as asset checks.
"""

from prefect import flow
from prefect_dbt import PrefectDbtRunner


@flow(log_prints=True, retries=2)
def run_analytics_dbt():
    """Nightly analytics refresh — all models and tests."""
    runner = PrefectDbtRunner(project_dir="dbt/jaffle_shop")
    runner.invoke(["build"])


if __name__ == "__main__":
    run_analytics_dbt()
