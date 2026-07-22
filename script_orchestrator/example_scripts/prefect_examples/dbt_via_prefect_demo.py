# /// script
# dependencies = ["prefect>=3.4", "prefect-dbt>=0.6"]
# ///

"""Prefect flow that runs a dbt project via prefect_dbt.

Adapted from Prefect's own example:
  https://docs.prefect.io/v3/examples/run-dbt-with-prefect

In Prefect: the entire dbt project runs as one opaque task — no per-model
visibility, no lineage between models, no column info, no test-based checks.

In Dagster (with `dbt_project_path` set — or auto-detected from the
`PrefectDbtSettings(project_dir=...)` call below): this whole file is
skipped in the regular Prefect asset build path, and the dbt project is
expanded into one native `@dbt_assets` Dagster asset per model — with
model-to-model lineage from `manifest.json`, column schemas from
`catalog.json`, and dbt tests as asset checks.
"""

from prefect import flow
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

# Literal paths so the orchestrator can auto-adopt them from AST inspection.
# Dynamic paths (e.g. `str(Path(some_var))`) work fine at runtime but won't
# be picked up by the AST scanner — set `dbt_project_path` in defs.yaml then.
DBT_PROJECT_DIR = "dbt/jaffle_shop"
DBT_PROFILES_DIR = "dbt/jaffle_shop"


@flow(log_prints=True, retries=2)
def run_analytics_dbt():
    """Nightly analytics refresh — deps → seed → run → test."""
    settings = PrefectDbtSettings(
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
    )
    runner = PrefectDbtRunner(settings=settings)

    for command in ["deps", "seed", "run", "test"]:
        print(f"Executing: dbt {command}")
        runner.invoke(command.split())
        print(f"Completed: dbt {command}\n")


if __name__ == "__main__":
    run_analytics_dbt()
