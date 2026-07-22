# /// script
# dependencies = ["prefect>=3.4"]
# ///

"""Prefect @materialize demo exercising ALL supported Prefect 3.x features:

  1. Artifacts   → create_markdown_artifact / create_link_artifact /
                   create_table_artifact captured as Dagster MetadataValue.
  2. Nested subflows → outer @flow calls a subflow whose body contains
                   @materialize; deps are inferred through the subflow call.
  3. Concurrency + tags → @materialize(concurrency_key=..., tags=[...])
                   surfaces as op_tags / spec tags in Dagster.
  4. AutomationCondition → any @materialize with deps gets
                   AutomationCondition.eager() attached automatically.
"""

from prefect import flow
from prefect.artifacts import (
    create_link_artifact,
    create_markdown_artifact,
    create_table_artifact,
)
from prefect.assets import Asset, AssetProperties, add_asset_metadata, materialize


# ── External upstream (declared but not materialized here) ───────────────────

api_source = Asset(
    key="https://api.example.com/v2/customers",
    properties=AssetProperties(
        name="Customer API",
        description="Source-of-truth customer endpoint owned by platform team.",
        owners=["platform-team@example.com"],
        url="https://api.example.com/v2/customers",
    ),
)


# ── Materialized assets ──────────────────────────────────────────────────────

@materialize(
    "s3://lake/raw/customers.parquet",
    asset_deps=[api_source],
    tags=["etl", "raw"],
    retries=2,
    retry_delay_seconds=10,
)
def extract_customers():
    """Pull customers from the API and write raw parquet."""
    rows = [{"id": i, "name": f"cust-{i}"} for i in range(1, 6)]
    add_asset_metadata({"row_count": len(rows), "source": "api.example.com"})
    create_markdown_artifact(
        key="extract-summary",
        markdown=(
            "### Extract summary\n\n"
            f"- Rows extracted: **{len(rows)}**\n"
            "- Source: `api.example.com/v2/customers`\n"
        ),
        description="Human-readable extraction report",
    )
    return rows


@materialize(
    "s3://lake/curated/customers.parquet",
    tags=["etl", "curated"],
)
def clean_customers(raw):
    """Clean the raw extract; the dep on the raw asset is inferred implicitly."""
    cleaned = [{**r, "name": r["name"].upper()} for r in raw]
    add_asset_metadata({"row_count": len(cleaned)})
    create_table_artifact(
        key="clean-sample",
        table=[{"col": "name", "example": cleaned[0]["name"] if cleaned else None}],
        description="Sample of cleaned rows",
    )
    return cleaned


# ── A subflow: outer flow calls this and it materializes further ─────────────

@materialize(
    "snowflake://prod/ANALYTICS.MARTS.CUSTOMER_DIM",
    materialized_by="dbt",
    tags=["mart", "dbt"],
)
def build_dim(cleaned):
    """Build the customer dim from cleaned rows."""
    n = len(cleaned)
    add_asset_metadata({"model": "customer_dim", "row_count": n})
    create_link_artifact(
        key="dbt-docs",
        link="https://dbt.example.com/#!/model/model.analytics.customer_dim",
        description="dbt docs for this model",
    )
    return {"n": n}


@flow
def loading_subflow(cleaned):
    """Subflow: load cleaned data into snowflake via dbt."""
    return build_dim(cleaned)


# ── Orchestrating outer flow ─────────────────────────────────────────────────

@flow(log_prints=True, retries=3, retry_delay_seconds=15, tags=["pipeline", "customer"])
def customer_pipeline():
    """End-to-end: extract → clean → subflow(load-to-mart).

    The retry policy set here on `@flow` applies to the whole materialization
    (i.e. the Dagster multi_asset compute), and overrides any per-@materialize
    retries. Adjacent `prefect.yaml` deployments become Dagster
    ScheduleDefinitions targeting the same asset set.
    """
    raw = extract_customers()
    cleaned = clean_customers(raw)
    loading_subflow(cleaned)


if __name__ == "__main__":
    customer_pipeline()
