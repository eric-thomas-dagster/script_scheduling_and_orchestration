# /// script
# dependencies = ["prefect>=3.4"]
# ///

"""Prefect @materialize demo — surfaces as native Dagster assets.

Exercises all the primitives the orchestrator understands:
  - Module-scope Asset(...) with AssetProperties (owners, description, url)
  - @materialize with a literal URI
  - @materialize with an Asset ref + asset_deps=[...]
  - materialized_by="dbt"  -> becomes a Dagster kind
  - add_asset_metadata({...}) inside the function body
    -> captured by the runtime shim and yielded as MaterializeResult metadata
"""

from prefect import flow
from prefect.assets import Asset, AssetProperties, add_asset_metadata, materialize


# ── Upstream: declared as an Asset so we can reference it in asset_deps ──────

raw_customers = Asset(
    key="postgres://prod-warehouse/public/customers_raw",
    properties=AssetProperties(
        name="Raw Customers Table",
        description="Source-of-truth customer table maintained by platform team.",
        owners=["platform-team@example.com"],
        url="https://internal.example.com/tables/customers_raw",
    ),
)


# ── Owned assets: declare via Asset() to attach properties (owners, url, …) ──

customer_dim_asset = Asset(
    key="s3://analytics-lake/curated/customer_dim.parquet",
    properties=AssetProperties(
        name="Customer Dimension",
        description="Cleaned, deduped customer dimension used by all analytics marts.",
        owners=["analytics-eng@example.com"],
        url="https://internal.example.com/lake/curated/customer_dim",
    ),
)


# ── Materialized outputs (each becomes a Dagster asset) ──────────────────────

@materialize(customer_dim_asset, asset_deps=[raw_customers])
def build_customer_dim():
    """Clean + dedupe raw customers, write out the dimension table."""
    rows = [
        {"id": 1, "name": "Acme"},
        {"id": 2, "name": "Globex"},
        {"id": 3, "name": "Initech"},
    ]
    add_asset_metadata({
        "row_count": len(rows),
        "source": "postgres://prod-warehouse/public/customers_raw",
        "notes": "deduplicated on customer_id",
    })
    return rows


@materialize(
    "snowflake://prod/ANALYTICS.MARTS.CUSTOMER_METRICS",
    asset_deps=["s3://analytics-lake/curated/customer_dim.parquet"],
    materialized_by="dbt",
)
def build_customer_metrics(customer_dim):
    """Aggregate the customer dimension into a metrics mart (dbt-managed)."""
    metric_count = 4
    add_asset_metadata({
        "metric_count": metric_count,
        "grain": "daily",
        "dbt_model": "customer_metrics",
    })
    return {"metrics": metric_count}


# ── Orchestrating flow ───────────────────────────────────────────────────────

@flow(log_prints=True)
def customer_analytics_pipeline():
    """Two-step Prefect pipeline emitting three assets (one external + two owned)."""
    dim = build_customer_dim()
    build_customer_metrics(dim)


if __name__ == "__main__":
    customer_analytics_pipeline()
