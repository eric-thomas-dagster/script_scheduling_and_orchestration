# /// script
# dependencies = ["prefect>=3.4"]
# ///

"""Cross-file *subflow* @materialize demo — the lineage case
materialize_cross_file_demo.py doesn't cover.

The story:
  - `materialize_cross_file_subflow_helpers.py` defines `enrich_orders`, a
    `@flow` (subflow) that wraps its own `@materialize`
    (`enriched_orders`). Other scripts import and call the SUBFLOW, not
    the inner `@materialize` — that's the difference from
    materialize_cross_file_demo.py, where the imported name IS the
    `@materialize` function.
  - This script imports `enrich_orders` and calls it inside its own
    `@flow`, passing in a locally-produced URI.
  - Dagster's parser discovers the edge while parsing THIS script (both the
    subflow call and its argument's producer are only visible here), but
    the target asset — `enriched_orders`'s own `@materialize` output —
    belongs to the helper file's multi_asset, not this one. A two-pass
    emit at the ScriptGithubComponent level resolves this: every script
    gets parsed once to build a global {target_uri: [dep_uri]} map, then
    each script's own AssetSpecs pick up whatever the map has for their
    own URIs when they're actually built.

Result: lineage `s3://prod/lake/raw/orders.parquet → s3://prod/lake/curated/
orders/enriched_orders.parquet → s3://prod/lake/models/order_risk_scores.
parquet` shows up as one arrow chain in the Dagster UI, spanning two
source files, with the middle hop reached through a subflow import.

Zero explicit `asset_deps=[]` anywhere. Zero yaml overrides. The Python
call graph across files — including through a subflow — IS the lineage
graph.
"""

from prefect import flow
from prefect.assets import materialize

from materialize_cross_file_subflow_helpers import enrich_orders


@materialize(
    "s3://prod/lake/raw/orders.parquet",
    materialized_by="python",
    tags=["etl", "raw"],
)
def ingest_raw_orders() -> str:
    """Land raw order events, hourly from the orders service."""
    return "s3://prod/lake/raw/orders.parquet"


@materialize(
    "s3://prod/lake/models/order_risk_scores.parquet",
    materialized_by="python",
    tags=["ml", "risk"],
)
def score_order_risk(enriched_orders_uri: str) -> str:
    """Score each enriched order for fraud/return risk."""
    return "s3://prod/lake/models/order_risk_scores.parquet"


@flow(name="cross_file_subflow_pipeline")
def cross_file_subflow_pipeline() -> str:
    """Chain a local @materialize → imported subflow (wrapping its own
    @materialize) → local @materialize.

    Dagster infers all three edges from the Python call graph. The middle
    hop crosses a file boundary THROUGH a subflow — `enrich_orders` is a
    `@flow`, not the `@materialize` itself.
    """
    orders = ingest_raw_orders()
    enriched = enrich_orders(orders)
    return score_order_risk(enriched)


if __name__ == "__main__":
    cross_file_subflow_pipeline()
