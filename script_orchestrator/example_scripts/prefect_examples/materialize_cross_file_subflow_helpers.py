# /// script
# dependencies = ["prefect>=3.4"]
# ///

"""Helper module for materialize_cross_file_subflow_demo.py — demonstrates
the cross-file *subflow* lineage case, as opposed to
materialize_cross_file_helpers.py's direct-call case.

The difference matters: here the caller imports a `@flow` (`enrich_orders`,
a subflow), not the inner `@materialize` directly. The subflow's own
`@materialize` (`enriched_orders`) is never itself imported or named by the
caller — it's purely internal to this file. Dagster's parser still needs to
attach the caller's argument as a dep on `enriched_orders`'s asset, but the
edge is discovered while parsing the CALLER (that's the only place both the
subflow call and its argument's producer are visible) while the target
asset belongs to THIS file's own multi_asset. Closing that gap requires a
two-pass emit at the ScriptGithubComponent level — see CLAUDE.md → "Cross-
file lineage gap" and `prefect_asset_support.py`'s `cross_file_extra_deps`.
"""

from prefect import flow
from prefect.assets import materialize


@materialize(
    "s3://prod/lake/curated/orders/enriched_orders.parquet",
    materialized_by="python",
    tags=["etl", "shared"],
)
def enriched_orders(raw_orders_uri: str) -> str:
    """Join raw orders against dimension tables.

    Realistic body would read `raw_orders_uri`, join customer/product dims,
    and write the result. The demo just returns the URI so the caller's op
    graph has data flow the parser can inspect.
    """
    return "s3://prod/lake/curated/orders/enriched_orders.parquet"


@flow(name="enrich_orders")
def enrich_orders(raw_orders_uri: str) -> str:
    """Subflow wrapping `enriched_orders` — the shape that matters here.

    Callers import and call THIS flow, never `enriched_orders` directly.
    """
    return enriched_orders(raw_orders_uri)
