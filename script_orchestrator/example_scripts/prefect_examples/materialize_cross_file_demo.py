# /// script
# dependencies = ["prefect>=3.4"]
# ///

"""Cross-file @materialize demo — shows Dagster inferring lineage across
file boundaries.

The story:
  - `materialize_cross_file_helpers.py` defines `build_feature_matrix`,
    a reusable @materialize function that other scripts import and call.
  - This script imports it directly (`from materialize_cross_file_helpers
    import build_feature_matrix`), then calls it inside a @flow alongside
    local @materialize functions.
  - Dagster's parser reads the import, walks into the helpers file, finds
    the imported @materialize, and — when the caller's @flow chains its
    output into another @materialize here — infers the dep edge.

Result: lineage `s3://prod/lake/raw/events → s3://prod/lake/curated/features/
feature_matrix.parquet → s3://prod/lake/models/scores.parquet` shows up as
one arrow chain in the Dagster UI, spanning two source files.

Zero explicit `asset_deps=[]` anywhere. Zero yaml overrides. The Python
call graph across files IS the lineage graph.
"""

from prefect import flow
from prefect.assets import materialize

from materialize_cross_file_helpers import build_feature_matrix


@materialize(
    "s3://prod/lake/raw/events.parquet",
    materialized_by="python",
    tags=["etl", "raw"],
)
def ingest_raw_events() -> str:
    """Land raw clickstream events, hourly from the CDN."""
    return "s3://prod/lake/raw/events.parquet"


@materialize(
    "s3://prod/lake/models/scores.parquet",
    materialized_by="python",
    tags=["ml", "scoring"],
)
def score_churn(features_uri: str) -> str:
    """Score per-user churn against the latest feature matrix."""
    return "s3://prod/lake/models/scores.parquet"


@flow(name="cross_file_churn_pipeline")
def cross_file_churn_pipeline() -> str:
    """Chain a local @materialize → imported @materialize → local @materialize.

    Dagster infers all three edges from the Python call graph. The middle
    hop crosses a file boundary via the top-level `from ... import
    build_feature_matrix` statement.
    """
    events = ingest_raw_events()
    features = build_feature_matrix(events)
    return score_churn(features)


if __name__ == "__main__":
    cross_file_churn_pipeline()
