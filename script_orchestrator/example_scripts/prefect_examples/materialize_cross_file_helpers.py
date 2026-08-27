# /// script
# dependencies = ["prefect>=3.4"]
# ///

"""Helper module for materialize_cross_file_demo.py — demonstrates cross-file
@materialize import resolution.

This file is IMPORTED by materialize_cross_file_demo.py; its own @materialize
function is exposed under the `build_feature_matrix` name, and the caller
uses it directly inside a @flow. The Dagster parser follows the import
statement, discovers this @materialize, and emits the dep edge back on the
caller's asset.

Prefect users commonly split scripts like this — a repo has a `helpers/` or
`assets/` module that ships reusable @materialize definitions, and top-level
scripts orchestrate them. Without cross-file resolution, Dagster's inferred
lineage would stop at the import boundary; with it, the arrow crosses the
file.
"""

from prefect.assets import materialize


@materialize(
    "s3://prod/lake/curated/features/feature_matrix.parquet",
    materialized_by="python",
    tags=["ml", "shared"],
)
def build_feature_matrix(events_uri: str) -> str:
    """Turn a raw-events URI into a feature-matrix Parquet URI.

    Realistic body would read `events_uri`, join dims, engineer features,
    and write the result. The demo just returns the URI so the caller's
    op graph has data flow the parser can inspect.
    """
    return "s3://prod/lake/curated/features/feature_matrix.parquet"
