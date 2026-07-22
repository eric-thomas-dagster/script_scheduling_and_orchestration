# /// script
# dependencies = ["prefect>=3.4"]
# ///

"""Prefect @materialize demo with IMPLICIT deps only.

`build_features` and `build_scores` don't declare `asset_deps=` — the
orchestrator recovers the dep edges by walking the @flow body's call graph:
  raw = load_raw()      → produces s3://raw/events.parquet
  feat = build_features(raw)   → depends on s3://raw/events.parquet
  scores = build_scores(feat)  → depends on s3://curated/features.parquet
"""

from prefect import flow
from prefect.assets import add_asset_metadata, materialize


@materialize("s3://raw/events.parquet")
def load_raw():
    add_asset_metadata({"event_count": 1_000_000})
    return list(range(3))


@materialize("s3://curated/features.parquet")
def build_features(raw):
    add_asset_metadata({"feature_count": 42, "input_rows": len(raw)})
    return [r * 2 for r in raw]


@materialize("s3://models/scores.parquet")
def build_scores(features):
    add_asset_metadata({"model_version": "v3.2", "n_predictions": len(features)})
    return [f + 1 for f in features]


@flow(log_prints=True)
def scoring_pipeline():
    raw = load_raw()
    features = build_features(raw)
    build_scores(features)


if __name__ == "__main__":
    scoring_pipeline()
