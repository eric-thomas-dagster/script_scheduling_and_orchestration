"""Cross-file @materialize lineage — the two-pass emit fix.

Background: `parse_prefect_assets(script_path, repo_root=...)` follows a
script's `from X import Y` statements into sibling files under `repo_root`
so the AST call-graph walker can infer asset-to-asset dep edges that cross
a file boundary, not just within one script.

That inference correctly computes a dep whenever the CONSUMER happens to be
a function defined in the file currently being parsed (see
`test_intermediate_hop_already_worked_and_still_does` below). But when the
consumer is a `@materialize` function that lives in the OTHER file — reached
either by importing its wrapping `@flow` (a subflow) or by calling the
imported `@materialize` directly as the last step of the chain — the dep
gets computed correctly but was previously discarded: the AssetSpec for
that consumer's URI is built when the OWNING file is parsed, and that parse
has no way to know what the calling file passed in.

`parse_prefect_assets` now surfaces these as `cross_file_extra_deps:
{target_uri: [dep_uri, ...]}`. `ScriptGithubComponent._build_cross_file_extra_deps_map`
merges this across every script in a first pass; `create_materialize_multi_asset`
re-attaches it to the owning AssetSpec in a second pass. These tests exercise
the parser layer directly (both passes simulated inline) since that's where
all the actual lineage-inference logic lives.
"""
from pathlib import Path

import pytest

from script_orchestrator.components.parsers.prefect_asset_support import (
    parse_prefect_assets,
)


def _write(path: Path, content: str) -> None:
    path.write_text(content)


def _merge_cross_file_extra_deps(*results: dict) -> dict:
    """Mirrors ScriptGithubComponent._build_cross_file_extra_deps_map's merge."""
    global_map: dict = {}
    for result in results:
        for target_uri, dep_uris in (result.get("cross_file_extra_deps") or {}).items():
            bucket = global_map.setdefault(target_uri, [])
            for dep_uri in dep_uris:
                if dep_uri not in bucket:
                    bucket.append(dep_uri)
    return global_map


def _final_deps(materialized_entry: dict, global_map: dict) -> list:
    """Mirrors create_materialize_multi_asset's merge of asset_deps + cross_file_extra_deps."""
    extra = global_map.get(materialized_entry["asset_key"], [])
    return list(dict.fromkeys(materialized_entry["asset_deps"] + extra))


def test_subflow_wrapped_cross_file_consumer(tmp_path: Path):
    """The documented gap: caller imports a @flow (not the inner
    @materialize directly); the subflow's own @materialize consumes the
    arg the caller passed. Both sides of the call are only visible while
    parsing the CALLER; the target asset is owned by the OTHER file."""
    repo = tmp_path
    _write(repo / "helpers.py", """
from prefect import flow
from prefect.assets import materialize

@materialize("s3://bucket/processed")
def process(raw):
    return raw

@flow
def subflow(raw_data):
    return process(raw_data)
""")
    _write(repo / "caller.py", """
from prefect import flow
from prefect.assets import materialize
from helpers import subflow

@materialize("s3://bucket/raw")
def load_raw():
    return [1, 2, 3]

@flow
def main_flow():
    raw = load_raw()
    return subflow(raw)
""")

    caller_result = parse_prefect_assets(repo / "caller.py", repo_root=repo)
    helpers_result = parse_prefect_assets(repo / "helpers.py", repo_root=repo)

    # The edge is discovered while parsing the caller...
    assert caller_result["cross_file_extra_deps"] == {
        "s3://bucket/processed": ["s3://bucket/raw"]
    }
    # ...but helpers.py's own parse, in isolation, can't see it.
    assert helpers_result["cross_file_extra_deps"] == {}

    global_map = _merge_cross_file_extra_deps(caller_result, helpers_result)
    process_entry = next(
        m for m in helpers_result["materialized"] if m["asset_key"] == "s3://bucket/processed"
    )
    assert _final_deps(process_entry, global_map) == ["s3://bucket/raw"]


def test_direct_call_cross_file_terminal_consumer(tmp_path: Path):
    """Same gap, no subflow indirection: the caller imports and calls a
    @materialize directly, and it's the LAST hop (nothing local consumes
    its output) — so nothing in the caller's own file needs the edge, but
    the imported function's own AssetSpec (built when its file is parsed)
    does."""
    repo = tmp_path
    _write(repo / "direct_helper.py", """
from prefect.assets import materialize

@materialize("s3://y/thing")
def build_thing(raw):
    return raw
""")
    _write(repo / "direct_caller.py", """
from prefect import flow
from prefect.assets import materialize
from direct_helper import build_thing

@materialize("s3://y/raw")
def load_raw():
    return [1]

@flow
def main_flow():
    r = load_raw()
    return build_thing(r)
""")

    caller_result = parse_prefect_assets(repo / "direct_caller.py", repo_root=repo)
    helper_result = parse_prefect_assets(repo / "direct_helper.py", repo_root=repo)

    global_map = _merge_cross_file_extra_deps(caller_result, helper_result)
    thing_entry = next(m for m in helper_result["materialized"] if m["asset_key"] == "s3://y/thing")
    assert _final_deps(thing_entry, global_map) == ["s3://y/raw"]


def test_intermediate_hop_already_worked_and_still_does(tmp_path: Path):
    """Sanity check on the pre-existing (already-shipped) case: an imported
    @materialize feeds a LOCAL consumer. The dep attaches directly to the
    caller's own AssetSpec — no cross_file_extra_deps involved at all.
    Must keep working unchanged."""
    repo = tmp_path
    _write(repo / "shared.py", """
from prefect.assets import materialize

@materialize("s3://shared/features")
def build_features(raw):
    return raw
""")
    _write(repo / "pipeline.py", """
from prefect import flow
from prefect.assets import materialize
from shared import build_features

@materialize("s3://shared/raw")
def ingest():
    return "raw"

@materialize("s3://shared/scores")
def score(features):
    return features

@flow
def pipeline():
    raw = ingest()
    features = build_features(raw)
    return score(features)
""")

    result = parse_prefect_assets(repo / "pipeline.py", repo_root=repo)
    score_entry = next(m for m in result["materialized"] if m["asset_key"] == "s3://shared/scores")
    assert score_entry["asset_deps"] == ["s3://shared/features"]


def test_same_file_subflow_unaffected(tmp_path: Path):
    """Sanity check: same-file subflow lineage (no repo_root needed at all)
    keeps working unchanged."""
    repo = tmp_path
    _write(repo / "samefile.py", """
from prefect import flow
from prefect.assets import materialize

@materialize("s3://x/a")
def make_a():
    return 1

@materialize("s3://x/b")
def make_b(a):
    return a

@flow
def subflow(raw):
    return make_b(raw)

@flow
def outer():
    a = make_a()
    return subflow(a)
""")

    result = parse_prefect_assets(repo / "samefile.py")
    b_entry = next(m for m in result["materialized"] if m["asset_key"] == "s3://x/b")
    assert b_entry["asset_deps"] == ["s3://x/a"]


def test_no_cross_file_edges_returns_empty_dict(tmp_path: Path):
    """cross_file_extra_deps is always present (never missing/None) even
    when there's nothing to report — callers rely on `.get(...)` /
    truthiness checks, not key existence."""
    repo = tmp_path
    _write(repo / "solo.py", """
from prefect.assets import materialize

@materialize("s3://solo/thing")
def make_thing():
    return 1
""")
    result = parse_prefect_assets(repo / "solo.py", repo_root=repo)
    assert result["cross_file_extra_deps"] == {}
