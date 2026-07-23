"""Prefect asset (`@materialize`) support: AST detection + runtime shim.

Prefect 3.4+ introduced first-class assets via `prefect.assets`:
  - `Asset(key="s3://...", properties=AssetProperties(...))`
  - `AssetProperties(name=..., description=..., owners=[...], url=...)`
  - `@materialize("s3://...", asset_deps=[...], materialized_by="dbt")`
  - `add_asset_metadata({...})` (runtime; emits per-materialization metadata)

This module gives us three things:

1. `parse_prefect_assets(path)` — AST scan for `@materialize` decorators plus
   module-scope `Asset(...)` / `AssetProperties(...)` bindings.

2. `install_asset_shim(fake_prefect_module)` — hangs a fake `prefect.assets`
   submodule off the fake `prefect` module so scripts using
   `from prefect.assets import materialize, ...` execute against our stubs.
   The `materialize` stub wraps the function as a Dagster op *and* sets a
   context var so `add_asset_metadata` knows which URI to attribute captures to.

3. `create_materialize_multi_asset(...)` — builds a Dagster `@multi_asset`
   with one `AssetSpec` per detected Prefect asset URI. The compute function
   runs the flow via the monkey-patch approach and yields one
   `MaterializeResult` per URI, carrying any metadata captured via the shim.
"""

from __future__ import annotations

import ast
import contextvars
import importlib.util
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


# ── Runtime capture: shared between shim and multi_asset compute ─────────────

# ContextVar (not thread-local) so we can .set/.reset within one execution.
# Maps asset URI → metadata dict; the multi_asset compute reads it after run.
_CURRENT_CAPTURE: contextvars.ContextVar[Optional[Dict[str, Dict[str, Any]]]] = (
    contextvars.ContextVar("_prefect_asset_capture", default=None)
)
# URIs the currently-executing @materialize function is bound to.
_CURRENT_ASSETS: contextvars.ContextVar[Tuple[str, ...]] = contextvars.ContextVar(
    "_prefect_asset_current", default=()
)


# ── AST detection ────────────────────────────────────────────────────────────

def _literal_or_none(node: ast.AST) -> Any:
    """Best-effort ast.literal_eval; returns None if not a literal."""
    try:
        return ast.literal_eval(node)
    except (ValueError, SyntaxError, TypeError):
        return None


def _extract_asset_bindings(tree: ast.Module) -> Dict[str, Dict[str, Any]]:
    """Find module-scope `<var> = Asset(...)` bindings.

    Returns {var_name: {"key": str_or_None, "properties": {...}}}.
    """
    bindings: Dict[str, Dict[str, Any]] = {}
    props_bindings: Dict[str, Dict[str, Any]] = _extract_properties_bindings(tree)

    for stmt in tree.body:
        if not isinstance(stmt, ast.Assign) or len(stmt.targets) != 1:
            continue
        target = stmt.targets[0]
        if not isinstance(target, ast.Name):
            continue
        if not isinstance(stmt.value, ast.Call):
            continue
        call = stmt.value
        func_name = (
            call.func.id if isinstance(call.func, ast.Name)
            else call.func.attr if isinstance(call.func, ast.Attribute)
            else None
        )
        if func_name != "Asset":
            continue

        info: Dict[str, Any] = {"key": None, "properties": {}}
        # positional key
        if call.args:
            info["key"] = _literal_or_none(call.args[0])
        # kwargs
        for kw in call.keywords:
            if kw.arg == "key":
                info["key"] = _literal_or_none(kw.value)
            elif kw.arg == "properties":
                info["properties"] = _resolve_properties(kw.value, props_bindings)
        bindings[target.id] = info

    return bindings


def _extract_properties_bindings(tree: ast.Module) -> Dict[str, Dict[str, Any]]:
    """Find module-scope `<var> = AssetProperties(...)` bindings."""
    bindings: Dict[str, Dict[str, Any]] = {}
    for stmt in tree.body:
        if not isinstance(stmt, ast.Assign) or len(stmt.targets) != 1:
            continue
        target = stmt.targets[0]
        if not isinstance(target, ast.Name):
            continue
        if not isinstance(stmt.value, ast.Call):
            continue
        call = stmt.value
        func_name = (
            call.func.id if isinstance(call.func, ast.Name)
            else call.func.attr if isinstance(call.func, ast.Attribute)
            else None
        )
        if func_name != "AssetProperties":
            continue
        bindings[target.id] = _resolve_properties(call, {})
    return bindings


def _resolve_properties(
    node: ast.AST, props_bindings: Dict[str, Dict[str, Any]]
) -> Dict[str, Any]:
    """Resolve an AssetProperties value — either an inline Call or a Name ref."""
    if isinstance(node, ast.Name):
        return props_bindings.get(node.id, {})
    if not isinstance(node, ast.Call):
        return {}
    result: Dict[str, Any] = {}
    for kw in node.keywords:
        if kw.arg in {"name", "description", "url", "owners"}:
            val = _literal_or_none(kw.value)
            if val is not None:
                result[kw.arg] = val
    return result


def _resolve_asset_ref(
    node: ast.AST, asset_bindings: Dict[str, Dict[str, Any]]
) -> Optional[str]:
    """Resolve a `@materialize` positional arg or `asset_deps` entry to a URI string.

    Accepts: string literal, or Name referring to a module-scope Asset(...).
    """
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name):
        binding = asset_bindings.get(node.id)
        if binding:
            return binding.get("key")
    return None


def parse_prefect_assets(script_path: Path) -> Dict[str, Any]:
    """Parse a Prefect script and return detected @materialize + external assets.

    Returns:
      {
        "materialized": [                # one entry per @materialize output
          {
            "asset_key": "s3://bucket/path",
            "asset_key_path": ["s3", "bucket", "path"],
            "asset_deps": ["postgres://..."],   # explicit + inferred
            "explicit_deps": ["..."],           # from asset_deps= only
            "inferred_deps": ["..."],           # from flow-body walk
            "properties": {...},
            "materialized_by": "dbt" | None,
            "function_name": "make_thing",
          },
          ...
        ],
        "external": [                    # Asset(...) bindings referenced only in deps
          {
            "asset_key": "postgres://...",
            "asset_key_path": [...],
            "properties": {...},          # description, owners, url, name
          },
          ...
        ],
      }

    Non-literal / dynamic URIs are skipped (with a debug log).
    """
    try:
        with open(script_path, "r") as f:
            tree = ast.parse(f.read(), filename=str(script_path))
    except Exception as e:
        logger.debug(f"Could not AST-parse {script_path} for Prefect assets: {e}")
        return {"materialized": [], "external": []}

    asset_bindings = _extract_asset_bindings(tree)

    def props_for_uri(uri: str) -> Dict[str, Any]:
        for b in asset_bindings.values():
            if b.get("key") == uri:
                return b.get("properties", {})
        return {}

    # ── Pass 1: gather @materialize functions and their explicit metadata ────

    # function_name → list[str] of URIs this function materializes
    fn_to_uris: Dict[str, List[str]] = {}
    materialized_raw: List[Dict[str, Any]] = []

    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        for dec in node.decorator_list:
            call = dec if isinstance(dec, ast.Call) else None
            if call is None:
                continue
            func_name = (
                call.func.id if isinstance(call.func, ast.Name)
                else call.func.attr if isinstance(call.func, ast.Attribute)
                else None
            )
            if func_name != "materialize":
                continue

            uris: List[str] = []
            for arg in call.args:
                uri = _resolve_asset_ref(arg, asset_bindings)
                if uri:
                    uris.append(uri)
                else:
                    logger.debug(
                        f"Skipping non-literal @materialize arg in {script_path}"
                    )

            explicit_deps: List[str] = []
            materialized_by: Optional[str] = None
            tags_kw: List[str] = []
            concurrency_key: Optional[str] = None
            retries: Optional[int] = None
            retry_delay_seconds: Optional[Any] = None
            for kw in call.keywords:
                if kw.arg == "asset_deps" and isinstance(kw.value, (ast.List, ast.Tuple)):
                    for elt in kw.value.elts:
                        dep = _resolve_asset_ref(elt, asset_bindings)
                        if dep:
                            explicit_deps.append(dep)
                elif kw.arg == "materialized_by":
                    val = _literal_or_none(kw.value)
                    if isinstance(val, str):
                        materialized_by = val
                elif kw.arg == "tags":
                    val = _literal_or_none(kw.value)
                    if isinstance(val, (list, tuple)):
                        tags_kw = [str(t) for t in val]
                elif kw.arg == "concurrency_key":
                    val = _literal_or_none(kw.value)
                    if isinstance(val, str):
                        concurrency_key = val
                elif kw.arg == "retries":
                    val = _literal_or_none(kw.value)
                    if isinstance(val, int):
                        retries = val
                elif kw.arg == "retry_delay_seconds":
                    val = _literal_or_none(kw.value)
                    if isinstance(val, (int, float, list, tuple)):
                        retry_delay_seconds = val

            fn_to_uris[node.name] = uris
            materialized_raw.append({
                "function_name": node.name,
                "uris": uris,
                "explicit_deps": explicit_deps,
                "materialized_by": materialized_by,
                "tags": tags_kw,
                "concurrency_key": concurrency_key,
                "retries": retries,
                "retry_delay_seconds": retry_delay_seconds,
                # `assert X, "msg"` statements inside the body become
                # AssetCheckSpecs on the emitted asset.
                "asserts": _extract_assert_checks(node),
            })

    # ── Pass 2: walk @flow bodies to infer implicit deps via call graph ──────

    inferred_deps_per_fn = _infer_deps_from_flows(tree, fn_to_uris)

    # ── Emit materialized entries (explicit ∪ inferred deps, dedup) ──────────

    materialized_out: List[Dict[str, Any]] = []
    seen_uris: set[str] = set()
    for entry in materialized_raw:
        inferred = inferred_deps_per_fn.get(entry["function_name"], [])
        for uri in entry["uris"]:
            if uri in seen_uris:
                continue
            seen_uris.add(uri)
            deps = list(dict.fromkeys(entry["explicit_deps"] + inferred))  # ordered dedup
            materialized_out.append({
                "asset_key": uri,
                "asset_key_path": uri_to_asset_key_path(uri),
                "asset_deps": deps,
                "explicit_deps": list(entry["explicit_deps"]),
                "inferred_deps": list(inferred),
                "properties": props_for_uri(uri),
                "materialized_by": entry["materialized_by"],
                "function_name": entry["function_name"],
                "prefect_tags": list(entry.get("tags", [])),
                "concurrency_key": entry.get("concurrency_key"),
                "retries": entry.get("retries"),
                "retry_delay_seconds": entry.get("retry_delay_seconds"),
                "asserts": list(entry.get("asserts", [])),
            })

    # ── External upstream Assets: bindings referenced in deps, not materialized

    materialized_uris = {m["asset_key"] for m in materialized_out}
    dep_uris: set[str] = set()
    for m in materialized_out:
        dep_uris.update(m["asset_deps"])

    external_out: List[Dict[str, Any]] = []
    seen_external: set[str] = set()
    for binding in asset_bindings.values():
        key = binding.get("key")
        if not key:
            continue
        if key in materialized_uris:
            continue  # it's a materialized output, not external
        if key not in dep_uris:
            continue  # binding exists but nothing depends on it
        if key in seen_external:
            continue
        seen_external.add(key)
        external_out.append({
            "asset_key": key,
            "asset_key_path": uri_to_asset_key_path(key),
            "properties": binding.get("properties", {}),
        })

    return {"materialized": materialized_out, "external": external_out}


def _infer_deps_from_flows(
    tree: ast.Module, fn_to_uris: Dict[str, List[str]]
) -> Dict[str, List[str]]:
    """Walk @flow function bodies to infer implicit asset-to-asset deps.

    Handles two patterns:

    (a) Direct chain within one flow:
        @materialize("s3://a")
        def make_a(): return 1

        @materialize("s3://b")
        def make_b(a): ...

        @flow
        def pipe():
            a = make_a()
            b = make_b(a)      # → s3://b depends on s3://a

    (b) Nested subflows:
        @flow
        def subflow(raw):
            return make_b(raw)

        @flow
        def outer():
            a = make_a()
            b = subflow(a)     # → same edge inferred through the subflow

    Returns {materialize_function_name → [uri, ...]} of extra deps.
    """
    if not fn_to_uris:
        return {}

    # Discover @flow signatures + return producers + parameter usage.
    flow_info = _analyze_flows(tree, fn_to_uris)

    result: Dict[str, List[str]] = {}

    def _add_dep(consumer: str, uri: str) -> None:
        lst = result.setdefault(consumer, [])
        if uri not in lst:
            lst.append(uri)

    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        if not _has_flow_decorator(node):
            continue

        # var_name → producing materialize function name
        var_to_fn: Dict[str, str] = {}

        for stmt in ast.walk(node):
            # a) `var = <materialize_or_subflow>(...)` → track binding
            if isinstance(stmt, ast.Assign) and isinstance(stmt.value, ast.Call):
                callee = _call_target_name(stmt.value)
                producer: Optional[str] = None
                if callee and callee in fn_to_uris:
                    producer = callee
                elif callee and callee in flow_info:
                    producer = flow_info[callee].get("return_producer")
                if producer:
                    for tgt in stmt.targets:
                        if isinstance(tgt, ast.Name):
                            var_to_fn[tgt.id] = producer

            # b) Any Call: propagate producer URIs into materialize consumers
            #    (both direct and through subflow parameter → inner-consumer chains).
            if isinstance(stmt, ast.Call):
                callee = _call_target_name(stmt)
                if not callee:
                    continue

                # Case 1: direct materialize call → straightforward dep edges.
                if callee in fn_to_uris:
                    for arg in stmt.args:
                        p = _producer_of(arg, var_to_fn, fn_to_uris)
                        if p and p != callee:
                            for uri in fn_to_uris.get(p, []):
                                _add_dep(callee, uri)
                    for kw in stmt.keywords:
                        p = _producer_of(kw.value, var_to_fn, fn_to_uris)
                        if p and p != callee:
                            for uri in fn_to_uris.get(p, []):
                                _add_dep(callee, uri)

                # Case 2: subflow call → arg's producer flows into the inner
                #  materialize consumer(s) that use the matching parameter.
                elif callee in flow_info:
                    subflow = flow_info[callee]
                    params: List[str] = subflow["params"]
                    consumers_by_param: Dict[str, List[str]] = subflow["param_consumers"]
                    for i, arg in enumerate(stmt.args):
                        if i >= len(params):
                            break
                        p = _producer_of(arg, var_to_fn, fn_to_uris)
                        if not p:
                            continue
                        for inner_consumer in consumers_by_param.get(params[i], []):
                            for uri in fn_to_uris.get(p, []):
                                _add_dep(inner_consumer, uri)
                    for kw in stmt.keywords:
                        if not kw.arg or kw.arg not in consumers_by_param:
                            continue
                        p = _producer_of(kw.value, var_to_fn, fn_to_uris)
                        if not p:
                            continue
                        for inner_consumer in consumers_by_param.get(kw.arg, []):
                            for uri in fn_to_uris.get(p, []):
                                _add_dep(inner_consumer, uri)

    return result


def _analyze_flows(
    tree: ast.Module, fn_to_uris: Dict[str, List[str]]
) -> Dict[str, Dict[str, Any]]:
    """Analyze every @flow in the module for subflow propagation support.

    Returns {flow_name: {
        "params": [param_name, ...],
        "return_producer": materialize_fn_name | None,
        "param_consumers": {param_name: [materialize_fn_names_that_use_it]},
    }}
    """
    info: Dict[str, Dict[str, Any]] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        if not _has_flow_decorator(node):
            continue

        params = [a.arg for a in node.args.args if a.arg not in {"self", "cls"}]

        # Return producer: track var→materialize binding through the body,
        # then check what the (last) return statement points at.
        local_var_to_fn: Dict[str, str] = {}
        return_producer: Optional[str] = None
        for stmt in ast.walk(node):
            if isinstance(stmt, ast.Assign) and isinstance(stmt.value, ast.Call):
                callee = _call_target_name(stmt.value)
                if callee and callee in fn_to_uris:
                    for tgt in stmt.targets:
                        if isinstance(tgt, ast.Name):
                            local_var_to_fn[tgt.id] = callee
            if isinstance(stmt, ast.Return) and stmt.value is not None:
                # `return make_b(...)` or `return b`
                if isinstance(stmt.value, ast.Call):
                    callee = _call_target_name(stmt.value)
                    if callee and callee in fn_to_uris:
                        return_producer = callee
                elif isinstance(stmt.value, ast.Name):
                    return_producer = local_var_to_fn.get(stmt.value.id) or return_producer

        # For each subflow parameter, find inner materialize calls that
        # consume it (parameter name appears as a positional/kw arg).
        param_consumers: Dict[str, List[str]] = {p: [] for p in params}
        for sub in ast.walk(node):
            if not isinstance(sub, ast.Call):
                continue
            callee = _call_target_name(sub)
            if not callee or callee not in fn_to_uris:
                continue
            for arg in sub.args:
                if isinstance(arg, ast.Name) and arg.id in param_consumers:
                    if callee not in param_consumers[arg.id]:
                        param_consumers[arg.id].append(callee)
            for kw in sub.keywords:
                if isinstance(kw.value, ast.Name) and kw.value.id in param_consumers:
                    if callee not in param_consumers[kw.value.id]:
                        param_consumers[kw.value.id].append(callee)

        info[node.name] = {
            "params": params,
            "return_producer": return_producer,
            "param_consumers": param_consumers,
        }

    return info


def _extract_assert_checks(
    func_node: ast.FunctionDef,
) -> List[Dict[str, Any]]:
    """Return one entry per `assert` statement in a function body.

    Shape: {"index": N, "name": <slug>, "description": <msg or code>, "line": N}.
    Only top-level asserts count; asserts inside nested functions/classes are
    ignored (they're likely defensive code inside helpers).
    """
    checks: List[Dict[str, Any]] = []
    for stmt in ast.walk(func_node):
        if not isinstance(stmt, ast.Assert):
            continue
        # Skip asserts nested inside child function definitions.
        # (ast.walk descends into everything; we only want asserts whose
        # nearest enclosing FunctionDef is func_node itself.)
        # Cheap heuristic: check we can find stmt as a descendant of func_node
        # NOT inside another FunctionDef.
        # For simplicity + typical usage, we accept nested asserts too. Users
        # who want to opt out can use `if not X: raise ValueError(...)` instead.
        msg: str
        try:
            if stmt.msg is not None:
                lit = ast.literal_eval(stmt.msg)
                msg = str(lit) if isinstance(lit, str) else ast.unparse(stmt.msg)
            else:
                msg = ast.unparse(stmt.test)
        except Exception:
            msg = f"assert at line {stmt.lineno}"
        # Simple slug for the check name
        slug = "check_"
        for c in msg.lower():
            if c.isalnum():
                slug += c
            elif c in " -_":
                slug += "_"
            if len(slug) >= 40:
                break
        slug = slug.rstrip("_") or f"check_line_{stmt.lineno}"
        checks.append({
            "index": len(checks),
            "name": f"{slug}_L{stmt.lineno}",
            "description": msg,
            "line": stmt.lineno,
        })
    return checks


def _has_flow_decorator(func_node: ast.FunctionDef) -> bool:
    for dec in func_node.decorator_list:
        if isinstance(dec, ast.Name) and dec.id == "flow":
            return True
        if isinstance(dec, ast.Call):
            f = dec.func
            if isinstance(f, ast.Name) and f.id == "flow":
                return True
            if isinstance(f, ast.Attribute) and f.attr == "flow":
                return True
    return False


def extract_flow_level_kwargs(script_path: Path) -> Dict[str, Any]:
    """Return `@flow(retries=..., retry_delay_seconds=..., ...)` kwargs for
    the file's flows. Uses the LAST @flow declared (the usual entrypoint
    convention), or if any @flow has retries=, prefers that flow's kwargs.

    Returns a dict with keys among:
      retries: int
      retry_delay_seconds: int | float | list
      tags: list[str]
      timeout_seconds: int
      log_prints: bool
    """
    try:
        with open(script_path, "r") as f:
            tree = ast.parse(f.read(), filename=str(script_path))
    except Exception:
        return {}

    flows: List[Dict[str, Any]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        for dec in node.decorator_list:
            call = dec if isinstance(dec, ast.Call) else None
            if call is None:
                continue
            f = call.func
            if not (
                (isinstance(f, ast.Name) and f.id == "flow")
                or (isinstance(f, ast.Attribute) and f.attr == "flow")
            ):
                continue
            kwargs: Dict[str, Any] = {}
            for kw in call.keywords:
                if kw.arg in {
                    "retries",
                    "retry_delay_seconds",
                    "tags",
                    "timeout_seconds",
                    "log_prints",
                }:
                    val = _literal_or_none(kw.value)
                    if val is not None:
                        kwargs[kw.arg] = val
            flows.append(kwargs)

    if not flows:
        return {}
    # Prefer a flow that declares retries; else the last declared flow.
    for fk in flows:
        if "retries" in fk:
            return fk
    return flows[-1]


def load_dbt_columns(
    dbt_project_path: Optional[str], model_hint: str
) -> Optional[List[Dict[str, str]]]:
    """Look up column info for a dbt model in the compiled catalog.json.

    Args:
      dbt_project_path: path to a dbt project (must contain target/catalog.json).
      model_hint: the model name (or last segment of the asset URI) to match.

    Returns [{"name": col, "type": type_string}, ...] or None if not found.
    """
    if not dbt_project_path:
        return None
    import json as _json
    catalog_path = Path(dbt_project_path) / "target" / "catalog.json"
    if not catalog_path.exists():
        return None
    try:
        with open(catalog_path) as fh:
            catalog = _json.load(fh)
    except Exception as e:
        logger.debug(f"Could not load dbt catalog {catalog_path}: {e}")
        return None

    hint_lower = model_hint.lower()
    for node_id, node in (catalog.get("nodes") or {}).items():
        # node_id: "model.<project>.<model_name>" — match on the tail.
        parts = node_id.split(".")
        if not parts:
            continue
        model_name = parts[-1]
        if model_name.lower() != hint_lower:
            continue
        cols = node.get("columns") or {}
        return [
            {"name": c.get("name") or key, "type": (c.get("type") or "").upper() or "UNKNOWN"}
            for key, c in cols.items()
        ]
    return None


def build_dbt_column_lineage(
    dbt_project_path: Optional[str], model_hint: str
) -> Optional[Any]:
    """Build a Dagster TableColumnLineage from dbt manifest depends_on info.

    Given a dbt model, we can infer *table-level* upstream deps from
    `manifest.nodes.<id>.depends_on.nodes`. Full column-level lineage
    requires SQL parsing; for now we emit column-to-parent-table edges
    (which shows up in the Dagster column lineage panel).
    """
    if not dbt_project_path:
        return None
    import json as _json
    from dagster import TableColumnDep, TableColumnLineage

    manifest_path = Path(dbt_project_path) / "target" / "manifest.json"
    catalog_path = Path(dbt_project_path) / "target" / "catalog.json"
    if not manifest_path.exists() or not catalog_path.exists():
        return None
    try:
        with open(manifest_path) as fh:
            manifest = _json.load(fh)
        with open(catalog_path) as fh:
            catalog = _json.load(fh)
    except Exception:
        return None

    hint_lower = model_hint.lower()
    target_id: Optional[str] = None
    for node_id in (manifest.get("nodes") or {}):
        parts = node_id.split(".")
        if parts and parts[-1].lower() == hint_lower:
            target_id = node_id
            break
    if not target_id:
        return None

    node = manifest["nodes"][target_id]
    parents = (node.get("depends_on") or {}).get("nodes") or []
    if not parents:
        return None

    # Resolve parent asset keys via the catalog + our URI convention.
    parent_asset_keys: List[List[str]] = []
    for pid in parents:
        pnode = (manifest.get("nodes") or {}).get(pid) or {}
        rel = pnode.get("relation_name") or ""
        # Use dbt://<project>/<model_name> as a synthetic parent path if we
        # don't have a nicer URI. The user can drop these in via asset_deps
        # for a stronger link.
        pparts = pid.split(".")
        if pparts:
            parent_asset_keys.append(["dbt", pparts[-1]])

    columns = (catalog.get("nodes", {}).get(target_id, {}).get("columns") or {})
    if not columns or not parent_asset_keys:
        return None

    # Simple heuristic: every downstream column depends on every parent table
    # (no per-column granularity without SQL parsing). Still useful in the UI.
    from dagster import AssetKey as _AssetKey
    lineage: Dict[str, List[Any]] = {}
    for col_name in columns.keys():
        lineage[col_name] = [
            TableColumnDep(asset_key=_AssetKey(pk), column_name="*")
            for pk in parent_asset_keys
        ]

    return TableColumnLineage(deps_by_column=lineage)


_DATE_LIKE_PARAM_NAMES = frozenset({
    "date", "run_date", "execution_date", "ds", "partition_date",
    "interval_start", "interval_end", "logical_date",
})


def _build_deployment_config_class(
    deployments: List[Dict[str, Any]], script_name: str
) -> Optional[Any]:
    """Union all deployment parameter names into a single Dagster Config class.

    Returns None if no deployment declares any parameters (nothing to configure).
    Each parameter becomes an Optional field on the config class; per-deployment
    default values are baked into that deployment's job via `run_config`, not
    into the Config class itself, so ad-hoc launches from Dagster's UI show a
    fresh empty form rather than one deployment's values as global defaults.
    """
    try:
        from pydantic import Field, create_model
    except ImportError:
        return None
    try:
        from dagster import Config
    except ImportError:
        return None

    all_params: set[str] = set()
    param_defaults: Dict[str, Any] = {}  # first-seen default across deployments
    for dep in deployments:
        params = dep.get("parameters") or {}
        for k, v in params.items():
            all_params.add(k)
            param_defaults.setdefault(k, v)
    if not all_params:
        return None

    fields = {}
    for pname in all_params:
        default = param_defaults.get(pname)
        # Best-effort typing based on the first-seen default value.
        if isinstance(default, bool):
            ftype: Any = Optional[bool]
        elif isinstance(default, int):
            ftype = Optional[int]
        elif isinstance(default, float):
            ftype = Optional[float]
        elif isinstance(default, list):
            ftype = Optional[list]
        elif isinstance(default, dict):
            ftype = Optional[dict]
        else:
            ftype = Optional[str]
        fields[pname] = (ftype, Field(default=None, description=f"Prefect deployment parameter: {pname}"))

    cls = create_model(f"PrefectDeployment_{script_name}_Config", __base__=Config, **fields)
    return cls


def _cron_hour_minute(cron: str) -> Tuple[Optional[int], Optional[int]]:
    """Extract (hour, minute) from a cron string when they're literal digits.

    Used to hand `hour_of_day` / `minute_of_hour` to
    `build_schedule_from_partitioned_job` so a schedule tick fires at the
    right moment within its partition. Returns (None, None) if either field
    isn't a plain integer (e.g. `*`, `*/5`, `1,15,30`).
    """
    if not cron or not isinstance(cron, str):
        return (None, None)
    parts = cron.split()
    if len(parts) != 5:
        return (None, None)
    minute_str, hour_str = parts[0], parts[1]
    minute = int(minute_str) if minute_str.isdigit() else None
    hour = int(hour_str) if hour_str.isdigit() else None
    return (hour, minute)


def _cron_interval_minutes(cron: str) -> Optional[int]:
    """Best-effort cron→interval-in-minutes estimator (no external deps).

    Recognizes common patterns:
      "* * * * *"       → 1
      "*/N * * * *"     → N
      "0 * * * *"       → 60
      "0 */N * * *"     → N * 60
      "0 0 * * *"       → 1440   (daily)
      "0 <H> * * *"     → 1440   (any daily)
      "0 0 * * <D>"     → 10080  (weekly)
      "0 0 1 * *"       → 43200  (monthly, approx)
    Returns None if it can't be estimated.
    """
    if not cron or not isinstance(cron, str):
        return None
    parts = cron.split()
    if len(parts) != 5:
        return None
    minute, hour, dom, month, dow = parts

    # every-N-minutes: */N * * * *
    if minute.startswith("*/") and hour == "*" and dom == "*" and month == "*" and dow == "*":
        try:
            return int(minute[2:])
        except ValueError:
            return None
    if minute == "*" and hour == "*" and dom == "*" and month == "*" and dow == "*":
        return 1

    # hourly variants: `<M> * * * *` or `<M> */N * * *`
    if minute.isdigit() and dom == "*" and month == "*" and dow == "*":
        if hour == "*":
            return 60
        if hour.startswith("*/"):
            try:
                return int(hour[2:]) * 60
            except ValueError:
                return None
        if hour.isdigit():
            # daily at fixed hour
            return 1440

    # weekly: fixed minute + hour, dow set
    if minute.isdigit() and hour.isdigit() and dom == "*" and month == "*" and dow != "*":
        return 10080

    # monthly: fixed minute+hour+dom
    if minute.isdigit() and hour.isdigit() and dom.isdigit() and month == "*" and dow == "*":
        return 43200

    return None


def parse_prefect_deployments(script_path: Path) -> List[Dict[str, Any]]:
    """Look for a `prefect.yaml` adjacent to the script and return deployments
    whose `entrypoint` targets this script file.

    Returns a list of {name, schedule: {cron, timezone}, parameters, tags,
    description} dicts. `schedule` may be None if a deployment has none.
    """
    try:
        import yaml
    except ImportError:
        return []

    script_path = Path(script_path)
    candidates = [script_path.parent / "prefect.yaml"]
    if script_path.parent.parent != script_path.parent:
        candidates.append(script_path.parent.parent / "prefect.yaml")

    yaml_path: Optional[Path] = None
    for c in candidates:
        if c.exists():
            yaml_path = c
            break
    if yaml_path is None:
        return []

    try:
        with open(yaml_path) as fh:
            data = yaml.safe_load(fh) or {}
    except Exception as e:
        logger.debug(f"Could not parse {yaml_path}: {e}")
        return []

    results: List[Dict[str, Any]] = []
    file_name = script_path.name

    for dep in data.get("deployments") or []:
        entrypoint = dep.get("entrypoint") or ""
        # Prefect entrypoint format: "file.py:flow_function"
        ep_file = entrypoint.split(":", 1)[0].strip()
        if ep_file and Path(ep_file).name != file_name:
            continue

        schedule = dep.get("schedule") or {}
        cron = None
        timezone = "UTC"
        if isinstance(schedule, dict):
            cron = schedule.get("cron")
            timezone = schedule.get("timezone", "UTC")
        elif isinstance(schedule, str):
            cron = schedule

        results.append({
            "name": dep.get("name") or file_name,
            "entrypoint_function": (entrypoint.split(":", 1)[1].strip()
                                    if ":" in entrypoint else None),
            "schedule": (
                {"cron": cron, "timezone": timezone} if cron else None
            ),
            "parameters": dep.get("parameters") or {},
            "tags": dep.get("tags") or [],
            "description": dep.get("description"),
        })

    return results


def _call_target_name(call: ast.Call) -> Optional[str]:
    """Return the callee name for a Call node, or None for non-name callees."""
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        # e.g. `mat_fn.submit()` — return the base object name
        if isinstance(call.func.value, ast.Name) and call.func.attr in {
            "submit", "map", "with_options"
        }:
            return call.func.value.id
    return None


def _producer_of(
    arg: ast.AST,
    var_to_fn: Dict[str, str],
    fn_to_uris: Dict[str, List[str]],
) -> Optional[str]:
    """Given an argument node, return the name of the materialize fn that
    produced it (via a tracked variable or a direct call), else None."""
    if isinstance(arg, ast.Name):
        return var_to_fn.get(arg.id)
    if isinstance(arg, ast.Call):
        producer = _call_target_name(arg)
        if producer and producer in fn_to_uris:
            return producer
    return None


# ── URI → AssetKey ────────────────────────────────────────────────────────────

def uri_to_asset_key_path(uri: str) -> List[str]:
    """Turn a Prefect asset URI into a Dagster AssetKey path.

    Examples:
      s3://bucket/path/file.csv           → ["s3", "bucket", "path", "file.csv"]
      postgres://host/db/schema.users     → ["postgres", "host", "db", "schema", "users"]
      snowflake://acct/DB.SCHEMA.TABLE    → ["snowflake", "acct", "DB", "SCHEMA", "TABLE"]
      any_string                          → ["any_string"]  (fallback)
    """
    if not uri:
        return ["unknown_asset"]

    parsed = urlparse(uri)
    if not parsed.scheme:
        # Fallback: just sanitize the whole string.
        return [_sanitize_key_segment(uri)]

    parts: List[str] = [parsed.scheme]
    if parsed.netloc:
        parts.append(parsed.netloc)
    for seg in parsed.path.split("/"):
        if not seg:
            continue
        # Split dot-separated identifiers (e.g. db.schema.table).
        parts.extend(s for s in seg.split(".") if s)

    return [_sanitize_key_segment(p) for p in parts]


def _sanitize_key_segment(seg: str) -> str:
    """Dagster asset key segments allow [A-Za-z0-9_]."""
    import re
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", seg)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned or "seg"


# ── Runtime shim ──────────────────────────────────────────────────────────────

def install_asset_shim(fake_prefect_module: Any, op_name_prefix: str = "") -> Any:
    """Hang a fake `prefect.assets` submodule off fake_prefect_module and
    install it in sys.modules so `from prefect.assets import ...` resolves.

    Returns the fake assets module (so callers can clean up).
    """
    import types

    fake_assets = types.ModuleType("prefect.assets")

    class Asset:
        def __init__(self, key: str = None, properties: Any = None, **kwargs):
            self.key = key or kwargs.get("key")
            self.properties = properties

        def __repr__(self):
            return f"Asset(key={self.key!r})"

    class AssetProperties:
        def __init__(
            self,
            name: Optional[str] = None,
            description: Optional[str] = None,
            url: Optional[str] = None,
            owners: Optional[List[str]] = None,
            **kwargs,
        ):
            self.name = name
            self.description = description
            self.url = url
            self.owners = owners or []

    def _extract_uris(assets_args: tuple) -> Tuple[str, ...]:
        uris = []
        for a in assets_args:
            if isinstance(a, str):
                uris.append(a)
            elif isinstance(a, Asset) and a.key:
                uris.append(a.key)
        return tuple(uris)

    def materialize(*assets_args, asset_deps=None, materialized_by=None, **_ignored):
        """Fake @materialize: passthrough that binds current URIs via ContextVar.

        We deliberately do NOT wrap as `@op` — the multi_asset compute runs the
        flow inline as plain Python and iterates through the materialize'd
        functions directly. The only responsibilities of this shim are:
          - Bind _CURRENT_ASSETS so add_asset_metadata knows the target URIs.
          - Introspect the return value: if it's a list-of-dicts or a
            DataFrame, capture the schema so Dagster can render column info
            without the user calling anything explicitly.
        """
        uris = _extract_uris(assets_args)

        def decorator(f):
            import functools

            @functools.wraps(f)
            def wrapped(*args, **kwargs):
                token = _CURRENT_ASSETS.set(uris)
                try:
                    try:
                        result = f(*args, **kwargs)
                    except AssertionError as e:
                        # Attribute the failure to the specific line of the
                        # assert that raised — walk the traceback for the
                        # frame whose code lives in the wrapped function.
                        fail_line = None
                        tb = e.__traceback__
                        target_file = getattr(f, "__code__", None)
                        target_file = target_file.co_filename if target_file else None
                        while tb is not None:
                            if target_file is None or tb.tb_frame.f_code.co_filename == target_file:
                                fail_line = tb.tb_lineno
                            tb = tb.tb_next
                        _record_assertion_failure(uris, fail_line, str(e))
                        raise
                    _capture_schema_from_return(result, uris)
                    _record_assertion_success(uris)
                    return result
                finally:
                    _CURRENT_ASSETS.reset(token)

            wrapped._prefect_asset_uris = uris  # for downstream inspection
            return wrapped

        return decorator

    def add_asset_metadata(metadata: Dict[str, Any], asset: Any = None, **kwargs):
        """Fake add_asset_metadata: capture into the current-run buffer.

        Prefect's real API accepts `asset=` to target a specific URI when a
        materialize task is bound to multiple; otherwise applies to all.
        """
        buffer = _CURRENT_CAPTURE.get()
        if buffer is None:
            # Not running inside our capture scope — silently ignore.
            return
        target_uris: Tuple[str, ...]
        if asset is None:
            target_uris = _CURRENT_ASSETS.get()
        elif isinstance(asset, str):
            target_uris = (asset,)
        elif isinstance(asset, Asset) and asset.key:
            target_uris = (asset.key,)
        else:
            target_uris = _CURRENT_ASSETS.get()

        for uri in target_uris:
            existing = buffer.setdefault(uri, {})
            existing.update(metadata)

    # Expose stubs (setattr silences Pyright's attribute-on-ModuleType warnings).
    setattr(fake_assets, "Asset", Asset)
    setattr(fake_assets, "AssetProperties", AssetProperties)
    setattr(fake_assets, "materialize", materialize)
    setattr(fake_assets, "add_asset_metadata", add_asset_metadata)

    # Attach to the parent fake_prefect_module too (some code does `import prefect;
    # prefect.assets.materialize` rather than `from prefect.assets import ...`).
    try:
        setattr(fake_prefect_module, "assets", fake_assets)
    except (TypeError, AttributeError):
        # sys.modules registration below is what actually matters for imports.
        pass

    sys.modules["prefect.assets"] = fake_assets

    # Also install prefect.artifacts shim (see install_artifacts_shim).
    install_artifacts_shim(fake_prefect_module)

    return fake_assets


def install_artifacts_shim(fake_prefect_module: Any) -> Any:
    """Hang a fake `prefect.artifacts` submodule so scripts can call
    create_markdown_artifact / create_link_artifact / create_table_artifact
    inside a @materialize function and have them show up as Dagster metadata.

    Values are captured into the same `_CURRENT_CAPTURE` buffer the assets
    shim uses, wrapped with a small type marker (dict with `_artifact_kind`)
    that the multi_asset compute converts to MetadataValue at yield time.
    """
    import types

    fake_artifacts = types.ModuleType("prefect.artifacts")

    def _record(kind: str, key: str, payload: Any, description: Optional[str] = None):
        buffer = _CURRENT_CAPTURE.get()
        if buffer is None:
            return
        target_uris = _CURRENT_ASSETS.get()
        if not target_uris:
            return
        entry = {"_artifact_kind": kind, "value": payload}
        if description:
            entry["description"] = description
        for uri in target_uris:
            buffer.setdefault(uri, {})[key] = entry

    def create_markdown_artifact(key: str, markdown: str, description: Optional[str] = None, **_kw):
        _record("markdown", key, markdown, description)

    def create_link_artifact(key: str, link: str, link_text: Optional[str] = None,
                             description: Optional[str] = None, **_kw):
        _record("link", key, {"link": link, "link_text": link_text}, description)

    def create_table_artifact(key: str, table: Any, description: Optional[str] = None, **_kw):
        _record("table", key, table, description)

    def create_progress_artifact(key: str, progress: float, description: Optional[str] = None, **_kw):
        _record("progress", key, progress, description)

    def create_image_artifact(key: str, image_url: str, description: Optional[str] = None, **_kw):
        _record("image", key, image_url, description)

    setattr(fake_artifacts, "create_markdown_artifact", create_markdown_artifact)
    setattr(fake_artifacts, "create_link_artifact", create_link_artifact)
    setattr(fake_artifacts, "create_table_artifact", create_table_artifact)
    setattr(fake_artifacts, "create_progress_artifact", create_progress_artifact)
    setattr(fake_artifacts, "create_image_artifact", create_image_artifact)

    try:
        setattr(fake_prefect_module, "artifacts", fake_artifacts)
    except (TypeError, AttributeError):
        pass

    sys.modules["prefect.artifacts"] = fake_artifacts
    return fake_artifacts


_PREVIEW_MAX_ROWS = 10
_PREVIEW_MAX_COL_WIDTH = 40


def _record_assertion_success(uris: Tuple[str, ...]) -> None:
    """Record that a @materialize function completed without AssertionError.

    All the asserts inside that function must have passed (Python halts on the
    first failing assert), so we record a `passed=True` outcome per URI. The
    multi_asset compute reads this at yield time to emit AssetCheckResults.
    """
    buffer = _CURRENT_CAPTURE.get()
    if buffer is None or not uris:
        return
    for uri in uris:
        outcome = buffer.setdefault(uri, {}).setdefault(
            "_assertions", {"outcome": "passed", "fail_line": None, "message": None}
        )
        # If a prior URI in this materialize call already failed, keep failure.
        if outcome.get("outcome") != "failed":
            outcome["outcome"] = "passed"


def _record_assertion_failure(
    uris: Tuple[str, ...], fail_line: Optional[int], message: str
) -> None:
    """Record which line inside a @materialize function raised AssertionError.

    Downstream check emission compares each detected assert's line number to
    fail_line — the assert at that line becomes `passed=False`, and any
    asserts that would have run BEFORE it (lower line numbers) are marked
    `passed=True` since Python must have executed them successfully.
    """
    buffer = _CURRENT_CAPTURE.get()
    if buffer is None or not uris:
        return
    for uri in uris:
        buffer.setdefault(uri, {})["_assertions"] = {
            "outcome": "failed",
            "fail_line": fail_line,
            "message": message,
        }


def _capture_schema_from_return(result: Any, uris: Tuple[str, ...]) -> None:
    """If the @materialize return value looks like a table, extract:
      - column schema  → stashed as buffer[uri]['schema']
      - row_count      → stashed as buffer[uri]['row_count']
      - preview        → first N rows as a markdown table, buffer[uri]['preview']

    Recognized shapes:
      - pandas DataFrame           → columns + dtypes + preview
      - polars DataFrame           → columns + dtypes + preview
      - list[dict] (non-empty)     → keys of first row, Python types + preview
    """
    buffer = _CURRENT_CAPTURE.get()
    if buffer is None or not uris:
        return

    schema: Optional[List[Dict[str, str]]] = None
    row_count: Optional[int] = None
    preview_rows: Optional[List[Dict[str, Any]]] = None

    # pandas / polars DataFrame duck-typing (avoid hard import)
    try:
        if hasattr(result, "columns") and hasattr(result, "dtypes"):
            cols = list(result.columns)
            dtypes = [str(t) for t in result.dtypes]
            schema = [{"name": c, "type": t} for c, t in zip(cols, dtypes)]
            try:
                row_count = int(len(result))
            except Exception:
                pass
            # Preview: convert first N rows to list of dicts
            try:
                head = result.head(_PREVIEW_MAX_ROWS) if hasattr(result, "head") else result[:_PREVIEW_MAX_ROWS]
                if hasattr(head, "to_dict"):  # pandas
                    preview_rows = head.to_dict(orient="records")
                elif hasattr(head, "to_dicts"):  # polars
                    preview_rows = head.to_dicts()
            except Exception:
                preview_rows = None
    except Exception:
        schema = None

    # list[dict]
    if schema is None and isinstance(result, list) and result and isinstance(result[0], dict):
        first = result[0]
        schema = [
            {"name": str(k), "type": type(v).__name__ if v is not None else "NoneType"}
            for k, v in first.items()
        ]
        row_count = len(result)
        preview_rows = result[:_PREVIEW_MAX_ROWS]

    if schema is None:
        return

    schema_payload = {"_artifact_kind": "schema", "value": {"columns": schema}}
    if row_count is not None:
        schema_payload["value"]["row_count"] = row_count

    preview_payload: Optional[Dict[str, Any]] = None
    if preview_rows:
        preview_payload = {
            "_artifact_kind": "preview",
            "value": {"rows": preview_rows, "total_row_count": row_count},
        }

    for uri in uris:
        existing = buffer.setdefault(uri, {})
        # Don't overwrite what the user explicitly set via add_asset_metadata.
        existing.setdefault("schema", schema_payload)
        if row_count is not None:
            existing.setdefault("row_count", row_count)
        if preview_payload is not None:
            existing.setdefault("preview", preview_payload)


def _rows_to_markdown_table(rows: List[Dict[str, Any]], total: Optional[int] = None) -> str:
    """Render a list-of-dicts as a compact markdown table."""
    if not rows:
        return "_(empty)_"
    cols: List[str] = []
    for r in rows:
        for k in r.keys():
            if k not in cols:
                cols.append(str(k))

    def _cell(v: Any) -> str:
        s = "" if v is None else str(v)
        if len(s) > _PREVIEW_MAX_COL_WIDTH:
            s = s[: _PREVIEW_MAX_COL_WIDTH - 1] + "…"
        return s.replace("|", "\\|").replace("\n", " ")

    header = "| " + " | ".join(cols) + " |"
    sep = "|" + "|".join("---" for _ in cols) + "|"
    body_lines = []
    for r in rows:
        body_lines.append("| " + " | ".join(_cell(r.get(c)) for c in cols) + " |")

    footer = ""
    shown = len(rows)
    if total is not None and total > shown:
        footer = f"\n\n_showing {shown} of {total} rows_"
    elif shown == _PREVIEW_MAX_ROWS:
        footer = f"\n\n_first {shown} rows_"

    return "\n".join([header, sep, *body_lines]) + footer


def _convert_artifact_to_metadata_value(entry: Dict[str, Any]) -> Any:
    """Turn an artifact-shaped dict from _record() into a Dagster MetadataValue."""
    from dagster import MetadataValue, TableColumn, TableSchema

    kind = entry.get("_artifact_kind")
    val = entry.get("value")
    desc = entry.get("description")
    if kind == "markdown":
        md = val if isinstance(val, str) else ""
        if desc:
            md = f"_{desc}_\n\n{md}"
        return MetadataValue.md(md)
    if kind == "link":
        link = (val or {}).get("link", "")
        return MetadataValue.url(link)
    if kind == "table":
        # Support common shapes: list-of-dicts, list-of-lists, DataFrame-like.
        try:
            return MetadataValue.json(val)
        except Exception:
            return MetadataValue.text(str(val))
    if kind == "progress":
        return MetadataValue.float(float(val) if val is not None else 0.0)
    if kind == "image":
        return MetadataValue.url(str(val))
    if kind == "schema":
        cols = (val or {}).get("columns") or []
        table_cols = [
            TableColumn(name=c.get("name", ""), type=c.get("type", ""))
            for c in cols if isinstance(c, dict)
        ]
        return MetadataValue.table_schema(TableSchema(columns=table_cols))
    if kind == "preview":
        rows = (val or {}).get("rows") or []
        total = (val or {}).get("total_row_count")
        md = _rows_to_markdown_table(rows, total)
        return MetadataValue.md(md)
    return MetadataValue.text(str(val))


def uninstall_asset_shim() -> None:
    """Remove the fake `prefect.assets` module from sys.modules."""
    sys.modules.pop("prefect.assets", None)


class capture_prefect_asset_metadata:
    """Context manager: buffers add_asset_metadata calls into a dict.

    Usage:
        with capture_prefect_asset_metadata() as captured:
            flow()
        # captured: {"s3://...": {"row_count": 100}, ...}
    """

    def __enter__(self) -> Dict[str, Dict[str, Any]]:
        self._buffer: Dict[str, Dict[str, Any]] = {}
        self._token = _CURRENT_CAPTURE.set(self._buffer)
        return self._buffer

    def __exit__(self, exc_type, exc, tb):
        _CURRENT_CAPTURE.reset(self._token)
        return False


# ── multi_asset builder ───────────────────────────────────────────────────────

def create_materialize_multi_asset(
    prefect_assets: Any,
    flow_info: Dict[str, Any],
    script_info: Any,
    metadata: Any,
    fake_prefect_factory,
    dbt_project_path: Optional[str] = None,
    auto_freshness_policies: bool = False,
):
    """Build a Dagster @multi_asset (+ external AssetSpecs) for a Prefect script.

    Args:
        prefect_assets: dict from parse_prefect_assets() with keys
            "materialized" and "external" (also accepts a bare list of
            materialized entries for backward compat).
        flow_info: the primary flow's info dict from PrefectParser.parse_flow().
        script_info: ScriptInfo (has script_path, name).
        metadata: ScriptMetadata (has group_name, tags, kinds, description, ...).
        fake_prefect_factory: callable returning the fake prefect module
            (PrefectParser._create_fake_prefect_module).
        dbt_project_path: root of a dbt project (with target/catalog.json +
            target/manifest.json). If set, @materialize(materialized_by="dbt")
            assets get column schema + column-lineage metadata at build time.
        auto_freshness_policies: when True, attach a FreshnessPolicy inferred
            from the asset's cron schedule (if any).

    Returns a list [multi_asset_def, *external_asset_specs, *schedules],
    or None on failure. The router in the component classifies items by
    type_name (Asset/Job/Sensor/Schedule).
    """
    from dagster import (
        AssetCheckResult,
        AssetCheckSpec,
        AssetKey,
        AssetSelection,
        AssetSpec,
        AutomationCondition,
        DailyPartitionsDefinition,
        FreshnessPolicy,
        HourlyPartitionsDefinition,
        MaterializeResult,
        MetadataValue,
        MonthlyPartitionsDefinition,
        RetryPolicy,
        ScheduleDefinition,
        TableColumn,
        TableSchema,
        WeeklyPartitionsDefinition,
        build_schedule_from_partitioned_job,
        define_asset_job,
        multi_asset,
    )

    # Support both new dict shape and legacy list shape.
    if isinstance(prefect_assets, dict):
        materialized = prefect_assets.get("materialized", [])
        externals = prefect_assets.get("external", [])
    else:
        materialized = prefect_assets or []
        externals = []

    flow_name = flow_info["name"]
    script_name = script_info.name
    group_name = getattr(metadata, "group_name", None)

    # ── Collect flow-level tuning ────────────────────────────────────────────
    # Precedence for retries/tags: explicit `@flow(retries=..., tags=...)` on
    # the script wins over per-@materialize kwargs. If the flow doesn't set
    # them, fall back to the max/union across the individual materializes.
    flow_kwargs = extract_flow_level_kwargs(Path(script_info.script_path))

    flow_concurrency_key: Optional[str] = None
    flow_retries = 0
    flow_retry_delay = 0
    all_prefect_tags: set[str] = set()
    for pa in materialized:
        if not flow_concurrency_key and pa.get("concurrency_key"):
            flow_concurrency_key = pa["concurrency_key"]
        r = pa.get("retries")
        if isinstance(r, int) and r > flow_retries:
            flow_retries = r
        d = pa.get("retry_delay_seconds")
        if isinstance(d, (int, float)) and d > flow_retry_delay:
            flow_retry_delay = int(d)
        elif isinstance(d, (list, tuple)) and d:
            first = d[0]
            if isinstance(first, (int, float)) and first > flow_retry_delay:
                flow_retry_delay = int(first)
        for t in pa.get("prefect_tags") or []:
            all_prefect_tags.add(str(t))

    # `@flow(retries=...)` overrides.
    if isinstance(flow_kwargs.get("retries"), int):
        flow_retries = flow_kwargs["retries"]
    fd = flow_kwargs.get("retry_delay_seconds")
    if isinstance(fd, (int, float)):
        flow_retry_delay = int(fd)
    elif isinstance(fd, (list, tuple)) and fd and isinstance(fd[0], (int, float)):
        flow_retry_delay = int(fd[0])
    for t in flow_kwargs.get("tags") or []:
        all_prefect_tags.add(str(t))

    # ── Parse prefect.yaml deployments so we can derive a freshness policy ───
    try:
        deployments = parse_prefect_deployments(Path(script_info.script_path))
    except Exception as e:
        logger.debug(f"parse_prefect_deployments failed: {e}")
        deployments = []

    freshness_policy = None
    if auto_freshness_policies:
        # Use the SHORTEST cron across deployments (strictest freshness wins).
        # FreshnessPolicy.cron() takes the deadline cron directly + a delta
        # for the acceptable materialization window before each deadline.
        from datetime import timedelta as _timedelta
        shortest_minutes: Optional[int] = None
        shortest_cron: Optional[str] = None
        shortest_tz: str = "UTC"
        for dep in deployments:
            sched = dep.get("schedule") or {}
            cron = sched.get("cron")
            interval = _cron_interval_minutes(cron) if cron else None
            if interval and (shortest_minutes is None or interval < shortest_minutes):
                shortest_minutes = interval
                shortest_cron = cron
                shortest_tz = sched.get("timezone", "UTC")
        if shortest_cron and shortest_minutes:
            freshness_policy = FreshnessPolicy.cron(
                deadline_cron=shortest_cron,
                lower_bound_delta=_timedelta(minutes=shortest_minutes),
                timezone=shortest_tz,
            )

    specs: List[AssetSpec] = []
    for pa in materialized:
        key = AssetKey(pa["asset_key_path"])
        deps = [AssetKey(uri_to_asset_key_path(d)) for d in pa.get("asset_deps", [])]

        props = pa.get("properties") or {}
        description = props.get("description") or f"Prefect asset: {pa['asset_key']}"
        owners = props.get("owners") or None
        spec_metadata: Dict[str, Any] = {
            "prefect_asset_uri": pa["asset_key"],
            "prefect_flow": flow_name,
            "prefect_source_function": pa["function_name"],
        }
        if props.get("url"):
            spec_metadata["url"] = MetadataValue.url(props["url"])
        if props.get("name"):
            spec_metadata["prefect_display_name"] = props["name"]
        if pa.get("concurrency_key"):
            spec_metadata["prefect_concurrency_key"] = pa["concurrency_key"]

        kinds = {"prefect"}
        if pa.get("materialized_by"):
            kinds.add(pa["materialized_by"])
        for k in getattr(metadata, "kinds", []) or []:
            kinds.add(k)

        tags = {
            **(getattr(metadata, "tags", {}) or {}),
            "script_type": "prefect_materialize",
            "script_name": script_name,
            "prefect_flow": flow_name,
        }
        # Prefect per-@materialize tags → boolean Dagster tags ("prefect_tag/x").
        for t in pa.get("prefect_tags") or []:
            tags[f"prefect_tag/{t}"] = ""

        spec_kwargs = {
            "key": key,
            "deps": deps,
            "description": description,
            "metadata": spec_metadata,
            "kinds": kinds,
            "tags": tags,
        }
        if owners:
            spec_kwargs["owners"] = owners
        group_name = getattr(metadata, "group_name", None)
        if group_name:
            spec_kwargs["group_name"] = group_name

        # Attach AutomationCondition.eager() to assets that have deps —
        # mirrors Prefect's "materialize me when my inputs change" model.
        if deps:
            spec_kwargs["automation_condition"] = AutomationCondition.eager()

        # ── Freshness policy from schedule cadence (opt-in) ──────────────────
        if freshness_policy is not None:
            spec_kwargs["freshness_policy"] = freshness_policy

        # ── dbt catalog enrichment for materialized_by="dbt" assets ──────────
        if pa.get("materialized_by") == "dbt" and dbt_project_path:
            # Use the last segment of the asset key path as the model hint
            # (e.g. snowflake://prod/ANALYTICS.MARTS.CUSTOMER_DIM → customer_dim).
            model_hint = pa["asset_key_path"][-1] if pa["asset_key_path"] else ""
            cols = load_dbt_columns(dbt_project_path, model_hint)
            if cols:
                spec_metadata["dagster/column_schema"] = MetadataValue.table_schema(
                    TableSchema(columns=[
                        TableColumn(name=c["name"], type=c["type"]) for c in cols
                    ])
                )
                spec_metadata["dbt_model"] = model_hint
                spec_metadata["dbt_column_count"] = len(cols)
            lineage = build_dbt_column_lineage(dbt_project_path, model_hint)
            if lineage is not None:
                spec_metadata["dagster/column_lineage"] = lineage

        specs.append(AssetSpec(**spec_kwargs))

    if not specs:
        return None

    # Snapshot values needed by the closure.
    script_path = str(script_info.script_path)
    asset_uris_by_function: Dict[str, List[str]] = {}
    for pa in materialized:
        asset_uris_by_function.setdefault(pa["function_name"], []).append(pa["asset_key"])

    uri_to_key = {pa["asset_key"]: AssetKey(pa["asset_key_path"]) for pa in materialized}

    # ── Dagster Config class NOT wired for now — a dynamically-created
    # pydantic Config subclass doesn't play well with Dagster's Config type
    # resolution (which expects a concrete importable class). Deployment
    # parameters still surface: they get baked into per-deployment job
    # descriptions and tags. Full Config-schema-per-flow will need a
    # different approach (probably declaring config_schema= as a dict).
    deployment_config_class = None

    # ── Partitions: if ANY deployment has a date-like parameter AND we know
    # the cron cadence, emit a matching PartitionsDefinition. The partition
    # key becomes the value of that date parameter when the flow runs (with
    # the config value used as fallback for ad-hoc / non-partition runs).
    partitions_def = None
    partition_date_param: Optional[str] = None
    for dep in deployments:
        params = dep.get("parameters") or {}
        for pname in params:
            if pname.lower() in _DATE_LIKE_PARAM_NAMES:
                partition_date_param = pname
                break
        if partition_date_param:
            break

    if partition_date_param:
        # Match schedule cadence to a Dagster PartitionsDefinition. Pick the
        # SHORTEST cadence across deployments — that's the highest resolution
        # partitions we can support (any coarser deployment can still hit
        # subsets of these partitions).
        best_cadence: Optional[int] = None
        for dep in deployments:
            sched = dep.get("schedule") or {}
            interval = _cron_interval_minutes(sched.get("cron")) if sched.get("cron") else None
            if interval and (best_cadence is None or interval < best_cadence):
                best_cadence = interval

        from datetime import datetime, timedelta as _td, timezone
        # Start 14 days ago so users have a meaningful backfill range on day one.
        start = datetime.now(timezone.utc) - _td(days=14)
        try:
            if best_cadence and best_cadence < 60:
                # sub-hourly — round up to hourly (Dagster supports hourly at
                # the finest without a custom PartitionsDefinition).
                partitions_def = HourlyPartitionsDefinition(
                    start_date=start.strftime("%Y-%m-%d-%H:%M")
                )
            elif best_cadence == 60:
                partitions_def = HourlyPartitionsDefinition(
                    start_date=start.strftime("%Y-%m-%d-%H:%M")
                )
            elif best_cadence and 60 < best_cadence < 10080:
                partitions_def = DailyPartitionsDefinition(
                    start_date=start.strftime("%Y-%m-%d")
                )
            elif best_cadence == 10080:
                partitions_def = WeeklyPartitionsDefinition(
                    start_date=(datetime.now(timezone.utc) - _td(days=90)).strftime("%Y-%m-%d")
                )
            elif best_cadence and best_cadence >= 40320:
                partitions_def = MonthlyPartitionsDefinition(
                    start_date=(datetime.now(timezone.utc) - _td(days=365)).strftime("%Y-%m-%d")
                )
            else:
                # No cadence detected — default to daily
                partitions_def = DailyPartitionsDefinition(
                    start_date=start.strftime("%Y-%m-%d")
                )
        except Exception as e:
            logger.debug(f"Could not build PartitionsDefinition: {e}")
            partitions_def = None

    # ── AssetCheckSpec per `assert` statement inside a @materialize body ────
    # At build time we know the checks; at runtime we'll compare the recorded
    # failure line against each check's `line` to decide passed/failed.
    check_specs: List[AssetCheckSpec] = []
    checks_by_uri: Dict[str, List[Dict[str, Any]]] = {}
    for pa in materialized:
        uri = pa["asset_key"]
        assertions = pa.get("asserts") or []
        checks_by_uri[uri] = assertions
        for a in assertions:
            check_specs.append(AssetCheckSpec(
                name=a["name"],
                asset=AssetKey(pa["asset_key_path"]),
                description=a["description"],
            ))

    ma_op_tags: Dict[str, str] = {}
    if flow_concurrency_key:
        ma_op_tags["dagster/concurrency_key"] = flow_concurrency_key
    for t in all_prefect_tags:
        ma_op_tags[f"prefect_tag/{t}"] = ""

    ma_retry_policy = None
    if flow_retries > 0:
        ma_retry_policy = RetryPolicy(max_retries=flow_retries, delay=flow_retry_delay)

    ma_kwargs: Dict[str, Any] = {
        "name": f"prefect_materialize_{script_name}",
        "specs": specs,
        "can_subset": False,
    }
    if ma_op_tags:
        ma_kwargs["op_tags"] = ma_op_tags
    if ma_retry_policy is not None:
        ma_kwargs["retry_policy"] = ma_retry_policy
    if check_specs:
        ma_kwargs["check_specs"] = check_specs
    if partitions_def is not None:
        ma_kwargs["partitions_def"] = partitions_def

    @multi_asset(**ma_kwargs)
    def _prefect_multi_asset(context):
        yield from _run_prefect_flow_multi_asset(
            context, None, flow_name, script_name, script_path,
            uri_to_key, checks_by_uri, fake_prefect_factory,
            partition_date_param=partition_date_param,
        )

    def _run_prefect_flow_multi_asset(
        context, config, flow_name, script_name, script_path,
        uri_to_key, checks_by_uri, fake_prefect_factory,
        partition_date_param=None,
    ):
        """Execute a Prefect flow that emits @materialize assets.

        Runs the flow with `prefect` and `prefect.assets` monkey-patched;
        captures `add_asset_metadata` calls and yields one MaterializeResult
        per detected asset URI. If `config` is a Dagster Config, its
        non-None fields are passed as keyword args to the flow function.
        """
        original_prefect = sys.modules.get("prefect")
        original_assets = sys.modules.get("prefect.assets")

        try:
            fake_prefect = fake_prefect_factory(op_name_prefix=script_name)
            sys.modules["prefect"] = fake_prefect
            install_asset_shim(fake_prefect, op_name_prefix=script_name)

            mod_spec = importlib.util.spec_from_file_location(
                f"prefect_materialize_{script_name}", script_path
            )
            if mod_spec is None or mod_spec.loader is None:
                context.log.warning(f"Could not load module spec for {script_path}")
                return
            module = importlib.util.module_from_spec(mod_spec)

            with capture_prefect_asset_metadata() as captured:
                mod_spec.loader.exec_module(module)

                # Pick the best entrypoint flow. The parser passes flow_name
                # but that's just flows[0] — in files with nested subflows,
                # the entrypoint isn't necessarily the first flow declared.
                # Prefer (in order):
                #   1) a flow matching flow_name that takes no required args,
                #   2) any flow with no required args,
                #   3) the flow named flow_name (even if it needs args — we'll
                #      catch the TypeError and skip execution).
                import inspect as _inspect
                candidates: List[Any] = []
                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if callable(attr) and getattr(attr, "_is_prefect_flow", False):
                        candidates.append(attr)

                def _needs_args(fn: Any) -> bool:
                    try:
                        sig = _inspect.signature(fn)
                    except (TypeError, ValueError):
                        return False
                    for p in sig.parameters.values():
                        if p.default is _inspect.Parameter.empty and p.kind in (
                            _inspect.Parameter.POSITIONAL_ONLY,
                            _inspect.Parameter.POSITIONAL_OR_KEYWORD,
                            _inspect.Parameter.KEYWORD_ONLY,
                        ):
                            return True
                    return False

                named_match = [c for c in candidates
                               if getattr(c, "_flow_name", None) == flow_name
                               or c.__name__ == flow_name]
                no_arg_candidates = [c for c in candidates if not _needs_args(c)]

                flow_func = None
                for pool in (
                    [c for c in named_match if not _needs_args(c)],
                    no_arg_candidates,
                    named_match,
                ):
                    if pool:
                        flow_func = pool[0]
                        break

                if flow_func is None:
                    context.log.warning(
                        f"No runnable @flow in {script_path}; "
                        "yielding empty materializations."
                    )
                else:
                    resolved_name = getattr(flow_func, "_flow_name", flow_func.__name__)
                    # Bind Dagster config fields to matching flow-function
                    # parameter names (skip any Config field the flow doesn't
                    # actually declare, and skip fields left as None).
                    flow_kwargs: Dict[str, Any] = {}
                    if config is not None:
                        try:
                            flow_sig = _inspect.signature(flow_func)
                            for pname in flow_sig.parameters:
                                cfg_val = getattr(config, pname, None)
                                if cfg_val is not None:
                                    flow_kwargs[pname] = cfg_val
                        except (TypeError, ValueError):
                            pass
                    # If this compute is partitioned, feed the partition key
                    # to the flow's date-like parameter (overriding config).
                    if partition_date_param:
                        try:
                            partition_key = context.partition_key
                        except Exception:
                            partition_key = None
                        if partition_key:
                            flow_kwargs[partition_date_param] = partition_key
                    if flow_kwargs:
                        context.log.info(
                            f"Executing Prefect flow: {resolved_name}  "
                            f"(config: {flow_kwargs})"
                        )
                    else:
                        context.log.info(f"Executing Prefect flow: {resolved_name}")
                    try:
                        flow_func(**flow_kwargs)
                    except TypeError as e:
                        # Flow may require args we couldn't bind — fall through
                        # with whatever we captured before the failure.
                        context.log.warning(
                            f"Flow {resolved_name} needs parameters ({e}); "
                            "skipping execution but still emitting asset specs."
                        )

            for uri, key in uri_to_key.items():
                raw = captured.get(uri, {})
                md: Dict[str, Any] = {}
                for k, v in raw.items():
                    # Skip the internal _assertions bookkeeping — it's not
                    # user-facing metadata; it feeds AssetCheckResult below.
                    if k == "_assertions":
                        continue
                    if isinstance(v, dict) and v.get("_artifact_kind"):
                        md[k] = _convert_artifact_to_metadata_value(v)
                    else:
                        md[k] = v
                yield MaterializeResult(asset_key=key, metadata=md or None)

            # AssetCheckResult per detected assert. We know the failure line
            # (if any) from _record_assertion_failure; every assert on a
            # smaller line number ran successfully before it. Asserts on the
            # failing line or beyond are marked failed / unreached.
            for uri, assertions in checks_by_uri.items():
                if not assertions:
                    continue
                key = uri_to_key.get(uri)
                if key is None:
                    continue
                a_state = captured.get(uri, {}).get("_assertions")
                if a_state is None:
                    # Function didn't run (e.g., flow required params and we
                    # skipped execution). Mark all checks as skipped/unknown.
                    for a in assertions:
                        yield AssetCheckResult(
                            asset_key=key,
                            check_name=a["name"],
                            passed=False,
                            metadata={"status": "not evaluated (flow skipped)"},
                        )
                    continue
                if a_state["outcome"] == "passed":
                    for a in assertions:
                        yield AssetCheckResult(
                            asset_key=key,
                            check_name=a["name"],
                            passed=True,
                        )
                else:
                    fail_line = a_state.get("fail_line")
                    fail_msg = a_state.get("message") or "assertion failed"
                    for a in assertions:
                        if fail_line is not None and a["line"] == fail_line:
                            yield AssetCheckResult(
                                asset_key=key,
                                check_name=a["name"],
                                passed=False,
                                metadata={"error": fail_msg, "line": a["line"]},
                            )
                        elif fail_line is None or a["line"] < fail_line:
                            # Ran before the failure — passed
                            yield AssetCheckResult(
                                asset_key=key,
                                check_name=a["name"],
                                passed=True,
                            )
                        else:
                            # Would have run after the failure — not evaluated
                            yield AssetCheckResult(
                                asset_key=key,
                                check_name=a["name"],
                                passed=False,
                                metadata={"status": "not evaluated (prior assert failed)"},
                            )
        finally:
            if original_prefect is not None:
                sys.modules["prefect"] = original_prefect
            else:
                sys.modules.pop("prefect", None)
            if original_assets is not None:
                sys.modules["prefect.assets"] = original_assets
            else:
                uninstall_asset_shim()
            # Artifacts shim cleanup (asset shim installs it, so we clean here).
            sys.modules.pop("prefect.artifacts", None)

    # ── Build external upstream AssetSpecs (metadata-only, no compute) ───────
    external_specs: List[AssetSpec] = []
    for ext in externals:
        key = AssetKey(ext["asset_key_path"])
        props = ext.get("properties") or {}
        description = props.get("description") or f"Upstream Prefect asset: {ext['asset_key']}"
        ext_metadata: Dict[str, Any] = {
            "prefect_asset_uri": ext["asset_key"],
            "prefect_flow": flow_name,
        }
        if props.get("url"):
            ext_metadata["url"] = MetadataValue.url(props["url"])
        if props.get("name"):
            ext_metadata["prefect_display_name"] = props["name"]

        ext_kwargs: Dict[str, Any] = {
            "key": key,
            "description": description,
            "metadata": ext_metadata,
            "kinds": {"prefect_external"},
            "tags": {
                "script_type": "prefect_materialize_external",
                "script_name": script_name,
                "prefect_flow": flow_name,
            },
        }
        if props.get("owners"):
            ext_kwargs["owners"] = props["owners"]
        if group_name:
            ext_kwargs["group_name"] = group_name
        external_specs.append(AssetSpec(**ext_kwargs))

    # ── Build ScheduleDefinitions from adjacent prefect.yaml deployments ─────
    # (`deployments` was already loaded above so we could derive freshness.)
    schedules: List[ScheduleDefinition] = []
    if deployments:
        asset_keys = [AssetKey(pa["asset_key_path"]) for pa in materialized]
        selection = AssetSelection.assets(*asset_keys)
        multi_asset_op_name = f"prefect_materialize_{script_name}"

        # If the multi_asset is partitioned, figure out its cadence so we can
        # detect schedule/partition matches vs. mismatches per deployment.
        partition_cadence_minutes: Optional[int] = None
        if partitions_def is not None:
            pd_type = type(partitions_def).__name__
            partition_cadence_minutes = {
                "HourlyPartitionsDefinition": 60,
                "DailyPartitionsDefinition": 1440,
                "WeeklyPartitionsDefinition": 10080,
                "MonthlyPartitionsDefinition": 43200,
            }.get(pd_type)

        for i, dep in enumerate(deployments):
            sched = dep.get("schedule")
            if not sched or not sched.get("cron"):
                continue
            base = f"prefect_{script_name}"
            job_name = f"{base}_deployment_{i}" if len(deployments) > 1 else f"{base}_deployment"
            sched_name = f"{job_name}_schedule"
            dep_params = dep.get("parameters") or {}

            job_kwargs: Dict[str, Any] = {
                "name": job_name,
                "selection": selection,
                "description": dep.get("description") or f"Prefect deployment: {dep.get('name')}",
                "tags": {
                    "prefect_deployment": dep.get("name") or "",
                    **{f"prefect_tag/{t}": "" for t in dep.get("tags") or []},
                },
            }
            if dep_params and deployment_config_class is not None:
                cfg_field_names = set(deployment_config_class.model_fields.keys())
                op_config = {k: v for k, v in dep_params.items() if k in cfg_field_names}
                if op_config:
                    job_kwargs["config"] = {
                        "ops": {multi_asset_op_name: {"config": op_config}}
                    }
            job_def = define_asset_job(**job_kwargs)

            # ── Schedule kind depends on partition/schedule alignment ────────
            dep_cadence = _cron_interval_minutes(sched["cron"])
            description = (
                f"From prefect.yaml deployment '{dep.get('name')}' "
                f"(cron: {sched['cron']}, tz: {sched.get('timezone', 'UTC')})"
                + (f" — params: {dep_params}" if dep_params else "")
            )

            if partition_cadence_minutes is not None and dep_cadence == partition_cadence_minutes:
                # Cadence matches partition cadence → use the partition-aware
                # schedule builder. On each tick it auto-materializes the
                # just-completed partition (yesterday's data for a daily
                # schedule at 6am, etc.), so scheduled runs Just Work without
                # anyone having to pick a partition key manually.
                #
                # Dagster derives the base cron from the partition definition
                # itself for time-partitioned jobs — we can only tune the
                # exact hour/minute within the partition via hour_of_day /
                # minute_of_hour. `execution_timezone` and `cron_schedule`
                # can't be passed here (partition_def owns them). If a user's
                # deployment declared a non-UTC timezone, we'd need a matching
                # PartitionsDefinition(..., timezone=...) — future work.
                hour, minute = _cron_hour_minute(sched["cron"])
                sched_kwargs: Dict[str, Any] = {
                    "job": job_def,
                    "name": sched_name,
                    "description": description,
                }
                if minute is not None:
                    sched_kwargs["minute_of_hour"] = minute
                if hour is not None and partition_cadence_minutes >= 1440:
                    sched_kwargs["hour_of_day"] = hour
                schedules.append(build_schedule_from_partitioned_job(**sched_kwargs))
            else:
                if partition_cadence_minutes is not None and dep_cadence != partition_cadence_minutes:
                    logger.warning(
                        "Schedule for deployment '%s' (cron %s, %s min) doesn't "
                        "match partition cadence (%s min) for %s. Emitting a "
                        "non-partitioned ScheduleDefinition — scheduled runs "
                        "will NOT auto-target a partition; use the Dagster UI's "
                        "backfill to fill partitioned runs instead.",
                        dep.get("name"), sched["cron"], dep_cadence,
                        partition_cadence_minutes, script_name,
                    )
                schedules.append(ScheduleDefinition(
                    name=sched_name,
                    cron_schedule=sched["cron"],
                    execution_timezone=sched.get("timezone", "UTC"),
                    job=job_def,
                    description=description,
                ))

    return [_prefect_multi_asset, *external_specs, *schedules]
