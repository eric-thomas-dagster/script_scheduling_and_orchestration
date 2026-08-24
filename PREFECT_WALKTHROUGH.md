# Prefect → Dagster: what you get for free

**For Prefect users** (or Prefect SEs helping their customers): this walk-through takes you through what `ScriptGithubComponent` gives you on top of an existing Prefect codebase — with the actual asset URLs from the demo deployment.

**Demo deployment**
- Org / deployment: `ericthomas-dagster/prod`
- Code location: `dagster-orchestrator`
- URL: https://ericthomas-dagster.dagster.cloud/prod/locations/dagster_orchestrator/assets

The code location loads two `ScriptGithubComponent` instances:
1. `prefect_demos` — points at `PrefectHQ/demos` (classic `@flow`/`@task` scripts)
2. `prefect_materialize_demos` — points at this repo's `script_orchestrator/example_scripts/prefect_examples/` (Prefect 3.4 `@materialize` + `AssetProperties`, plus a `prefect_dbt` example)

Total: **33 assets** across the two, with real cross-tool lineage.

---

## Section 1 — "Bring your Prefect repo, no rewrite"

Existing Prefect flows classify as Prefect and get wrapped as first-class Dagster assets automatically. **No `.yaml` companion files, no code changes to the .py files.**

**What to click**: any wrapper like `prefect_producer_customer_data` in the UI.

**What to point at**:
- The asset shows up in the Dagster catalog with `[prefect, python, etl]` kinds — automatically inferred from imports (`import pandas`, `import requests`, etc.).
- Click into the asset → internal **op graph**. Each `@task` in the Prefect flow became a Dagster `@op` with the same name and retry policy.
- Prefect: `@task(retries=3, retry_delay_seconds=10)` → Dagster: `@op(retry_policy=RetryPolicy(max_retries=3, delay=10))`.
- Task-DAG shape preserved. Prefect execution semantics kept.

**Story for a Prefect user**: "You don't rewrite your flows. Your existing `@flow` + `@task` code loads as-is into a Dagster asset — with your retry policies, task ordering, and structure intact. What you gain is a first-class asset catalog + everything else in the sections below."

---

## Section 2 — "@materialize → Dagster assets"

Prefect 3.4 introduced `Asset()`, `AssetProperties`, `@materialize`, and `add_asset_metadata`. These are the direct dial to Dagster's asset model.

**What to click**: the chain from `postgres:/prod_warehouse/public/customers_raw` (external upstream) through `s3:/analytics_lake/curated/customer_dim/parquet` to `snowflake:/prod/ANALYTICS/MARTS/CUSTOMER_METRICS`.

**Source**: `script_orchestrator/example_scripts/prefect_examples/materialize_assets_demo.py`

**What to point at**:

| Prefect primitive | Dagster surface |
|---|---|
| `Asset(key="postgres://...", properties=AssetProperties(name=..., owners=..., url=...))` | AssetSpec with owners + description + external URL, appears in the lineage graph even though no flow in your repo materializes it |
| `@materialize("s3://...", asset_deps=[raw_customers])` | Dagster `@asset` with a **URI-parsed AssetKey** and a real dependency edge to `raw_customers` |
| `materialized_by="dbt"` | `dbt` shows up as a Dagster **kind** on the asset — filterable in the catalog |
| `add_asset_metadata({"row_count": ...})` inside the flow body | Captured at runtime, emitted as typed `MaterializeResult` metadata (visible on the materialization event) |

**Story**: "Your Prefect `@materialize` calls define the same shape Dagster uses — no translation layer, no metadata format loss."

---

## Section 3 — "Runtime artifacts as typed metadata"

Prefect's `create_markdown_artifact`, `create_link_artifact`, `create_table_artifact` capture human-readable output alongside flow runs. `ScriptGithubComponent` shims these at runtime and yields them as Dagster `MetadataValue`.

**What to click**: `materialize_full_demo` — materialize it from the UI, then open the materialization event.

**Source**: `script_orchestrator/example_scripts/prefect_examples/materialize_full_demo.py`

**What to point at** (in the materialization event tab after a run):
- `create_markdown_artifact(key="extract-summary", markdown=...)` → **`MetadataValue.md`** (renders as markdown in the Dagster UI)
- `create_link_artifact(key=..., link=...)` → `MetadataValue.url` (clickable)
- `create_table_artifact(key=..., table=[...])` → `MetadataValue.table_schema` (structured table view)

**Story**: "Your existing artifacts — the ones you already write for humans reading Prefect run pages — surface unchanged as Dagster metadata."

---

## Section 4 — "Implicit dependency inference"

You don't have to hand-write `asset_deps=[...]` for every `@materialize`. If your flow body calls another `@materialize`-decorated function, or invokes a subflow that materializes something, the parser walks the AST and infers the dependency.

**What to click**: `s3:/raw/events/parquet` → `s3:/curated/features/parquet` → `s3:/models/scores/parquet`.

**Source**: `script_orchestrator/example_scripts/prefect_examples/materialize_implicit_deps_demo.py`

**What to point at**:
- Three chained `@materialize` outputs.
- None of them have `asset_deps=[...]` set explicitly.
- The lineage arrows come from the call graph — `features` is passed to `scores` as a function arg, so a dep edge is inferred.

**Story**: "You already write your Prefect flow the way you think about the data. That call-graph shape IS the dependency graph. We don't ask you to re-express it."

---

## Section 5 — "Partitioned Prefect flows → partitioned Dagster assets"

Prefect flows with parameters can become partitioned Dagster assets — one materialization per partition key.

**What to click**: any of the outputs of `materialize_partitioned_demo`.

**Source**: `script_orchestrator/example_scripts/prefect_examples/materialize_partitioned_demo.py`

**What to point at**:
- The asset shows a **partition strip** in the UI.
- Each partition maps to a Prefect run with a specific parameter value.
- Backfills, per-partition retries, per-partition freshness — all standard Dagster features on top of your existing Prefect parameterization.

---

## Section 6 — "prefect_dbt → per-model dbt assets"

Prefect's `PrefectDbtRunner` wraps your entire dbt project as ONE opaque task. In Prefect you see "run dbt" — 1 log stream, no per-model observability. Dagster expands the same code into **one asset per dbt model** with:
- Model-to-model lineage from `manifest.json`
- Column names + types on each spec from `catalog.json`
- dbt tests → Dagster **asset checks**

**What to click**: any of the `jaffle_shop_*` assets (`stg_customers`, `stg_orders`, `customers`, `orders`, etc. — expanded from `dbt/jaffle_shop`).

**Source**: `script_orchestrator/example_scripts/prefect_examples/dbt_via_prefect_demo.py`

**What to point at**:
- The dbt DAG in Dagster — same shape as `dbt docs serve` would show, but embedded in Dagster's unified graph with the Prefect-side assets.
- Click a model → column schema from catalog.json.
- Asset checks tab → each `dbt test` becomes a check on the corresponding model.
- Lineage edges from Prefect `@materialize` upstreams (if you had any writing to sources) → dbt models → downstream `@materialize` targets → all one graph.

**Story**: "Your Prefect flow that runs dbt is opaque to you today. In Dagster the SAME code becomes the whole dbt DAG, with tests as checks and columns visible on every model."

---

## Section 7 — "Freshness SLAs from your existing schedule"

`prefect.yaml` deployments have a cron. Dagster reads that same cron and auto-attaches a `FreshnessPolicy` — the UI then shows PASS / WARN / FAIL for each asset based on when it was last materialized vs when the schedule says it should have been.

**Enabled by**: `auto_freshness_policies: true` in `defs.yaml` (both defs sets have it on in this demo).

**What to click**: any asset with a schedule attached (e.g. `prefect_prefect_flow_example` which has a `0 */6 * * *` cron).

**What to point at**:
- Asset detail page → **Freshness** section, with a computed policy (e.g. "materialize every 6 hours").
- Freshness state widget (PASS / WARN / FAIL).
- **Alertable** — a freshness violation triggers a Dagster alert policy (Slack, email, webhook, PagerDuty).

**Story**: "The cron is already in your `prefect.yaml`. You don't write another spec. Dagster reads it, computes an SLA, and alerts if it's missed. No duplicated config."

---

## Section 8 — "Unified lineage across Prefect + dbt + everything else"

The point of all seven sections above: **one graph, one catalog**. Prefect `@materialize` outputs flow into dbt models flow into downstream Prefect `@materialize` targets, all in the same lineage view.

**What to click**: Assets → Global asset graph view. Filter to `kind = prefect` and `kind = dbt`. You'll see both sets, connected by dependency edges.

**Story**: "You don't have to choose between Prefect for flexibility and Dagster for observability. Your Prefect code becomes the source of truth for what runs; Dagster becomes the source of truth for what exists and how it's connected."

---

## What's NOT captured (the honest list)

- **Cross-tool subflow calls across files** aren't followed — the AST parser walks within a script, but doesn't chase a `subflow_from_other_module()` call.
- **Dynamic Prefect asset URIs** (e.g. `Asset(f"s3://{bucket}/{key}")` where `bucket` is a runtime var) don't produce stable AssetKeys — the parser can only see literal strings.
- **Airflow HITL** (human-in-the-loop) tasks — no equivalent in the Dagster mapping today.
- **Prefect blocks** are honored at runtime (the flow still runs with them), but they aren't projected into Dagster resources.

---

## For the SE conversation

The one-slide summary:

> "Bring your Prefect repo as-is. Get a first-class asset catalog, freshness SLAs, dbt integration, unified lineage, and Dagster's alerting on top — with **zero** rewrite. Prefect 3.4 `@materialize` scripts light up the full asset model automatically; classic `@flow`/`@task` scripts work too, just without the URI-level assets."

Follow-ups the SE should be ready for:
1. **"What about our custom Prefect blocks?"** — runtime yes, projected as Dagster resources no (roadmap).
2. **"Does this replace our Prefect workers?"** — no, it orchestrates alongside them. The flow still runs as Prefect at execution time.
3. **"What about cost?"** — orchestration on Dagster+, execution wherever your Prefect workers run today. Dagster's non-isolated + in-process pattern is a latency optimization for lightweight orchestration.

---

## How this demo is wired (for reproducibility)

- `dagster_orchestrator/pyproject.toml` — pinned deps (`script-orchestrator[prefect]`, `dbt-core`, `dbt-duckdb`, `prefect-dbt`, `dagster-dbt`)
- `dagster_orchestrator/Dockerfile` — clones `PrefectHQ/demos` + `this repo` + `dbt-labs/jaffle_shop`, runs `dbt deps + seed + run + docs generate`, then `build_state.py` bakes the two state.json files into the image
- `dagster_orchestrator/dagster_orchestrator/defs/prefect_demos/defs.yaml` — classic Prefect wrapper config
- `dagster_orchestrator/dagster_orchestrator/defs/prefect_materialize_demos/defs.yaml` — @materialize + dbt config
- `script_orchestrator/example_scripts/prefect_examples/*.py` — the Prefect scripts the second location wraps
- `script_orchestrator/script_orchestrator/components/script_github_component.py` — the component itself
