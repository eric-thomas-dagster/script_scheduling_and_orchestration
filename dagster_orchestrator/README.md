# dagster_orchestrator — Universal Orchestration Template

One Dagster project. One component. One `defs.yaml`. Handles everything.

## What it handles

| File type | How Dagster handles it |
|---|---|
| Plain Python script | Asset that runs the script via subprocess |
| Prefect flow | Flow → Dagster asset; `@materialize` → native multi_asset with lineage, metadata, artifacts, schedules, and freshness policies |
| Airflow DAG | DAG tasks → Dagster ops; runs via Airflow executor |
| dag-factory YAML | YAML-defined DAGs → Dagster ops |
| Cosmos DAG (`import cosmos`) | Airflow wrapper discarded → native `@dbt_assets` via dagster-dbt |

All from a single `defs.yaml` pointing at one repo.

## Prefect flows in Dagster

Since Prefect joined Dagster Labs, this project treats Prefect flows as
first-class citizens — you keep all of Prefect's execution semantics and
gain Dagster's asset model, catalog, freshness SLAs, and unified lineage
across dbt / Airflow / everything else in the same code location.

### What surfaces from a Prefect flow

| Prefect | Dagster mapping |
|---|---|
| `@flow` | One `@multi_asset` compute unit (whole flow runs as one Dagster op) |
| `@materialize("s3://…")` | One Dagster asset per URI, with the URI parsed into a multi-segment `AssetKey` |
| `Asset(key=…, properties=AssetProperties(...))` | Description, owners, url, name applied to the AssetSpec |
| `asset_deps=[…]` | Real Dagster `deps=[AssetKey…]` edges |
| Implicit `subflow(x)` chains | Deps inferred by walking the `@flow` body's call graph, including through nested subflows |
| `materialized_by="dbt"` | Adds `dbt` kind + dbt catalog/lineage enrichment (see below) |
| `@materialize(tags=[…], retries=N, retry_delay_seconds=…)` | Op tags, RetryPolicy |
| `@flow(retries=N, retry_delay_seconds=…, tags=[…])` | Overrides per-materialize values at the flow level |
| `add_asset_metadata({…})` | Captured at runtime, emitted as typed MaterializeResult metadata |
| `create_markdown_artifact / create_link_artifact / create_table_artifact` | `MetadataValue.md` / `.url` / `.json` / `.table_schema` |
| `list[dict]` or DataFrame return values | Column schema + `row_count` auto-extracted into `TableSchemaMetadataValue` |
| `prefect.yaml` deployments | Each `deployments:` entry becomes a Dagster job + `ScheduleDefinition` |

### Things Dagster adds on top

- **AutomationCondition.eager()** attached to any asset with deps — the "materialize me when my inputs change" model, declarative.
- **Freshness policies** auto-derived from the deployment schedule (opt-in via `auto_freshness_policies: true`) — Dagster's UI shows PASS/WARN/FAIL freshness state per asset, backed by the same cron the flow already ships with.
- **External upstream assets** — any `Asset(...)` binding referenced only in `asset_deps=[…]` gets a metadata-only `AssetSpec` (owners, description, url) so the platform team's postgres table appears in the graph with its full metadata even though no flow in your repo produces it.
- **dbt column schema + lineage** — for `materialized_by="dbt"` assets, columns from `target/catalog.json` land on the spec at build time and column-to-parent-table lineage shows up in Dagster's UI.
- **`prefect-dbt` → native `@dbt_assets`** — Prefect flows that run dbt via `PrefectDbtRunner` (which in Prefect wrap the entire dbt project as one opaque task) get expanded into one Dagster asset per model, with model-to-model lineage from `manifest.json`, column info from `catalog.json`, and dbt tests as asset checks. The `project_dir=` is auto-detected from `PrefectDbtRunner(project_dir="…")` calls if `dbt_project_path` isn't set globally.
- **Unified lineage** across Prefect + dbt + Airflow + plain Python in one graph.

### Configuring the Prefect path

```yaml
# defs.yaml
prefect_enabled: true
prefect_version: ">=3.4"          # @materialize needs Prefect 3.4+
auto_freshness_policies: true     # opt-in: derive FreshnessPolicy from schedules
dbt_project_path: dbt/jaffle_shop # optional: enables dbt catalog enrichment
```

See [`defs/prefect_demos/`](dagster_orchestrator/defs/prefect_demos/defs.yaml)
for a live example pointed at `github.com/PrefectHQ/demos`.

### Validated against real code (Prefect)

The Prefect mapping is exercised against both local demos we ship AND
Prefect's own external demo repo:

**In this project** — five `@materialize` demos plus plain-flow examples in
[`script_orchestrator/example_scripts/prefect_examples/`](../script_orchestrator/example_scripts/prefect_examples/):
- `materialize_assets_demo.py` — basic `@materialize` with `Asset` + `AssetProperties`
- `materialize_implicit_deps_demo.py` — implicit dep inference through subflow calls
- `materialize_full_demo.py` — artifacts, subflows, concurrency, freshness policies
- `materialize_partitioned_demo.py` — partition-based backfills + `assert` → asset checks
- `dbt_via_prefect_demo.py` — `prefect_dbt` flow → native `@dbt_assets`

**External** — [`defs/prefect_demos/`](dagster_orchestrator/defs/prefect_demos/defs.yaml)
is wired to [github.com/PrefectHQ/demos](https://github.com/PrefectHQ/demos)
(Prefect's own sales-engineering demos, cloned with `--recurse-submodules`
so submodule-symlinked files resolve). Covers hello-world, artifacts
(weather), async subflows (pokemon-weight), crypto-prices with
multi-deployment schedules, and retries.

If Prefect updates its demos, we pull the new versions on the next clone.
Point `repo_url:` at your own Prefect repo and the same mapping applies
with no upstream code changes.

## Airflow + Cosmos in Dagster

Airflow DAGs get parsed and mapped to Dagster ops/assets by the same
`ScriptGithubComponent`. When a DAG imports `cosmos` and a dbt project is
configured (or auto-discovered), that whole DAG gets replaced with native
`@dbt_assets` — one Dagster asset per dbt model, with model-to-model
lineage from `manifest.json`, column info from `catalog.json`, and dbt
tests as asset checks. See `## Cosmos DAG outcomes` below for the
per-DAG classification.

### Validated against real code (Airflow + Cosmos)

Same story on the Airflow side — local Airflow DAGs plus Astronomer's
canonical Cosmos DAG set:

**In this project** — six Airflow 3.x DAG patterns in
[`script_orchestrator/example_scripts/airflow_3x_examples/`](../script_orchestrator/example_scripts/airflow_3x_examples/):
- `simple_etl_3x.py` — linear extract → transform → load
- `data_pipeline_3x.py` — branching task graph
- `report_generator_3x.py` — dataset producer
- `report_from_processed_data_3x.py` — dataset consumer (producer/consumer lineage)
- `multi_input_report_3x.py` — multiple inlet datasets
- `customer_etl_factory.py` — DAG factory pattern → Dagster partitioned asset

Plus Airflow 2.x + dag-factory YAML examples in
[`airflow_2x_examples/`](../script_orchestrator/example_scripts/airflow_2x_examples/).

**External** — [`defs/orchestrator/`](dagster_orchestrator/defs/orchestrator/defs.yaml)
is wired to [github.com/astronomer/astronomer-cosmos](https://github.com/astronomer/astronomer-cosmos)'s
`dev/dags/` folder — Astronomer's canonical Cosmos + Airflow DAG set.
Cosmos DAGs that just run dbt get replaced with native `@dbt_assets`;
non-Cosmos DAGs are mapped as regular Airflow assets.

Point `repo_url:` at your own DAG repo and the same treatment applies
with no upstream code changes.

## Architecture

`ScriptGithubComponent` (from `script-orchestrator`) is the single component used here:

```
ScriptGithubComponent (StateBackedComponent)
│  handles: Python, Prefect, Airflow, dag-factory YAML, Cosmos+dbt
│  state: clones repo, discovers scripts, caches to /tmp/dagster_orchestrator/state.json
│
│  when dbt_project_path is set:
│    - Cosmos DAG files (import cosmos) are routed to native @dbt_assets
│    - Creates one Dagster job per "replaced" Cosmos DAG
│    - Emits cosmos_migration_summary asset with full classification report
│    - Generates dbt_docs asset (replaces dbt_docs.py Cosmos DAG)
```

## Setup

```bash
cd dagster_orchestrator
uv venv && uv pip install -e ".[dev]"
```

Configure `defs/orchestrator/defs.yaml`, then:

```bash
uv run dagster dev
```

On first run the component will:
1. Clone the repo to the local cache
2. Discover all scripts (state is written to `/tmp/dagster_orchestrator/state.json`)
3. Run `dbt deps` + `dbt parse` to compile the dbt manifest
4. Register all assets, jobs, and schedules

## Configuring for your repo

### Owned repo — use companion YAML files

Drop a `<script>.yaml` next to any script to configure it:

```yaml
# my_script.yaml
schedule: "0 6 * * *"
enabled: true
```

### External repo — use file_overrides in defs.yaml

```yaml
file_overrides:
  basic_cosmos_dag:
    cosmos_action: replace     # replace | absorbed | skip
    schedule: "@daily"
  example_cosmos_python_models:
    cosmos_action: skip
    reason: "Requires Databricks — not available here"
  jaffle_shop_kubernetes:
    enabled: false
  my_plain_airflow_dag:
    schedule: "0 6 * * *"     # works for non-Cosmos files too
```

`file_overrides` takes precedence over auto-detection for both Cosmos and regular scripts.

## Cosmos DAG outcomes

Every Cosmos DAG falls into one of three buckets — visible in the
`cosmos_migration_summary` asset:

| Outcome | Meaning | Dagster result |
|---|---|---|
| **replaced** | DAG was just running dbt via Cosmos | `@dbt_assets` + job created; Cosmos DAG can be disabled in Airflow |
| **absorbed** | Airflow concept Dagster handles as a built-in | No job needed (sensor, resource, or asset selection does the job) |
| **skipped** | Infrastructure-specific (Kubernetes, virtualenv, etc.) | Documented, nothing created |

## dbt profile

The jaffle_shop dbt project uses PostgreSQL by default. For local dev without a
database server, use DuckDB:

```bash
uv pip install dbt-duckdb
```

Add to `~/.dbt/profiles.yml`:

```yaml
jaffle_shop:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /tmp/jaffle_shop.duckdb
      threads: 4
```

Then set `dbt_target: dev` in `defs.yaml` (already the default).
