# dagster_orchestrator — Universal Orchestration Template

One Dagster project. One component. One `defs.yaml`. Handles everything.

## What it handles

| File type | How Dagster handles it |
|---|---|
| Plain Python script | Asset that runs the script via subprocess |
| Prefect flow | Flow parameters → Dagster config; runs via Prefect |
| Airflow DAG | DAG tasks → Dagster ops; runs via Airflow executor |
| dag-factory YAML | YAML-defined DAGs → Dagster ops |
| Cosmos DAG (`import cosmos`) | Airflow wrapper discarded → native `@dbt_assets` via dagster-dbt |

All from a single `defs.yaml` pointing at one repo.

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
