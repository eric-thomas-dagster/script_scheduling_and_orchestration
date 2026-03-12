# dbt_cosmos_orchestrator

A Dagster project that reads [Astronomer Cosmos](https://github.com/astronomer/astronomer-cosmos)
DAGs (Airflow + dbt) and converts them to **native Dagster dbt assets**.

## What it does

The `CosmosGithubComponent`:

1. Clones `astronomer/astronomer-cosmos` (shallow, cached locally)
2. Scans `dev/dags/` for Cosmos DAG files
3. For each **applicable** DAG → creates a Dagster job using `dagster-dbt`
4. For `dbt_docs.py` → creates a `dbt_docs` asset that runs `dbt docs generate`
5. **Skips** non-applicable DAGs (Kubernetes, virtualenv, file-watcher, etc.) with documented reasons
6. Creates a `cosmos_migration_summary` asset you can materialise to see the full report

### Converted DAGs

| Cosmos DAG | Dagster job | Notes |
|---|---|---|
| `basic_cosmos_dag.py` | `cosmos__basic_cosmos_dag` | All models, daily |
| `basic_cosmos_task_group.py` | `cosmos__basic_cosmos_task_group` | Customers + orders |
| `cosmos_seed_dag.py` | `cosmos__cosmos_seed_dag` | Seeds only |
| `example_cosmos_dbt_build.py` | `cosmos__example_cosmos_dbt_build` | Full dbt build |
| `cosmos_manifest_example.py` | `cosmos__cosmos_manifest_example` | Manifest-based |
| `cosmos_manifest_selectors_example.py` | `cosmos__cosmos_manifest_selectors_example` | `tag:daily` |
| `dbt_docs.py` | `dbt_docs` asset | Generates HTML docs |
| … and more | | |

### Skipped DAGs (and why)

| Cosmos DAG | Reason |
|---|---|
| `jaffle_shop_kubernetes.py` | Kubernetes execution — not needed in Dagster |
| `example_virtualenv*.py` | Virtualenv isolation is Airflow-specific |
| `example_watcher.py` | File-watcher triggers → use Dagster sensors |
| `performance_dag.py` | Cosmos/Airflow benchmarking harness |
| `example_task_mapping.py` | Airflow dynamic task mapping |
| `cross_project_*.py` | Multi-project setup; use separate code locations |

## Setup

### 1. Install dependencies

```bash
cd dbt_cosmos_orchestrator
uv venv && uv pip install -e ".[dev]"
```

### 2. Configure a dbt profile

The jaffle_shop project uses **PostgreSQL** by default. You have two options:

**Option A — PostgreSQL** (matches original Cosmos DAGs)

Add to `~/.dbt/profiles.yml`:

```yaml
jaffle_shop:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: your_user
      pass: your_password
      dbname: your_db
      schema: public
      threads: 4
```

**Option B — DuckDB** (no server required, great for demos)

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

Then set `dbt_target: dev` in `defs/cosmos/defs.yaml`.

### 3. Launch

```bash
uv run dagster dev
```

On first launch the component will:
- Clone `astronomer/astronomer-cosmos` to `~/.dagster/cosmos_cache/astronomer_cosmos/`
- Run `dbt deps` + `dbt parse` to compile the manifest
- Register all assets, jobs, and schedules

## Project structure

```
dbt_cosmos_orchestrator/
├── pyproject.toml
└── dbt_cosmos_orchestrator/
    ├── __init__.py
    ├── components/
    │   ├── __init__.py
    │   └── cosmos_github_component.py   ← the main logic
    └── defs/
        ├── __init__.py
        └── cosmos/
            └── defs.yaml                ← points at astronomer-cosmos repo
```

## Customisation

Edit `defs/cosmos/defs.yaml` to:

- **Point at a different repo** — change `repo_url` / `dbt_project_path`
- **Use a private repo** — clone it manually into `cache_dir`
- **Exclude deprecated models globally** — set `global_dbt_exclude: "tag:deprecated"`
- **Use a different branch** — change `branch`
