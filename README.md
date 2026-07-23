# Script Orchestrator

A Dagster component that orchestrates existing Python scripts, Airflow DAGs, and Prefect flows — **no code changes required**.

## Why Use This?

**Already doing data engineering in Python, Airflow, or Prefect? Bring it into Dagster without a rewrite — and get everything Dagster adds on top for free.**

### Key Benefits

🎯 **One Platform, All Your Workflows**
- Prefect flows, Airflow DAGs, dbt projects, and plain Python scripts — orchestrated in the same Dagster deployment, sharing one asset graph.
- Since Prefect joined Dagster Labs, this project treats Prefect flows as first-class citizens: keep Prefect's execution semantics, gain Dagster's asset model, catalog, and UI on top.

🚀 **Everything Dagster Adds on Top**
- **Rich metadata** — logs, timing, Prefect artifacts, table schemas, and dbt column info automatically captured.
- **Cross-tool lineage** — Prefect assets, dbt models, and Airflow datasets connect in one graph via matching asset keys.
- **Freshness SLAs** — auto-derived from your existing schedules (`prefect.yaml` deployments, Airflow DAG cron); Dagster's UI shows PASS/WARN/FAIL per asset.
- **AutomationConditions** — `eager()` attached automatically so assets rematerialize when upstreams change.
- **Backfills, partitions, asset checks** — Dagster features work on top of Prefect/Airflow-defined workflows without changing them.

🛤️ **Practical Migration Path**
- **Start immediately** — run existing Prefect/Airflow code as-is in Dagster.
- **Gradual adoption** — migrate piece-by-piece to native Dagster patterns.
- **No rewrite required** — keep working code working while you modernize.
- **Same scripts, richer output** — Prefect scripts using `@materialize`, `add_asset_metadata`, or `create_markdown_artifact` surface those in Dagster's UI with no changes to user code.

### How It Works

This component takes a practical approach:

- 🔍 **Parse & Extract** — automatically reads schedules, parameters, decorators, and `prefect.yaml` deployments from Prefect/Airflow.
- 📊 **Capture Metadata** — logs, execution time, artifacts, table schemas, and dbt column info automatically captured.
- 🎯 **Map to Dagster Primitives** — Prefect `@materialize` → Dagster `AssetSpec`; Airflow tasks → ops; dbt via Cosmos → native `@dbt_assets`.
- 🔄 **Intelligent Fallback** — when gaps exist, falls back to in-process or subprocess execution.
- ⚠️ **Known Limitations** — some features unsupported (e.g., Airflow HITL, dynamically-constructed Prefect asset URIs).

This approach supports most Prefect and Airflow workflows while providing a clear migration path to native Dagster patterns.

### Prefect flow support

See [`dagster_orchestrator/README.md`](dagster_orchestrator/README.md#prefect-flows-in-dagster)
for the full mapping table. Highlights of what surfaces from a Prefect flow:

| Prefect | Dagster mapping |
|---|---|
| `@materialize("s3://…")` | Native Dagster asset with the URI parsed into a multi-segment `AssetKey` |
| `Asset(properties=AssetProperties(...))` | Owners, description, url, name on the AssetSpec |
| `asset_deps=[…]` + subflow chains | Real Dagster `deps=[…]` (explicit + inferred by walking `@flow` bodies, including through nested subflows) |
| `materialized_by="dbt"` | Adds `dbt` kind + column schema/lineage enrichment from `target/catalog.json` |
| `@flow(retries=N, tags=[…])`, `@materialize(retries=N, tags=[…])` | RetryPolicy + op tags |
| `add_asset_metadata({…})` (runtime) | Captured, emitted as typed MaterializeResult metadata |
| `create_markdown_artifact / create_link_artifact / create_table_artifact` | `MetadataValue.md / .url / .json / .table_schema` |
| `list[dict]` / DataFrame return values | Column schema + row_count auto-extracted |
| `prefect.yaml` deployments | Dagster jobs + `ScheduleDefinition`s; opt-in `CronFreshnessPolicy` |

Enable via `defs.yaml`:
```yaml
prefect_enabled: true
prefect_version: ">=3.4"          # @materialize needs Prefect 3.4+
auto_freshness_policies: true     # opt-in: derive FreshnessPolicy from schedules
dbt_project_path: dbt/jaffle_shop # optional: enables dbt catalog enrichment
```

## Validated Against Real Code

The mapping isn't demoware. This project points at *real, external* Prefect
and Airflow repos and turns their flows/DAGs into first-class Dagster assets
with zero code changes on the upstream side. That means:

- If you fork the upstream repo you can reproduce every screenshot yourself.
- If Prefect / Astronomer update their examples, we pull the new versions.
- The parser exercises the same corner cases real users hit.

### Prefect

**[dagster_orchestrator/defs/prefect_demos/](dagster_orchestrator/dagster_orchestrator/defs/prefect_demos/defs.yaml)**
is wired to Prefect's own sales-engineering demo repo,
[github.com/PrefectHQ/demos](https://github.com/PrefectHQ/demos) — cloned
with `--recurse-submodules` so its symlinked example files resolve. Flows
covered include:

| Prefect demo | What it exercises |
|---|---|
| `demos/hello_world/01_hello_world.py` | Basic `@flow` mapping |
| `demos/artifacts/weather.py` | `create_markdown_artifact` → `MetadataValue.md` |
| `demos/async_subflows/pokemon_weight.py` | Nested `@flow` calls with dep inference |
| `demos/crypto_prices/schedule_specific_parameters.py` | Multi-deployment schedules with per-deployment parameters |
| `demos/retries/04_retries.py` | `@task(retries=…)` → `RetryPolicy` |

Plus [our own Prefect asset demos](script_orchestrator/example_scripts/prefect_examples/):
[materialize_assets_demo.py](script_orchestrator/example_scripts/prefect_examples/materialize_assets_demo.py) (basic `@materialize`),
[materialize_implicit_deps_demo.py](script_orchestrator/example_scripts/prefect_examples/materialize_implicit_deps_demo.py) (implicit dep inference through subflows),
[materialize_full_demo.py](script_orchestrator/example_scripts/prefect_examples/materialize_full_demo.py) (artifacts, subflows, concurrency, freshness),
[materialize_partitioned_demo.py](script_orchestrator/example_scripts/prefect_examples/materialize_partitioned_demo.py) (partition-based backfills + asset checks from `assert`),
[dbt_via_prefect_demo.py](script_orchestrator/example_scripts/prefect_examples/dbt_via_prefect_demo.py) (`prefect_dbt` → native `@dbt_assets`).

### Airflow + Cosmos

**[dagster_orchestrator/defs/orchestrator/](dagster_orchestrator/dagster_orchestrator/defs/orchestrator/defs.yaml)**
is wired to
[github.com/astronomer/astronomer-cosmos](https://github.com/astronomer/astronomer-cosmos)'s
`dev/dags/` folder — the canonical set of Cosmos + Airflow DAGs Astronomer
ships as their reference. When a DAG calls Cosmos to run dbt, the whole DAG
gets replaced with native `@dbt_assets` (one Dagster asset per dbt model,
with lineage, column info, and dbt tests as asset checks).

Plus [six Airflow 3.x DAG patterns of our own](script_orchestrator/example_scripts/airflow_3x_examples/):
[simple_etl_3x.py](script_orchestrator/example_scripts/airflow_3x_examples/simple_etl_3x.py),
[data_pipeline_3x.py](script_orchestrator/example_scripts/airflow_3x_examples/data_pipeline_3x.py),
[report_generator_3x.py](script_orchestrator/example_scripts/airflow_3x_examples/report_generator_3x.py),
[report_from_processed_data_3x.py](script_orchestrator/example_scripts/airflow_3x_examples/report_from_processed_data_3x.py),
[multi_input_report_3x.py](script_orchestrator/example_scripts/airflow_3x_examples/multi_input_report_3x.py),
[customer_etl_factory.py](script_orchestrator/example_scripts/airflow_3x_examples/customer_etl_factory.py)
(DAG factory pattern → Dagster partitioned asset).

Point the orchestrator at your own repo (`repo_url:` in `defs.yaml`) and
the same mapping applies to your Prefect flows and Airflow DAGs.

## Quick Start

```bash
# With uv (recommended):
cd script_orchestrator
uv run dg dev

# Or with venv:
source .venv/bin/activate
dg dev
```

Open http://localhost:3000 to see the Dagster UI with your orchestrated scripts.

## What You'll See

The project includes comprehensive examples organized by technology:

```
example_scripts/
├── airflow_examples/      # Airflow DAGs with advanced features
├── prefect_examples/      # Prefect flows with job mode support
├── python_examples/       # Plain Python scripts with YAML config
├── basic_python/          # Simple Python examples
└── distributed_compute/   # Spark and Dask jobs
```

## Airflow Support

### What's Supported

**Automatically Parsed & Mapped to Dagster:**
- ✅ **Schedules** - Cron schedules automatically extracted from DAG definitions
- ✅ **Parameters** - DAG params and task arguments captured
- ✅ **Task Dependencies** - Complex task graphs with branching
- ✅ **Datasets** - Producer/consumer patterns map to Dagster assets
- ✅ **XCom** - Data passing mapped to op outputs/inputs where possible
- ✅ **Asset Checks** - SQL checks map to Dagster asset checks
- ✅ **dag-factory YAML** - YAML-based DAG definitions parsed and converted
- ✅ **Metadata & Logs** - Execution logs and timing automatically captured

**Execution Strategy:**
1. **Native Dagster Mapping** (preferred):
   - Tasks with outlets → Dagster assets
   - Tasks without outlets → Dagster ops in jobs
   - DAG dependencies → Asset dependencies
   - Leverages Dagster's execution engine

2. **In-Process Fallback** (when needed):
   - Falls back to subprocess execution for unsupported features
   - Still captures logs, metadata, and schedules
   - Provides compatibility with most Airflow features

**Known Limitations:**
- ⚠️ **Human-in-the-Loop (HITL)** - Interactive tasks not supported
- ⚠️ **Airflow UI Features** - Task instance clearing, manual runs with modified params
- ⚠️ **Some Operators** - Highly Airflow-specific operators may need fallback mode

### Smart Pattern Mapping

The component intelligently maps Airflow patterns to better Dagster equivalents:

**🎯 dag-factory → Partitioned Assets**

Airflow's dag-factory pattern (creating multiple similar DAGs) becomes Dagster partitioned assets:

```python
# Airflow dag-factory: 3 separate DAGs
customer_a_etl_dag
customer_b_etl_dag
customer_c_etl_dag

# ↓ Becomes in Dagster ↓

# Single partitioned asset
@asset(partitions_def=CustomersPartitionsDefinition(["customer_a", "customer_b", "customer_c"]))
def customer_etl(context):
    customer_id = context.partition_key  # "customer_a", "customer_b", or "customer_c"
    ...
```

**Benefits:**
- One asset definition instead of N DAGs
- Backfill all customers at once
- Better performance and observability
- Native Dagster partitioning features

**Other Smart Mappings:**
- **Airflow Datasets → Dagster Assets** - Asset-based scheduling with lineage
- **Task outlets → Asset materialization** - Produces tracked data assets
- **SQL checks → Asset checks** - Native data quality validation
- **XCom → Op outputs** - Type-safe data passing between ops

### Airflow Examples
- `customer_etl_factory.py` - dag-factory pattern → partitioned assets
- `dag_with_xcom.py` - XCom data passing
- `dag_with_datasets_*.py` - Dataset producer/consumer pattern
- `dag_with_branching.py` - Conditional task execution
- `dag_with_params.py` - Runtime parameters
- `standard_dag_with_checks.py` - Quality checks
- `xcom_example.yaml` - dag-factory YAML with XCom
- `multi_task_job_example.yaml` - dag-factory multi-DAG jobs
- `quality_checks_example.yaml` - dag-factory with checks

## Prefect Support

### What's Supported

**Automatically Parsed & Mapped to Dagster:**
- ✅ **Flow Detection** - Automatically discovers Prefect flows
- ✅ **Parameters** - Flow parameters and task arguments captured
- ✅ **Task Dependencies** - Flow task graphs parsed and converted
- ✅ **Retry Policies** - Retry configuration mapped where possible
- ✅ **Metadata & Logs** - Execution logs and timing automatically captured
- ✅ **Configuration** - Flow and task configuration extracted

**Execution Strategy:**
1. **Native Dagster Mapping** (when possible):
   - Flow → Dagster asset (default) or job
   - Tasks → Dagster ops
   - Leverages Dagster's execution engine

2. **In-Process Fallback** (when needed):
   - Falls back to subprocess execution for complex features
   - Still captures logs, metadata, and configuration
   - Provides compatibility with most Prefect features

**Execution Modes:**
- **`graph_asset`** (default): Maps flow to Dagster asset with ops for each task
- **`job`**: Maps flow to Dagster job for operational tasks

**Known Limitations:**
- ⚠️ **Task Runners** - Complex concurrency/distributed runners may use fallback
- ⚠️ **Prefect Cloud Features** - Automations, webhooks, notifications
- ⚠️ **Some Decorators** - Advanced Prefect-specific decorators may need fallback mode

### Prefect Examples
- `01_hello_world.py` - Simple flow
- `02_simple_web_scraper.py` - Flow with dependencies
- `03_run_api_sourced_etl.py` - Job mode example (not an asset)
- `test_job_mode.py` - Explicit job mode configuration
- `conditionally_retry_with_delay.py` - Retry policies
- `local_concurrency_with_task_runner.py` - Task runner config
- `simple_map_example.py` - Mapped tasks

### Job Mode Configuration
Prefect flows can be configured to run as Dagster jobs instead of assets:

```yaml
# 03_run_api_sourced_etl.yaml
prefect_mapping:
  mode: job  # Creates a Dagster job, not an asset
```

Use **job mode** for operational tasks (notifications, cleanup) that don't produce data assets.

## Python Scripts

Plain Python scripts work with simple YAML configuration:

### Python Script (`extract_data.py`)
```python
#!/usr/bin/env python3
import json
from pathlib import Path

def main():
    data = {"records": [...]}
    Path("/tmp/data.json").write_text(json.dumps(data))
    print("✅ Done!")

if __name__ == "__main__":
    main()
```

### YAML Config (`extract_data.yaml`)
```yaml
description: "Extracts data from source"
group: data_pipeline
schedule:
  cron_schedule: "0 2 * * *"  # Daily at 2am
  timezone: "UTC"
depends_on: []  # No dependencies
```

### Python Examples
- `test_argparse.py` - Scripts with argparse configuration
- `test_sys_argv.py` - Scripts using sys.argv
- `test_partitioned.py` - Time-partitioned scripts
- `detected_resources.py` - Auto-detected resource usage

## Distributed Compute

### Spark Jobs
```python
# spark_word_count.py - runs via spark-submit
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WordCount").getOrCreate()
# Your Spark code here
```

### Dask Jobs
```python
# dask_dataframe_analysis.py - distributed pandas
import dask.dataframe as dd

df = dd.read_csv("large_file.csv")
result = df.groupby("category").mean().compute()
```

## Configuration

Edit `.env` in script_orchestrator/ to configure:

```bash
# Local mode (default)
USE_LOCAL_SCRIPTS=true
SCRIPTS_DIR=example_scripts

# Airflow support
AIRFLOW_ENABLED=true
AIRFLOW_VERSION=3.1
AIRFLOW_AUTO_INSTALL=false

# Prefect support
PREFECT_ENABLED=true

# GitHub mode (optional)
# USE_LOCAL_SCRIPTS=false
# SCRIPTS_REPO_URL=https://github.com/your-org/scripts
# SCRIPTS_DIR=scripts
# GITHUB_TOKEN=your_token
```

## YAML Configuration Reference

### Component Configuration (defs.yaml)

The `script_orchestrator/script_orchestrator/defs/scripts/defs.yaml` file configures the component:

```yaml
type: script_orchestrator.components.ScriptGithubComponent
attributes:
  # Source Configuration
  use_local: true                    # Use local scripts (true) or clone from GitHub (false)
  scripts_directory: example_scripts # Directory containing scripts (relative to project root)
  repo_url: null                     # GitHub repo URL (required if use_local=false)
  repo_branch: main                  # Git branch to use
  github_token: null                 # GitHub token for private repos (or use env var)

  # Airflow Configuration
  airflow_enabled: true              # Enable Airflow DAG discovery
  airflow_version: '3.1'            # Target Airflow version (e.g., '2.9', '3.1')
  airflow_auto_install: true        # Auto-install Airflow if not present/mismatched

  # Prefect Configuration
  prefect_enabled: true              # Enable Prefect flow discovery
  prefect_version: null              # Target Prefect version (e.g., '3.0')
  prefect_auto_install: true        # Auto-install Prefect if not present/mismatched
```

**Component Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `use_local` | bool | `false` | Use local scripts instead of cloning from GitHub |
| `scripts_directory` | str | `"scripts"` | Directory containing script files |
| `repo_url` | str | `null` | GitHub repository URL (required if `use_local=false`) |
| `repo_branch` | str | `"main"` | Git branch to clone/pull |
| `github_token` | str | `null` | GitHub personal access token for private repos |
| `airflow_enabled` | bool | `true` | Enable Airflow DAG discovery and execution |
| `airflow_version` | str | `null` | Target Airflow version (auto-installs if specified) |
| `airflow_auto_install` | bool | `true` | Automatically install target Airflow version |
| `prefect_enabled` | bool | `true` | Enable Prefect flow discovery and execution |
| `prefect_version` | str | `null` | Target Prefect version (auto-installs if specified) |
| `prefect_auto_install` | bool | `true` | Automatically install target Prefect version |

### Script Companion YAML Files

Each script can have a companion YAML file for Dagster-specific metadata (group, owners, tags, schedules, etc.):

- **Python/Airflow scripts**: `example.py` → `example.yaml`
- **dag-factory YAMLs**: `example_dag_factory.yaml` → `example_dag_factory.dagster.yaml`

The available options depend on the script type.

#### Python Scripts

```yaml
# example.yaml (for example.py)
enabled: true
description: "Extracts data from API"
group: data_pipeline
owners:
  - "team:data_engineering"
  - "person:alice@example.com"
tags:
  category: "etl"
  priority: "high"
kinds:
  - python
  - api
script_type: python

# Scheduling
schedule:
  cron_schedule: "0 2 * * *"  # Cron expression
  timezone: "UTC"             # Timezone for schedule

# Dependencies
depends_on:
  - other_script_name         # Other scripts this depends on

# Partitioning (time-based)
partition:
  parameter: date             # Script parameter name for partition key
  schedule: daily             # daily, hourly, weekly, monthly
  start_date: "2024-01-01"   # Start date for partitions (defaults to 30 days ago)
  timezone: "UTC"            # Timezone for schedule (default: UTC)
  date_format: "%Y-%m-%d"    # Date format passed to script (default: %Y-%m-%d)

# Partitioning (static list - alternative to time-based)
# partition:
#   parameter: region
#   values: [us, uk, de, jp]

# Retry Policy
retry_policy:
  max_retries: 3
  delay: 60                   # Seconds between retries
  backoff: exponential        # exponential or linear

# Execution
timeout: 3600                 # Timeout in seconds
```

**Python YAML Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `enabled` | bool | No | Enable/disable this script (default: `true`) |
| `description` | str | No | Human-readable description |
| `group` | str | No | Asset group name for organization |
| `owners` | list[str] | No | Owner emails or team tags |
| `tags` | dict | No | Key-value tags for filtering |
| `kinds` | list[str] | No | Asset kinds for compute/storage type |
| `script_type` | str | No | Must be `"python"` |
| `schedule` | dict | No | Cron schedule configuration |
| `depends_on` | list[str] | No | List of asset keys this depends on |
| `partition` | dict | No | Time-based partitioning config (see below) |
| `retry_policy` | dict | No | Retry behavior configuration |
| `timeout` | int | No | Execution timeout in seconds |

**Partition Configuration:**

Partitions allow you to process data in chunks - either time-based (daily, hourly) or static lists (regions, customers, etc.).

**How partition keys are passed to your script:**
- **Without config**: Partition key is passed as a **positional argument** (`sys.argv[1]`)
- **With argparse config**: Partition key is passed as `--<parameter> <value>`
- **With sys.argv config**: Partition key is appended after other positional arguments

```python
# WITHOUT config (positional argument):
# python script.py 2024-01-15
import sys
partition_key = sys.argv[1] if len(sys.argv) > 1 else None

# WITH argparse config (named argument):
# python script.py --output-dir /tmp --region us
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--output-dir', type=str)
parser.add_argument('--region', type=str)  # partition parameter
args = parser.parse_args()
partition_key = args.region
```

**Time-Based Partitions** (daily, hourly, weekly, monthly):

```yaml
partition:
  parameter: date
  schedule: daily             # hourly, daily, weekly, or monthly
  start_date: "2024-01-01"   # Optional
  timezone: "UTC"            # Optional
  date_format: "%Y-%m-%d"    # Optional
```

**Static Partitions** (fixed list like regions, customers):

```yaml
partition:
  parameter: region
  values:                     # List of partition keys
    - us
    - uk
    - de
    - jp
```

**Dynamic Partitions** (partitions that can be added/removed at runtime):

```yaml
partition:
  parameter: customer_id
  dynamic: true               # Allows adding partitions via Dagster UI
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `partition.parameter` | str | Yes | Name of the parameter (for documentation/metadata) |
| `partition.schedule` | str | No | Time-based: `hourly`, `daily`, `weekly`, `monthly` |
| `partition.values` | list[str] | No | Static: List of partition keys (e.g., `["us", "uk", "de"]`) |
| `partition.dynamic` | bool | No | Dynamic: Allow runtime partition management (default: `false`) |
| `partition.start_date` | str | No | Time-based: Start date YYYY-MM-DD (default: 30 days ago) |
| `partition.timezone` | str | No | Time-based: Timezone (default: `UTC`) |
| `partition.date_format` | str | No | Time-based: Date format (default: `%Y-%m-%d`) |

**Note:** Choose ONE partition type: `schedule` (time-based), `values` (static), OR `dynamic`.

#### Airflow DAGs

For standard Airflow DAGs (Python files with `.py` extension):

```yaml
# dag_example.yaml (for dag_example.py)
enabled: true
description: "Airflow DAG with branching logic"
group: airflow_examples
owners:
  - "team:data_engineering"
tags:
  category: "airflow"
  complexity: "intermediate"
kinds:
  - python
  - airflow
script_type: airflow

airflow_mapping:
  enabled: true
  dag_id: "my_dag_id"         # Optional: override DAG ID
```

**Airflow YAML Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `enabled` | bool | No | Enable/disable this DAG |
| `script_type` | str | Yes | Must be `"airflow"` |
| `airflow_mapping.enabled` | bool | No | Enable Airflow mapping (default: `true`) |
| `airflow_mapping.dag_id` | str | No | Override DAG ID from Python file |

**dag-factory YAML Companion Files:**

dag-factory YAML files (Airflow's YAML DAG format) use Airflow's native schema for DAG definitions. To specify Dagster-specific metadata (group, owners, tags, etc.), create a companion file with the `.dagster.yaml` suffix:

```yaml
# example_dag_factory.yaml (Airflow dag-factory format)
customer_etl:
  schedule_interval: '@daily'
  tasks:
    # ... Airflow DAG definition

# example_dag_factory.dagster.yaml (Dagster metadata companion)
enabled: true
group: airflow_examples
owners: ["team:data_engineering"]
tags:
  category: "airflow"
  pattern: "dag_factory"
```

The component automatically discovers and loads `.dagster.yaml` companion files. If no companion file exists, default metadata is used with `group: "scripts"`. See [Airflow dag-factory documentation](https://github.com/astronomer/dag-factory) for the dag-factory YAML format.

#### Prefect Flows

```yaml
# flow_example.yaml (for flow_example.py)
enabled: true
script_type: prefect
description: "Prefect flow with parallel tasks"
group: prefect_examples
owners:
  - "team:data_engineering"
tags:
  pattern: "concurrent"
  complexity: "intermediate"
kinds:
  - python
  - prefect

prefect_mapping:
  enabled: true
  fallback_on_error: true     # Fallback to subprocess if mapping fails
  mode: "graph_asset"         # "graph_asset" or "job"

# Optional: Schedule configuration
schedule:
  cron_schedule: "0 3 * * *"
  timezone: "UTC"
```

**Prefect YAML Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `enabled` | bool | No | Enable/disable this flow |
| `script_type` | str | Yes | Must be `"prefect"` |
| `prefect_mapping.enabled` | bool | No | Enable Prefect flow mapping (default: `true`) |
| `prefect_mapping.fallback_on_error` | bool | No | Fallback to subprocess execution on errors |
| `prefect_mapping.mode` | str | No | `"graph_asset"` (default) or `"job"` |
| `schedule` | dict | No | Cron schedule (same as Python scripts) |

**Prefect Modes:**

- **`graph_asset`** (default): Creates a Dagster asset with ops for each task
  - Use for flows that produce data
  - Appears in asset graph
  - Supports asset dependencies

- **`job`**: Creates a Dagster job (not an asset)
  - Use for operational tasks (notifications, cleanup, etc.)
  - Does not appear in asset graph
  - Executed on-demand or via schedules/sensors

### Common Patterns

#### 1. Asset Dependencies
```yaml
# downstream.yaml
depends_on:
  - upstream_asset_name       # This asset depends on upstream_asset_name
  - another_asset
```

#### 2. Schedules
```yaml
schedule:
  cron_schedule: "0 2 * * *"  # Daily at 2 AM
  timezone: "America/New_York"
```

Common cron patterns:
- `"0 * * * *"` - Every hour
- `"0 2 * * *"` - Daily at 2 AM
- `"0 2 * * MON"` - Weekly on Monday at 2 AM
- `"0 0 1 * *"` - Monthly on the 1st

#### 3. Partitions
```yaml
partitions:
  type: daily                 # daily, hourly, weekly, monthly
  start_date: "2024-01-01"
  end_date: null              # null = ongoing
```

#### 4. Retry Policies
```yaml
retry_policy:
  max_retries: 3
  delay: 60                   # Initial delay in seconds
  backoff: exponential        # exponential or linear
```

## Features

### Core Capabilities
- 🔄 **Dependency Management** - Scripts depend on other scripts/assets
- ⏰ **Scheduling** - Cron schedules per script/DAG/flow
- 🏷️ **Rich Metadata** - Groups, tags, owners, descriptions
- 🔁 **Retry Policies** - Configurable retries with backoff
- 📊 **Execution Tracking** - Stdout, execution time, metadata
- 🌳 **Lineage Graph** - Visual dependency tree
- 🔧 **Git or Local** - Scripts from GitHub or local directory

### Orchestration Support
- 🎯 **Airflow** - DAGs, dag-factory, XCom, datasets, checks
- 🌊 **Prefect** - Flows, job mode, task runners, retries
- 🐍 **Python** - Plain scripts with YAML config
- ⚡ **Distributed** - Spark, Dask, Ray jobs

### Advanced Features
- 📦 **Auto-install** - Dependencies from requirements.txt
- 🔍 **Resource Detection** - Auto-detect imports (pandas, requests, etc.)
- 🎛️ **Config Extraction** - Parse argparse, sys.argv automatically
- 🔀 **Job vs Asset** - Flexible mapping of tasks to Dagster primitives
- ✅ **Quality Checks** - Airflow checks map to asset checks

## Architecture

The component uses Dagster's `StateBackedComponent` pattern:
- **State management** - Cached script discovery for fast loading
- **YAML validation** - Pydantic models with schema validation
- **Pluggable parsers** - Separate parsers for Airflow, Prefect, Python
- **Type routing** - Automatic routing to assets vs jobs

## For Your Demo

**Key talking points:**

1. **No code changes required** - Show Airflow DAG/Prefect flow has no Dagster imports
2. **Automatic parsing** - Schedules, parameters, and config automatically extracted
3. **Smart execution** - Maps to Dagster primitives where possible, falls back when needed
4. **Logs & metadata** - Execution logs, timing, and metadata automatically captured
5. **Visual lineage** - Show dependency graph for cross-tool workflows (Airflow + Prefect + Python)
6. **YAML for Python scripts** - Simple config for plain scripts
7. **Practical migration path** - Run existing code while gradually adopting Dagster patterns
8. **Unified observability** - One UI for Airflow, Prefect, and Python scripts
9. **Honest about gaps** - Clear about what's native vs fallback, known limitations

**Demo flow:**
- Show an Airflow DAG with datasets → mapped to Dagster assets
- Show a Prefect flow with tasks → mapped to Dagster ops
- Show cross-tool dependencies (Prefect depends on Airflow asset)
- Show logs/metadata captured from both
- Discuss: where we map natively, where we use fallback, and the migration path

## Project Structure

```
script_scheduling_and_orchestration/
├── README.md                           # This file
└── script_orchestrator/                # Dagster project
    ├── pyproject.toml                  # Dependencies (UV)
    ├── dg.toml                         # Dagster CLI config
    ├── dagster.yaml                    # Dagster configuration
    ├── .env                            # Environment variables
    ├── example_scripts/                # Example scripts by type
    │   ├── airflow_examples/           # Airflow DAGs
    │   ├── prefect_examples/           # Prefect flows
    │   ├── python_examples/            # Python scripts
    │   ├── basic_python/               # Simple examples
    │   └── distributed_compute/        # Spark/Dask
    ├── script_orchestrator/            # Python package
    │   ├── components/                 # Component implementation
    │   │   ├── script_github_component.py  # Main component
    │   │   ├── parsers/                # Airflow/Prefect/Python parsers
    │   │   └── utils/                  # Resource detection, etc.
    │   └── defs/                       # Dagster definitions
    │       └── scripts/                # Component instance
    │           └── defs.yaml           # Component config
    └── script_orchestrator_tests/      # Tests
```

## Documentation

- [Dagster Docs](https://docs.dagster.io)
- [Dagster Components](https://docs.dagster.io/concepts/components)
- [Dagster+ Features](https://dagster.io/cloud)
- [Airflow Migration](https://docs.dagster.io/integrations/airflow)
- [Prefect Migration](https://docs.dagster.io/guides/migrations)

## License

See LICENSE file for details.
