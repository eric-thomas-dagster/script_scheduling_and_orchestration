# Script Orchestrator

A Dagster component that orchestrates existing Python scripts, Airflow DAGs, and Prefect flows with YAML-based configuration - **no code changes required**.

## Perfect for Airflow & Prefect Users

This project demonstrates how teams can run their existing orchestration code in Dagster without rewriting:
- ✅ **Airflow DAGs** run as-is with full feature support
- ✅ **Prefect flows** run as-is with configuration mapping
- ✅ **Python scripts** work with simple YAML configuration
- ✅ **Distributed compute** for Spark, Dask, and Ray jobs

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

### Core Features
- ✅ **Standard Airflow DAGs** - Run existing DAGs without modification
- ✅ **dag-factory YAML** - Full support for dag-factory pattern
- ✅ **XCom** - Data passing between tasks
- ✅ **Datasets** - Producer/consumer patterns map to Dagster assets
- ✅ **Task Dependencies** - Complex task graphs with branching
- ✅ **Parameters** - Runtime configuration via params
- ✅ **Asset Checks** - Map Airflow checks to Dagster asset checks

### Airflow Examples
- `dag_with_xcom.py` - XCom data passing
- `dag_with_datasets_*.py` - Dataset producer/consumer pattern
- `dag_with_branching.py` - Conditional task execution
- `dag_with_params.py` - Runtime parameters
- `standard_dag_with_checks.py` - Quality checks
- `xcom_example.yaml` - dag-factory YAML with XCom
- `multi_task_job_example.yaml` - dag-factory multi-DAG jobs
- `quality_checks_example.yaml` - dag-factory with checks

### How It Works
Airflow DAGs are automatically detected and converted to Dagster assets/jobs:
- **Tasks with outlets** → Dagster assets
- **Tasks without outlets** → Dagster ops in jobs
- **DAG dependencies** → Asset dependencies
- **Datasets** → Asset keys for lineage

## Prefect Support

### Core Features
- ✅ **Prefect Flows** - Run existing flows without modification
- ✅ **Job Mode** - Map flows to Dagster jobs (not assets)
- ✅ **Task Runners** - Local concurrency and task execution
- ✅ **Retries** - Configurable retry policies with delays
- ✅ **Flow Parameters** - Runtime configuration
- ✅ **Subflows** - Nested flow support

### Prefect Examples
- `01_hello_world.py` - Simple flow
- `02_simple_web_scraper.py` - Flow with dependencies
- `03_run_api_sourced_etl.py` - Job mode example (not an asset)
- `test_job_mode.py` - Explicit job mode configuration
- `conditionally_retry_with_delay.py` - Retry policies
- `local_concurrency_with_task_runner.py` - Task runner config
- `simple_map_example.py` - Mapped tasks

### Job Mode
Prefect flows can be configured to run as Dagster jobs instead of assets:

```yaml
# 03_run_api_sourced_etl.yaml
mode: job  # Creates a Dagster job, not an asset
```

This is useful for operational tasks that don't produce data assets.

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

1. **No code changes** - Show Airflow DAG/Prefect flow has no Dagster imports
2. **Run as-is** - Existing orchestration code works unchanged
3. **YAML for Python scripts** - Simple config for plain scripts
4. **Automatic discovery** - Add new script+YAML, reload, it appears
5. **Visual lineage** - Show dependency graph for cross-tool workflows
6. **Airflow features** - XCom, datasets, dag-factory all work
7. **Prefect features** - Job mode, retries, task runners all work
8. **Migration path** - Start simple, gradually adopt Dagster features
9. **Unified observability** - One UI for Airflow, Prefect, Python scripts

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
