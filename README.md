# Script Orchestrator

A Dagster project that demonstrates how to orchestrate existing Python scripts with YAML-based configuration for scheduling and dependencies.

## Perfect for Prefect Users

This demo shows how teams can migrate from Prefect (or similar tools) to Dagster without rewriting their scripts. Just add YAML files for scheduling and dependencies!

## Quick Start

```bash
# With uv (recommended):
uv run dg dev

# Or with venv:
source venv/bin/activate
dg dev
```

Open http://localhost:3000 to see the Dagster UI with your script assets.

## What You'll See

Three example scripts with a dependency chain:

```
extract_data → transform_data → generate_report
```

Each script:
- ✅ Is a normal Python script (no Dagster imports required)
- ✅ Has a YAML file defining schedule and dependencies
- ✅ Shows up as an asset in Dagster
- ✅ Can be materialized individually or as part of the chain

## Example Files

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

## Configuration

Edit `.env` to configure:

```bash
# Local mode (default)
USE_LOCAL_SCRIPTS=true
SCRIPTS_DIR=example_scripts

# OR GitHub mode
# USE_LOCAL_SCRIPTS=false
# SCRIPTS_REPO_URL=https://github.com/your-org/scripts
# SCRIPTS_DIR=scripts
# GITHUB_TOKEN=your_token
```

## Features

- 🔄 **Dependency Management** - Scripts can depend on other scripts
- ⏰ **Scheduling** - Each script can have its own cron schedule
- 🏷️ **Rich Metadata** - Groups, tags, owners, descriptions
- 🔁 **Retry Policies** - Configurable retries with backoff
- 📊 **Execution Tracking** - See stdout, execution time, and more
- 🌳 **Lineage Graph** - Visual dependency tree
- 🔧 **Git or Local** - Use scripts from GitHub or local directory
- 🔀 **Multiple Script Types** - Python, Prefect, Spark, Dask
- 📦 **Dependency Installation** - Auto-install from requirements.txt
- ⚡ **Distributed Compute** - Orchestrate Spark/Dask/Ray jobs

## Script Types Supported

### 1. Python Scripts (example_scripts/extract_data.py)
Plain Python scripts with no modifications needed.

### 2. Prefect Flows (example_scripts/prefect_flow_example.py)
Existing Prefect workflows run in Dagster orchestration.

### 3. Spark Jobs (example_scripts/spark_job_example.py)
Submit to Spark cluster via `spark-submit`.

### 4. Dask Jobs (example_scripts/dask_analysis_example.py)
Distributed Python compute via Dask cluster.

## For Your Demo

**Key talking points:**

1. **No script changes required** - Show `extract_data.py` has no Dagster imports
2. **Simple YAML config** - Show `extract_data.yaml` for schedule/dependencies
3. **Automatic discovery** - Add a new script+YAML, reload, it appears
4. **Visual lineage** - Show the dependency graph in UI
5. **Rich observability** - Show execution logs, metadata, timing
6. **Migration path** - Start simple, gradually adopt Dagster features
7. **Prefect compatibility** - Show Prefect flow running in Dagster
8. **Distributed compute** - Show Spark/Dask job patterns
9. **Production deployment** - See DEPLOYMENT.md for Docker/VPC setup

## Next Steps

See [DEMO_GUIDE.md](./DEMO_GUIDE.md) for:
- Detailed feature walkthrough
- Demo talking points
- Migration strategy
- Advanced configuration

## Documentation

- [Dagster Docs](https://docs.dagster.io)
- [Dagster+ Features](https://dagster.io/cloud)
- [Migration Guides](https://docs.dagster.io/guides/migrations)
