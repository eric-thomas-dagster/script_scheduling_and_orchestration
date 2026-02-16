# Example Scripts - Organized by Use Case

This directory contains example scripts demonstrating different Dagster orchestration patterns.

## Directory Structure

```
example_scripts/
├── basic_python/              # Simple Python scripts (ETL pipeline)
│   ├── extract_data.py        # Data extraction from API
│   ├── transform_data.py      # Data transformation
│   └── generate_report.py     # Report generation
│
├── distributed_compute/       # Heavy compute with Spark/Dask
│   ├── spark_job_example.py   # PySpark for billions of rows
│   └── dask_analysis_example.py  # Dask for analytics/ML
│
└── orchestrator_migration/    # Migrating from other orchestrators
    └── prefect_flow_example.py   # Existing Prefect flow
```

## Use Case Categories

### 1. Basic Python (`basic_python/`)

**Purpose:** Simple Python scripts forming an ETL pipeline

**Pattern:** Dagster orchestrates regular Python scripts
**Execution:** subprocess.run(["python", "script.py"])
**Dependencies:** extract → transform → report

**When to use:**
- Simple Python workloads
- Standard ETL pipelines
- Scripts with dependencies
- Scheduled batch jobs

**Scripts:**
- `extract_data.py` - Fetches data from external API
- `transform_data.py` - Cleans and transforms data
- `generate_report.py` - Creates summary report

### 2. Distributed Compute (`distributed_compute/`)

**Purpose:** Heavy compute on large datasets with Spark/Dask

**Pattern:** Dagster orchestrates external distributed jobs
**Execution:** Real PySpark and Dask frameworks
**Scale:** Billions of rows, 20+ executors

**When to use:**
- Processing billions of rows
- Heavy aggregations
- Distributed ML/analytics
- Scale-out compute

**Scripts:**
- `spark_job_example.py` - Market maker rules on order book snapshots
  - Uses actual PySpark with DataFrames
  - Local[4] for demo, production uses cluster
  - Processes 100k snapshots, applies rules, detects violations

- `dask_analysis_example.py` - Analytics and ML on aggregated data
  - Uses actual Dask with LocalCluster
  - 4 workers, 8 threads, live dashboard
  - Correlations, anomaly detection, reports

### 3. Orchestrator Migration (`orchestrator_migration/`)

**Purpose:** Migrating from other orchestrators (Prefect, Airflow, etc.)

**Pattern:** Run existing orchestrator code via Dagster
**Execution:** subprocess.run(["prefect", "deployment", "run", ...])
**Migration:** Lift-and-shift, then gradually modernize

**When to use:**
- Migrating from Prefect/Airflow
- Preserve existing workflows
- Gradual migration strategy
- Show Dagster orchestrating legacy code

**Scripts:**
- `prefect_flow_example.py` - Existing Prefect flow
  - Works with/without Prefect installed (mock decorators)
  - Demonstrates migration path
  - Can run via Dagster, then gradually convert to native

## How Scripts Are Discovered

The `ScriptGithubComponent` automatically discovers scripts:

1. Scans `example_scripts/` directory (including subdirectories)
2. Finds all `.py` files
3. Looks for matching `.yaml` metadata files
4. Creates Dagster assets based on metadata
5. Handles dependencies, schedules, retries

## Adding New Scripts

### Option 1: Add to Existing Category

```bash
# Add to basic_python/
cp my_script.py example_scripts/basic_python/
cp my_script.yaml example_scripts/basic_python/

# Or distributed_compute/
cp my_spark_job.py example_scripts/distributed_compute/
cp my_spark_job.yaml example_scripts/distributed_compute/
```

### Option 2: Create New Category

```bash
# Create new subdirectory
mkdir example_scripts/my_category/

# Add scripts
cp my_script.py example_scripts/my_category/
cp my_script.yaml example_scripts/my_category/

# Add README
cat > example_scripts/my_category/README.md << 'EOF'
# My Category

Description of what scripts in this category do...
EOF
```

## YAML Metadata Format

Each script needs a `.yaml` file:

```yaml
enabled: true
script_type: python  # python, spark, dask, prefect
description: "What this script does"
group: my_group
depends_on:
  - other_script_name
schedule:
  cron_schedule: "0 9 * * *"
  timezone: "UTC"
retry_policy:
  max_retries: 3
  delay: 60
```

## Testing

```bash
# Start Dagster
uv run dg dev

# Check UI
# All scripts from all subdirectories appear as assets
# Grouped by YAML metadata "group" field

# Materialize assets
# Click any asset → Materialize
```

## See Also

- `../EXECUTOR_PATTERNS.md` - Different execution patterns
- `../STREAMING_ARCHITECTURE.md` - Sensors + external assets
- `../DEPLOYMENT.md` - Production deployment
