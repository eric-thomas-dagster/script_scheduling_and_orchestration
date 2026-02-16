# Production Deployment Guide

## Dependency Management in Docker Code Locations

### The Challenge
When cloning scripts from a Git repo, those scripts may have their own dependencies that aren't in your Dagster code location's Docker image.

### Solution Options

#### Option 1: Install Dependencies at Component Load (Recommended for Dev)
```yaml
# defs/scripts/defs.yaml
attributes:
  use_local: false
  repo_url: "https://github.com/your-org/scripts"
  scripts_directory: "scripts"
  install_dependencies: true  # Install from requirements.txt
```

**How it works:**
- Component checks for `requirements.txt` in cloned repo
- Runs `pip install -r requirements.txt` at load time
- Dependencies available for all script executions

**Pros:**
- Simple, automatic
- Works for rapid iteration

**Cons:**
- Installs at dagster code location startup (slower startups)
- Dependencies not cached in Docker layer
- Not recommended for production

#### Option 2: Pre-bake Dependencies in Docker (Recommended for Production)

**Dockerfile approach:**
```dockerfile
FROM dagster/dagster-cloud:latest

# Install your Dagster code
COPY . /opt/dagster/app
WORKDIR /opt/dagster/app
RUN pip install -e .

# Clone and install script dependencies at build time
RUN git clone https://github.com/your-org/scripts /tmp/scripts && \
    pip install -r /tmp/scripts/requirements.txt && \
    rm -rf /tmp/scripts/.git

# Or if you have known dependencies
RUN pip install prefect spark-submit dask[complete] clickhouse-driver
```

**Pros:**
- Fast startup
- Dependencies cached in Docker layer
- Production-ready
- Predictable environment

**Cons:**
- Need to rebuild Docker image when script dependencies change
- Less flexible for rapid iteration

#### Option 3: Hybrid Deployment with Separate Compute

For Spark/Ray/Dask jobs that need heavy dependencies:

```python
# spark_job.yaml
script_type: spark

# Dagster submits to external Spark cluster
# Job runs in Spark's environment, not Dagster's
```

**How it works:**
- Dagster code location stays lightweight
- Heavy compute jobs run on dedicated clusters (Spark/Dask/Ray)
- Jobs use their cluster's environment, not Dagster's

**Example: Spark**
```yaml
script_type: spark
# Dagster runs: spark-submit --master spark://cluster:7077 job.py
# Job executes on Spark cluster with its own dependencies
```

**Example: Dask**
```yaml
script_type: dask
# Script connects to Dask scheduler
# Work distributed across Dask workers
```

## Recommended Architecture

### Development Environment
```
┌─────────────────────────┐
│ Dagster Code Location   │
│ (Local/Dev)             │
│                         │
│ - Auto-install deps     │
│ - Fast iteration        │
│ - install_dependencies  │
│   = true                │
└─────────────────────────┘
```

### Production Environment
```
┌─────────────────────────┐     ┌──────────────────┐
│ Dagster Code Location   │────▶│ External Compute │
│ (Docker)                │     │                  │
│                         │     │ - Spark Cluster  │
│ - Pre-baked deps        │     │ - Dask Cluster   │
│ - Lightweight           │     │ - Ray Cluster    │
│ - Fast startup          │     │                  │
└─────────────────────────┘     └──────────────────┘
          │
          │ For simple scripts:
          ▼
   Dependencies baked
   into Docker image
```

## Example Dockerfile for Polymarket Use Case

```dockerfile
FROM dagster/dagster-cloud:latest

# System dependencies
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install Dagster code location
COPY pyproject.toml setup.py ./
COPY script_orchestrator ./script_orchestrator
RUN pip install -e .

# Install common script dependencies
# (These are the deps your cloned scripts need)
RUN pip install \
    prefect==2.14.0 \
    clickhouse-driver==0.2.6 \
    pandas==2.1.0 \
    numpy==1.24.0 \
    pymongo==4.5.0

# For Spark jobs: just need spark-submit available
# (Jobs run on Spark cluster with its own environment)
RUN pip install pyspark==3.5.0  # Just for client

# For Dask: connection libs only
# (Work runs on Dask cluster)
RUN pip install dask[distributed]==2023.10.0  # Just for client

# Set up work directory
WORKDIR /opt/dagster/app

# The component will clone scripts at runtime
# But dependencies are already installed!
```

## Security Considerations

### VPC/Network Isolation
```yaml
# In Dagster Cloud hybrid deployment:
#
# Agent runs in your VPC
# ├── Dagster Code Location (Docker)
# │   └── Clones scripts from private GitHub
# │       (uses GitHub token from env vars)
# │
# ├── Connects to private resources:
# │   ├── ClickHouse (internal)
# │   ├── MongoDB (internal)
# │   ├── Spark Cluster (internal)
# │   └── Dask Cluster (internal)
# │
# └── Metadata goes to Dagster Cloud
#     (run logs, asset metadata, lineage)
```

### Secrets Management
```yaml
# Environment variables in code location:
SCRIPTS_REPO_URL: "https://github.com/polymarket/scripts"
GITHUB_TOKEN: "${GITHUB_TOKEN}"  # From secrets manager
CLICKHOUSE_HOST: "internal-ch.vpc"
CLICKHOUSE_PASSWORD: "${CH_PASSWORD}"
MONGO_URI: "${MONGO_URI}"
SPARK_MASTER: "spark://internal-spark:7077"
```

## Multi-Tenancy (DeFi vs US)

### Option A: Separate Code Locations (Recommended)
```
Dagster Cloud Deployment
├── Code Location: defi
│   └── Clones: github.com/polymarket/defi-scripts
│       - ClickHouse: defi-ch.vpc
│       - Access: DeFi team only
│
└── Code Location: us-markets
    └── Clones: github.com/polymarket/us-scripts
        - MongoDB: us-mongo.vpc
        - ClickHouse: us-ch.vpc
        - Access: US team only
```

**Benefits:**
- Complete isolation
- Different IAM roles
- Different networks/VPCs
- Independent scaling
- Clear security boundary

### Option B: Single Code Location with Asset Groups
```
Code Location: polymarket
├── Group: defi (tags: domain=defi)
└── Group: us (tags: domain=us)
```

**Benefits:**
- Shared dependencies
- Unified view
- Simpler deployment

**Trade-offs:**
- Less isolation
- Requires careful access controls

## Real-World Example: Market Maker Rules

```yaml
# High-frequency order book analysis
script_type: spark  # Heavy compute on Spark cluster

depends_on:
  - ingest_order_books  # Runs after ingestion

# Triggered by sensor, not schedule
# Sensor watches for new data in ClickHouse

# When executed:
# 1. Dagster checks dependency met
# 2. Dagster submits to Spark cluster
# 3. Spark loads 100M+ snapshots from ClickHouse
# 4. Spark computes aggregations (parallel)
# 5. Spark writes violations back to ClickHouse
# 6. Dagster records metadata (runtime, rows, etc.)
```

## Performance Considerations

### For 1500+ Tasks
**Don't do this:**
```python
# ❌ Create 1500 separate Dagster assets
for i in range(1500):
    @asset(name=f"task_{i}")
    def task_i(): ...
```

**Do this:**
```python
# ✅ Use Spark/Dask for parallel work
@asset
def market_analysis():
    # Dagster orchestrates ONE job
    # Spark/Dask does 1500 parallel tasks
    spark.submit_job("analyze_markets.py")
    # Job internally parallelizes to 1500 tasks
```

**Key point for Polymarket:**
- Dagster orchestrates the JOB
- Spark/Dask/Ray parallelizes the WORK
- 1 Dagster asset = 1 distributed compute job = N parallel tasks

## Event-Driven Patterns

### Example: Trigger on New Data
```python
# sensors.py
@sensor(
    job_name="market_maker_rules",
    minimum_interval_seconds=60
)
def new_order_books_sensor(context):
    """
    Triggers market maker rule job when new order books arrive.
    Checks ClickHouse for new snapshots.
    """
    # Check ClickHouse for new data
    latest_snapshot = check_clickhouse_for_new_snapshots()

    if latest_snapshot:
        yield RunRequest(
            run_key=f"snapshot_{latest_snapshot}",
            tags={"snapshot_id": latest_snapshot}
        )
```

## Questions for Polymarket

1. **Dependency Management**: Do you prefer Option 1 (auto-install) for dev or Option 2 (Docker) for prod?

2. **Compute**: For Spark/Dask jobs, do you have existing clusters or need help setting those up?

3. **Multi-tenancy**: Option A (separate code locations) or Option B (single location)?

4. **Migration**: Do you have existing Prefect flows we can show running?

5. **Real-time**: Which jobs need event-driven triggers vs schedules?
