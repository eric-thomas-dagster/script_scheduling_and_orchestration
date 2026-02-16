# Orchestrator Migration Scripts

Examples showing how to migrate from other orchestrators (Prefect, Airflow) to Dagster.

## Migration Strategy

**Phase 1: Lift and Shift**
- Run existing orchestrator code via Dagster
- Dagster orchestrates, original framework executes
- Zero code changes to flows/DAGs
- Gain Dagster visibility and scheduling

**Phase 2: Gradual Modernization**
- Convert high-value flows to native Dagster
- Keep complex/stable flows in original framework
- Hybrid approach during transition

**Phase 3: Full Native**
- All workflows as Dagster assets
- Unified platform
- Maximum value from Dagster features

## Scripts

### prefect_flow_example.py
**Purpose:** Existing Prefect flow orchestrated by Dagster
**Script Type:** prefect
**Migration Phase:** Phase 1 (Lift and Shift)

**What it does:**
- Demonstrates running Prefect flow via Dagster
- Works with/without Prefect installed (mock decorators)
- Shows migration path for Prefect users

**Code structure:**
```python
# Mock decorators work without Prefect installed
try:
    from prefect import flow, task
except ImportError:
    # Demo mode - mock decorators
    def flow(name=None):
        def decorator(func):
            return func
        return decorator

@flow(name="data_processing_flow")
def data_processing_flow():
    # Your existing Prefect flow code
    data = extract()
    transformed = transform(data)
    load(transformed)
```

**How Dagster runs it:**
```python
# Component executes as:
subprocess.run(["python", "prefect_flow_example.py"])

# In production with Prefect:
subprocess.run([
    "prefect", "deployment", "run",
    "data_processing_flow/production"
])
```

**Demo vs Production:**
- Demo: Runs as Python script with mock decorators
- Production: Actually calls `prefect deployment run`

## Migration Paths

### From Prefect

**Current state:**
```python
# Prefect deployment
@flow(name="my_flow")
def my_flow():
    extract()
    transform()
    load()

# Scheduled via Prefect
```

**Phase 1: Dagster orchestrates**
```yaml
# prefect_flow.yaml
enabled: true
script_type: prefect
schedule:
  cron_schedule: "0 9 * * *"
```

```python
# Dagster asset
@asset
def script_prefect_flow():
    subprocess.run(["prefect", "deployment", "run", ...])
    # Prefect flow runs, Dagster tracks
```

**Phase 2: Hybrid**
```python
# Convert simple flows to Dagster
@asset
def extract_data():
    # Native Dagster
    return data

# Keep complex flows in Prefect
@asset
def complex_prefect_workflow():
    subprocess.run(["prefect", ...])
```

**Phase 3: Full Native**
```python
# All native Dagster
@asset
def extract_data():
    return data

@asset
def transform_data(extract_data):
    return transform(extract_data)

@asset
def load_data(transform_data):
    load(transform_data)
```

### From Airflow

Similar pattern works for Airflow:

**Phase 1:**
```yaml
# airflow_dag.yaml
enabled: true
script_type: python
description: "Existing Airflow DAG"
```

```python
@asset
def script_airflow_dag():
    subprocess.run([
        "python", "/path/to/airflow/dags/my_dag.py"
    ])
```

**Or trigger via API:**
```python
@asset
def trigger_airflow_dag():
    import requests
    requests.post(
        "http://airflow:8080/api/v1/dags/my_dag/dagRuns",
        json={"conf": {}}
    )
```

## Why This Works

### Advantages
- ✅ **Zero changes** to existing Prefect/Airflow code
- ✅ **Keep expertise** - team knows Prefect/Airflow
- ✅ **Gain visibility** - Dagster tracks everything
- ✅ **Better scheduling** - Dagster sensors, partitions
- ✅ **Gradual migration** - no big bang rewrite
- ✅ **Risk mitigation** - keep working flows running

### What You Get
- Dagster UI for all workflows
- Unified observability
- Better dependency management
- Asset-based thinking
- Event-driven triggers (sensors)
- Partitioning and backfills
- Dagster+ features (if using)

## Running Example

```bash
# Direct execution (demo mode)
python orchestrator_migration/prefect_flow_example.py

# Via Dagster
uv run dg dev
# Materialize script_prefect_flow_example in UI

# With actual Prefect (production):
# 1. Deploy Prefect flow: prefect deploy
# 2. Update component to call: prefect deployment run
# 3. Dagster orchestrates, Prefect executes
```

## Real-World Example

**Polymarket migration scenario:**

```
Before (Prefect):
- 50 Prefect flows
- Complex dependencies
- Scheduled via Prefect
- Works but limited observability

Phase 1 (Lift and Shift):
- Wrap all 50 flows as Dagster assets
- Dagster schedules, Prefect executes
- Gain Dagster visibility
- Zero code changes
- 2-4 weeks

Phase 2 (Hybrid):
- Convert 10 simple flows to native Dagster
- Keep 40 complex flows in Prefect
- Best of both worlds
- 2-3 months

Phase 3 (Native):
- All flows native Dagster
- Full platform capabilities
- Team trained on Dagster
- 6-12 months
```

## Adding Your Flows

### For Prefect
```bash
# 1. Copy your flow
cp /path/to/my_flow.py orchestrator_migration/

# 2. Create YAML
cat > orchestrator_migration/my_flow.yaml << 'EOF'
enabled: true
script_type: prefect
description: "My Prefect flow"
schedule:
  cron_schedule: "0 9 * * *"
EOF

# 3. Dagster auto-discovers
uv run dg dev
```

### For Airflow
```bash
# 1. Copy or reference DAG
cp /path/to/my_dag.py orchestrator_migration/

# 2. Create YAML
cat > orchestrator_migration/my_dag.yaml << 'EOF'
enabled: true
script_type: python
description: "Airflow DAG wrapped"
EOF

# 3. Update script to trigger via API or run directly
```

## Use Case

Perfect for demonstrating:
- ✓ Migration from Prefect/Airflow
- ✓ Zero-downtime transition
- ✓ Gradual modernization strategy
- ✓ Hybrid orchestration
- ✓ Risk-free adoption path
- ✓ Preserving existing investments

## See Also

- `../../POLYMARKET_DEMO.md` - Full demo script
- `../../DEPLOYMENT.md` - Production deployment
- Dagster docs: https://docs.dagster.io/integrations
