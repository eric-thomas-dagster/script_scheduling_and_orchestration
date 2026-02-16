# Polymarket Demo - Script Orchestration

## Demo Focus

Show how Polymarket can **immediately** start using Dagster to orchestrate their existing workloads without rewrites.

## Key Messages

1. ✅ **Keep your existing code** - Python scripts, Prefect flows work as-is
2. ✅ **Simple YAML config** - Schedule and dependencies via YAML
3. ✅ **Distributed compute ready** - Orchestrate Spark/Dask/Ray jobs
4. ✅ **Event-driven** - Sensors trigger on new data, not just schedules
5. ✅ **Production ready** - Hybrid deployment in your VPC

## What We're NOT Showing (But Can Speak To)

- ❌ Real-time/low latency (not Dagster's strength - we're event-driven batch)
- ❌ Multi-tenancy demo (you'll explain separate code locations)
- ❌ Hybrid deployment details (you'll explain agent in VPC)

## Demo Flow (15 min)

### 1. The Problem (2 min)
"You have workloads in multiple places - scripts, Prefect flows, Spark jobs, analysts' code. You need orchestration NOW, not after a 6-month migration."

### 2. Pattern 1: Simple Script Migration (3 min)
**Show:** `example_scripts/extract_data.py`
- It's just Python
- No Dagster imports
- Runs as-is

**Show:** `example_scripts/extract_data.yaml`
```yaml
schedule:
  cron_schedule: "0 2 * * *"
depends_on: []
```

**Show:** Dagster UI
- Assets auto-discovered
- Lineage graph
- Execute and show logs

### 3. Pattern 2: Prefect Flows (3 min)
**Show:** `example_scripts/prefect_flow_example.py`
```python
@flow(name="data-processing-flow")
def data_processing_flow():
    data = fetch_data()
    results = process_data(data)
    return save_results(results)
```

**Key point:** "Your existing Prefect code works. Dagster orchestrates it."

**Show:** YAML
```yaml
script_type: prefect
```

### 4. Pattern 3: Distributed Compute (4 min)
**Show:** `example_scripts/spark_job_example.yaml`
```yaml
script_type: spark
depends_on:
  - extract_data
```

**Explain:**
- "For your market maker rules use case with billions of rows..."
- "Dagster orchestrates the job, Spark parallelizes the work"
- "1 Dagster asset = 1 Spark job = 1500 parallel tasks"
- "Not 1500 Dagster assets"

**Show:** Dask example
```yaml
script_type: dask
# Dask for Python-native parallel compute
```

### 5. Event-Driven Patterns (2 min)
**Explain (don't demo):**
```python
@sensor
def new_order_books_sensor():
    # Check ClickHouse for new snapshots
    if new_data:
        yield RunRequest()
```

"Triggers jobs when data arrives, not on a schedule. Perfect for your order book snapshots."

### 6. Production Deployment (1 min)
**Show:** `DEPLOYMENT.md` (don't read, just reference)

**Key points:**
- "Hybrid deployment - agent in your VPC"
- "Scripts cloned from your private GitHub"
- "Connects to your ClickHouse, MongoDB, Spark cluster"
- "Only metadata goes to Dagster Cloud"
- "Dependencies baked into Docker image"

## Questions to Address

### "How do we handle 1500+ tasks?"
**Answer:** "You don't create 1500 Dagster assets. Create 1 asset that submits to Spark/Dask. They handle the parallelism."

### "Can we run our Prefect code?"
**Answer:** "Yes, as-is. Just add `script_type: prefect` in YAML. We orchestrate it."

### "What about dependencies?"
**Answer:** "Two options: (1) Auto-install at runtime for dev, (2) Bake into Docker for prod. See DEPLOYMENT.md"

### "How do we separate DeFi and US workloads?"
**Answer:** "Separate code locations. Complete isolation, different IAM, different networks. Single Dagster+ contract."

### "What about security?"
**Answer:** "Hybrid deployment - everything runs in your VPC. Dagster agent is in your network. Only metadata goes to cloud."

### "Do we need to rewrite everything?"
**Answer:** "No. This demo shows scripts running as-is. Migrate incrementally. Some teams stay at this level forever."

### "What about real-time?"
**Answer:** "Dagster is event-driven batch, not low-latency streaming. For 100ms order book snapshots, we'd trigger jobs on data arrival via sensors, but processing is batch. For true real-time, you'd use Flink/Kafka and observe it as external assets."

## Demo Commands

```bash
# Start demo
cd script_orchestrator
uv run dg dev

# Or with venv
source venv/bin/activate
dg dev

# Opens at http://localhost:3000
```

## What to Show in UI

1. **Assets tab** - Show all discovered scripts
2. **Lineage graph** - Show dependency chains
3. **Asset details** - Show metadata (schedule, owners, etc.)
4. **Run history** - Show execution logs, timing
5. **Schedules** - Show all schedules
6. **Code location** - Show defs.yaml structure

## What to Have Open

1. Browser: Dagster UI
2. Editor: `example_scripts/` folder
3. Editor: `example_scripts/extract_data.yaml`
4. Terminal: Running `uv run dg dev`
5. PDF: `DEPLOYMENT.md` (for reference)

## Key Differentiators from Prefect

1. **Asset-centric** - Data artifacts, not just tasks
2. **Lineage built-in** - No extra setup
3. **Single pane of glass** - Scripts + Spark + dbt + everything
4. **Type-aware** - Knows what's a Python script vs Spark job
5. **Production-ready UI** - Better for ops teams
6. **Hybrid by default** - Cloud control, local execution

## Closing

"You can start using this TODAY. Clone your scripts repo, add YAML files, deploy. Then gradually adopt more Dagster features - asset checks, partitions, dbt integration. But you're productive immediately."

## Follow-Up Materials

- This repo (scripts + Prefect + Spark + Dask examples)
- DEPLOYMENT.md (production setup)
- Architecture diagram (separate code locations)
- Pricing discussion (offline)
