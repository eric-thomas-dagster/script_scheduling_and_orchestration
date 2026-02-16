# Executor Patterns Demo - Complete Setup ✓

## What's Loaded

```
✓ 11 Assets
✓ 2 Sensors
✓ 1 Job
```

## Two Executor Patterns Demonstrated

### Pattern 1: Orchestrate External Jobs (What You Had)

**Assets:**
1. `script_extract_data` - Python script
2. `script_transform_data` - Python script
3. `script_generate_report` - Python script
4. `script_prefect_flow_example` - External Prefect flow
5. `script_spark_job_example` - External PySpark job
6. `script_dask_analysis_example` - External Dask script

**How it works:**
- Dagster runs as orchestrator
- Assets call subprocess to run external jobs
- Jobs run on their own clusters (Spark cluster, Dask cluster, etc.)
- Good for: Existing jobs, separate clusters, complex workflows

### Pattern 2: Native Executor (NEW!)

**Assets:**
7. `native_dask_computation` - Runs ON Dask workers via dask_executor
8. `native_dask_heavy_compute` - Heavy compute distributed to Dask workers

**Jobs:**
9. `dask_executor_job` - Job explicitly using dask_executor

**How it works:**
- Dagster distributes ops directly to Dask workers
- No subprocess, native execution
- Ops run ON the Dask cluster
- Good for: Python-native code, simpler deployment, unified cluster

### External Assets (Observability)

10. `order_book_stream` - External Kafka/Flink stream
11. `order_book_clickhouse` - External ClickHouse table
12. `spark_cluster` - External Spark infrastructure

### Sensors (Event-Driven)

1. `order_book_data_sensor` - Monitors ClickHouse, triggers at threshold
2. `market_event_priority_sensor` - High-priority urgent events

## Key Differences

### Pattern 1: External Orchestration
```python
@asset
def script_spark_job_example():
    # Dagster orchestrates
    result = subprocess.run(["spark-submit", "job.py"])
    # Job runs on Spark cluster
    return result
```

**Pros:**
- Use existing Spark/Dask/Prefect jobs
- Separate clusters for different workloads
- Framework-specific features available
- Clear separation of concerns

**Cons:**
- Process spawn overhead
- Need to manage multiple clusters
- More complex deployment

### Pattern 2: Native Executor
```python
from dagster_dask import dask_executor

@asset(compute_kind="dask")
def native_dask_computation():
    # THIS CODE runs on Dask workers
    process_data()  # Distributed by Dagster
    return result

@job(executor_def=dask_executor)
def my_job():
    op1()  # Runs on Dask worker
    op2()  # Runs on Dask worker
```

**Pros:**
- No subprocess overhead
- Native Dagster integration
- One unified cluster
- Fine-grained op-level parallelism

**Cons:**
- Python-only (no external frameworks)
- Less framework-specific features
- Tighter coupling

## Available Executors

Dagster supports many executors (we demo Dask):

- ✅ **In-process** (default)
- ✅ **Multi-process** (local parallelism)
- ✅ **Dask** (distributed Python) ← **WE DEMO THIS**
- ✅ **Docker** (each op in container)
- ✅ **Kubernetes** (each op in pod, Dagster+)
- ✅ **Celery** (task queue distribution)

## For Polymarket Use Case

### Recommended Architecture

**Use Pattern 1 for:**
- Heavy Spark batch jobs (billions of order book snapshots)
- Processing 12M snapshots from ClickHouse
- Market maker rule enforcement at scale
- Writing violations back to ClickHouse

**Use Pattern 2 for:**
- Python analytics on aggregated results
- ML model scoring and anomaly detection
- Report generation and compliance checks
- Cross-market correlation analysis

**Example Pipeline:**
```
External Kafka Stream (observed)
    ↓
External ClickHouse (observed)
    ↓
Sensor triggers when 10M+ rows
    ↓
Pattern 1: Spark job (heavy compute)
    ├─ Load 10M snapshots
    ├─ Distribute across 20 executors
    ├─ Apply market maker rules
    └─ Write violations
        ↓
Pattern 2: Dask executor (analytics)
    ├─ Load aggregated results
    ├─ ML anomaly detection
    ├─ Cross-market correlations
    └─ Generate reports
```

## Testing the Demo

### Start Dagster
```bash
cd script_orchestrator
uv run dg dev
```

### Test Pattern 1 (External Jobs)
1. Materialize `script_spark_job_example`
   - ✓ See actual Spark stages execute
   - ✓ Shows Spark version, master, workers
   - ✓ Processes 100k snapshots with real DataFrames
   - ✓ Applies market maker rules with filtering

2. Materialize `script_dask_analysis_example`
   - ✓ Starts Dask LocalCluster with 4 workers
   - ✓ Shows dashboard link: http://localhost:8787
   - ✓ Processes 100k records with Dask DataFrames
   - ✓ Computes correlations and anomalies

### Test Pattern 2 (Native Executor)
1. Materialize `native_dask_computation`
   - ✓ Code runs ON Dask workers
   - ✓ "Running on Dask worker" in logs
   - ✓ Demonstrates native distribution

2. Materialize `native_dask_heavy_compute`
   - ✓ Heavier computation distributed
   - ✓ 50 markets processed on workers
   - ✓ Shows statistics from distributed work

3. Run job `dask_executor_job`
   - ✓ Job explicitly configured with dask_executor
   - ✓ All ops run on Dask cluster
   - ✓ Shows Dask-native execution

## Real Frameworks Installed

```
✓ PySpark 4.1.1 - Actual Spark with local[4] cluster
✓ Dask 2026.1.2 - Actual Dask with distributed scheduler
✓ dagster-dask 0.28.14 - Native Dask executor
✓ NumPy, Pandas, PyArrow - Data processing
```

## Demo Talking Points

### For Prefect Migration Story
"You can bring your existing Prefect flows as-is using Pattern 1. We orchestrate them via subprocess, just like your current setup. No code changes required. Then gradually migrate to native Dagster assets as you modernize."

### For Spark at Scale
"For your billions of order book snapshots, we use Pattern 1: Dagster orchestrates Spark jobs on your existing Spark cluster. Sensor monitors ClickHouse every 30 seconds, triggers when 10M rows accumulated. Spark distributes work across 20 executors, processes in 3-5 minutes."

### For Python Analytics
"For analytics, ML, and reporting after Spark aggregation, we use Pattern 2: Native Dask executor. Your Python ops run distributed across Dask workers. No subprocess overhead, unified cluster, fine-grained parallelism. Perfect for feature engineering and model scoring."

### For Executor Flexibility
"Dagster supports 6+ executors. Today we show Dask, but you could use Docker (each op in container), Kubernetes (each op in pod), or Celery (task queue). Choose based on your infrastructure and use case. Mix and match in same pipeline."

### For Event-Driven Architecture
"We observe your streaming infrastructure (Kafka, ClickHouse) as external assets. Sensors check for thresholds. When 10M snapshots ready, automatically triggers batch processing. Event-driven, not just time-based. Smart batching for efficiency."

## Files Created

- `EXECUTOR_PATTERNS.md` - Comprehensive guide to both patterns
- `REAL_SPARK_DASK.md` - Details on actual frameworks vs simulation
- `defs/dask_executor_example/defs.py` - Pattern 2 example assets
- This file - Quick reference for demo

## Next Steps

1. Open Dagster UI: `uv run dg dev`
2. Explore both patterns in Assets page
3. Materialize assets to see execution
4. Check logs for Spark stages and Dask workers
5. Open Dask dashboard link from logs
6. Show sensors in automation tab
7. Explain external assets in lineage view

Perfect for technical demos showing Dagster's flexibility!
