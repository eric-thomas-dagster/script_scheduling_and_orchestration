# Dagster Executor Patterns: Two Ways to Use Distributed Compute

## The Two Patterns

Dagster supports **two distinct patterns** for distributed compute. This demo shows both!

```
┌─────────────────────────────────────────────────────────────────┐
│ PATTERN 1: Orchestrate External Jobs (script_spark_job_example) │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   Dagster Asset                                                 │
│        │                                                         │
│        ├──▶ subprocess.run(["spark-submit", "job.py"])         │
│        │    Job runs on external Spark cluster                 │
│        │                                                         │
│        ├──▶ subprocess.run(["python", "dask_script.py"])       │
│        │    Script connects to external Dask cluster           │
│        │                                                         │
│        └──▶ subprocess.run(["prefect", "deployment", "run"])   │
│             Prefect flow runs on Prefect infrastructure        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ PATTERN 2: Native Execution (native_dask_computation)           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   Dagster Asset (with dask_executor)                           │
│        │                                                         │
│        ├──▶ @op runs on Dask Worker 1                          │
│        ├──▶ @op runs on Dask Worker 2                          │
│        └──▶ @op runs on Dask Worker 3                          │
│                                                                 │
│   Dagster distributes ops directly to Dask workers             │
│   No subprocess, native execution                               │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Pattern 1: Orchestrate External Jobs

**What we have now:**
- `script_spark_job_example` - Runs Spark via subprocess
- `script_dask_analysis_example` - Runs Dask via subprocess
- `script_prefect_flow_example` - Runs Prefect via subprocess

**How it works:**
```python
@asset
def script_spark_job_example(context):
    # Dagster runs this as orchestrator
    result = subprocess.run([
        "spark-submit",
        "--master", "spark://cluster:7077",
        "spark_job.py"
    ])
    # Job runs on external Spark cluster
    return result
```

**When to use:**
- ✅ You have existing Spark/Dask/Prefect jobs
- ✅ Jobs are complex with many files/dependencies
- ✅ Want to use separate compute clusters
- ✅ Jobs need specific execution environments
- ✅ Migrating from other orchestrators (Airflow, Prefect)

**Advantages:**
- Use existing jobs with minimal changes
- Separate clusters for different workload types
- Jobs can use framework-specific features (Spark UI, Dask dashboard)
- Clear separation of orchestration vs execution

## Pattern 2: Native Executor (NEW)

**What we just added:**
- `native_dask_computation` - Asset runs ON Dask workers
- `native_dask_heavy_compute` - Ops distributed to Dask
- `dask_executor_job` - Job with explicit dask_executor

**How it works:**
```python
from dagster_dask import dask_executor

@asset(compute_kind="dask")
def native_dask_computation(context):
    # THIS CODE runs on Dask workers
    # Dagster distributes it natively
    for chunk in data:
        process(chunk)  # Runs on Dask worker
    return result

@job(executor_def=dask_executor.configured({
    "cluster": {"local": {"n_workers": 4}}
}))
def my_job():
    # All ops in this job run on Dask cluster
    op1()
    op2()
```

**When to use:**
- ✅ Writing new Python-native workloads
- ✅ Want simpler deployment (one cluster)
- ✅ Need fine-grained distribution of ops
- ✅ Python code without external dependencies
- ✅ Want Dagster to manage parallelism

**Advantages:**
- No subprocess overhead
- Native Dagster integration
- Fine-grained parallelism (op-level)
- Simpler deployment model
- Unified compute cluster

## Available Executors in Dagster

Our demo shows **Dask executor**, but Dagster supports many:

### 1. In-Process Executor (Default)
```python
# No configuration needed
@asset
def my_asset():
    return "runs in same process"
```

### 2. Multi-Process Executor
```python
from dagster import multiprocess_executor

@job(executor_def=multiprocess_executor)
def my_job():
    # Ops run in separate processes on same machine
    op1()
    op2()
```

### 3. Dask Executor (✓ In our demo)
```python
from dagster_dask import dask_executor

@job(executor_def=dask_executor.configured({
    "cluster": {"local": {"n_workers": 4}}
}))
def my_job():
    # Ops run on Dask cluster workers
    op1()
    op2()
```

### 4. Docker Executor
```python
from dagster_docker import docker_executor

@job(executor_def=docker_executor)
def my_job():
    # Each op runs in its own Docker container
    op1()  # Container 1
    op2()  # Container 2
```

### 5. Kubernetes Executor (Dagster+)
```python
from dagster_k8s import k8s_job_executor

@job(executor_def=k8s_job_executor)
def my_job():
    # Each op runs in its own K8s pod
    op1()  # Pod 1
    op2()  # Pod 2
```

### 6. Celery Executor
```python
from dagster_celery import celery_executor

@job(executor_def=celery_executor)
def my_job():
    # Ops distributed via Celery task queue
    op1()
    op2()
```

## Polymarket Use Case: Which Pattern?

### For Order Book Processing

**Pattern 1 (External Spark):**
```python
# Best for: Massive batch processing, billions of rows
@asset
def market_maker_rules():
    subprocess.run([
        "spark-submit",
        "--master", "spark://cluster:7077",
        "--executor-memory", "8g",
        "--num-executors", "20",
        "market_maker_rules.py"
    ])
    # Spark job processes 12M snapshots
    # Distributed across 20 executors
    # Writes violations to ClickHouse
```

**Pattern 2 (Native Dask Executor):**
```python
# Best for: Python analytics, ML scoring, reports
@asset(compute_kind="dask")
def market_analytics(context):
    # Load aggregated data
    # Run ML models
    # Generate reports
    # All distributed across Dask workers
```

**Recommendation:**
- Use **Pattern 1** for heavy Spark jobs (billions of rows)
- Use **Pattern 2** for Python analytics, ML, reporting
- Use **both** in same pipeline for different stages

## Demo Assets

### Pattern 1: External Orchestration
- `script_spark_job_example` - Orchestrates PySpark job
- `script_dask_analysis_example` - Orchestrates Dask script
- `script_prefect_flow_example` - Orchestrates Prefect flow

### Pattern 2: Native Execution
- `native_dask_computation` - Runs ON Dask workers
- `native_dask_heavy_compute` - Heavy compute on Dask
- `dask_executor_job` - Job using dask_executor

## Configuration

### Dask Executor Config

**Local Cluster (Demo):**
```python
dask_executor.configured({
    "cluster": {
        "local": {
            "n_workers": 4,
            "threads_per_worker": 2,
            "memory_limit": "1GB"
        }
    }
})
```

**Remote Cluster (Production):**
```python
dask_executor.configured({
    "cluster": {
        "existing": {
            "address": "tcp://dask-scheduler:8786"
        }
    }
})
```

**Kubernetes Dask Cluster:**
```python
dask_executor.configured({
    "cluster": {
        "dask_kubernetes": {
            "n_workers": 10,
            "resources": {
                "CPU": 2,
                "memory": "4Gi"
            }
        }
    }
})
```

## Testing

```bash
# Start Dagster
cd script_orchestrator
uv run dg dev
```

**Test Pattern 1 (External):**
1. Materialize `script_spark_job_example`
2. See Spark logs with stage execution
3. Materialize `script_dask_analysis_example`
4. See Dask dashboard link

**Test Pattern 2 (Native):**
1. Materialize `native_dask_computation`
2. See "Running on Dask worker" in logs
3. Materialize `native_dask_heavy_compute`
4. See ops distributed across workers
5. Run job `dask_executor_job`
6. See job using dask_executor explicitly

## Key Differences

| Aspect | Pattern 1: External | Pattern 2: Native |
|--------|-------------------|------------------|
| **Execution** | subprocess.run() | Direct on executor |
| **Distribution** | Framework handles | Dagster handles |
| **Overhead** | Process spawn | No process spawn |
| **Complexity** | Higher (2 systems) | Lower (1 system) |
| **Use case** | Existing jobs | New Python code |
| **Flexibility** | Framework-specific | Dagster-native |
| **Deployment** | Multiple clusters | One cluster |
| **Monitoring** | Framework + Dagster | Dagster only |

## Production Recommendations

### Use Pattern 1 when:
- Migrating existing Spark/Dask/Prefect jobs
- Jobs require framework-specific features
- Need separate clusters for isolation
- Jobs are large, complex, multi-file
- Want to preserve existing monitoring/tooling

### Use Pattern 2 when:
- Writing new Dagster-native code
- Want simpler deployment (one cluster)
- Need fine-grained op-level parallelism
- Pure Python workloads
- Want unified monitoring in Dagster

### Best Practice: Use Both!
```python
# Heavy batch processing: Pattern 1 (External Spark)
@asset
def process_billions_of_snapshots():
    subprocess.run(["spark-submit", "heavy_job.py"])

# Analytics: Pattern 2 (Native Dask)
@asset(deps=[process_billions_of_snapshots], compute_kind="dask")
def analyze_results(context):
    # Runs on Dask executor
    load_results()
    run_ml_models()
    generate_reports()

# Best of both worlds!
```

## Reference

- Dagster Executors: https://docs.dagster.io/guides/operate/run-executors
- Dask Executor: https://docs.dagster.io/_apidocs/libraries/dagster-dask
- Docker Executor: https://docs.dagster.io/_apidocs/libraries/dagster-docker
- K8s Executor: https://docs.dagster.io/_apidocs/libraries/dagster-k8s
