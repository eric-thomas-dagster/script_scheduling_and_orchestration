# Distributed Compute Scripts

Heavy compute workloads using PySpark and Dask for processing large-scale data.

## Scripts

### spark_job_example.py
**Purpose:** Market maker rule enforcement on order book snapshots
**Script Type:** spark
**Triggered by:** Sensor (order_book_data_sensor)
**Scale:** Billions of rows

**What it does:**
- Processes order book snapshots at scale (demo: 100k rows)
- Applies market maker rules (spread, volume thresholds)
- Detects violations and flags markets
- Writes results back to ClickHouse (simulated)

**Uses actual PySpark:**
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[4]").getOrCreate()
df = spark.createDataFrame(data)
result = df.groupBy("market_id").agg(...)
```

**Demo vs Production:**
- Demo: local[4] with 100k rows
- Production: spark://cluster:7077 with billions of rows

**Output:**
```
Spark version: 4.1.1
Master: local[4]
Snapshots processed: 100,000
Markets analyzed: 50
Violations detected: varies
```

### dask_analysis_example.py
**Purpose:** Analytics and ML on aggregated market data
**Script Type:** dask
**Depends on:** extract_data
**Scale:** Parallel analytics across markets

**What it does:**
- Loads aggregated market statistics
- Computes cross-market correlations
- Runs anomaly detection (threshold-based)
- Generates compliance reports
- All distributed across Dask workers

**Uses actual Dask:**
```python
from dask.distributed import Client, LocalCluster
cluster = LocalCluster(n_workers=4)
client = Client(cluster)
ddf = dd.from_pandas(data, npartitions=16)
result = ddf.groupby('market_id').agg(...).compute()
```

**Demo vs Production:**
- Demo: LocalCluster with 4 workers, 100k rows
- Production: Remote cluster with 12+ workers, millions of rows

**Output:**
```
Dashboard: http://127.0.0.1:8787/status
Workers: 4
Records: 100,000
Markets: 100
Anomalies detected: varies
```

## Architecture Pattern

These scripts demonstrate **Pattern 1: External Orchestration**

```
Dagster Asset
    │
    ├─ subprocess.run(["python", "spark_job.py"])
    │  └─ PySpark session connects to cluster
    │
    └─ subprocess.run(["python", "dask_script.py"])
       └─ Dask client connects to scheduler
```

Dagster orchestrates, Spark/Dask execute.

## Polymarket Use Case

### Spark Job: Market Maker Rules
Triggered by sensor when ClickHouse accumulates 10M+ order book snapshots:

1. **Extract:** Read 10M snapshots from ClickHouse (100ms intervals, 1 hour)
2. **Transform:** Compute spreads, aggregations by market
3. **Apply Rules:** Check spread thresholds, volume requirements
4. **Load:** Write violations to ClickHouse for alerting

### Dask Analysis: Deep Analytics
Runs after Spark aggregation completes:

1. **Load:** Aggregated market statistics from Spark
2. **Correlate:** Compute cross-market price correlations
3. **Detect:** ML-based anomaly detection
4. **Report:** Generate compliance and risk reports

## Running Locally

```bash
# Spark job (direct)
python distributed_compute/spark_job_example.py
# Shows Spark stages, processes 100k rows

# Dask analysis (direct)
python distributed_compute/dask_analysis_example.py
# Starts local cluster, opens dashboard at :8787

# Or via Dagster
uv run dg dev
# Materialize script_spark_job_example
# Materialize script_dask_analysis_example
```

## Requirements

Already installed:
- ✓ PySpark 4.1.1
- ✓ Dask 2026.1.2
- ✓ NumPy, Pandas, PyArrow

## Configuration

### Spark (for production):
Change `local[4]` to cluster master:
```python
spark = SparkSession.builder \
    .master("spark://production-spark:7077") \
    .config("spark.executor.instances", "20") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()
```

### Dask (for production):
Change LocalCluster to remote:
```python
from dask.distributed import Client
client = Client("tcp://production-dask:8786")
```

## Use Case

Perfect for demonstrating:
- ✓ Processing billions of rows with Spark
- ✓ Distributed Python analytics with Dask
- ✓ Event-driven batch processing
- ✓ Market maker rule enforcement
- ✓ ML/analytics on aggregated data
- ✓ Real framework usage (not simulation)
