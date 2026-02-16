# Using Real Spark and Dask ✓

## Installed Frameworks

Successfully installed actual distributed compute frameworks:

```bash
✓ PySpark 4.1.1
✓ Dask 2026.1.2 with distributed scheduler
✓ NumPy 2.4.2
✓ Pandas 3.0.0
✓ PyArrow 23.0.0
```

## Spark Job (spark_job_example.py)

### Uses Actual PySpark APIs

- **SparkSession** with local[4] master (4 cores)
- **Spark DataFrames** for structured data processing
- **Aggregations**: groupBy, agg, count, avg, sum
- **Filtering**: Complex conditions with multiple rules
- **Transformations**: withColumn for spread calculations
- **Real Spark operations**: Stage execution, shuffle partitions

### Demo Output
```
Spark version: 4.1.1
Master: local[4]
App name: Polymarket-MarketMakerRules
Partitions: 8

Snapshots loaded: 100,000
Markets analyzed: 50
Violations detected: 0
Total snapshots processed: 100,000
```

### What's Real vs Simulated

**Real:**
- ✅ Actual Spark cluster (local mode)
- ✅ Real DataFrame operations and transformations
- ✅ Real parallel execution across 4 cores
- ✅ Real Spark SQL engine optimizations
- ✅ Real stage/task execution visible in logs

**Simulated:**
- 📝 Data generation (in prod: read from ClickHouse)
- 📝 Writing results (in prod: write to ClickHouse)
- 📝 Using local[4] instead of production cluster

## Dask Analysis (dask_analysis_example.py)

### Uses Actual Dask APIs

- **LocalCluster** with 4 workers, 2 threads each
- **Dask DataFrames** for parallel processing
- **Distributed Client** connecting to scheduler
- **Aggregations**: groupby with multiple functions
- **Pivot tables** and correlation analysis
- **Real dashboard** at http://127.0.0.1:8787

### Demo Output
```
Dashboard: http://127.0.0.1:8787/status
Workers: 4
Total threads: 8
Scheduler: tcp://127.0.0.1:51798

Records generated: 100,000
Markets: 100
Partitions: 16
Statistics computed for 100 markets
Anomalies detected: [varies]
```

### What's Real vs Simulated

**Real:**
- ✅ Actual Dask LocalCluster with 4 workers
- ✅ Real distributed scheduler and workers
- ✅ Real Dask DataFrames with lazy evaluation
- ✅ Real parallel task execution across workers
- ✅ Real dashboard for monitoring
- ✅ Real correlation and pivot operations

**Simulated:**
- 📝 Data generation (in prod: load from S3/ClickHouse)
- 📝 Writing results (in prod: write to S3/ClickHouse)
- 📝 Using LocalCluster instead of production cluster

## Why This Matters for Demos

### Before (Simulated)
```python
# Just printed text
print("Starting Spark job...")
print("Processing 100k rows...")
print("Done!")
```

### After (Real Frameworks)
```python
# Actual Spark/Dask operations
spark = SparkSession.builder.master("local[4]").getOrCreate()
df = spark.createDataFrame(data)
result = df.groupBy("market_id").agg(avg("spread"))
```

**Demo Impact:**
- ✅ Show real Spark SQL execution plans
- ✅ Show real Dask dashboard with live worker stats
- ✅ Show actual parallel processing across cores
- ✅ Show real framework logs and stage execution
- ✅ Demonstrate actual API usage patterns
- ✅ Much more credible for technical audiences

## Production Deployment

For production with actual clusters:

### Spark
```python
# Change from local[4] to cluster master
spark = SparkSession.builder \
    .master("spark://production-spark:7077") \
    .config("spark.executor.instances", "20") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()

# Use actual ClickHouse connector
df = spark.read.format("jdbc") \
    .option("url", "jdbc:clickhouse://...") \
    .load()
```

### Dask
```python
# Change from LocalCluster to remote cluster
from dask.distributed import Client
client = Client("tcp://production-dask:8786")

# Use actual data sources
df = dd.read_parquet("s3://bucket/data/*.parquet")
```

## Testing

Both scripts tested and working:

```bash
# Spark
uv run python example_scripts/spark_job_example.py
✓ Completes in ~10 seconds
✓ Processes 100k rows across 4 cores
✓ Shows Spark stage execution

# Dask
uv run python example_scripts/dask_analysis_example.py
✓ Completes in ~5 seconds
✓ Starts 4 workers with scheduler
✓ Dashboard available at http://localhost:8787
✓ Processes 100k rows with 16 partitions
```

## Benefits for Polymarket Demo

1. **Credibility**: Shows we actually understand Spark/Dask, not just talking about them
2. **Technical Depth**: Can show actual DataFrames, execution plans, task graphs
3. **Interactive**: Can open Dask dashboard during demo
4. **Realistic**: Demonstrates actual patterns they'd use in production
5. **Scalability Story**: "This runs locally with 4 cores, in prod you'd use 20+ executors"

## Next Steps

- Run `uv run dg dev` and materialize the Spark/Dask assets
- Show Dask dashboard link in materialization logs
- Point out Spark stage execution in logs
- Emphasize: "Same code, different cluster config for production"
