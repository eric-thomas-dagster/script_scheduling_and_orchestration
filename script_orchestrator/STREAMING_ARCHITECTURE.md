# Streaming + Batch Architecture for Polymarket Use Case

## The Challenge: 100ms Order Book Snapshots + Billions of Rows

**Requirements:**
- Order book snapshots every ~100ms
- Billions of rows accumulated
- Market maker rule enforcement
- Heavy aggregations and computations
- Reliability + observability

## Architecture: Observe Streaming, Orchestrate Batch

```
┌─────────────────────────────────────────────────────────────────┐
│ STREAMING LAYER (Not orchestrated by Dagster)                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Order Books  ──▶  Kafka/Kinesis  ──▶  Flink/Spark Streaming  │
│  (100ms freq)      (event stream)      (continuous processing)  │
│                                                                 │
│                           │                                     │
│                           ▼                                     │
│                    ClickHouse Table                            │
│                  (billions of snapshots)                        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                            │
                            │ Dagster OBSERVES via External Assets
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ DAGSTER ORCHESTRATION LAYER                                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. EXTERNAL ASSETS (Observe, don't execute)                   │
│     • order_book_stream (Kafka topic)                          │
│     • order_book_clickhouse (ClickHouse table)                 │
│     • spark_cluster (Compute infrastructure)                   │
│                                                                 │
│  2. SENSORS (Event-driven triggers)                            │
│     • order_book_data_sensor                                   │
│       - Checks ClickHouse every 30s                            │
│       - Threshold: 10M new snapshots OR 1 hour elapsed         │
│       - Triggers ─────────┐                                    │
│                           │                                     │
│  3. BATCH PROCESSING ◀────┘                                    │
│     • Spark Job: Market Maker Rules                            │
│       - Loads 10M-100M snapshots from ClickHouse              │
│       - Distributes across 20 Spark workers                    │
│       - Computes aggregations (parallel)                       │
│       - Applies rule logic (parallel)                          │
│       - Writes violations back to ClickHouse                   │
│       - Duration: 2-5 minutes                                  │
│                                                                 │
│     • Dask Job: Deep Analytics (optional)                      │
│       - Further analysis on aggregated results                 │
│       - ML model scoring                                       │
│       - Report generation                                      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Key Patterns

### 1. External Assets: Observe Streaming Infrastructure

```python
# external_assets.py
from dagster import external_asset, AssetSpec

order_book_stream = external_asset(
    AssetSpec(
        key="order_book_stream",
        description="Kafka stream with 100ms order book snapshots. "
                    "Dagster observes for lineage, doesn't execute.",
        metadata={
            "kafka_topic": "order-books-raw",
            "throughput": "10,000 msg/sec",
            "retention": "7 days"
        }
    )
)

order_book_clickhouse = external_asset(
    AssetSpec(
        key="order_book_clickhouse",
        deps=["order_book_stream"],  # Fed by stream
        description="ClickHouse table, billions of rows"
    )
)
```

**Benefits:**
- ✅ Lineage: See stream → ClickHouse → Spark job flow
- ✅ Monitoring: Track health of streaming pipeline
- ✅ Documentation: All systems visible in one place
- ✅ Governance: Data team sees full data flow

### 2. Sensors: Event-Driven Triggers

```python
# sensors/order_book_sensor.py
@sensor(
    name="order_book_data_sensor",
    minimum_interval_seconds=30  # Check every 30s
)
def order_book_data_sensor(context):
    # Query ClickHouse for new data
    cursor = context.cursor or get_last_processed_timestamp()

    query = f"""
        SELECT
            max(snapshot_timestamp) as latest,
            count(*) as new_rows
        FROM order_books
        WHERE snapshot_timestamp > '{cursor}'
    """

    result = clickhouse_client.execute(query)
    latest_ts, new_rows = result[0]

    # Threshold: 10M rows OR 1 hour elapsed
    if new_rows < 10_000_000 and time_since(cursor) < 3600:
        return SkipReason(f"Only {new_rows:,} new rows, waiting for threshold")

    # Trigger Spark job
    yield RunRequest(
        run_key=f"market_maker_{latest_ts}",
        asset_selection=[AssetKey("script_spark_job_example")],
        tags={
            "snapshot_count": str(new_rows),
            "data_timestamp": latest_ts
        }
    )

    # Update cursor
    context.update_cursor(latest_ts)
```

**Benefits:**
- ✅ Event-driven: React to data arrival, not clock time
- ✅ Batching: Process 10M rows at once (efficient)
- ✅ Backpressure: Don't overwhelm cluster
- ✅ Cursor: Track progress, avoid reprocessing

### 3. Distributed Compute: Spark for Heavy Lifting

```yaml
# spark_job_example.yaml
script_type: spark
description: "Market maker rule enforcement on 10M+ snapshots"

# Triggered by sensor, not scheduled
# sensor: order_book_data_sensor

depends_on:
  - order_book_clickhouse  # External asset dependency
```

```python
# spark_job_example.py (actual Spark code)
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MarketMakerRules") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .getOrCreate()

# Load from ClickHouse
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:clickhouse://ch-cluster:8123/markets") \
    .option("dbtable", "order_books") \
    .option("user", "spark") \
    .load()

# Filter to recent snapshots
recent = df.filter(df.snapshot_timestamp > last_processed)

# Compute aggregations (distributed across 20 workers)
aggregated = recent.groupBy("market_id", "snapshot_window") \
    .agg(
        sum("volume").alias("total_volume"),
        avg("spread").alias("avg_spread"),
        count("*").alias("snapshot_count")
    )

# Apply market maker rules (parallel processing)
violations = aggregated.filter(
    (aggregated.total_volume > threshold) &
    (aggregated.avg_spread < min_spread)
)

# Write violations back to ClickHouse
violations.write \
    .format("jdbc") \
    .option("url", "jdbc:clickhouse://ch-cluster:8123/markets") \
    .option("dbtable", "mm_violations") \
    .mode("append") \
    .save()
```

**Dagster's role:**
- ✅ Triggers: Sensor starts the job when data ready
- ✅ Monitors: Tracks job progress and logs
- ✅ Retries: Automatically retries on failure
- ✅ Metadata: Records row counts, execution time, etc.
- ✅ Lineage: Shows ClickHouse → Spark → Results flow

**Spark's role:**
- ✅ Execution: Runs the actual computation
- ✅ Parallelism: Distributes work across workers
- ✅ Optimization: Query optimization, caching, etc.

### 4. Priority Processing for Urgent Events

```python
@sensor(
    name="market_event_priority_sensor",
    minimum_interval_seconds=10  # Check more frequently
)
def market_event_priority_sensor(context):
    """
    Monitors for urgent market events requiring immediate processing.

    Examples:
    - Price anomalies (>10% deviation)
    - Large trades (>$1M)
    - Liquidity drops (<50% of normal)
    """

    # Check monitoring system/alert queue
    urgent_events = check_monitoring_alerts()

    if not urgent_events:
        return SkipReason("No urgent events")

    for event in urgent_events:
        yield RunRequest(
            run_key=f"urgent_{event.id}",
            tags={
                "priority": "urgent",
                "event_type": event.type
            }
        )
```

**Benefits:**
- ✅ Fast response: Check every 10s vs 30s
- ✅ Priority: Tagged as urgent for queue prioritization
- ✅ Separate logic: Doesn't wait for 10M row threshold

## NOT Low-Latency Real-Time

**Important:** This is **NOT** sub-second processing. This is:

❌ **NOT:** Process each 100ms snapshot individually
✅ **YES:** Batch process 10M snapshots every 5-10 minutes

❌ **NOT:** Real-time alerting (use Flink/Kafka Streams for that)
✅ **YES:** Event-driven batch processing with sensors

❌ **NOT:** Millisecond latency
✅ **YES:** Minute-to-minutes latency for heavy aggregations

## For True Real-Time (Not Dagster)

If you need **actual** sub-second latency:

```
Order Books ──▶ Kafka ──▶ Flink Streaming ──▶ Alerts
(100ms)         (stream)   (real-time rules)  (<1 second)

                                │
                                │ Dagster observes as external asset
                                ▼
                          Observability
```

**Flink/Kafka Streams for:**
- Real-time alerting (<1 second)
- Live dashboards
- Immediate rule violations

**Dagster for:**
- Heavy batch aggregations (billions of rows)
- Complex analytics (Spark/Dask)
- Report generation
- Data quality checks

## Answering Polymarket's Questions

### Q: "Can Dagster handle 1500 tasks?"
**A:** "Don't create 1500 Dagster assets. Create 1 asset that triggers 1 Spark job. Spark parallelizes the work across 1500 tasks internally. Dagster orchestrates the JOB, Spark parallelizes the WORK."

### Q: "How do we handle 100ms snapshots?"
**A:** "The streaming pipeline (Kafka + Flink) handles ingestion at 100ms. Dagster observes the stream as an external asset for lineage. Sensors monitor for batch thresholds (10M rows) and trigger Spark jobs for heavy compute."

### Q: "What about real-time?"
**A:** "Dagster is event-driven batch, not sub-second real-time. For real-time alerts, use Flink. Dagster orchestrates the heavy batch analytics that Flink can't handle efficiently."

### Q: "How do we avoid queuing?"
**A:** "Sensors with priority tags. Urgent events get a separate sensor (10s check interval) with priority:urgent tag. Use Dagster+ run queue configuration to prioritize urgent runs."

### Q: "How does this scale?"
**A:**
- Streaming: Kafka/Flink scales horizontally
- Storage: ClickHouse shards/replication
- Compute: Spark cluster scales (add more workers)
- Orchestration: Dagster+ (managed, no ops burden)

## Demo Talking Points

1. **"We observe your streaming pipeline"** - Show external assets
2. **"Sensors trigger when ready"** - Show sensor checking ClickHouse
3. **"Spark does the heavy lifting"** - Show Spark job with billions of rows
4. **"Not replacing Kafka/Flink"** - We orchestrate batch, not streaming
5. **"Single pane of glass"** - All systems visible: Stream → Storage → Compute → Results

## Production Checklist

- [ ] Streaming pipeline (Kafka/Flink) deployed
- [ ] ClickHouse configured with partitions
- [ ] Spark cluster provisioned (20+ workers)
- [ ] Dagster hybrid agent in VPC
- [ ] Sensors configured with correct thresholds
- [ ] External assets defined for lineage
- [ ] Priority sensor for urgent events
- [ ] Monitoring/alerts configured
- [ ] Run queue configuration (priority handling)
