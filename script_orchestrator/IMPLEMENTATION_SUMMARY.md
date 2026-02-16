# Implementation Summary: Streaming Architecture with Event-Driven Sensors

## What Was Implemented

### 1. External Assets (Observe, Don't Execute)
**Location:** `script_orchestrator/defs/external_assets/defs.py`

Using `AssetSpec` (the correct API for Dagster 1.12.14), we defined three external assets that Dagster observes for lineage but doesn't execute:

- **order_book_stream**: Kafka/Flink stream with 100ms order book snapshots
- **order_book_clickhouse**: ClickHouse table with billions of rows (downstream of stream)
- **spark_cluster**: External Spark compute infrastructure

These represent systems that run independently of Dagster. Dagster tracks them for lineage and monitoring.

### 2. Event-Driven Sensors
**Location:** `script_orchestrator/defs/sensors/defs.py`

Two sensors for event-driven batch processing:

- **order_book_data_sensor** (checks every 30s)
  - Monitors ClickHouse for new data
  - Triggers Spark jobs when threshold met (e.g., 10M new snapshots)
  - Implements batching pattern for efficiency

- **market_event_priority_sensor** (checks every 10s)
  - Handles urgent market events
  - Priority tagged for queue management
  - Bypasses normal batching for critical events

### 3. Simplified definitions.py
**Location:** `script_orchestrator/definitions.py`

Now just 10 lines:
```python
"""Dagster definitions for script orchestration."""

from pathlib import Path
from dotenv import load_dotenv
from dagster import load_from_defs_folder

# Load environment variables from .env file
load_dotenv()

# Load all definitions from defs folder
# This includes: scripts component, sensors, and external assets
defs = load_from_defs_folder(project_root=Path(__file__).parent.parent)
```

Everything is auto-discovered from the `defs/` folder:
- `defs/scripts/` - ScriptGithubComponent for Python/Prefect/Spark/Dask scripts
- `defs/sensors/` - Event-driven sensors
- `defs/external_assets/` - External system observations

### 4. Architecture Pattern: Observe Streaming, Orchestrate Batch

```
┌─────────────────────────────────────────────────────┐
│ STREAMING LAYER (Not orchestrated by Dagster)      │
│  Order Books (100ms) → Kafka → Flink → ClickHouse  │
└─────────────────────────────────────────────────────┘
                        │
                        │ Dagster OBSERVES (external assets)
                        ▼
┌─────────────────────────────────────────────────────┐
│ DAGSTER ORCHESTRATION LAYER                         │
│  1. External Assets (observe streaming)             │
│  2. Sensors (check thresholds every 30s)            │
│  3. Batch Jobs (Spark processes 10M+ rows)          │
└─────────────────────────────────────────────────────┘
```

**Key Point:** Dagster does NOT handle 100ms snapshots. The streaming pipeline does that. Dagster observes the stream and triggers batch jobs when enough data accumulates.

## What's in the Demo

### Assets (9 total)
From `defs/scripts/`:
1. `script_extract_data` - Python data extraction
2. `script_transform_data` - Python transformation (depends on extract)
3. `script_generate_report` - Python report generation (depends on transform)
4. `script_prefect_flow_example` - Prefect flow (works without Prefect installed)
5. `script_spark_job_example` - Spark job pattern (simulated, shows billions of rows pattern)
6. `script_dask_analysis_example` - Dask parallel processing pattern

From `defs/external_assets/`:
7. `order_book_stream` - External Kafka/Flink stream
8. `order_book_clickhouse` - External ClickHouse table
9. `spark_cluster` - External Spark infrastructure

### Sensors (2 total)
From `defs/sensors/`:
1. `order_book_data_sensor` - Monitors for batch thresholds
2. `market_event_priority_sensor` - Handles urgent events

### Key Features
- ✅ State-backed component pattern
- ✅ Multiple script types (Python, Prefect, Spark, Dask)
- ✅ YAML-based metadata (schedules, dependencies, retries)
- ✅ Event-driven sensors (not just time-based)
- ✅ External asset observation (streaming infrastructure)
- ✅ Batch processing pattern (10M+ rows at once)
- ✅ Priority handling (urgent events bypass normal queue)
- ✅ Auto-discovery via `load_from_defs_folder`

## Running the Demo

```bash
cd script_orchestrator
uv run dg dev
```

Opens Dagster UI at `http://localhost:3000` (or alternate port if 3000 is busy)

## Documentation

- `STREAMING_ARCHITECTURE.md` - Comprehensive guide to streaming + batch architecture
- `DEPLOYMENT.md` - Production deployment patterns
- `POLYMARKET_DEMO.md` - 15-minute demo script with talking points

## Architecture Decisions

1. **Why SourceAsset → AssetSpec?**
   - `SourceAsset` was correct for older Dagster versions
   - For Dagster 1.12.14, `AssetSpec` is the current API
   - Both represent external assets Dagster observes

2. **Why sensors in defs folder?**
   - Auto-discovery via `load_from_defs_folder`
   - Cleaner definitions.py (just one line)
   - Follows Dagster component pattern

3. **Why NOT real-time?**
   - 100ms snapshots = streaming ingestion (Kafka/Flink)
   - Dagster = batch orchestration (Spark jobs every 5-10 minutes)
   - Sensors check every 30s, trigger when 10M rows accumulated
   - This is event-driven BATCH, not sub-second real-time

4. **Why external assets?**
   - Shows full lineage: Stream → Storage → Compute → Results
   - Dagster doesn't manage streaming infrastructure
   - Observability without execution
   - Single pane of glass for entire data ecosystem

## Next Steps

1. **Connect to real ClickHouse** - Replace simulated sensor with actual queries
2. **Connect to real Spark cluster** - Submit actual Spark jobs via spark-submit
3. **Add Prefect flows** - Show migration from Prefect to Dagster
4. **Add more external assets** - Show broader data ecosystem
5. **Implement priority queue** - Use Dagster+ run queue configuration

## Success Metrics

This demo addresses:
- ✅ "Can Dagster handle 1500 tasks?" - Yes, 1 asset = 1 Spark job = 1500 internal tasks
- ✅ "How do we handle 100ms snapshots?" - Streaming pipeline ingests, Dagster observes
- ✅ "What about real-time?" - Use Flink for real-time, Dagster for batch
- ✅ "How do we avoid queuing?" - Sensors with priority tags + Dagster+ configuration
- ✅ "How does this scale?" - Kafka/Flink (streaming), Spark (compute), Dagster+ (orchestration)
