# Demo Mode Fixes for Spark and Dask Scripts

## Problem
When trying to materialize Spark assets, you got:
```
subprocess.CalledProcessError: Command '['spark-submit', ...]' returned non-zero exit status 126.
```

This happened because Spark wasn't installed locally, and the component was trying to run `spark-submit`.

## Solution
Similar to the Prefect fix, I've updated all distributed compute scripts to run as regular Python for demo purposes.

### Changes Made

#### 1. Component Execution (script_github_component.py:419-427)
**Before:**
```python
elif script_type == "spark":
    result = subprocess.run(
        ["spark-submit", str(script_info.script_path)],  # Requires Spark installed
        ...
    )
```

**After:**
```python
elif script_type == "spark":
    # Run Spark job as Python for demo (production would use spark-submit)
    # In production: ["spark-submit", "--master", "spark://cluster:7077", ...]
    result = subprocess.run(
        [sys.executable, str(script_info.script_path)],  # Works without Spark
        ...
    )
```

#### 2. Spark Script (spark_job_example.py)
- Added mock PySpark imports (works with/without PySpark installed)
- Updated to show realistic Polymarket scale:
  - 12.45M order book snapshots (1 hour @ 100ms intervals)
  - 20 Spark executors with 160 cores
  - 45.2 GB data processed
  - Market maker rule violations: 342
- Clear demo vs production documentation

#### 3. Dask Script (dask_analysis_example.py)
- Added mock Dask imports (works with/without Dask installed)
- Updated to show realistic analytics pattern:
  - 1,247 markets analyzed
  - ML anomaly detection (IsolationForest)
  - Cross-market correlation analysis
  - Compliance report generation
- Already runs as Python (no change needed to component)

## Demo vs Production

### Demo Mode (Current Setup)
- ✅ Works without Spark/Dask/Prefect installed
- ✅ Shows realistic output simulating distributed processing
- ✅ Demonstrates the orchestration pattern
- ✅ All assets can be materialized successfully

### Production Mode
For actual production deployment:

**Spark:**
```python
# In component, change back to:
result = subprocess.run(
    ["spark-submit",
     "--master", "spark://production-spark:7077",
     "--executor-memory", "8g",
     "--executor-cores", "4",
     "--num-executors", "20",
     str(script_info.script_path)],
    ...
)
```

**Dask:**
```python
# Already runs as Python, but in production the script would:
from dask.distributed import Client
client = Client("tcp://production-dask:8786")
# ... actual distributed processing ...
```

**Prefect:**
```python
# In component, change to:
result = subprocess.run(
    ["prefect", "deployment", "run", f"{flow_name}/{deployment_name}"],
    ...
)
```

## Now Everything Works

You can now materialize all assets including:
- ✅ `script_spark_job_example` - Simulates Spark processing billions of rows
- ✅ `script_dask_analysis_example` - Simulates Dask ML analytics
- ✅ `script_prefect_flow_example` - Runs Prefect flow logic

All while showing the correct orchestration patterns for the Polymarket use case.

## Test It

```bash
cd script_orchestrator
uv run dg dev
```

Then in the UI:
1. Go to Assets
2. Click on `script_spark_job_example`
3. Click "Materialize" - should now succeed!
4. Check logs to see simulated Spark output with realistic numbers
