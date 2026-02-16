# UV Environment Fix ✓

## Problem

When running `uv run dg dev`, the Spark and Dask scripts were failing with:
```
subprocess.CalledProcessError: Command '['/opt/homebrew/opt/python@3.11/bin/python3.11', ...]' returned non-zero exit status 1.
```

## Root Cause

Two different Python environments:

1. **uv environment** (`script_orchestrator/.venv/`)
   - Has PySpark, Dask, NumPy, Pandas, PyArrow
   - Used by `uv run python`
   - ✓ Scripts work perfectly here

2. **Homebrew Python 3.11** (`/opt/homebrew/opt/python@3.11/`)
   - Used by `dg` CLI
   - Does NOT have PySpark, Dask, etc.
   - ✗ Scripts fail here

The component was using `sys.executable` (Homebrew Python) to run scripts, which didn't have the dependencies.

## Solution

Updated `ScriptGithubComponent` to use `uv run python` instead of `sys.executable`:

```python
# Before
result = subprocess.run(
    [sys.executable, str(script_info.script_path)],
    ...
)

# After
python_cmd = ["uv", "run", "python"]
result = subprocess.run(
    python_cmd + [str(script_info.script_path)],
    ...
)
```

Now all scripts run in the uv environment with all dependencies available.

## What This Fixes

✓ **Spark scripts** - PySpark now available
✓ **Dask scripts** - Dask + NumPy + Pandas now available
✓ **All script types** - Consistent environment
✓ **Dependencies** - Everything in pyproject.toml available

## File Changed

- `script_orchestrator/components/script_github_component.py`
  - Lines ~408-445: All script execution paths
  - Changed from `sys.executable` to `uv run python`

## Testing

```bash
cd script_orchestrator
uv run dg dev

# Then in UI:
# Materialize script_spark_job_example - Should work!
# Materialize script_dask_analysis_example - Should work!
```

## Why This Works

`uv run python` always uses the project's virtual environment, ensuring:
- PySpark 4.1.1
- Dask 2026.1.2 with distributed
- NumPy, Pandas, PyArrow
- Any other dependencies in pyproject.toml

No matter which Python `dg` uses, scripts run in the right environment.

## Alternative Solutions Considered

### 1. Install globally (rejected)
```bash
pip install pyspark dask numpy pandas pyarrow
```
**Problem:** Defeats purpose of isolated environments

### 2. Use sys.executable from uv (rejected)
```python
python_cmd = [shutil.which("python") or sys.executable]
```
**Problem:** Still uses wrong Python when `dg` is global

### 3. Use uv run python (✓ chosen)
```python
python_cmd = ["uv", "run", "python"]
```
**Benefit:** Always uses project environment, works everywhere

## Production Deployment

For production, you'd still change to actual tools:

```python
# Spark (production)
["spark-submit", "--master", "spark://cluster:7077", "job.py"]

# Dask (production)
# Script connects to remote cluster
["python", "dask_job.py"]  # Uses production Python with deps

# Prefect (production)
["prefect", "deployment", "run", "flow/deployment"]
```

But for demo with `uv run dg dev`, this fix ensures everything works locally.
