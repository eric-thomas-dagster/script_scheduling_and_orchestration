# Asset-Based Orchestration: Airflow 3.x → Dagster

Airflow 3.x introduced **asset-based scheduling** (formerly datasets), which is conceptually identical to Dagster's core asset model. This guide explains how to migrate this pattern.

## The Pattern

### Decision Criteria

**The key question: Does the task have `outlets` (produce an asset)?**

- ✅ **Has outlets** → Dagster `@asset` (even if it also depends on other assets)
- ❌ **No outlets** → Dagster `@op` in `@job` (terminal operation)

### Airflow 3.x Examples

```yaml
# Task with outlets → Asset
update_data:
  tasks:
    fetch_data:
      outlets:
        - __type__: airflow.sdk.Asset
          name: "raw_data"

# Task with outlets AND asset schedule → Still an Asset!
transform_data:
  schedule:
    - __type__: airflow.sdk.Asset
      name: "raw_data"
  tasks:
    clean_data:
      outlets:
        - __type__: airflow.sdk.Asset
          name: "cleaned_data"

# Task with asset schedule but NO outlets → Op Job
send_alert:
  schedule:
    - __type__: airflow.sdk.Asset
      name: "cleaned_data"
  tasks:
    email_team:
      # No outlets! Just sends email
      python_callable: my_module.send_email
```

### Dagster Equivalent

**Key insight: Check for `outlets`, not dependencies!**

```python
# Task with outlets → Asset
@asset
def raw_data():
    """Has outlets: [raw_data]"""
    return fetch()

# Task with outlets AND depends on asset → Still an Asset!
@asset(deps=[raw_data])
def cleaned_data():
    """Has outlets: [cleaned_data] - produces data, so it's an asset!"""
    return clean(raw_data)

# Task with NO outlets, only consumes → Op Job
@op
def email_team():
    """No outlets - terminal operation."""
    send_email()

@job
def alert_job():
    email_team()

@asset_sensor(asset_key=AssetKey("cleaned_data"), job=alert_job)
def alert_sensor(context, asset_event):
    """Trigger job when cleaned_data materializes."""
    yield RunRequest()
```

**The flow:** `raw_data` (asset) → `cleaned_data` (asset) → `alert_job` (op job)

**NOT:** `raw_data` (asset) → `cleaned_data` (op job) ❌

---

## Current Support Status

### ✅ Fully Detected

The parser **fully understands** asset-based patterns:

- ✅ **Global defaults** (`default:` section)
- ✅ **Asset outlets** (tasks that produce assets)
- ✅ **Asset-based schedules** (DAGs triggered by assets)
- ✅ **Asset dependency flow** (producer → consumer relationships)

Example output:
```
DAG: update_iss_coordinates
  Asset Outlets: ['iss_coordinates']

DAG: process_iss_coordinates
  Schedule (assets): ['iss_coordinates']

Flow: update_iss_coordinates → produces → iss_coordinates → triggers → process_iss_coordinates
```

### ⚠️ Conversion Pattern

**Current behavior**: Creates separate graph assets (no dependency between them)

**Recommended**: For asset-based DAGs, manually refactor to use Dagster assets

---

## Migration Approaches

### Option 1: Manual Asset Refactoring (Recommended)

When you see asset-based DAGs in the YAML:

1. **Identify the pattern**:
   - Producer DAG has `outlets`
   - Consumer DAG has asset-based `schedule`

2. **Create Dagster assets directly**:
   ```python
   # Instead of relying on YAML conversion
   # Write native Dagster assets

   @asset
   def producer_asset():
       # Logic from producer DAG
       pass

   @asset(deps=[producer_asset])
   def consumer_asset():
       # Logic from consumer DAG
       pass
   ```

3. **Remove the YAML files** - they're now redundant

### Option 2: Keep as Graph Assets

If you need to preserve the DAG structure for now:

```yaml
# The YAML will create graph assets
# But they won't have the asset dependency relationship
```

You can manually add Dagster asset dependencies later:
```python
@asset(deps=[AssetKey("script_update_iss_coordinates")])
def processed_asset():
    # Explicitly depend on the graph asset
    pass
```

---

## Example: ISS Coordinates

### Original Airflow YAML

```yaml
default:
  start_date: 2025-09-01

update_iss_coordinates:
  schedule: "@daily"
  tasks:
    update_coordinates:
      python_callable: include.tasks.asset_example_tasks._update_iss_coordinates
      outlets:
        - __type__: airflow.sdk.Asset
          name: "iss_coordinates"

process_iss_coordinates:
  schedule:
    - __type__: airflow.sdk.Asset
      name: "iss_coordinates"
  tasks:
    read_coordinates:
      python_callable: include.tasks.asset_example_tasks._read_iss_coordinates
```

### Recommended Dagster Code

**Option 1: Asset + Op Job** (Most accurate mapping)

```python
from dagster import asset, op, job, asset_sensor, RunRequest, AssetKey

# Producer: DAG with outlets → Asset
@asset(name="iss_coordinates", compute_kind="api")
def iss_coordinates():
    """Fetch ISS coordinates - this IS an asset (produces data file)."""
    _update_iss_coordinates()

# Consumer: DAG with asset schedule → Op Job
@op
def read_coordinates():
    """Read and print - NOT an asset (just a side effect)."""
    _read_iss_coordinates()

@job
def process_iss_coordinates_job():
    read_coordinates()

@asset_sensor(asset_key=AssetKey("iss_coordinates"), job=process_iss_coordinates_job)
def iss_coordinates_sensor(context, asset_event):
    """Trigger job when iss_coordinates materializes."""
    yield RunRequest()
```

**Option 2: Asset + Asset** (If you want to track processing)

```python
@asset(name="iss_coordinates")
def iss_coordinates():
    _update_iss_coordinates()

@asset(deps=[iss_coordinates])
def processed_iss_coordinates():
    """Track that we processed it (shows in lineage)."""
    _read_iss_coordinates()
```

**When to use each:**
- **Op Job**: Consumer just performs operations (prints, sends alerts, etc.)
- **Asset**: Consumer produces trackable data or you want execution in lineage

### Why This is Better

| Aspect | YAML Graph Assets | Native Dagster Assets |
|--------|------------------|---------------------|
| **Dependency** | Separate, no connection | Explicit `deps=[...]` |
| **Scheduling** | Separate schedules | Automatic propagation |
| **Lineage** | Not visible | Full lineage graph |
| **Dagster UI** | Two separate graphs | Connected asset graph |
| **Backfills** | Manual coordination | Asset-aware backfills |

---

## Detection in Logs

When the parser encounters asset-based DAGs, you'll see:

```
Parsing dag-factory DAG: update_iss_coordinates
Task update_coordinates produces asset: iss_coordinates
Parsing dag-factory DAG: process_iss_coordinates
DAG process_iss_coordinates scheduled by assets: ['iss_coordinates']
```

This indicates you should consider the manual refactoring approach.

---

## Future Enhancement

**Potential automatic conversion**: The parser could automatically create Dagster assets instead of graph assets when it detects:

- DAGs with `outlets` → Create Dagster assets
- DAGs with asset schedules → Add asset dependencies

This would require architectural changes to the component's asset creation logic.

---

## Summary

| Pattern | Parser Support | Conversion | Recommendation |
|---------|---------------|------------|----------------|
| **Declarative DAGs** | ✅ Full | ✅ Graph assets | Use YAML conversion |
| **Partitioned Factory** | ✅ Full | ✅ Partitioned assets | Use YAML conversion |
| **Asset-based** | ✅ Detection | ⚠️ Manual needed | Write native Dagster assets |

**Bottom line**: For asset-based Airflow DAGs, use them as a guide to write proper Dagster assets rather than relying on automatic conversion.
