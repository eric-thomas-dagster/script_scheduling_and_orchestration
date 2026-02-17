# The Simple Pattern: Asset Jobs & Op Jobs

## Key Insight

You don't need `@graph_asset` for most Airflow migrations! Use:

1. **Asset Jobs** - Collection of assets (for DAGs with outlets)
2. **Op Jobs** - Collection of ops (for DAGs without outlets)

---

## The Pattern

```python
from dagster import (
    asset, op, job,
    define_asset_job, AssetSelection,
    asset_sensor, RunRequest
)

# ============================================================================
# Assets (from Airflow DAGs with outlets)
# ============================================================================

@asset
def data1():
    return fetch()

@asset(deps=[data1])
def data2():
    return transform()

@asset(deps=[data2])
def data3():
    return aggregate()

# Asset Job - groups assets together (represents the Airflow DAG)
data_pipeline_job = define_asset_job(
    name="data_pipeline",
    selection=AssetSelection.all()  # Or specific assets
)

# ============================================================================
# Ops (from Airflow DAG without outlets)
# ============================================================================

@op
def send_email():
    email.send("Pipeline complete!")

@op
def post_to_slack():
    slack.post("Pipeline complete!")

# Op Job - groups ops together (represents the Airflow DAG)
@job
def notify_completion():
    send_email()
    post_to_slack()

# ============================================================================
# Connect them
# ============================================================================

@asset_sensor(
    asset_key=AssetKey("data3"),  # Watch for asset
    job=notify_completion         # Trigger op job
)
def sensor(context, asset_event):
    yield RunRequest()
```

---

## When to Use Each Pattern

### Asset Jobs
```python
# Use for Airflow DAGs with outlets
define_asset_job(
    name="my_etl_pipeline",
    selection=[asset1, asset2, asset3]
)
```

**When:**
- Airflow DAG has `outlets`
- Produces data that should be tracked
- Want to see lineage in Dagster UI

### Op Jobs
```python
# Use for Airflow DAGs without outlets
@job
def my_operations():
    op1()
    op2()
    op3()
```

**When:**
- Airflow DAG has NO outlets
- Just performs operations (alerts, cleanup, etc.)
- No data to track

### Graph Assets (Rare!)
```python
# Only use if you need to mix ops and assets in complex ways
@graph_asset
def complex_asset():
    intermediate = op1()
    return op2(intermediate)
```

**When:**
- You need fine-grained control over op execution
- Complex transformation logic
- Most migrations DON'T need this!

---

## Real Example: ISS Coordinates

### Airflow YAMLs

```yaml
# DAG 1: Has outlets
update_iss_coordinates:
  tasks:
    update:
      outlets: [{name: "iss_coordinates"}]

# DAG 2: No outlets
process_iss_coordinates:
  schedule: [{name: "iss_coordinates"}]
  tasks:
    read: {}  # No outlets
```

### Dagster (Simple!)

```python
# Asset (from DAG with outlets)
@asset
def iss_coordinates():
    _update_iss_coordinates()

# Asset job
update_job = define_asset_job(
    "update_iss_coordinates",
    selection=[iss_coordinates]
)

# Op job (from DAG without outlets)
@op
def read_coordinates():
    _read_iss_coordinates()

@job
def process_iss_coordinates():
    read_coordinates()

# Connect them
@asset_sensor(asset_key=AssetKey("iss_coordinates"), job=process_iss_coordinates)
def sensor(context, asset_event):
    yield RunRequest()
```

---

## Benefits of This Approach

### ✅ Simpler
- No need for `@graph_asset` in most cases
- Assets are just assets
- Jobs are just jobs

### ✅ More Flexible
- Can group assets differently than in Airflow
- Can run subsets: `define_asset_job(selection=[asset1, asset2])`
- Can mix and match job definitions

### ✅ Clearer Intent
- Asset jobs → "Materialize these data assets"
- Op jobs → "Perform these operations"

### ✅ Better Dagster Integration
- Asset jobs work seamlessly with:
  - Asset sensors
  - Asset checks
  - Freshness policies
  - Auto-materialization

---

## Multiple Jobs from Same Assets

You can define different jobs for different purposes:

```python
# Assets
@asset
def raw_data():
    return fetch()

@asset(deps=[raw_data])
def cleaned_data():
    return clean()

@asset(deps=[cleaned_data])
def aggregated_data():
    return aggregate()

# Different job definitions
full_pipeline = define_asset_job(
    "full_pipeline",
    selection=AssetSelection.all()
)

just_raw = define_asset_job(
    "fetch_only",
    selection=[raw_data]
)

clean_and_aggregate = define_asset_job(
    "transform_pipeline",
    selection=[cleaned_data, aggregated_data]
)
```

---

## Summary

**Most Airflow migrations need:**
1. ✅ `@asset` for tasks with outlets
2. ✅ `define_asset_job()` to group assets
3. ✅ `@op` + `@job` for tasks without outlets
4. ✅ `@asset_sensor` to connect them

**Rarely need:**
5. ❌ `@graph_asset` (only for complex op/asset mixing)

**The rule:**
- Airflow DAG with outlets → **Asset job**
- Airflow DAG without outlets → **Op job**
- Connect with sensors

That's it! Simple and idiomatic Dagster. 🎯
