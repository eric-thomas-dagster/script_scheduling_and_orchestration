# Airflow DAG → Dagster Job Patterns

## The Key Principle

**Airflow DAGs map to different Dagster constructs based on whether they have `outlets`:**

```
Does the Airflow DAG have outlets?
├─ YES → Dagster @asset or @graph_asset
│   ├─ Single task → @asset
│   └─ Multiple tasks → @graph_asset (ops that produce an asset)
│
└─ NO → Dagster @job
    ├─ Single task → @job with one @op
    └─ Multiple tasks → @job with multiple @ops
```

---

## Pattern 1: DAG with Outlets → Asset

### Single Task
```yaml
# Airflow
my_data_dag:
  tasks:
    fetch_data:
      outlets: [{name: "my_data"}]
```

```python
# Dagster
@asset
def my_data():
    return fetch_data()
```

### Multiple Tasks
```yaml
# Airflow
my_data_dag:
  tasks:
    extract:
      outlets: [{name: "my_data"}]
    transform:
      dependencies: [extract]
    load:
      dependencies: [transform]
```

```python
# Dagster - Option A: Graph Asset
@op
def extract():
    return fetch()

@op
def transform(data):
    return clean(data)

@op
def load(data):
    store(data)

@graph_asset
def my_data():
    """The entire DAG becomes a graph asset."""
    data = extract()
    transformed = transform(data)
    load(transformed)
    return transformed  # The asset value

# OR Option B: Chain of Assets (if intermediate steps should be tracked)
@asset
def extracted_data():
    return fetch()

@asset(deps=[extracted_data])
def transformed_data():
    return transform()

@asset(deps=[transformed_data])
def my_data():
    return load()
```

---

## Pattern 2: DAG without Outlets → Job

### Single Task
```yaml
# Airflow
send_alert_dag:
  schedule: [{name: "my_data"}]  # Triggered by asset
  tasks:
    send_email:
      # NO outlets - just sends email
```

```python
# Dagster
@op
def send_email():
    email.send("Report ready!")

@job
def send_alert():  # The DAG becomes a job
    send_email()

@asset_sensor(asset_key=AssetKey("my_data"), job=send_alert)
def alert_sensor(context, asset_event):
    yield RunRequest()
```

### Multiple Tasks
```yaml
# Airflow
notify_stakeholders_dag:
  schedule: [{name: "my_data"}]
  tasks:
    format_message:
      # NO outlets
    send_to_management:
      dependencies: [format_message]
      # NO outlets
    send_to_team:
      dependencies: [format_message]
      # NO outlets
    log_sent:
      dependencies: [send_to_management, send_to_team]
      # NO outlets
```

```python
# Dagster
@op
def format_message():
    return "Report is ready!"

@op
def send_to_management(message):
    email.send(to="management@co.com", body=message)

@op
def send_to_team(message):
    email.send(to="team@co.com", body=message)

@op
def log_sent():
    print("Notifications sent")

@job
def notify_stakeholders():  # The entire DAG becomes a job
    """Multiple ops within one job - represents the Airflow DAG."""
    message = format_message()
    send_to_management(message)
    send_to_team(message)
    log_sent()

@asset_sensor(asset_key=AssetKey("my_data"), job=notify_stakeholders)
def notification_sensor(context, asset_event):
    yield RunRequest()
```

---

## Pattern 3: Asset Chain → Multiple Assets

When you have multiple DAGs, each with outlets:

```yaml
# Airflow - 3 separate DAGs, all with outlets
raw_data_dag:
  tasks:
    fetch:
      outlets: [{name: "raw_data"}]

clean_data_dag:
  schedule: [{name: "raw_data"}]
  tasks:
    clean:
      outlets: [{name: "cleaned_data"}]  # ← Still has outlets!

aggregate_dag:
  schedule: [{name: "cleaned_data"}]
  tasks:
    aggregate:
      outlets: [{name: "aggregated_data"}]  # ← Still has outlets!
```

```python
# Dagster - 3 assets (NOT jobs!)
@asset
def raw_data():
    return fetch()

@asset(deps=[raw_data])
def cleaned_data():
    return clean()  # Has outlets → still an asset!

@asset(deps=[cleaned_data])
def aggregated_data():
    return aggregate()  # Has outlets → still an asset!
```

---

## Decision Matrix

| Airflow DAG Structure | Has Outlets? | # of Tasks | Dagster Pattern |
|-----------------------|--------------|-----------|-----------------|
| Single task, produces data | ✅ Yes | 1 | `@asset` |
| Multiple tasks, produces data | ✅ Yes | 2+ | `@graph_asset` or multiple `@asset`s |
| Single task, no data produced | ❌ No | 1 | `@job` with 1 `@op` |
| Multiple tasks, no data produced | ❌ No | 2+ | `@job` with multiple `@op`s |

---

## Common Patterns

### Pattern: ETL Pipeline
```python
# DAG with outlets → Graph Asset
@graph_asset
def customer_data():
    raw = extract_customers()
    clean = transform_customers(raw)
    return load_customers(clean)
```

### Pattern: Notification/Alert System
```python
# DAG without outlets → Job
@job
def send_alerts():
    data = fetch_report()
    email_management(data)
    post_to_slack(data)
```

### Pattern: Data Pipeline + Notification
```python
# First DAG with outlets → Asset
@asset
def daily_metrics():
    return calculate_metrics()

# Second DAG without outlets → Job
@job
def notify_metrics_ready():
    format_email()
    send_email()
    log_notification()

@asset_sensor(asset_key=AssetKey("daily_metrics"), job=notify_metrics_ready)
def metrics_sensor(context, asset_event):
    yield RunRequest()
```

---

## Why Jobs for Terminal Operations?

**Jobs are perfect for DAGs that don't produce data assets:**

✅ Notifications (email, Slack, PagerDuty)
✅ Cleanup operations
✅ Triggering external systems
✅ Logging/auditing
✅ Health checks

These are all operations (side effects) that don't produce trackable data assets.

---

## Summary

**The mapping is simple:**
- **Airflow DAG with `outlets`** → Dagster `@asset` or `@graph_asset`
- **Airflow DAG without `outlets`** → Dagster `@job` with `@op`s

**Jobs are not second-class citizens!** They're the correct pattern for operational workflows that don't produce data assets.
