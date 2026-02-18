# Resource Detection: Auto-Tagging & Categorization

## 🎯 The Right Approach

Instead of generating resource files or trying to modify scripts, **use resource detection for automatic categorization and visibility**.

Scripts work unchanged. Resources are detected and used to enhance the Dagster UI experience.

---

## What Happens Now

### 1. Automatic Detection

When you point at an external repo with Python scripts, Airflow DAGs, or Prefect flows:

```python
# Your script (unchanged) - Python, Airflow, or Prefect
import psycopg2
import boto3
import requests

conn = psycopg2.connect(...)
s3 = boto3.client('s3', ...)
response = requests.get(...)
```

**Works for:**
- ✅ Python scripts
- ✅ Airflow DAG files (`.py`)
- ✅ Airflow dag-factory YAML (detects from task callables)
- ✅ Prefect flow files (`.py`)

### 2. Auto-Generated Tags & Kinds

The component detects imports and automatically adds:

```python
@asset(
    # Kinds show as icons/badges in UI
    tags={
        "dagster/kind/postgres": "",
        "dagster/kind/s3": "",
        "dagster/kind/http": "",

        # Filterable tags
        "uses_postgres": "",
        "uses_s3": "",
        "uses_http": "",
        "resource_type_database": "",
        "resource_type_storage": "",
        "resource_type_api": "",
    },

    # Metadata for details
    metadata={
        "detected_resources": {
            "resources": ["postgres", "s3", "http"],
            "details": {
                "postgres": {"type": "database", "import": "psycopg2"},
                "s3": {"type": "storage", "import": "boto3"},
                "http": {"type": "api", "import": "requests"}
            }
        }
    }
)
def my_asset(context):
    # Script runs unchanged!
    subprocess.run(["python", "script.py"])
```

### 3. Dagster UI Benefits

**In the Asset Graph:**
- 🐘 PostgreSQL icon/badge
- ☁️ S3 icon/badge
- 🌐 HTTP icon/badge
- 🌊 Airflow icon (for Airflow DAGs)
- 🔮 Prefect icon (for Prefect flows)
- **Multiple kinds shown together!** (e.g., both "airflow" and "postgres")

**Filtering:**
- Show only assets that use PostgreSQL
- Show only assets that use S3
- Filter by resource type (database, storage, api)
- Filter by framework (airflow, prefect, python)

**Metadata View:**
- See all detected resources
- See import details
- Understand dependencies

---

## Examples

### Example 1: Airflow DAG with Resources

#### Your Airflow DAG (External Repo)

```python
# dags/etl_pipeline.py
from airflow import DAG
from airflow.decorators import task
import psycopg2
import boto3

@task
def extract_from_db():
    conn = psycopg2.connect(...)
    # Extract data
    return data

@task
def upload_to_s3(data):
    s3 = boto3.client('s3')
    # Upload to S3

with DAG('etl_pipeline') as dag:
    data = extract_from_db()
    upload_to_s3(data)
```

#### What Gets Detected

```
🔧 Detected resources in Airflow DAG: postgres, s3
```

#### Asset in Dagster UI

Shows **BOTH** framework and resource kinds:
- 🌊 airflow
- 🐘 postgres
- ☁️ s3

Tags for filtering:
- `dagster/kind/airflow`
- `dagster/kind/postgres`
- `dagster/kind/s3`
- `uses_postgres`
- `uses_s3`
- `resource_type_database`
- `resource_type_storage`

---

### Example 2: Prefect Flow with Resources

#### Your Prefect Flow (External Repo)

```python
# flows/data_pipeline.py
from prefect import flow, task
import redis
import requests

@task
def fetch_api_data():
    response = requests.get('https://api.example.com/data')
    return response.json()

@task
def cache_data(data):
    r = redis.Redis(host='localhost')
    r.set('data', json.dumps(data))

@flow
def data_pipeline():
    data = fetch_api_data()
    cache_data(data)
```

#### What Gets Detected

```
🔧 Detected resources in Prefect flow: http, redis
```

#### Asset in Dagster UI

Shows **BOTH** framework and resource kinds:
- 🔮 prefect
- 🌐 http
- 💾 redis

Tags for filtering:
- `dagster/kind/prefect`
- `dagster/kind/http`
- `dagster/kind/redis`
- `uses_http`
- `uses_redis`
- `resource_type_api`
- `resource_type_cache`

---

### Example 4: dag-factory YAML with Resources

#### Your dag-factory YAML

```yaml
# config/data_pipeline.yaml
dag:
  dag_id: data_processing
  tasks:
    extract:
      operator_type: python
      python_callable: include.tasks.etl_tasks.extract_data
    transform:
      operator_type: python
      python_callable: include.tasks.etl_tasks.transform_data
```

#### Task Files

```python
# include/tasks/etl_tasks.py
import psycopg2
import pandas as pd

def extract_data():
    conn = psycopg2.connect(...)
    df = pd.read_sql("SELECT * FROM users", conn)
    return df

def transform_data(df):
    # Transform with pandas
    return transformed_df
```

#### What Gets Detected

```
🔧 Detected resources in Airflow tasks: postgres, pandas
```

Component resolves `python_callable` paths to actual Python files and detects resources from those files!

#### Asset in Dagster UI

Shows **BOTH** framework and resource kinds:
- 🌊 airflow
- 🐘 postgres
- 📊 pandas

---

### Example 5: Multi-Resource Script

### Your Script (External Repo)

```python
"""
Data pipeline script.

Owner: data-team@company.com
"""

import psycopg2
import boto3
import requests
import redis

def main():
    # Database
    conn = psycopg2.connect(host='localhost', database='mydb')
    users = conn.cursor().execute("SELECT * FROM users").fetchall()

    # Cloud storage
    s3 = boto3.client('s3')
    s3.upload_file('data.csv', 'bucket', 'data.csv')

    # API
    data = requests.get('https://api.example.com/data').json()

    # Cache
    r = redis.Redis(host='localhost')
    r.set('count', len(users))

if __name__ == '__main__':
    main()
```

### What Gets Generated

```
🔧 Detected resources: postgres, s3, http, redis
```

### Asset Definition

```python
@asset(
    name="script_data_pipeline",
    tags={
        # Kinds (show as icons)
        "dagster/kind/postgres": "",
        "dagster/kind/s3": "",
        "dagster/kind/http": "",
        "dagster/kind/redis": "",

        # Filterable tags
        "uses_postgres": "",
        "uses_s3": "",
        "uses_http": "",
        "uses_redis": "",

        # Type tags
        "resource_type_database": "",
        "resource_type_storage": "",
        "resource_type_api": "",
        "resource_type_cache": "",
    },
    metadata={
        "detected_resources": {
            "resources": ["postgres", "s3", "http", "redis"],
            "details": {
                "postgres": {"type": "database", "import": "psycopg2"},
                "s3": {"type": "storage", "import": "boto3"},
                "http": {"type": "api", "import": "requests"},
                "redis": {"type": "cache", "import": "redis"}
            }
        },
        # ... doc metadata, performance metadata, etc.
    }
)
def script_data_pipeline(context):
    # Script runs as-is!
    subprocess.run(["python", "data_pipeline.py"])
```

---

## UI Experience

### Asset Catalog

```
┌─────────────────────────────────────────────────┐
│ script_data_pipeline                            │
│ 🐘 postgres  ☁️ s3  🌐 http  💾 redis         │
│                                                 │
│ Data pipeline script                            │
│ Owner: data-team@company.com                    │
└─────────────────────────────────────────────────┘
```

### Filter Assets

```
Filter by:
☐ Uses PostgreSQL
☐ Uses S3
☐ Uses HTTP
☐ Uses Redis

Resource Types:
☐ Database
☐ Storage
☐ API
☐ Cache
```

### Asset Details > Metadata

```json
{
  "detected_resources": {
    "resources": ["postgres", "s3", "http", "redis"],
    "details": {
      "postgres": {
        "type": "database",
        "import": "psycopg2"
      },
      ...
    }
  }
}
```

---

## Benefits

### ✅ Scripts "Just Work"
- No modification needed
- Original code runs unchanged
- External repos work out of the box

### ✅ Better Visibility
- See what each asset uses at a glance
- Icons/badges in the UI
- Clear dependency documentation

### ✅ Powerful Filtering
- Find all assets using PostgreSQL
- Find all assets using S3
- Filter by resource type

### ✅ Security & Compliance
- Know what external services are accessed
- Identify credentials needed
- Plan security reviews

### ✅ Migration Planning
- Understand dependencies
- Plan resource consolidation
- Identify shared services

---

## Use Cases

### 1. Discovery
**"What does this asset use?"**
- Look at tags/kinds
- See icons in UI
- Check metadata

### 2. Filtering
**"Show me all assets that use PostgreSQL"**
- Filter by `uses_postgres` tag
- Or by kind `postgres`

### 3. Security Review
**"What external services are accessed?"**
- Check detected_resources metadata
- See all imports
- Understand data flow

### 4. Migration Planning
**"Which assets need S3 credentials?"**
- Filter by `uses_s3`
- See all S3-dependent assets
- Plan credential distribution

### 5. Cost Optimization
**"Which assets use expensive cloud services?"**
- Filter by storage/compute types
- Identify high-cost assets
- Plan optimization

---

## How It Works Across Frameworks

### For Python Scripts
- Directly analyzes the script file
- Detects all imports
- Adds resource kinds/tags to the asset

### For Airflow DAGs
- Analyzes the DAG Python file for imports
- Adds **both** `airflow` kind AND detected resource kinds
- Shows multiple badges in UI

### For dag-factory YAML
- Reads the YAML configuration
- Resolves `python_callable` paths to actual Python files
  - Example: `include.tasks.etl.extract` → `include/tasks/etl.py`
- Detects resources from all task files
- Deduplicates across all tasks
- Adds **both** `airflow` kind AND detected resource kinds
- Works for both assets (with outlets) and op jobs (asset_schedule)

### For Prefect Flows
- Analyzes the Prefect flow Python file for imports
- Adds **both** `prefect` kind AND detected resource kinds
- Shows multiple badges in UI

**Key Insight:** The same resource detector works across all frameworks, just pointed at different Python files!

---

## Supported Resources (30+)

All detected and added as kinds/tags:

**Databases:** postgres, mysql, sqlite, sqlalchemy
**Storage:** s3, gcs, azure_blob
**APIs:** http (requests, httpx)
**Queues:** rabbitmq, kafka, redis
**Data:** pandas, spark, dask
**ML:** tensorflow, pytorch, scikit_learn
**Monitoring:** datadog, prometheus
**Communication:** slack, sendgrid, twilio

---

## Configuration

### Enable/Disable Resource Detection

```yaml
# component.yaml
resource_detection:
  enabled: true  # default
```

### Custom Resource Patterns

Extend detection in your fork:

```python
# resource_detector.py
RESOURCE_PATTERNS = {
    # ... existing patterns ...
    'your_lib': ('your_resource', 'your_type', []),
}
```

---

## Comparison: Old vs New Approach

### ❌ Old Approach (Generated Resources)

```python
# Generated detected_resources.py (user must integrate)
@resource
def postgres_resource(config): ...

# User must update script to use resources
@asset(required_resource_keys={"postgres"})
def my_asset(context):
    conn = context.resources.postgres  # Script needs modification!
```

**Problems:**
- Extra work for user
- Scripts need modification
- Doesn't "just work"
- Generated file might not be used

### ✅ New Approach (Auto-Tagging)

```python
# Automatically added
@asset(
    tags={
        "dagster/kind/postgres": "",
        "uses_postgres": "",
    },
    metadata={"detected_resources": ...}
)
def my_asset(context):
    # Script runs unchanged!
    subprocess.run(["python", "script.py"])
```

**Benefits:**
- ✅ Zero user work
- ✅ Scripts unchanged
- ✅ "Just works"
- ✅ Better UI experience
- ✅ Powerful filtering

---

## Future Enhancements

### Phase 1 (Current)
✅ Detect resources from imports
✅ Add as kinds (icons/badges)
✅ Add as tags (filtering)
✅ Add as metadata (documentation)

### Phase 2 (Future)
- Detect credential usage patterns
- Suggest environment variable names
- Generate credential documentation
- Security scan reports

### Phase 3 (Future)
- Optional resource generation (on-demand)
- Smart import rewriting (opt-in)
- Resource pooling/sharing
- Cost tracking integration

---

## Summary

**Goal:** External repos "just work" in Dagster

**Solution:** Detect resources → Add tags/kinds/metadata

**Supports:**
- ✅ Python scripts
- ✅ Airflow DAG files
- ✅ Airflow dag-factory YAML (detects from task callables)
- ✅ Prefect flow files
- ✅ All orchestration frameworks + their resource usage

**Result:**
- 📜 Scripts run unchanged
- 🏷️ Assets automatically tagged with **multiple kinds** (framework + resources)
- 🎨 UI shows icons/badges for both framework and resources
- 🔍 Powerful filtering by framework OR resource type
- 📊 Clear visibility into what each asset uses
- 🔒 Security insights across all frameworks

**Examples:**
- Airflow DAG → Shows "airflow" + "postgres" + "s3" kinds
- Prefect flow → Shows "prefect" + "http" + "redis" kinds
- Python script → Shows detected resources only

**Zero user work. Maximum value.** 🎉
