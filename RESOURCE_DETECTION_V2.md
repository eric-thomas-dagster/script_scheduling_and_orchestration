# Resource Detection: Auto-Tagging & Categorization

## 🎯 The Right Approach

Instead of generating resource files or trying to modify scripts, **use resource detection for automatic categorization and visibility**.

Scripts work unchanged. Resources are detected and used to enhance the Dagster UI experience.

---

## What Happens Now

### 1. Automatic Detection

When you point at an external repo with Python scripts:

```python
# Your script (unchanged)
import psycopg2
import boto3
import requests

conn = psycopg2.connect(...)
s3 = boto3.client('s3', ...)
response = requests.get(...)
```

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

**Filtering:**
- Show only assets that use PostgreSQL
- Show only assets that use S3
- Filter by resource type (database, storage, api)

**Metadata View:**
- See all detected resources
- See import details
- Understand dependencies

---

## Example: Multi-Resource Script

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

**Result:**
- 📜 Scripts run unchanged
- 🏷️ Assets automatically tagged
- 🎨 UI shows icons/badges
- 🔍 Powerful filtering
- 📊 Clear visibility
- 🔒 Security insights

**Zero user work. Maximum value.** 🎉
