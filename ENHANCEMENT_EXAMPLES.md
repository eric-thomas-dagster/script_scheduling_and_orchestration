# Enhancement Examples: Performance Monitoring, Documentation Extraction, and Asset Checks

This document shows examples of the three enhancements that were just implemented.

## Overview

✅ **#4 - Performance Monitoring**: Automatic tracking of execution time, memory usage, and CPU usage
✅ **#5 - Documentation Extraction**: Extract and use docstrings for asset metadata
✅ **#7 - Asset Checks**: Auto-generate asset checks from assert statements in scripts

---

## Example 1: Python Script with Documentation and Checks

**File:** `example_scripts/python_examples/data_processor.py`

```python
"""
Process customer data and validate quality.

This script reads raw customer data, performs cleaning and validation,
and outputs processed results.

Input: raw_customer_data.csv
Output: processed_customer_data.parquet
Owner: data-team@company.com
Schedule: Daily at 2 AM
SLA: 4 hours
Tags: etl, customers, data-quality
"""

import pandas as pd
import os

def main():
    # Read raw data
    df = pd.read_csv('data/raw_customer_data.csv')

    # Data quality checks
    assert len(df) > 0, "Input data is empty"
    assert df['customer_id'].nunique() == len(df), "Duplicate customer IDs found"
    assert df['email'].notna().all(), "Missing email addresses"
    assert df['created_at'].notna().all(), "Missing creation dates"

    # Process data
    df_clean = df.copy()
    df_clean['email'] = df_clean['email'].str.lower()
    df_clean['created_at'] = pd.to_datetime(df_clean['created_at'])

    # Validate output
    assert len(df_clean) == len(df), "Rows were lost during processing"
    assert not df_clean.isna().any().any(), "NULL values in output"

    # Save results
    output_path = 'data/processed_customer_data.parquet'
    df_clean.to_parquet(output_path)

    # Validate file was created
    assert os.path.exists(output_path), "Output file not created"

    print(f"Processed {len(df_clean)} customers")

if __name__ == '__main__':
    main()
```

### What Gets Generated

#### 1. Documentation Extraction

The component extracts the docstring and creates:

```python
@asset(
    name="script_data_processor",
    description="Process customer data and validate quality. This script reads raw customer data, performs cleaning and validation, and outputs processed results.",
    metadata={
        "script_name": "data_processor",
        "script_path": "/path/to/data_processor.py",
        "script_type": "python",
        # Extracted from docstring:
        "doc_input": "raw_customer_data.csv",
        "doc_output": "processed_customer_data.parquet",
        "doc_owner": "data-team@company.com",
        "doc_schedule": "Daily at 2 AM",
        "doc_sla": "4 hours",
        "doc_tags": ["etl", "customers", "data-quality"],
    }
)
def script_data_processor(context):
    # ... execution with performance monitoring ...
```

#### 2. Performance Monitoring

When the asset runs, it automatically tracks and emits:

```python
{
    "execution_time_seconds": 12.34,
    "execution_time": "12.3s",
    "memory_used_mb": 145.67,
    "memory_peak_mb": 256.89,
    "cpu_percent": 87.5,
    "performance_monitoring": "✅ Full monitoring",
    # ... other metadata ...
}
```

#### 3. Asset Checks

The component detects the assert statements and creates checks:

```python
AssetCheckSpec(
    name="check_size_1",
    asset=AssetKey("script_data_processor"),
    description="Validates that len(df) > 0"
)

AssetCheckSpec(
    name="check_assertion_2",
    asset=AssetKey("script_data_processor"),
    description="Duplicate customer IDs found"
)

AssetCheckSpec(
    name="check_assertion_3",
    asset=AssetKey("script_data_processor"),
    description="Missing email addresses"
)

# ... more checks ...
```

---

## Example 2: Minimal Script

**File:** `example_scripts/python_examples/simple_report.py`

```python
"""
Generate daily sales report.

Owner: sales-ops@company.com
"""

def main():
    sales = fetch_sales_data()

    assert len(sales) > 0
    assert sales['amount'].sum() >= 0

    generate_report(sales)

if __name__ == '__main__':
    main()
```

### What You Get

- **Description**: "Generate daily sales report."
- **Metadata**: `{"doc_owner": "sales-ops@company.com"}`
- **Performance**: Execution time, memory usage
- **Checks**: 2 asset checks for the assertions

---

## Example 3: Airflow DAG with Auto-Generation

All the enhancements also work with auto-generated assets from Airflow YAMLs!

**File:** `example_scripts/airflow_examples/etl_pipeline.yaml`

```yaml
etl_pipeline:
  description: "Extract, transform, and load customer data"
  tags: ["etl", "customers"]

  tasks:
    extract:
      python_callable: include.tasks.etl.extract_data
      outlets:
        - name: "raw_data"

    transform:
      python_callable: include.tasks.etl.transform_data
      dependencies: [extract]
      outlets:
        - name: "clean_data"
```

**File:** `include/tasks/etl.py`

```python
"""
ETL pipeline tasks.

Owner: data-eng@company.com
SLA: 2 hours
"""

def extract_data():
    """Extract raw data from source."""
    data = fetch_from_api()
    assert len(data) > 0, "No data received"
    return data

def transform_data():
    """Transform and validate data."""
    data = load_raw_data()
    assert data is not None
    # ... transform ...
    return cleaned_data
```

### Result

- **Assets**: `raw_data`, `clean_data` with rich descriptions
- **Asset Job**: `etl_pipeline` to materialize both
- **Performance**: Tracked for each asset materialization
- **Checks**: Auto-generated from asserts in `extract_data` and `transform_data`

---

## How to Test

### 1. Create a Test Script

```bash
cd script_orchestrator/example_scripts/python_examples
cat > test_script.py << 'EOF'
"""
Test script for enhancements.

Input: test_input.csv
Output: test_output.csv
Owner: engineering@company.com
Tags: test, demo
"""

import pandas as pd

df = pd.DataFrame({'a': [1, 2, 3]})

# These will become asset checks
assert len(df) > 0
assert df['a'].max() <= 10
assert df['a'].min() >= 0

df.to_csv('output.csv')
print(f"Processed {len(df)} rows")
EOF
```

### 2. Create YAML Config

```yaml
# test_script.yaml
enabled: true
group_name: "examples"
schedule:
  cron_schedule: "0 0 * * *"
  timezone: "UTC"
```

### 3. Run Dagster

```bash
cd script_orchestrator
dagster dev
```

### 4. Check the UI

Navigate to the asset in the Dagster UI and you should see:

- **Description**: "Test script for enhancements."
- **Metadata**:
  - `doc_input`: "test_input.csv"
  - `doc_output`: "test_output.csv"
  - `doc_owner`: "engineering@company.com"
  - `doc_tags`: ["test", "demo"]
- **Checks**: 3 asset checks detected
- **After Running**:
  - `execution_time`: "0.5s"
  - `memory_used_mb`: "45.2"
  - `cpu_percent`: "12.5"

---

## Benefits

### Documentation Extraction (#5)

✅ **No Manual Documentation**: Docstrings automatically become asset metadata
✅ **Structured Metadata**: Input/Output/Owner/SLA extracted from docstrings
✅ **Searchable**: Tags and owners make assets discoverable
✅ **Onboarding**: New team members can understand what each asset does

### Performance Monitoring (#4)

✅ **Zero Configuration**: Automatically tracks all script executions
✅ **Performance Insights**: See which scripts are slow or memory-intensive
✅ **Trend Analysis**: Track performance over time
✅ **Cost Optimization**: Identify expensive operations

### Asset Checks (#7)

✅ **Data Quality**: Assertions become Dagster checks
✅ **Early Detection**: Catch data issues immediately
✅ **No Extra Code**: Checks are generated from existing assertions
✅ **Visibility**: See all quality checks in the UI

---

## Advanced: Custom Check Implementation

The generated checks are placeholders. For real validation, implement them:

```python
from dagster import asset_check, AssetCheckResult

@asset_check(asset="script_data_processor", name="check_row_count")
def check_row_count(context):
    """Validate processed data has expected row count."""
    # Load the data
    df = pd.read_parquet('data/processed_customer_data.parquet')

    # Check
    row_count = len(df)
    passed = row_count > 0

    return AssetCheckResult(
        passed=passed,
        metadata={
            "row_count": row_count,
            "threshold": 0,
        }
    )
```

Add these to your `Definitions`:

```python
Definitions(
    assets=[...],
    asset_checks=[check_row_count, ...],  # Custom implementations
)
```

---

## Next Steps

1. **Add more checks**: Implement real validation logic for generated checks
2. **Performance dashboards**: Build dashboards from performance metadata
3. **Alerting**: Set up alerts based on performance degradation
4. **Documentation standards**: Establish team conventions for docstrings

These three enhancements work together to provide better observability, documentation, and data quality for your orchestrated scripts!
