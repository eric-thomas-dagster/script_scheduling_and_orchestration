# Basic Python Scripts

Simple Python scripts demonstrating standard ETL pipeline patterns.

## Scripts

### extract_data.py
**Purpose:** Extract data from external API
**Schedule:** Daily at 9 AM UTC
**Output:** Raw data in JSON format

Simulates fetching data from an external API endpoint. In production, this would connect to real data sources.

### transform_data.py
**Purpose:** Transform and clean extracted data
**Depends on:** extract_data
**Schedule:** After extract_data completes

Takes raw data and applies transformations:
- Data cleaning
- Type conversions
- Filtering invalid records

### generate_report.py
**Purpose:** Generate summary report
**Depends on:** transform_data
**Schedule:** After transform_data completes

Creates summary reports from transformed data:
- Aggregations
- Statistics
- Report formatting

## Pipeline Flow

```
extract_data (9 AM daily)
    ↓
transform_data (after extract)
    ↓
generate_report (after transform)
```

## Running Locally

```bash
# Single script
python basic_python/extract_data.py

# Or via Dagster
uv run dg dev
# Then materialize assets in UI
```

## Use Case

Perfect for demonstrating:
- ✓ Basic Python orchestration
- ✓ Script dependencies
- ✓ Scheduled execution
- ✓ Simple ETL patterns
- ✓ Prefect/Airflow equivalent workflows
