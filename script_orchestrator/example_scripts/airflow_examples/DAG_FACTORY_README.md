# DAG Factory → Dagster Partitioned Assets

This demonstrates how Airflow's DAG Factory pattern maps to Dagster's partitioned assets.

## What is DAG Factory?

In Airflow, DAG Factory is a pattern where you generate multiple similar DAGs from configuration:

```yaml
# Traditional Airflow DAG Factory
customers:
  - customer_a
  - customer_b
  - customer_c

# Generates 3 separate DAGs:
# - customer_a_etl_dag
# - customer_b_etl_dag
# - customer_c_etl_dag
```

## Dagster Equivalent: Partitioned Assets

Instead of N separate DAGs, Dagster uses **one partitioned asset** where each partition represents a customer:

```python
@asset(partitions_def=StaticPartitionsDefinition(["customer_a", "customer_b", "customer_c"]))
def customer_etl(context):
    customer_id = context.partition_key  # "customer_a", "customer_b", or "customer_c"
    # Run ETL for this specific customer
```

## Benefits

1. **Single Asset Definition**: One asset instead of N duplicate DAGs
2. **Better Visualization**: See all partitions in one view
3. **Backfill Support**: Easily backfill specific customers
4. **Dynamic Growth**: Add new customers without changing code

## Configuration

### Example: customer_etl_factory.yaml

```yaml
enabled: true
description: "DAG Factory - Customer ETL as partitioned asset"
group: "dag_factory_examples"
script_type: airflow

# DAG Factory Configuration
dag_factory:
  enabled: true
  partition_key: "customer_id"  # Parameter that varies

  # Option 1: Static partitions (fixed list)
  partition_values:
    - "customer_a"
    - "customer_b"
    - "customer_c"
  dynamic: false

  # Option 2: Dynamic partitions (add/remove at runtime)
  # dynamic: true
  # partition_values: null

# Enable Airflow mapping
airflow_mapping:
  enabled: true

# Optional schedule
schedule:
  cron_schedule: "0 3 * * *"
  timezone: "UTC"
```

### Python DAG Template

```python
from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="customer_etl_template",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    params={"customer_id": "template"}  # Will be replaced with actual value
)
def customer_etl_pipeline():
    @task
    def extract_customer_data(**context):
        customer_id = context["params"]["customer_id"]
        # Extract for this customer
        return data

    @task
    def transform_customer_data(data):
        # Transform
        return data

    @task
    def load_customer_data(data):
        # Load
        pass

    load_customer_data(transform_customer_data(extract_customer_data()))

customer_etl_dag = customer_etl_pipeline()
```

## How It Works

1. **Detection**: Component detects `dag_factory.enabled: true` in YAML
2. **Partition Creation**: Creates StaticPartitionsDefinition or DynamicPartitionsDefinition
3. **Asset Creation**: Single partitioned asset replaces multiple DAGs
4. **Execution**: Partition key passed as Airflow variable: `AIRFLOW_VAR_CUSTOMER_ID=customer_a`

## Usage in Dagster UI

### Viewing Partitions
- Navigate to the asset in Dagster UI
- See all partitions: `customer_a`, `customer_b`, `customer_c`
- Each partition can be materialized independently

### Materializing
```bash
# Materialize one partition
dagster asset materialize script_customer_etl_factory --partition customer_a

# Materialize all partitions
dagster asset materialize script_customer_etl_factory --all-partitions

# Backfill date range (if using time partitions)
dagster asset materialize script_customer_etl_factory --partition-range 2024-01-01 2024-01-31
```

## Migration Path

### Before (Airflow DAG Factory)
```python
# Generates 100 separate DAGs
for customer in customers:
    create_dag(f"customer_{customer}_etl", customer_id=customer)
```

### After (Dagster Partitioned Asset)
```yaml
# Single partitioned asset with 100 partitions
dag_factory:
  enabled: true
  partition_key: "customer_id"
  partition_values: ["customer1", "customer2", ..., "customer100"]
```

## Dynamic Partitions (Growing Customer List)

For customers that are added/removed over time:

```yaml
dag_factory:
  enabled: true
  partition_key: "customer_id"
  dynamic: true  # Allow adding/removing partitions at runtime
```

Add new customer:
```python
from dagster import add_dynamic_partitions

add_dynamic_partitions("customer_etl_factory_partitions", ["new_customer_d"])
```

## Real-World Use Cases

1. **Multi-Tenant SaaS**: One partition per tenant/customer
2. **Regional Processing**: One partition per region (us-east, us-west, eu-west)
3. **Product Lines**: One partition per product category
4. **Data Sources**: One partition per data source (salesforce, hubspot, etc.)

## Notes

- Partition key is passed as Airflow variable: `AIRFLOW_VAR_{PARTITION_KEY}={partition_value}`
- Access in DAG with: `context["params"][partition_key]` or `Variable.get(partition_key)`
- All partitions share the same schedule (if configured)
- Each partition can be retried independently
