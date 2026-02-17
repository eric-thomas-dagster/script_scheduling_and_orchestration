# DAG Factory Patterns → Dagster

This component supports **two different "DAG Factory" patterns** from Airflow, each mapping to different Dagster concepts.

## Pattern 1: Declarative DAG Factory (Astronomer dag-factory)

**Source**: [Astronomer dag-factory library](https://github.com/astronomer/dag-factory)

### What is it?

Define **multiple different DAGs** declaratively in YAML files. Each DAG can have its own:
- Task structure
- Schedule
- Configuration
- Dependencies

### Airflow Example

```yaml
# example_dag_factory.yaml
customer_etl:
  schedule_interval: '0 3 * * *'
  tasks:
    extract_customers:
      operator: airflow.operators.bash.BashOperator
      bash_command: 'python extract.py'
    transform_customers:
      operator: airflow.operators.bash.BashOperator
      bash_command: 'python transform.py'
      dependencies: [extract_customers]

product_sync:
  schedule_interval: '@hourly'
  tasks:
    fetch_products:
      operator: airflow.operators.bash.BashOperator
      bash_command: 'python fetch.py'
```

**Airflow Result**: 2 separate DAGs (customer_etl, product_sync)

### Dagster Mapping

Each DAG definition becomes a **separate Dagster graph asset** with individual ops for each task:

- ✅ **customer_etl** asset with ops: [extract_customers, transform_customers]
- ✅ **product_sync** asset with ops: [fetch_products]

Each task becomes a Dagster op, preserving the full DAG structure.

### When to Use

- Multiple **different** workflows (ETL, sync, reporting, monitoring)
- Each workflow has different logic/purpose
- Want to manage all workflows in one YAML file

### Key Benefits

1. **Declarative Management**: Define all DAGs in YAML
2. **Op-Level Visibility**: Each task is a separate Dagster op
3. **Individual Task Retry**: Retry any task independently
4. **Dependency Preservation**: Task dependencies properly wired
5. **Operator Support**: Bash, Python, Dummy, and any Airflow operator via runtime

---

## Pattern 2: Parameterized DAG Factory (Classic Factory Pattern)

**Source**: Programmatic DAG generation pattern

### What is it?

Generate **multiple instances of the SAME DAG** with different parameters. Common for:
- Multi-tenant SaaS (one DAG per customer)
- Regional processing (one DAG per region)
- Multi-source ingestion (one DAG per data source)

### Airflow Example

```python
# Airflow DAG Factory (programmatic)
for customer in ["customer_a", "customer_b", "customer_c"]:
    create_dag(
        dag_id=f"customer_{customer}_etl",
        customer_id=customer
    )
```

**Airflow Result**: 3 separate DAGs with identical structure

### Dagster Mapping

One **partitioned asset** where each partition represents an instance:

```python
@asset(
    partitions_def=StaticPartitionsDefinition([
        "customer_a", "customer_b", "customer_c"
    ])
)
def customer_etl(context):
    customer_id = context.partition_key
    # Run ETL for this customer
```

**Dagster Result**: 1 asset with 3 partitions

### YAML Configuration

```yaml
# customer_etl_factory.yaml
enabled: true
script_type: airflow

dag_factory:
  enabled: true
  partition_key: "customer_id"
  partition_values: ["customer_a", "customer_b", "customer_c"]
  dynamic: false  # or true for runtime-modifiable partitions
```

### When to Use

- **Same workflow**, different entities (customers, regions, sources)
- Need to backfill specific instances
- Want to track execution per entity
- Entities may grow/shrink over time (use dynamic: true)

### Key Benefits

1. **Single Definition**: One asset instead of N DAGs
2. **Partition Visibility**: See all instances in one view
3. **Selective Execution**: Run for specific partitions
4. **Backfill Support**: Backfill date ranges per partition
5. **Dynamic Growth**: Add/remove partitions at runtime

---

## Comparison Table

| Aspect | Declarative (Astronomer) | Parameterized (Factory) |
|--------|-------------------------|------------------------|
| **Purpose** | Multiple different workflows | Multiple instances of same workflow |
| **Input** | YAML with DAG definitions | Python/YAML with parameters |
| **Airflow Result** | N different DAGs | N identical DAGs with different params |
| **Dagster Result** | N separate graph assets | 1 partitioned asset (N partitions) |
| **Structure** | Each DAG can be different | All instances identical |
| **Example Use Cases** | ETL + Sync + Reports | Per-customer, per-region, per-source |
| **Dagster Feature** | Graph assets with ops | Partitioned assets |
| **Task Visibility** | Each task = separate op | Asset-level execution |

---

## Supported Airflow Operators

### Pattern 1 (Declarative YAML)

Each operator type is supported as a Dagster op:

1. **BashOperator** ✅
   - Executes bash commands via subprocess
   - Stdout/stderr captured and logged

2. **PythonOperator** ✅
   - Resolves Python callables from:
     - `python_callable: "module.path.function"`
     - `python_callable_file` + `python_callable_name`
   - Executes function and captures result

3. **DummyOperator/EmptyOperator** ✅
   - No-op tasks for DAG structure

4. **Generic Operators** ✅ (NEW!)
   - S3Operator, SnowflakeOperator, HttpOperator, etc.
   - Automatically imports and executes via Airflow runtime
   - Requires Airflow and provider packages installed

### Pattern 2 (Parameterized)

- Executes the full Python DAG file as subprocess
- Partition key passed as environment variable: `AIRFLOW_VAR_{KEY}={value}`
- Full Airflow compatibility

---

## Examples

### Pattern 1: Declarative YAML

See `example_dag_factory.yaml` - defines 3 different DAGs:
- customer_etl (4 tasks)
- product_sync (3 tasks)
- analytics_report (4 tasks with parallel execution)

### Pattern 2: Parameterized Factory

See `customer_etl_factory.py` + `customer_etl_factory.yaml` - one DAG with 3 customer partitions

---

## Migration Path

### From Astronomer dag-factory

1. Copy your dag-factory YAML files to the scripts directory
2. Component auto-detects and creates graph assets
3. Each DAG becomes an asset, each task becomes an op
4. View in Dagster UI with full op graph

### From Programmatic Factory Pattern

1. Create YAML config with `dag_factory.enabled: true`
2. List partition values (customers, regions, etc.)
3. Component creates partitioned asset
4. Execute per partition in Dagster UI

---

## Implementation Notes

- **Both patterns supported simultaneously**
- Auto-detection based on YAML structure
- Declarative: presence of `tasks` key at root level
- Parameterized: presence of `dag_factory` config block
- Can mix both patterns in same scripts directory
