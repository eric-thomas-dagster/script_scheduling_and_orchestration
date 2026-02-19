# Script Orchestrator

A Dagster component for orchestrating and migrating existing Airflow and Prefect workflows.

## Features

- **Discover and load scripts** from local directories or GitHub repositories
- **Support for multiple orchestrators**: Airflow 2.x, Airflow 3.x, and Prefect 3.x
- **Automatic version detection**: Filters DAGs based on installed orchestrator version
- **dag-factory support**: Load Airflow DAGs defined in YAML
- **Asset materialization**: Execute workflows and track data assets in Dagster
- **Dataset/Inlet/Outlet support**: Map Airflow datasets to Dagster assets with full lineage tracking

## Asset Lineage

The component automatically extracts and visualizes data lineage from Airflow DAGs using Assets (Airflow 3.x) or Datasets (Airflow 2.x).

### Cross-DAG Lineage

When DAGs produce and consume datasets, the component creates individual Dagster assets with proper dependencies:

```python
from airflow.sdk import Asset, dag, task

# Producer DAG
raw_data = Asset("raw_data")

@dag(schedule="@hourly")
def extract_data():
    @task(outlets=[raw_data])
    def fetch():
        ...

# Consumer DAG
processed_data = Asset("processed_data")

@dag(schedule=[raw_data])  # Triggered when raw_data updates
def process_data():
    @task(outlets=[processed_data])
    def transform():
        ...
```

In Dagster, this creates:
- `airflow_dataset_raw_data` (from extract_data)
- `airflow_dataset_processed_data` (from process_data, depends on raw_data)

### Multi-Input Assets

DAGs can depend on multiple upstream datasets:

```python
customer_data = Asset("customer_data")
sales_data = Asset("sales_data")
report = Asset("combined_report")

@dag(schedule=[customer_data, sales_data])  # Wait for BOTH
def generate_report():
    @task(outlets=[report])
    def combine():
        ...
```

The resulting `airflow_dataset_combined_report` asset will show dependencies on both upstream assets in the lineage graph.

### Configuration

To enable asset lineage for Python DAGs, create a companion YAML file:

```yaml
# my_dag.yaml
enabled: true
script_type: airflow
group_name: airflow  # Optional: organize in asset groups
airflow_mapping:
  enabled: true
  mode: graph_asset
```

## Quick Start

### Prerequisites

- Python 3.10-3.14
- [uv](https://docs.astral.sh/uv/) package manager

### Installation

1. **Clone the repository**:
   ```bash
   cd script_orchestrator
   ```

2. **Install dependencies**:
   ```bash
   uv sync
   ```

3. **Verify setup** (recommended):
   ```bash
   python verify_setup.py
   ```

   This will check that all dependencies are properly installed and offer to fix common issues.

4. **Start the Dagster web server**:
   ```bash
   uv run dg dev
   ```

5. **Open the Dagster UI**: Navigate to http://localhost:3000 (or the port shown in the terminal)

## Configuration

Configuration is managed through `.env` file and `defs/scripts/defs.yaml`.

### Environment Variables (`.env`)

```bash
# Scripts location
USE_LOCAL_SCRIPTS=true
SCRIPTS_DIR=example_scripts

# Orchestrator versions
AIRFLOW_ENABLED=true
AIRFLOW_VERSION=3.1
AIRFLOW_AUTO_INSTALL=true

PREFECT_ENABLED=true
# PREFECT_VERSION=3.0
# PREFECT_AUTO_INSTALL=true
```

### Component Configuration (`defs/scripts/defs.yaml`)

```yaml
type: script_orchestrator.components.ScriptGithubComponent
attributes:
  use_local: true
  scripts_directory: example_scripts
  airflow_enabled: true
  airflow_version: '3.1'
  airflow_auto_install: true
  prefect_enabled: true
```

## Troubleshooting

### Airflow command not found

If you see an error like `Failed to spawn: airflow`, the Airflow console script may not be installed properly. This is a known issue with `uv`.

**Fix**:
```bash
uv pip install --reinstall apache-airflow
```

Or run the verification script which will detect and offer to fix this:
```bash
python verify_setup.py
```

### Database initialization fails

The Airflow database is automatically initialized at `.airflow/airflow.db` in the project directory. If initialization fails:

1. Check that the `airflow` command works: `uv run airflow version`
2. Manually initialize: `AIRFLOW_HOME=.airflow uv run airflow db migrate`
3. Check file permissions on the `.airflow` directory

### Version compatibility

- **Airflow 2.x and Prefect 3.x**: Cannot coexist (SQLAlchemy version conflict)
- **Airflow 3.x and Prefect 3.x**: Fully compatible (both use SQLAlchemy 2.0)

If you need to support Airflow 2.x workflows, disable Prefect or run in separate environments.

## Project Structure

```
script_orchestrator/
├── .env                           # Environment configuration
├── .airflow/                      # Airflow home directory (auto-created)
├── defs/
│   └── scripts/
│       └── defs.yaml             # Component configuration
├── example_scripts/              # Sample workflows
│   ├── airflow/                 # Airflow DAGs
│   ├── prefect/                 # Prefect flows
│   └── dag_factory/             # YAML-defined DAGs
├── script_orchestrator/
│   └── components/
│       ├── script_github_component.py  # Main component
│       └── parsers/                    # Airflow/Prefect parsers
├── pyproject.toml               # Python dependencies
└── verify_setup.py              # Setup verification script
```

## Development

### Adding New Scripts

1. Place scripts in the `example_scripts/` directory (or configured `SCRIPTS_DIR`)
2. Organize by orchestrator type: `airflow/`, `prefect/`, `dag_factory/`
3. Scripts are auto-discovered on startup

### Airflow Version Targeting

DAGs can specify required Airflow versions using decorators:

```python
from airflow.decorators import dag

@dag(
    schedule=None,
    tags=["airflow-version:2.x"]  # Only loads with Airflow 2.x
)
def my_dag_2x():
    pass
```

Or in dag-factory YAML:

```yaml
my_dag:
  default_args:
    tags: ["airflow-version:3.x"]  # Only loads with Airflow 3.x
```

### Running Tests

```bash
uv run pytest
```

## Resources

- [Dagster Documentation](https://docs.dagster.io/)
- [Airflow Documentation](https://airflow.apache.org/)
- [Prefect Documentation](https://docs.prefect.io/)
- [dag-factory Documentation](https://github.com/ajbosco/dag-factory)
