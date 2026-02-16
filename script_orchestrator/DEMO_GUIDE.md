# Script Orchestrator - Dagster Demo Guide

## Overview

This Dagster project demonstrates how teams currently using tools like Prefect can quickly migrate their existing Python scripts to Dagster with minimal changes. Scripts are discovered automatically from a Git repository (or local directory), and their scheduling and dependencies are defined via simple YAML files alongside each script.

## Key Features

✅ **Zero Code Changes**: Your existing Python scripts work as-is
✅ **YAML-Based Configuration**: Define schedules and dependencies in simple YAML files
✅ **Automatic Discovery**: Scripts are automatically discovered and turned into Dagster assets
✅ **Dependency Management**: Define which scripts depend on others
✅ **Flexible Source**: Use local directory or Git repository
✅ **Rich Metadata**: Automatic tracking of execution time, output, and more
✅ **Scheduling**: Each script can have its own cron schedule
✅ **Retries**: Configure retry policies per script

## Quick Start

### 1. Install Dependencies

```bash
cd script_orchestrator
pip install -e ".[dev]"
pip install python-dotenv pyyaml gitpython
```

### 2. Run the Demo

The project includes example scripts in `example_scripts/`:

```bash
# Start Dagster
dagster dev
```

Open http://localhost:3000 to see the Dagster UI.

### 3. Explore the Assets

You'll see three script assets with their dependency chain:

```
extract_data → transform_data → generate_report
```

Each asset shows:
- **Description** from the YAML file
- **Schedule** information
- **Dependencies** between scripts
- **Tags and owners** for organization

### 4. Run the Pipeline

Click "Materialize All" to run all three scripts in dependency order, or materialize them individually.

## Project Structure

```
script_orchestrator/
├── example_scripts/              # Example Python scripts
│   ├── extract_data.py          # Script 1: Data extraction
│   ├── extract_data.yaml        # Configuration for script 1
│   ├── transform_data.py        # Script 2: Data transformation
│   ├── transform_data.yaml      # Configuration for script 2 (depends on 1)
│   ├── generate_report.py       # Script 3: Report generation
│   └── generate_report.yaml     # Configuration for script 3 (depends on 2)
├── script_orchestrator/
│   ├── components/
│   │   └── script_github_component.py  # Core component logic
│   ├── schemas/
│   │   └── script_metadata.py   # YAML schema definitions
│   ├── defs/scripts/            # Generated state files
│   └── definitions.py           # Dagster definitions
└── .env                         # Configuration

```

## YAML Configuration Format

Each Python script can have an optional YAML file with the same name:

```yaml
# my_script.yaml
enabled: true
description: "What this script does"
group: my_group_name
owners:
  - team@company.com
tags:
  environment: production
  team: data-engineering
kinds:
  - python        # Shows Python icon in Dagster UI

# Dependencies - list other scripts this depends on
depends_on:
  - other_script_name
  - another_script

# Scheduling
schedule:
  cron_schedule: "0 2 * * *"  # Daily at 2am
  timezone: "UTC"
  default_status: "RUNNING"   # or "STOPPED"

# Retry configuration
retry_policy:
  max_retries: 3
  delay: 60                    # seconds
  backoff: "EXPONENTIAL"       # or "LINEAR"
  jitter: "FULL"              # optional: "FULL" or "PLUS_MINUS"
```

## Configuration Modes

### Local Mode (Default for Demo)

Uses scripts from a local directory:

```bash
# .env
USE_LOCAL_SCRIPTS=true
SCRIPTS_DIR=example_scripts
```

### GitHub Mode (Production)

Clones scripts from a GitHub repository:

```bash
# .env
USE_LOCAL_SCRIPTS=false
SCRIPTS_REPO_URL=https://github.com/your-org/your-scripts-repo
SCRIPTS_REPO_BRANCH=main
SCRIPTS_DIR=scripts
GITHUB_TOKEN=your_github_token  # Only needed for private repos
```

## Migration Path for Prefect Users

### Step 1: Keep Your Existing Scripts

Your Python scripts don't need to change. They can:
- Use any libraries you're already using
- Read/write files, databases, APIs
- Have their own logging and error handling
- Use command-line arguments (if needed)

### Step 2: Add YAML Files

Create a YAML file for each script with:
1. Schedule (when it should run)
2. Dependencies (which scripts must run first)
3. Description and ownership info

### Step 3: Organize in Git

```
your-scripts-repo/
├── scripts/
│   ├── extract_customers.py
│   ├── extract_customers.yaml
│   ├── transform_customers.py
│   ├── transform_customers.yaml
│   ├── load_to_warehouse.py
│   └── load_to_warehouse.yaml
```

### Step 4: Point Dagster at Your Repo

```bash
# In Dagster Cloud or local .env
SCRIPTS_REPO_URL=https://github.com/your-org/your-scripts-repo
SCRIPTS_DIR=scripts
```

### Step 5: Gradually Enhance

Over time, you can:
1. Convert scripts to use Dagster's context and resources
2. Add data quality checks
3. Use Dagster's partition system for incremental processing
4. Add data lineage and column-level metadata
5. Integrate with dbt, Spark, or other tools

## Demo Talking Points

For your demo with the Prefect user:

1. **"Your scripts work as-is"** - Show how the example scripts are just normal Python with no Dagster imports

2. **"Simple YAML for scheduling"** - Show the YAML files and how they define schedules and dependencies

3. **"Automatic asset creation"** - Show how the component discovers scripts and creates assets automatically

4. **"Visual dependency graph"** - Show the lineage graph in Dagster UI

5. **"Rich execution metadata"** - Run a script and show the stdout, execution time, etc. in the UI

6. **"Dagster+ features"** - Mention:
   - Centralized logging and alerting
   - Branch deployments for testing
   - dbt Cloud integration (if they use dbt)
   - Data quality checks with asset checks
   - Column-level lineage

7. **"Migration path"** - Explain:
   - Start with scripts + YAML (what we're showing)
   - Gradually adopt Dagster patterns
   - Eventually refactor into proper assets/resources if desired
   - But no pressure - scripts can stay as scripts!

## Advanced Features

### Adding More Scripts

Just add a `.py` file and optional `.yaml` file to your scripts directory. Reload definitions in Dagster to discover them.

### Script Dependencies

Scripts declare dependencies by name (without the `script_` prefix):

```yaml
depends_on:
  - extract_data    # Will depend on the asset named "script_extract_data"
  - other_script
```

### Disabling Scripts

To temporarily disable a script without deleting it:

```yaml
enabled: false
```

### Custom Groups

Organize scripts into groups:

```yaml
group: data_ingestion  # or reporting, transforms, etc.
```

### Asset Kinds

Use kinds to get nice icons in the UI:

```yaml
kinds:
  - python    # Python icon
  - dbt       # dbt icon
  - spark     # Spark icon
```

## Troubleshooting

### Scripts not discovered?

Check the diagnostic asset:
1. Go to Assets
2. Find "script_orchestrator_diagnostics"
3. Materialize it
4. Check metadata for errors

### Script execution fails?

1. Check if dependencies are met
2. Verify the script runs standalone: `python example_scripts/extract_data.py`
3. Check logs in the Dagster UI for stdout/stderr

### Git issues?

If using GitHub mode and git isn't available:
- Switch to local mode: `USE_LOCAL_SCRIPTS=true`
- Or use Dagster Hybrid deployment for git access

## Next Steps

After this demo, consider:

1. **Try with real scripts**: Point this at your actual scripts repository
2. **Deploy to Dagster+**: Get centralized orchestration, logging, and alerts
3. **Add asset checks**: Layer on data quality checks without changing scripts
4. **Integrate dbt**: If you have dbt models, they integrate seamlessly
5. **Explore sensors**: Trigger scripts based on events, not just schedules

## Questions?

- Dagster Docs: https://docs.dagster.io
- Dagster Slack: https://dagster.io/slack
- Migration Guides: https://docs.dagster.io/guides/migrations
