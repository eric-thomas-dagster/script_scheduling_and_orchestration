# Script Orchestrator - Demo Summary

## What Was Built

A complete Dagster project that demonstrates **script-based orchestration** - perfect for teams migrating from Prefect or similar workflow tools.

## 🎯 The Problem It Solves

Teams have existing Python scripts (or Prefect flows) and want to:
1. ✅ Keep their scripts unchanged
2. ✅ Add scheduling and dependencies
3. ✅ Get centralized orchestration and observability
4. ✅ Gradually migrate to Dagster features over time

## 🏗️ What's Included

### Core Components

1. **ScriptGithubComponent** (`script_orchestrator/components/script_github_component.py`)
   - Discovers Python scripts from Git repo or local directory
   - Creates Dagster assets automatically
   - Handles dependencies between scripts
   - Supports scheduling and retry policies

2. **YAML Schema** (`script_orchestrator/schemas/script_metadata.py`)
   - Defines the structure for script configuration
   - Schedule, dependencies, retries, metadata

3. **Example Scripts** (`example_scripts/`)
   - `extract_data.py` + `extract_data.yaml`
   - `transform_data.py` + `transform_data.yaml` (depends on extract)
   - `generate_report.py` + `generate_report.yaml` (depends on transform)

### Configuration

- **`.env`** - Environment configuration (local vs GitHub mode)
- **`config.yaml`** - Component configuration (auto-generated)
- **`_template.yaml`** - Template for creating new script configurations

## 🚀 Quick Start

```bash
cd script_orchestrator
pip install -e ".[dev]"
dagster dev
```

Then open http://localhost:3000

## 📊 What You'll See in Dagster UI

### Assets Tab
- `script_extract_data` - First in the chain
- `script_transform_data` - Depends on extract
- `script_generate_report` - Depends on transform
- `script_orchestrator_diagnostics` - System status

### Lineage Graph
```
extract_data → transform_data → generate_report
```

### Schedules Tab
- Three schedules (one per script)
- Each with their own cron expression
- Can be turned on/off individually

### When You Materialize
- See stdout/stderr from script execution
- Execution time and duration
- Success/failure status
- Rich metadata

## 🎤 Demo Talking Points

### 1. No Code Changes Required
```python
# This is a normal Python script - no Dagster imports!
#!/usr/bin/env python3
import json
from pathlib import Path

def main():
    # Your existing code here
    pass

if __name__ == "__main__":
    main()
```

### 2. Simple YAML Configuration
```yaml
# extract_data.yaml
description: "Extracts data from source"
schedule:
  cron_schedule: "0 2 * * *"
depends_on: []  # No dependencies
```

### 3. Automatic Discovery
- Add `my_script.py` + `my_script.yaml`
- Reload definitions
- New asset appears automatically

### 4. Dependency Management
```yaml
# transform_data.yaml
depends_on:
  - extract_data  # Will run after extract_data
```

### 5. Rich Observability
- Execution logs (stdout/stderr)
- Execution time tracking
- Dependency visualization
- Schedule monitoring
- Retry tracking

### 6. Flexible Deployment
- **Local Mode**: Test with local scripts
- **Git Mode**: Pull scripts from GitHub
- **Dagster+**: Deploy to cloud with one command

## 🎯 Perfect For Prefect Users Because...

1. **Keep Your Scripts**: No need to rewrite existing workflows
2. **YAML Config**: Similar to Prefect's flow decorators, but declarative
3. **Dependencies**: Explicit dependency chains (like Prefect's upstream/downstream)
4. **Scheduling**: Cron-based scheduling per script
5. **Observability**: Rich UI with logs, metadata, and lineage
6. **Migration Path**: Start simple, adopt Dagster features gradually

## 🛠️ How It Works

### Discovery Phase
1. Component reads `USE_LOCAL_SCRIPTS` from `.env`
2. Scans `example_scripts/` directory
3. Finds all `.py` files
4. Looks for matching `.yaml` files
5. Parses YAML metadata
6. Writes state to `script_orchestrator/defs/scripts/scripts_state.json`

### Asset Creation Phase
1. Component reads state file
2. For each script, creates a Dagster asset:
   - Name: `script_{script_name}`
   - Dependencies: From `depends_on` in YAML
   - Schedule: From `schedule` in YAML
   - Retry policy: From `retry_policy` in YAML
3. Asset function runs the script as subprocess
4. Captures stdout, stderr, execution time
5. Emits rich metadata to Dagster

### Execution Phase
1. User materializes asset (or schedule triggers)
2. Asset checks dependencies are met
3. Runs script: `python /path/to/script.py`
4. Captures output and timing
5. Shows results in Dagster UI

## 📦 What to Show in Demo

### Show 1: The Scripts (No Dagster Code!)
Open `example_scripts/extract_data.py` and show it's just normal Python.

### Show 2: The YAML Configuration
Open `example_scripts/extract_data.yaml` and explain the fields:
- Schedule (when it runs)
- Dependencies (what it depends on)
- Retry policy (how failures are handled)

### Show 3: The Dagster UI
1. **Assets tab**: Show the three script assets
2. **Lineage graph**: Show the dependency chain
3. **Materialize**: Click "Materialize all" and watch them execute in order
4. **Logs**: Show stdout from the scripts
5. **Metadata**: Show execution time, file paths, etc.
6. **Schedules**: Show the three schedules

### Show 4: Adding a New Script
1. Create `cleanup.py` (simple Python script)
2. Create `cleanup.yaml` with schedule and dependency on `generate_report`
3. Reload definitions
4. Show new asset appears in UI
5. Materialize to demonstrate it works

### Show 5: GitHub Mode
1. Show how to switch to GitHub mode in `.env`
2. Point to a real scripts repo
3. Explain how this enables centralized orchestration

## 🎓 Migration Path

### Phase 1: Lift and Shift
- Keep all existing scripts unchanged
- Add YAML files for scheduling/dependencies
- Deploy to Dagster/Dagster+
- Get immediate benefits: UI, logging, alerting

### Phase 2: Add Observability
- Add asset checks for data quality
- Emit custom metadata from scripts
- Add column-level lineage
- Integrate with dbt, Spark, etc.

### Phase 3: Gradual Enhancement
- Convert scripts to use Dagster context
- Use Dagster resources (databases, APIs)
- Adopt partitioning for incremental processing
- Add sensors for event-driven workflows

### Phase 4: Full Dagster (Optional)
- Refactor into proper Dagster assets
- Use asset factories for dynamic generation
- Leverage Dagster's testing framework
- Advanced features: multi-assets, ops, graphs

**Key Point**: Each phase is optional. You can stay at Phase 1 indefinitely!

## 🤔 Common Questions

**Q: Do I need to change my scripts?**
A: No! They work as-is.

**Q: What if my script needs arguments?**
A: Modify the component to pass environment variables or use Dagster's config system.

**Q: Can I use this with private repos?**
A: Yes, set `GITHUB_TOKEN` in `.env` or Dagster Cloud environment variables.

**Q: What about secrets?**
A: Scripts can read from environment variables (set in Dagster Cloud) or use Dagster resources.

**Q: Can I mix scripts and native Dagster assets?**
A: Yes! They all show up in the same UI and can depend on each other.

**Q: What about performance?**
A: Each script runs as a subprocess. For high-performance needs, consider migrating to native Dagster assets over time.

## 📚 Next Steps After Demo

1. **Try with real scripts**: Point at your actual scripts repo
2. **Deploy to Dagster+**: Get centralized orchestration
3. **Add alerting**: Configure Slack/email alerts
4. **Explore Dagster features**: Asset checks, sensors, partitions
5. **Schedule a follow-up**: Discuss migration strategy

## 📞 Resources

- This demo: `script_orchestrator/`
- Detailed guide: `DEMO_GUIDE.md`
- Dagster docs: https://docs.dagster.io
- Dagster Slack: https://dagster.io/slack
- Migration guides: https://docs.dagster.io/guides/migrations

---

**Built with ❤️ to help teams migrate to Dagster without the pain of rewriting everything!**
