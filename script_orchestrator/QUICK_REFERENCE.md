# Quick Reference - Script Orchestrator Demo

## 🚀 Start Demo

```bash
cd script_orchestrator
dagster dev
# Open http://localhost:3000
```

## 📝 Key Demo Script

### 1. Introduction (30 seconds)
"Today I'll show you how to orchestrate your existing Python scripts with Dagster - without changing a single line of code."

### 2. Show the Scripts (1 minute)
```bash
# Open in editor
code example_scripts/extract_data.py
```
**Say**: "Notice this is just normal Python. No Dagster imports, no special decorators."

### 3. Show the YAML (1 minute)
```bash
# Open in editor
code example_scripts/extract_data.yaml
```
**Say**: "Here's where the magic happens. YAML files define:
- When it runs (cron schedule)
- What it depends on (other scripts)
- How to retry on failure"

### 4. Show Dagster UI (2 minutes)
- **Assets tab**: "Here are our three scripts as Dagster assets"
- **Lineage graph**: "Visual dependency chain"
- **Click Materialize All**: "Watch them execute in order"
- **Check logs**: "See stdout from the scripts"
- **Show metadata**: "Execution time, file paths, etc."

### 5. Show Dependencies (1 minute)
```bash
code example_scripts/transform_data.yaml
```
**Point to**:
```yaml
depends_on:
  - extract_data
```
**Say**: "This script won't run until extract_data completes successfully."

### 6. Show Schedules (1 minute)
- Click **Schedules** tab
- Show three schedules
- **Say**: "Each script has its own schedule. Turn them on/off independently."

### 7. Add New Script (2 minutes - if time)
```bash
# Create new file
cat > example_scripts/send_alert.py << 'EOF'
#!/usr/bin/env python3
print("🚨 Alert: Pipeline complete!")
EOF

cat > example_scripts/send_alert.yaml << 'EOF'
description: "Sends alert after report"
depends_on:
  - generate_report
schedule:
  cron_schedule: "0 5 * * *"
  timezone: "UTC"
EOF

# Reload in UI
```
**Say**: "Just added two files. Reload, and it appears!"

### 8. GitHub Mode (1 minute)
**Show `.env` file**:
```bash
# Switch to GitHub mode
USE_LOCAL_SCRIPTS=false
SCRIPTS_REPO_URL=https://github.com/your-org/scripts
```
**Say**: "Point at your GitHub repo, and all your team's scripts are orchestrated."

### 9. Dagster+ Benefits (1 minute)
**Say**: "Deploy to Dagster+ and get:
- Centralized orchestration
- Branch deployments for testing
- Alerting (Slack, email, PagerDuty)
- SSO and RBAC
- dbt Cloud integration
- No infrastructure to manage"

### 10. Migration Path (1 minute)
**Say**: "The best part? You can stay at this level forever, or gradually adopt more Dagster features:
- Phase 1: Scripts + YAML (what we showed)
- Phase 2: Add data quality checks
- Phase 3: Use Dagster resources
- Phase 4: Full Dagster assets (optional)"

### 11. Q&A

## 💡 Key Talking Points

### ✅ DO Emphasize
- **No code changes** required
- **Simple YAML** configuration
- **Visual lineage** and observability
- **Flexible deployment** (local, GitHub, Dagster+)
- **Gradual migration** path

### ❌ DON'T Say
- "You need to rewrite everything"
- "Dagster is complicated"
- "You must use all features"
- "Prefect is bad"

### 🎯 Positioning
**For Prefect users**: "Keep your scripts. Add YAML. Get Dagster's observability."
**Migration path**: "Start simple. Add features over time. No big rewrite."
**Value prop**: "Same scripts. Better visibility. Easier operations."

## 🎤 Demo Flow Variations

### Short Demo (5 min)
1. Show script (no Dagster code)
2. Show YAML (schedule + dependencies)
3. Show UI (lineage + materialize)
4. Show Dagster+ benefits

### Medium Demo (10 min)
1. Short demo content
2. Add new script live
3. Show GitHub mode
4. Explain migration path

### Long Demo (15 min)
1. Medium demo content
2. Deep dive on YAML options
3. Show diagnostics asset
4. Discuss integrations (dbt, etc.)
5. Q&A

## 🐛 Troubleshooting

### Assets don't appear
```bash
# Check state file
cat script_orchestrator/defs/scripts/scripts_state.json
# Reload definitions in UI (top right)
```

### Script fails
```bash
# Test script directly
python example_scripts/extract_data.py
# Check logs in Dagster UI
```

### Schedules don't run
- Make sure Dagster daemon is running (`dagster dev` starts it)
- Check schedule status (should be "Running")
- Verify cron expression (use crontab.guru)

## 📊 Metrics to Highlight

- **Time to onboard**: Minutes (just add YAML files)
- **Code changes**: Zero
- **Learning curve**: Low (just YAML)
- **Observability**: High (full lineage, logs, metadata)

## 🎁 Leave-Behinds

- GitHub repo: [your fork of this project]
- Dagster docs: https://docs.dagster.io
- Dagster Slack: https://dagster.io/slack
- Migration guide: `DEMO_GUIDE.md`
- Your contact info

## 📞 Follow-Up Questions

1. "How many Python scripts do you currently have?"
2. "What orchestration tool are you using today?"
3. "What's your biggest pain point with current tool?"
4. "Would you like to see this with your actual scripts?"
5. "When would you like to schedule a follow-up?"

---

**Remember**: Keep it simple. Show, don't tell. Focus on their pain points.
