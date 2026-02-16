#!/bin/bash
# Verification script for Script Orchestrator demo

set -e

# Detect Python command
if command -v python &> /dev/null; then
    PYTHON=python
elif command -v python3 &> /dev/null; then
    PYTHON=python3
else
    echo "❌ Error: Python not found"
    exit 1
fi

echo "🔍 Verifying Script Orchestrator Demo Setup..."
echo ""

# Check if we're in the right directory
if [ ! -f "pyproject.toml" ]; then
    echo "❌ Error: Not in script_orchestrator directory"
    echo "   Please cd to script_orchestrator/ first"
    exit 1
fi
echo "✅ In correct directory"

# Check Python version
PYTHON_VERSION=$($PYTHON --version 2>&1 | awk '{print $2}')
echo "✅ Python version: $PYTHON_VERSION"

# Check if package is installed
if ! $PYTHON -c "import script_orchestrator" 2>/dev/null; then
    echo "⚠️  Package not installed. Installing..."
    pip install -e ".[dev]" -q
    echo "✅ Package installed"
else
    echo "✅ Package already installed"
fi

# Check if .env exists
if [ ! -f ".env" ]; then
    echo "❌ Error: .env file not found"
    exit 1
fi
echo "✅ .env file exists"

# Check if example scripts exist
SCRIPT_COUNT=$(ls -1 example_scripts/*.py 2>/dev/null | grep -v _template | wc -l | tr -d ' ')
if [ "$SCRIPT_COUNT" -lt 3 ]; then
    echo "❌ Error: Not enough example scripts found"
    exit 1
fi
echo "✅ Found $SCRIPT_COUNT example scripts"

# Check if YAML files exist
YAML_COUNT=$(ls -1 example_scripts/*.yaml 2>/dev/null | grep -v _template | wc -l | tr -d ' ')
if [ "$YAML_COUNT" -lt 3 ]; then
    echo "❌ Error: Not enough YAML config files found"
    exit 1
fi
echo "✅ Found $YAML_COUNT YAML config files"

# Test loading definitions
echo "🔍 Testing Dagster definitions..."
if $PYTHON -c "from script_orchestrator.definitions import defs; graph = defs.resolve_asset_graph(); asset_count = len(graph.get_all_asset_keys()); print(f'  Found {asset_count} assets'); assert asset_count >= 3" 2>/dev/null; then
    echo "✅ Definitions loaded successfully"
else
    echo "❌ Error loading definitions"
    $PYTHON -c "from script_orchestrator.definitions import defs; defs.resolve_asset_graph()"
    exit 1
fi

# Test running one script
echo "🔍 Testing script execution..."
if $PYTHON example_scripts/extract_data.py > /dev/null 2>&1; then
    echo "✅ Scripts are executable"
else
    echo "❌ Error running scripts"
    exit 1
fi

# Check if data was created
if [ -f "/tmp/dagster_scripts_demo/extracted_data.json" ]; then
    echo "✅ Script output verified"
else
    echo "⚠️  Script output not found (non-critical)"
fi

# Summary
echo ""
echo "================================================"
echo "✅ ALL CHECKS PASSED!"
echo "================================================"
echo ""
echo "Your demo is ready! To start:"
echo ""
echo "  uv run dg dev"
echo ""
echo "Or with venv:"
echo "  source venv/bin/activate && dg dev"
echo ""
echo "Then open: http://localhost:3000"
echo ""
echo "Demo materials:"
echo "  - README.md           - Quick overview"
echo "  - DEMO_GUIDE.md       - Detailed guide"
echo "  - DEMO_SUMMARY.md     - Full summary"
echo "  - QUICK_REFERENCE.md  - Demo script"
echo ""
echo "Good luck with your demo! 🚀"
echo ""
