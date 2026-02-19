#!/usr/bin/env python3
"""
Verify that the script_orchestrator project is set up correctly.

Run this after cloning the project to ensure all dependencies are properly installed.
"""
import sys
import subprocess
from pathlib import Path


def check_command(cmd_name, test_cmd):
    """Check if a command is available and working."""
    try:
        result = subprocess.run(
            test_cmd,
            capture_output=True,
            text=True,
            timeout=10,
            check=False
        )
        if result.returncode == 0 or result.returncode == 1:  # Some commands return 1 but still work
            print(f"✅ {cmd_name} is available")
            return True
        else:
            print(f"⚠️  {cmd_name} command exists but returned error: {result.returncode}")
            return False
    except FileNotFoundError:
        print(f"❌ {cmd_name} command not found")
        return False
    except Exception as e:
        print(f"❌ Error checking {cmd_name}: {e}")
        return False


def check_package_installed(package_name):
    """Check if a Python package is installed."""
    try:
        result = subprocess.run(
            ["uv", "run", "python", "-c", f"import {package_name}; print('OK')"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False
        )
        if result.returncode == 0 and "OK" in result.stdout:
            print(f"✅ {package_name} package is installed")
            return True
        else:
            print(f"❌ {package_name} package is not installed or has issues")
            return False
    except Exception as e:
        print(f"❌ Error checking {package_name}: {e}")
        return False


def fix_airflow_console_script():
    """Attempt to fix missing Airflow console script."""
    print("\n🔧 Attempting to fix Airflow console script...")
    try:
        result = subprocess.run(
            ["uv", "pip", "install", "--reinstall", "apache-airflow"],
            capture_output=True,
            text=True,
            timeout=120,
            check=False
        )
        if result.returncode == 0:
            print("✅ Airflow reinstalled successfully")
            return True
        else:
            print(f"❌ Failed to reinstall Airflow: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ Error reinstalling Airflow: {e}")
        return False


def main():
    print("=" * 60)
    print("Script Orchestrator Setup Verification")
    print("=" * 60)
    print()

    all_checks_passed = True

    # Check uv is available
    print("Checking dependencies...")
    if not check_command("uv", ["uv", "--version"]):
        print("\n❌ uv is required. Install from: https://docs.astral.sh/uv/")
        return 1

    print()

    # Check critical packages
    print("Checking Python packages...")
    packages_ok = True
    packages_ok &= check_package_installed("dagster")
    packages_ok &= check_package_installed("airflow")
    packages_ok &= check_package_installed("prefect")

    print()

    # Check Airflow console script
    print("Checking Airflow console script...")
    airflow_cmd_ok = check_command("airflow", ["uv", "run", "airflow", "version"])

    if not airflow_cmd_ok and packages_ok:
        print("\n⚠️  Airflow package is installed but 'airflow' command not found.")
        print("This is a known issue with uv where console scripts aren't installed properly.")
        response = input("\nWould you like to fix this automatically? (y/n): ")
        if response.lower() == 'y':
            if fix_airflow_console_script():
                airflow_cmd_ok = check_command("airflow", ["uv", "run", "airflow", "version"])

    all_checks_passed = packages_ok and airflow_cmd_ok

    print()
    print("=" * 60)
    if all_checks_passed:
        print("✅ All checks passed! You're ready to run:")
        print("   uv run dg dev")
    else:
        print("❌ Some checks failed. Please fix the issues above.")
        print()
        print("Common fixes:")
        print("  - Install dependencies: uv sync")
        print("  - Fix Airflow script: uv pip install --reinstall apache-airflow")
        print("  - Check .env file exists and has correct settings")
    print("=" * 60)

    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    sys.exit(main())
