"""Dagster definitions for script orchestration."""

from pathlib import Path
from dotenv import load_dotenv
from dagster import load_from_defs_folder

# Load environment variables from .env file
load_dotenv()

# Load all definitions from defs folder
# This includes: scripts component, sensors, and external assets
defs = load_from_defs_folder(project_root=Path(__file__).parent.parent)
