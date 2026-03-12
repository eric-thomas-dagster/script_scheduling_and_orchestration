"""Dagster definitions module."""

# Export the defs so Dagster can find them
from script_orchestrator.defs.scripts import defs

__all__ = ["defs"]
