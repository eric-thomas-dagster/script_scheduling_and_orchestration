from dagster import Definitions

from dagster_orchestrator.defs.orchestrator import defs as _orchestrator_defs
from dagster_orchestrator.defs.prefect_demos import defs as _prefect_demos_defs

defs = Definitions.merge(_orchestrator_defs, _prefect_demos_defs)

__all__ = ["defs"]
