"""Universal orchestrator definitions — one component, all workflow types."""

import logging
from pathlib import Path

import yaml
from dagster import Definitions

from script_orchestrator.components import ScriptGithubComponent

logger = logging.getLogger(__name__)

_config_path = Path(__file__).parent / "defs.yaml"
with open(_config_path) as _f:
    _raw = yaml.safe_load(_f)

# Strip blank strings so Pydantic/dataclass defaults are respected
_attrs = {k: v for k, v in _raw.get("attributes", {}).items() if v != ""}

component = ScriptGithubComponent(**_attrs)

_state_dir = Path("/tmp/dagster_orchestrator")
_state_dir.mkdir(parents=True, exist_ok=True)
_state_file = _state_dir / "state.json"


class _Ctx:
    path = _state_dir


try:
    defs = component.build_defs_from_state(_Ctx(), _state_file if _state_file.exists() else None)
except Exception as exc:
    logger.warning(
        "ScriptGithubComponent failed — returning empty Definitions.\n"
        "This usually means the repo has not been cloned yet or dbt has not "
        "been configured.  Error: %s",
        exc,
    )
    defs = Definitions()
