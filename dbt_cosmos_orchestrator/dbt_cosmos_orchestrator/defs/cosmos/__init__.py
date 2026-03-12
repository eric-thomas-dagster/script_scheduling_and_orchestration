"""Cosmos dbt component definitions.

Reads defs.yaml, instantiates CosmosGithubComponent, and builds Dagster Definitions.

On first run this will:
  1. Clone astronomer/astronomer-cosmos to ~/.dagster/cosmos_cache/astronomer_cosmos/
  2. Run ``dbt deps`` + ``dbt parse`` to compile the manifest

Subsequent runs re-use the cached clone (and compiled manifest) and are fast.
"""

import logging
from pathlib import Path

import yaml
from dagster import Definitions

from dbt_cosmos_orchestrator.components import CosmosGithubComponent

logger = logging.getLogger(__name__)

_config_path = Path(__file__).parent / "defs.yaml"

with open(_config_path) as _f:
    _raw = yaml.safe_load(_f)

_attrs = _raw.get("attributes", {})
# Strip empty strings so dataclass defaults are respected
_attrs = {k: v for k, v in _attrs.items() if v != ""}

component = CosmosGithubComponent(**_attrs)


class _LoadContext:
    """Minimal stand-in for ComponentLoadContext — only ``path`` is used."""

    def __init__(self, path: Path) -> None:
        self.path = path


_context = _LoadContext(Path(__file__).parent)

try:
    defs = component.build_defs(_context)  # type: ignore[arg-type]
except Exception as exc:
    logger.warning(
        "CosmosGithubComponent.build_defs failed — returning empty Definitions.\n"
        "This usually means the repo is not yet cloned or dbt deps/parse have not run.\n"
        "Error: %s",
        exc,
    )
    defs = Definitions()
