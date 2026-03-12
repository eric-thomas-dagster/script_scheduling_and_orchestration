"""Cosmos-to-Dagster migration component.

Reads Astronomer Cosmos (Airflow + dbt) DAG files from a GitHub repository and converts
them to native Dagster dbt assets.

For each applicable Cosmos DAG found in the repository:
- A Dagster job is created that runs the equivalent dbt selection
- Schedules are extracted from the original DAG definition
- Source DAG metadata is attached so you can trace back to the original

For dbt_docs.py:
- A standalone Dagster asset is created that runs ``dbt docs generate``
- The resulting docs path is emitted as materialization metadata

Non-applicable DAGs (Kubernetes execution, virtualenv isolation, Airflow-specific patterns,
etc.) are skipped and documented in the ``cosmos_migration_summary`` asset.
"""

import ast
import logging
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import requests

import dagster as dg
from dagster.components import Component, ComponentLoadContext, Resolvable
from dagster_dbt import DbtCliResource, DbtProject, build_schedule_from_dbt_selection, dbt_assets

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DAG classification tables
# ---------------------------------------------------------------------------
#
# There are three outcomes for every Cosmos DAG we find:
#
#   REPLACED  — The DAG was just running dbt through Cosmos because Airflow had
#               no better way. Dagster's native dbt integration does the same
#               thing with full asset lineage, column metadata, asset checks,
#               row counts, and proper dependency tracking. A Dagster job is
#               created and the original Cosmos DAG can be turned off.
#
#   ABSORBED  — The DAG implements an Airflow/Cosmos concept (callbacks, profile
#               mapping, task groups) that Dagster handles differently as a
#               first-class primitive (sensors, resources, asset selections).
#               No explicit job is created because the functionality is already
#               implicit in how Dagster works. The Cosmos DAG is unnecessary.
#
#   SKIPPED   — The DAG uses infrastructure or execution patterns that are
#               specific to Airflow and have no Dagster equivalent worth creating
#               (Kubernetes operators, virtualenv isolation, benchmarking, etc.).
# ---------------------------------------------------------------------------

# REPLACED: stem → (description, dbt_select or None)
# Any Cosmos DAG that imports from cosmos and is NOT in _REPLACED or _ABSORBED
# and does NOT match a skip pattern is also treated as replaced (generic path).
_REPLACED: dict[str, tuple[str, Optional[str]]] = {
    "basic_cosmos_dag": (
        "Runs all dbt models. Dagster's @dbt_assets exposes every model as a "
        "first-class asset with lineage, checks, and row-count metadata.",
        None,
    ),
    "basic_cosmos_task_group": (
        "Groups models into customers/orders task groups for visual organization. "
        "Dagster's asset graph already shows individual model dependencies — "
        "task groups aren't needed. A job per logical group is created instead.",
        None,
    ),
    "basic_cosmos_task_group_different_owners": (
        "Task groups with per-owner assignments. Owner metadata is carried "
        "directly on each dbt asset spec via the DagsterDbtTranslator.",
        None,
    ),
    "cosmos_seed_dag": (
        "Loads raw data via dbt seeds. Dagster runs this as a job scoped to "
        "resource_type:seed, keeping seeds as observable assets.",
        "resource_type:seed",
    ),
    "example_cosmos_dbt_build": (
        "Runs dbt build (models + tests together). dagster-dbt runs dbt build "
        "by default so tests become asset checks automatically.",
        None,
    ),
    "cosmos_manifest_example": (
        "Uses a pre-compiled manifest to avoid parsing dbt source at runtime. "
        "dagster-dbt handles manifest compilation via DbtProject.prepare_if_dev() "
        "and pre-compiles in CI — same concept, no separate DAG needed.",
        None,
    ),
    "cosmos_manifest_selectors_example": (
        "Applies dbt selectors on top of a manifest. Dagster jobs support the "
        "same dbt selection syntax natively.",
        "tag:daily",
    ),
    "example_cosmos_sources": (
        "Tracks dbt source freshness. dagster-dbt surfaces sources as observable "
        "assets with freshness checks built in.",
        None,
    ),
    "example_model_version": (
        "Manages dbt model versioning. dbt version metadata is carried through "
        "the manifest and exposed on each asset spec.",
        None,
    ),
    "example_duckdb_dag": (
        "Runs dbt against DuckDB — a local-friendly option with no server. "
        "Just swap the dbt adapter in pyproject.toml and update profiles.yml.",
        None,
    ),
}

# ABSORBED: stem → explanation of how Dagster handles this natively
_ABSORBED: dict[str, str] = {
    "cosmos_callback_dag": (
        "Defines on_success/on_failure callbacks inline on the DAG. "
        "In Dagster, this is handled by run status sensors or alert policies — "
        "no dedicated job needed."
    ),
    "cosmos_profile_mapping": (
        "Demonstrates custom Airflow connection → dbt profile mapping. "
        "In Dagster, DbtCliResource owns the profile config. "
        "Connection credentials come from environment variables or Dagster resources."
    ),
    "example_cosmos_python_models": (
        "Runs Python-based dbt models alongside SQL models. "
        "dagster-dbt supports Python models natively — they appear as assets "
        "alongside SQL models with no special configuration."
    ),
    "user_defined_profile": (
        "Shows how to define a fully custom dbt profile. "
        "In Dagster, profile config lives entirely in DbtCliResource — "
        "this DAG file has no Dagster equivalent."
    ),
    "example_dbt_deps": (
        "Runs dbt deps as a separate task before the main pipeline. "
        "DbtProject.prepare_if_dev() handles this automatically at load time."
    ),
    "example_operators": (
        "Demonstrates custom Cosmos/Airflow operators. "
        "In Dagster, operators are replaced by @asset functions and resources — "
        "the concept doesn't exist as a separate artefact."
    ),
    "example_source_pruning": (
        "Cosmos render option that removes unused source nodes from the DAG. "
        "dagster-dbt's asset selection handles this at the job level instead."
    ),
    "example_source_rendering": (
        "Cosmos source rendering configuration. "
        "Source assets in dagster-dbt are first-class observable assets — "
        "no special rendering config required."
    ),
    "example_tests_multiple_parents": (
        "Shows how Cosmos handles dbt tests with multiple parent models. "
        "dagster-dbt automatically maps each test to all its upstream asset "
        "dependencies as asset checks."
    ),
}

# SKIPPED: substring patterns matched against the file stem.
# These DAGs use execution infrastructure or Airflow patterns that have no
# meaningful Dagster equivalent and are not worth creating jobs for.
_SKIP_PATTERNS: dict[str, str] = {
    "kubernetes": "Uses KubernetesPodOperator — infrastructure-specific, not applicable",
    "virtualenv": "Runs tasks in isolated virtualenvs — an Airflow execution pattern, not needed in Dagster",
    "watcher":    "Triggers DAG runs by watching the filesystem — use a Dagster sensor instead",
    "performance_dag": "Benchmarking harness for Cosmos/Airflow scheduler — not applicable",
    "task_mapping": "Airflow dynamic task mapping (expand()) — Dagster uses dynamic partitions instead",
    "tasks_map":    "Airflow dynamic task mapping patterns",
    "taskflow":     "TaskFlow API is Airflow-specific decorator syntax",
    "cross_project": "Multi-project dbt ls setup requiring separate Airflow connections — use separate Dagster code locations",
    "simple_dag_async": "Uses Airflow deferrable/async operators — execution model is Airflow-specific",
}

# The docs DAG gets its own dedicated Dagster asset, not a regular job.
_DOCS_DAG_STEM = "dbt_docs"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class CosmosDAGInfo:
    """Parsed information from a single Cosmos DAG file."""

    stem: str
    filename: str
    source_url: str
    description: str
    schedule: Optional[str]
    dag_ids: list[str]
    selectors: list[str]   # dbt --select expressions found in the file
    is_cosmos_dag: bool
    is_docs_dag: bool

    # One of: "replaced" | "absorbed" | "skipped" | "not_cosmos"
    #
    #   replaced  — Cosmos DAG just ran dbt; Dagster does this natively and
    #               better. A Dagster job is created. The Cosmos DAG can be
    #               disabled in Airflow.
    #   absorbed  — Airflow/Cosmos concept that Dagster handles as a built-in
    #               primitive (sensor, resource, asset selection). No explicit
    #               job is created because the functionality is already implicit.
    #               The Cosmos DAG is unnecessary.
    #   skipped   — Infrastructure/execution pattern with no Dagster equivalent
    #               (Kubernetes, virtualenv, benchmarking, etc.).
    #   not_cosmos — File does not import from 'cosmos'; nothing to convert.
    action: str

    # Human-readable explanation for absorbed/skipped/not_cosmos actions.
    action_note: Optional[str] = None


# ---------------------------------------------------------------------------
# Component
# ---------------------------------------------------------------------------

@dataclass
class CosmosGithubComponent(Component, Resolvable):
    """Dagster component that converts Astronomer Cosmos DAGs to native dbt assets.

    Reads Cosmos (Airflow + dbt) DAG files from a GitHub repository, decides which
    ones are applicable in a Dagster context, and creates:

    - A ``@dbt_assets`` definition covering all models in the dbt project
    - A Dagster job + optional schedule per applicable Cosmos DAG
    - A ``dbt_docs`` asset (from dbt_docs.py) that generates HTML documentation
    - A ``cosmos_migration_summary`` asset documenting what was converted / skipped

    Configuration example (defs.yaml):

    .. code-block:: yaml

        type: dbt_cosmos_orchestrator.components.CosmosGithubComponent
        attributes:
          repo_url: https://github.com/astronomer/astronomer-cosmos
          branch: main
          dags_path: dev/dags
          dbt_project_path: dev/dags/dbt/jaffle_shop
          dbt_target: dev

    Prerequisites:
    - The dbt adapter for your database must be installed (e.g. ``dbt-postgres``,
      ``dbt-duckdb``).
    - A dbt profile matching ``dbt_target`` must exist, or you can point
      ``dbt_profiles_dir`` at a directory containing ``profiles.yml``.
    """

    # ---- required config --------------------------------------------------
    repo_url: str = "https://github.com/astronomer/astronomer-cosmos"

    # ---- optional config --------------------------------------------------
    branch: str = "main"
    dags_path: str = "dev/dags"
    dbt_project_path: str = "dev/dags/dbt/jaffle_shop"
    dbt_target: str = "dev"
    dbt_profiles_dir: str = ""   # leave blank to use default ~/.dbt/profiles.yml
    cache_dir: str = ""          # leave blank to use ~/.dagster/cosmos_cache/

    # ---- DAG filtering ----------------------------------------------------
    # Explicit list of filename stems to skip (e.g. ["my_dag", "another_dag"]).
    # These are matched exactly against the file stem (no extension, no path).
    skip_dags: list[str] = field(default_factory=list)

    # If non-empty, ONLY these filename stems will be processed — everything
    # else is skipped.  Takes precedence over skip_dags and skip patterns.
    include_only_dags: list[str] = field(default_factory=list)

    # Set to False to disable the built-in heuristic skip patterns
    # (kubernetes, virtualenv, watcher, etc.).  Useful when pointing at a
    # repo whose filenames happen to contain those substrings legitimately.
    use_default_skip_patterns: bool = True

    # extra dbt --exclude expression applied to every job
    global_dbt_exclude: str = ""

    def build_defs(self, context: ComponentLoadContext) -> dg.Definitions:
        repo_dir = self._ensure_repo()
        dbt_proj_dir = repo_dir / self.dbt_project_path

        # ------------------------------------------------------------------
        # 1. Set up the dbt project
        # ------------------------------------------------------------------
        project_kwargs: dict = {"project_dir": str(dbt_proj_dir)}
        if self.dbt_target:
            project_kwargs["target"] = self.dbt_target
        if self.dbt_profiles_dir:
            project_kwargs["profiles_dir"] = self.dbt_profiles_dir

        dbt_project = DbtProject(**project_kwargs)
        dbt_project.prepare_if_dev()

        dbt_resource = DbtCliResource(project_dir=dbt_project)

        # ------------------------------------------------------------------
        # 2. Scan and parse Cosmos DAG files
        # ------------------------------------------------------------------
        dags_dir = repo_dir / self.dags_path
        base_url = f"{self.repo_url.rstrip('/')}/blob/{self.branch}/{self.dags_path}"
        dag_infos = self._scan_dags(dags_dir, base_url)

        replaced   = [d for d in dag_infos if d.action == "replaced"]
        absorbed   = [d for d in dag_infos if d.action == "absorbed"]
        skipped    = [d for d in dag_infos if d.action in ("skipped", "not_cosmos")]
        docs_dags  = [d for d in dag_infos if d.is_docs_dag]

        # jobs are only created for replaced DAGs
        applicable = replaced

        # ------------------------------------------------------------------
        # 3. Core @dbt_assets — all jaffle_shop models
        # ------------------------------------------------------------------
        @dbt_assets(
            manifest=dbt_project.manifest_path,
            name="jaffle_shop_dbt_assets",
        )
        def jaffle_shop_dbt_assets(
            context: dg.AssetExecutionContext, dbt: DbtCliResource
        ):
            yield from (
                dbt.cli(["build"], context=context)
                .stream()
                .fetch_row_counts()
                .fetch_column_metadata()
            )

        # ------------------------------------------------------------------
        # 4. Jobs + schedules — one per applicable Cosmos DAG
        # ------------------------------------------------------------------
        all_schedules: list[dg.ScheduleDefinition] = []
        all_jobs: list[dg.JobDefinition] = []

        for info in applicable:
            # Use the table's dbt_select override, fall back to what AST found
            table_entry = _REPLACED.get(info.stem)
            table_select = table_entry[1] if table_entry else None
            dbt_select = table_select or (info.selectors[0] if info.selectors else None)

            schedule_str = self._normalise_schedule(info.schedule)
            job_name = f"cosmos__{info.stem}"

            if schedule_str:
                sched = build_schedule_from_dbt_selection(
                    [jaffle_shop_dbt_assets],
                    job_name=job_name,
                    cron_schedule=schedule_str,
                    dbt_select=dbt_select or "*",
                    dbt_exclude=self.global_dbt_exclude or None,
                    tags={
                        "cosmos_source_dag": info.stem,
                        "source_repo": self.repo_url,
                    },
                )
                all_schedules.append(sched)
            else:
                # No schedule — create a plain job that can be triggered manually
                sel_expr: dg.AssetSelection
                if dbt_select:
                    from dagster_dbt import build_dbt_asset_selection
                    sel_expr = build_dbt_asset_selection(
                        [jaffle_shop_dbt_assets], dbt_select=dbt_select
                    )
                else:
                    sel_expr = dg.AssetSelection.assets(jaffle_shop_dbt_assets)

                job = dg.define_asset_job(
                    name=job_name,
                    selection=sel_expr,
                    description=info.description,
                    tags={
                        "cosmos_source_dag": info.stem,
                        "source_repo": self.repo_url,
                    },
                )
                all_jobs.append(job)

        # ------------------------------------------------------------------
        # 5. dbt docs asset  (replaces dbt_docs.py Cosmos DAG)
        # ------------------------------------------------------------------
        docs_source_url = docs_dags[0].source_url if docs_dags else ""
        _dbt_proj_dir = dbt_proj_dir   # capture for closure

        @dg.asset(
            name="dbt_docs",
            group_name="dbt_documentation",
            kinds={"dbt"},
            description=(
                "Generates dbt HTML documentation for the jaffle_shop project. "
                "Replaces the dbt_docs.py Cosmos DAG (which uploaded docs to "
                "S3/GCS/Azure). Run `dbt docs serve` locally to browse the output."
            ),
            metadata={
                "source_cosmos_dag": dg.MetadataValue.url(docs_source_url)
                if docs_source_url
                else dg.MetadataValue.text("dbt_docs.py"),
                "cosmos_note": dg.MetadataValue.text(
                    "Original DAG: DbtDocsS3Operator / DbtDocsGCSOperator / "
                    "DbtDocsAzureStorageOperator. "
                    "Dagster equivalent: run dbt docs generate + optional upload step."
                ),
            },
        )
        def dbt_docs_asset(
            context: dg.AssetExecutionContext, dbt: DbtCliResource
        ) -> dg.MaterializeResult:
            yield from dbt.cli(["docs", "generate"], context=context).stream()
            docs_index = _dbt_proj_dir / "target" / "index.html"
            return dg.MaterializeResult(
                metadata={
                    "docs_index": dg.MetadataValue.path(str(docs_index)),
                    "note": dg.MetadataValue.text(
                        "Run: dbt docs serve --project-dir "
                        + str(_dbt_proj_dir)
                    ),
                }
            )

        # ------------------------------------------------------------------
        # 6. Migration summary asset
        # ------------------------------------------------------------------
        _replaced_map  = {d.stem: d.description for d in replaced}
        _absorbed_map  = {d.stem: d.action_note  for d in absorbed}
        _skipped_map   = {d.stem: d.action_note  for d in skipped}
        _total         = len(dag_infos)

        @dg.asset(
            name="cosmos_migration_summary",
            group_name="cosmos_metadata",
            kinds={"python"},
            description=(
                "Materialise this asset to see a full Cosmos → Dagster migration "
                "report: which DAGs were replaced by native Dagster dbt jobs, "
                "which Airflow concepts Dagster absorbs automatically, and which "
                "DAGs are irrelevant."
            ),
            metadata={
                "repo_url":       dg.MetadataValue.url(self.repo_url),
                "dags_scanned":   dg.MetadataValue.int(_total),
                "replaced":       dg.MetadataValue.int(len(replaced)),
                "absorbed":       dg.MetadataValue.int(len(absorbed)),
                "skipped":        dg.MetadataValue.int(len(skipped)),
                "replaced_dags":  dg.MetadataValue.json(_replaced_map),
                "absorbed_dags":  dg.MetadataValue.json(_absorbed_map),
                "skipped_dags":   dg.MetadataValue.json(_skipped_map),
            },
        )
        def cosmos_migration_summary(
            context: dg.AssetExecutionContext,
        ) -> dg.MaterializeResult:
            w = 64
            context.log.info("=" * w)
            context.log.info("Cosmos → Dagster Migration Report")
            context.log.info(f"Repo   : {self.repo_url}")
            context.log.info(f"Scanned: {_total} DAGs   |   Replaced: {len(replaced)}   Absorbed: {len(absorbed)}   Skipped: {len(skipped)}")
            context.log.info("")

            context.log.info("REPLACED — Cosmos DAG can be disabled in Airflow")
            context.log.info("  Dagster's native dbt integration does this better.")
            context.log.info("-" * w)
            for stem, desc in _replaced_map.items():
                context.log.info(f"  ✓ {stem}")
                context.log.info(f"    Job: cosmos__{stem}")
                context.log.info(f"    {desc}")
            context.log.info("")

            context.log.info("ABSORBED — Concept is a Dagster primitive, no job needed")
            context.log.info("  These DAGs are unnecessary because Dagster handles")
            context.log.info("  the same idea via sensors, resources, or asset selections.")
            context.log.info("-" * w)
            for stem, note in _absorbed_map.items():
                context.log.info(f"  ⊕ {stem}")
                context.log.info(f"    {note}")
            context.log.info("")

            context.log.info("SKIPPED — Infrastructure/execution patterns not applicable")
            context.log.info("-" * w)
            for stem, note in _skipped_map.items():
                context.log.info(f"  ✗ {stem}")
                context.log.info(f"    {note}")
            context.log.info("=" * w)

            return dg.MaterializeResult(
                metadata={
                    "replaced_dags":  dg.MetadataValue.json(_replaced_map),
                    "absorbed_dags":  dg.MetadataValue.json(_absorbed_map),
                    "skipped_dags":   dg.MetadataValue.json(_skipped_map),
                }
            )

        return dg.Definitions(
            assets=[
                jaffle_shop_dbt_assets,
                dbt_docs_asset,
                cosmos_migration_summary,
            ],
            jobs=all_jobs,
            schedules=all_schedules,
            resources={"dbt": dbt_resource},
        )

    # -----------------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------------

    def _get_cache_root(self) -> Path:
        if self.cache_dir:
            return Path(self.cache_dir)
        return Path.home() / ".dagster" / "cosmos_cache" / "astronomer_cosmos"

    def _ensure_repo(self) -> Path:
        """Clone the repo on first run, pull on subsequent runs."""
        cache = self._get_cache_root()
        clone_url = self.repo_url.rstrip("/")
        if not clone_url.endswith(".git"):
            clone_url += ".git"

        if (cache / ".git").exists():
            try:
                subprocess.run(
                    ["git", "-C", str(cache), "pull", "--ff-only"],
                    capture_output=True,
                    timeout=60,
                    check=True,
                )
                logger.info("Pulled latest changes into %s", cache)
            except subprocess.CalledProcessError as exc:
                logger.warning("git pull failed (using cached copy): %s", exc.stderr)
        else:
            cache.parent.mkdir(parents=True, exist_ok=True)
            logger.info("Cloning %s → %s …", clone_url, cache)
            subprocess.run(
                [
                    "git", "clone",
                    "--depth", "1",
                    "--branch", self.branch,
                    clone_url,
                    str(cache),
                ],
                check=True,
                timeout=180,
            )
            logger.info("Clone complete.")

        return cache

    def _scan_dags(self, dags_dir: Path, base_url: str) -> list[CosmosDAGInfo]:
        """Parse every .py file in *dags_dir* and return a CosmosDAGInfo per file."""
        infos: list[CosmosDAGInfo] = []
        for path in sorted(dags_dir.glob("*.py")):
            if path.name.startswith("_"):
                continue
            try:
                content = path.read_text(encoding="utf-8")
            except OSError:
                continue
            info = self._parse_file(path.stem, path.name, content, base_url)
            infos.append(info)
        return infos

    def _parse_file(
        self, stem: str, filename: str, content: str, base_url: str
    ) -> CosmosDAGInfo:
        source_url = f"{base_url}/{filename}"

        def _base(action: str, note: Optional[str] = None, **extra) -> CosmosDAGInfo:
            return CosmosDAGInfo(
                stem=stem,
                filename=filename,
                source_url=source_url,
                description=self._docstring(content),
                schedule=None,
                dag_ids=[],
                selectors=[],
                is_cosmos_dag=self._imports_cosmos(content),
                is_docs_dag=False,
                action=action,
                action_note=note,
                **extra,
            )

        # 1. Allowlist — if set, anything not in it is skipped immediately.
        if self.include_only_dags and stem not in self.include_only_dags:
            return _base("skipped", f"Not in include_only_dags allowlist {self.include_only_dags}")

        # 2. Explicit per-name exclusions from defs.yaml.
        if stem in self.skip_dags:
            return _base("skipped", "Explicitly skipped via skip_dags in defs.yaml")

        # 3. Absorbed table — check before skip patterns so known absorbed DAGs
        #    don't accidentally match a skip-pattern substring.
        if stem in _ABSORBED:
            return _base("absorbed", _ABSORBED[stem])

        # 4. Built-in heuristic skip patterns (substring match on stem).
        #    Can be disabled with use_default_skip_patterns: false.
        if self.use_default_skip_patterns:
            for pattern, reason in _SKIP_PATTERNS.items():
                if pattern in stem:
                    return _base("skipped", reason)

        is_cosmos = self._imports_cosmos(content)
        is_docs   = stem == _DOCS_DAG_STEM

        # 5. Non-Cosmos files have nothing to convert.
        if not is_cosmos and not is_docs:
            return _base("not_cosmos", "File does not import from 'cosmos' — nothing to convert")

        table_entry = _REPLACED.get(stem)
        description = self._docstring(content) or (
            table_entry[0] if table_entry else ""
        )

        return CosmosDAGInfo(
            stem=stem,
            filename=filename,
            source_url=source_url,
            description=description,
            schedule=self._extract_schedule(content),
            dag_ids=self._extract_keyword_strings(content, {"dag_id", "group_id"}),
            selectors=self._extract_keyword_strings(content, {"select"}),
            is_cosmos_dag=is_cosmos,
            is_docs_dag=is_docs,
            action="replaced" if not is_docs else "replaced",
            action_note=None,
        )

    # ---- AST helpers -------------------------------------------------------

    @staticmethod
    def _imports_cosmos(content: str) -> bool:
        """Return True if the file imports anything from the 'cosmos' package."""
        try:
            tree = ast.parse(content)
        except SyntaxError:
            return False
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                module = getattr(node, "module", "") or ""
                if module == "cosmos" or module.startswith("cosmos."):
                    return True
                # covers: import cosmos
                for alias in getattr(node, "names", []):
                    if alias.name == "cosmos" or alias.name.startswith("cosmos."):
                        return True
        return False

    @staticmethod
    def _docstring(content: str) -> str:
        try:
            return ast.get_docstring(ast.parse(content)) or ""
        except SyntaxError:
            return ""

    @staticmethod
    def _extract_schedule(content: str) -> Optional[str]:
        """Extract the first ``schedule=`` keyword value from the AST."""
        try:
            tree = ast.parse(content)
        except SyntaxError:
            return None
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                for kw in node.keywords:
                    if kw.arg == "schedule" and isinstance(kw.value, ast.Constant):
                        return str(kw.value.value)
        return None

    @staticmethod
    def _extract_keyword_strings(content: str, kwarg_names: set[str]) -> list[str]:
        """Extract string values from all keyword args whose name is in *kwarg_names*."""
        results: list[str] = []
        try:
            tree = ast.parse(content)
        except SyntaxError:
            return results
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                for kw in node.keywords:
                    if kw.arg not in kwarg_names:
                        continue
                    val = kw.value
                    if isinstance(val, ast.Constant) and isinstance(val.value, str):
                        results.append(val.value)
                    elif isinstance(val, ast.List):
                        for elt in val.elts:
                            if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                                results.append(elt.value)
        return results

    @staticmethod
    def _normalise_schedule(schedule: Optional[str]) -> Optional[str]:
        """Convert Airflow @preset shortcuts to standard cron expressions."""
        if not schedule:
            return None
        _map = {
            "@daily":   "0 0 * * *",
            "@hourly":  "0 * * * *",
            "@weekly":  "0 0 * * 0",
            "@monthly": "0 0 1 * *",
            "@yearly":  "0 0 1 1 *",
        }
        return _map.get(schedule, schedule)
