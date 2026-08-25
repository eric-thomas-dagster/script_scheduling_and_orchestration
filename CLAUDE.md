# Working notes for Claude Code sessions

Deploy target, tribal knowledge, and past-mistakes-to-avoid. Update this
file when you learn something you'd have wanted to know at the start of
the session.

## What this repo is

Demo repo for the **script-orchestrator** Dagster component. Purpose:
show Prefect / Airflow users how their existing DAGs / flows / dbt
projects become first-class Dagster assets without a rewrite. Everything
here — Prefect example scripts, the `script_orchestrator` component
library, the `dagster_orchestrator` code location, the walkthrough +
demo-script docs — is aimed at that story.

Two customer-facing subprojects:

- [`script_orchestrator/`](script_orchestrator/) — the component library
  (`ScriptGithubComponent`). Published as `script-orchestrator` on PyPI
  and via git URL. Consumers pin extras: `[prefect]`, `[airflow]`,
  `[dask]`, `[pyspark]`, `[all]`.
- [`dagster_orchestrator/`](dagster_orchestrator/) — the Dagster+
  Serverless code location we deploy to `ericthomas-dagster/prod`. Two
  defs modules under `dagster_orchestrator/defs/`:
    - `prefect_demos/` — orchestrates the upstream `PrefectHQ/demos` repo
      (classic `@flow` / `@task` scripts).
    - `prefect_materialize_demos/` — orchestrates the example scripts
      inside this repo (`prefect_examples/`), including the Prefect 3.4
      `@materialize` primitives and `dbt_via_prefect_demo.py`.

## Deploying to Dagster+ (ericthomas-dagster/prod)

```bash
cd dagster_orchestrator
set -a && source ~/dagster-graphql-agent/.env && set +a  # or wherever a valid DAGSTER_CLOUD_API_TOKEN lives
dg plus deploy --yes
```

**Deploys off `master`.** `dg plus deploy` reads the current git branch —
`master` targets `prod`, any other branch creates a branch deployment.

**Auth:** `DAGSTER_CLOUD_API_TOKEN` must be exported. It's user-scoped, so
different `.env` files across `~/` may have differently-scoped or
rotated tokens. If a `401 Unauthorized` comes back, don't guess at other
tokens — ask the user which env to source.

**How long:** 8–12 minutes end-to-end. Bulk of it is `uv sync` in the
Docker builder + `dbt deps/seed/run/docs generate` on jaffle_shop
(happens at image build time, see `Dockerfile`).

**Where output lands:** background task logs are truncated to the last
few lines (see `tail -5 /private/tmp/claude-501/.../tasks/<id>.output`).
The successful tail is:

```
#15 <N>s Wrote state to /tmp/dagster_orchestrator_prefect_demos/state.json (<bytes>)
#15 <N>s Wrote state to /tmp/dagster_orchestrator_prefect_materialize_demos/state.json (<bytes>)
Updated code location dagster_orchestrator in dagster-cloud.
Agent synced changes to dagster_orchestrator. Changes should now be visible in Dagster Cloud.
```

State-file byte size is the fastest signal that the parser found scripts.
Empty / minimal state.json ≈ 166 bytes; a healthy prefect_materialize_demos
state is 15–20 KB.

## How the script-orchestrator dep is wired

`dagster_orchestrator/pyproject.toml` pins `script-orchestrator[prefect]`
via git URL:

```toml
[tool.uv.sources]
script-orchestrator = { git = "https://github.com/eric-thomas-dagster/script_scheduling_and_orchestration", subdirectory = "script_orchestrator", branch = "master" }
```

Workflow for pushing an upstream fix:
1. Edit `script_orchestrator/**`.
2. Commit + push to `master` on this repo.
3. `cd dagster_orchestrator && uv lock --upgrade-package script-orchestrator`
   → confirms new SHA lands in `uv.lock`.
4. Redeploy.

The lock upgrade step is easy to forget; without it, the Docker build
reuses the old cached SHA.

## `_build_dbt_defs` (formerly `_build_cosmos_dbt_defs`)

Since `f42cb7b` this function is called any time `dbt_project_path` is
set, and its Cosmos-migration half (scan + per-DAG jobs/schedules +
`cosmos_migration_summary` asset) is gated on `self.airflow_enabled`.
Prefect-only deploys get exactly the pure `@dbt_assets` expansion and
nothing else — no phantom Cosmos summary asset.

### Deliberately NOT emitting a `dbt_docs` asset

Prior versions of this file emitted a `dbt_docs_asset` that ran
`dbt docs generate`. **Dropped in <followup>** because:

- In Dagster+ Serverless the compute container is ephemeral; `target/
  index.html` gets written and immediately discarded when the run ends.
- No volume, no webserver, no upload — the docs are generated into the
  void, but the asset in the catalog LOOKS meaningful. Worse than not
  having it.
- If a customer wants real docs regen, they wire an `@op` that uploads
  the built docs to their actual serving location (S3 / GitHub Pages /
  Netlify / etc.). We don't guess at their sink.

Don't reintroduce it "for completeness" — it was intentionally cut.

## `dbt_project_path` resolution

Historically the resolver did `state.repo_path / dbt_project_path`.
That broke when the dbt project ships with the code location (as
jaffle_shop does here, cloned into `/app/dbt/jaffle_shop` at Docker
build time) instead of inside the scripts repo. As of `d89f0e7`, the
resolver tries `state.repo_path` first then `Path.cwd()`. Absolute paths
pass through.

Practical result: `dbt_project_path: dbt/jaffle_shop` in defs.yaml works
whether the customer vendors dbt inside their scripts repo or installs
it alongside their Dagster code location. Don't hardcode `/app/...` in
defs.yaml — it works but it's container-specific and reads badly to a
customer copying the config.

## State persistence lives in /tmp (temporary)

`build_state.py` writes to `/tmp/dagster_orchestrator_prefect_demos/` and
`/tmp/dagster_orchestrator_prefect_materialize_demos/`. The `Dockerfile`
COPYs those dirs from the builder into the runtime image. Each
`defs/*/prefect_(demos|materialize_demos)/__init__.py` bypasses the
`StateBackedComponent` framework entirely and calls
`build_defs_from_state` directly with a hand-rolled `_Ctx()` whose
`.path` points at the /tmp state dir.

Follow-up worth doing: move state out of /tmp into somewhere under
`/app`. `/tmp` for persistent app state is a code smell and confuses
anyone reading the deploy setup.

## The four kinds of lineage (canonical framing)

Used in `PREFECT_DEMO_SCRIPT.md` and worth reusing when the user asks
about lineage:

1. **Explicit asset deps** — declared in Prefect source via
   `@materialize(asset_deps=[…])`.
2. **AST-inferred asset deps** — parser walks the flow body call graph;
   works through same-file subflows. Cross-file subflows are a known
   gap; being worked on.
3. **Operator-declared asset deps** — `file_overrides.depends_on:` in
   `defs.yaml`. No source-repo change required. This is the migration
   story that matters most for real Prefect estates.
4. **Op-level data-flow edges** — inside a single wrapper asset,
   `@task` → `@op`; function-argument passing becomes real op DAG
   edges via a fake-Prefect-module monkey-patch.

## Things the user has corrected me on

- Renaming things "PREFECT_*" when the demo is Prefect-scoped is
  important — generic names get lost. See the `PREFECT_DEMO_SCRIPT.md`
  and `PREFECT_WALKTHROUGH.md` filenames.
- Don't hardcode container paths (`/app/...`, `/tmp/...`) in yaml config
  that a customer would read as a template.
- Deploy is my job when they say "deploy" — don't stop to ask for the
  invocation. Auth might need help; the build command doesn't.
