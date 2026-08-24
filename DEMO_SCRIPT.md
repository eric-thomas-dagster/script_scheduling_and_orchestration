# Demo script — Prefect → Dagster (~15 min)

Companion to `WALKTHROUGH.md`. That doc lays out capabilities; this doc is the **talking track** — what to say, in what order, while clicking through the UI.

**Deployment**: https://ericthomas-dagster.dagster.cloud/prod/locations/dagster_orchestrator/assets
**Pre-check before you present**:
- Sensors and schedules are running.
- You've done a warm-up load of the asset graph page so it renders instantly.
- Have a second tab open on the Runs page for Section 3 (running a materialization live).

---

## Opening — 30 seconds

> "This deployment orchestrates a Prefect repo — actually two: Prefect's own `PrefectHQ/demos` and a companion set of scripts I wrote that use the newer Prefect 3.4 asset primitives. What you're going to see is what happens when we pull those scripts into Dagster without a rewrite. Prefect execution semantics stay; Dagster's asset catalog, lineage graph, freshness SLAs, and dbt-native views come on top for free."

Click into **Assets → Global Asset Lineage**. Zoom out so all ~33 assets show at once.

> "That's the whole graph — one code location, 33 assets, three sources: classic Prefect flows on the left, Prefect `@materialize` assets in the middle, and dbt models on the right. All wired together with real dependency edges."

---

## Section 1 — "Bring your Prefect repo, no rewrite" (2 min)

**Click**: `prefect_model_training` (in the `ml_pipeline` group)

> "This is a plain Prefect flow — I didn't touch its source. Standard `@flow` decorator on top, a couple of `@task`s inside. Look at the kinds: `[ml, s3]` — those came from us saying so in the defs.yaml file_overrides, no code change in the Prefect repo. Owners `ml-platform@example.com` — same story."

**Click the "Definition" tab** or expand the op graph.

> "This is the important part. Every `@task` in the original Prefect flow is a first-class Dagster `@op` inside this asset. If the original had `@task(retries=3, retry_delay_seconds=10)` — and it did — those retry semantics are preserved as a Dagster `RetryPolicy` on the op. Your Prefect user's `@task` is running as a Dagster `@op` at execution time, keeping the retry-and-caching behavior they wrote."

**Click on** `prefect_model_inference` (in the same group).

> "And there's the lineage. `model_inference` depends on `model_training`. I didn't fork the PrefectHQ demos repo to add that arrow — I said it in yaml on our side:"

```yaml
file_overrides:
  model_inference:
    depends_on: [model_training]
```

> "For any script in a repo we don't own, we can layer on lineage, owners, kinds, group names — all without touching upstream code. That's the 'zero rewrite' promise made concrete."

---

## Section 2 — "@materialize → real Dagster assets" (3 min)

**Search bar → "customer_dim"**. Click `s3:/analytics_lake/curated/customer_dim/parquet`.

> "Now let's look at the modern Prefect flow. This asset was declared in a Prefect `@materialize` — the URI itself is the asset key. Not a wrapped-script name, the actual `s3://` path."

**Point at the metadata panel** — owners, description, URL.

> "Owners `analytics-eng@example.com`. Description came from `AssetProperties(description=...)` on the Prefect Asset. There's a link out to our internal data catalog — that's also from `AssetProperties(url=...)`. All auto-mapped."

**Click the upstream** — `postgres:/prod_warehouse/public/customers_raw`.

> "This upstream is an *external* asset — nothing in our codebase materializes it. But the Prefect code declared `Asset(key='postgres://...', properties=AssetProperties(name='Raw Customers Table', owners=['platform-team@...']))` — so Dagster's catalog shows the platform team's postgres table with its full context, even though we don't own it. External assets are how we cross team boundaries in one graph."

**Click the downstream** — `snowflake:/prod/ANALYTICS/MARTS/CUSTOMER_METRICS`.

> "Notice the two kinds: `[dbt, prefect]`. The Prefect code said `materialized_by='dbt'` on the `@materialize`. Dagster's UI now knows this asset is dbt-managed. The dbt tag is filterable, groupable, alertable."

---

## Section 3 — "Runtime artifacts as typed metadata" (2 min)

**Click** `s3:/lake/curated/customers/parquet` (from `materialize_full_demo`).

**Click "Materialize"** in the top right. Wait ~20s.

> "The Prefect flow behind this asset calls `create_markdown_artifact`, `create_link_artifact`, and `create_table_artifact` — Prefect's built-in mechanism for capturing run output. Let's run it and see what Dagster does with those."

**Once the materialization completes → click into the materialization event**.

> "Look at the metadata panel. That markdown block? That's the `create_markdown_artifact` call. It renders in Dagster's UI the same way it would in Prefect's. Same for the link — clickable, points to whatever URL the flow generated. The table artifact became a `TableSchemaMetadataValue` — sortable, viewable in the UI."

> "This is the runtime side. AssetProperties gave us static metadata at definition time; `add_asset_metadata` and artifacts give us dynamic metadata per run. Both flow through the same Dagster catalog."

---

## Section 4 — "Implicit dependency inference" (2 min)

**Search bar → "scores"**. Click `s3:/models/scores/parquet`.

> "This is my favorite. Look at the lineage — three assets in a chain, `raw/events → curated/features → models/scores`. The Prefect code that produced these? *None* of them set `asset_deps=[…]` explicitly."

**Show the source panel** (or bring up `materialize_implicit_deps_demo.py` in a second tab).

> "In the flow, we just call one function from another — pass the result of `build_features(events)` into `train_scores(features)`. That call-graph shape IS the dependency graph. The parser walks the AST and builds Dagster deps edges from what the code already says."

> "This is the point: Prefect users don't have to duplicate their dep graph in a separate config to get lineage. The code IS the config."

---

## Section 5 — "Partitions and schedules from prefect.yaml" (2 min)

**Search bar → "partitioned"**. Click any output of `materialize_partitioned_demo`.

> "Partitioned Prefect flow → partitioned Dagster asset. Each partition here is one Prefect run with a specific parameter value. If we open the partitions strip, we can drag-select a range and backfill — which is Dagster's UI on top of Prefect's parameterization."

**Now click** `Schedules` in the left nav.

> "Only a handful of schedules right now — one per `prefect.yaml` deployment entry we found in the source repos. But look at the cron: `0 */6 * * *`. That's read straight from the Prefect deployment YAML. No copy-paste on our side."

---

## Section 6 — "Freshness SLAs" (2 min)

**Back to** the `prefect_prefect_flow_example` asset (has a schedule).

**Click the "Freshness" section**.

> "Because we set `auto_freshness_policies: true` in the defs.yaml, Dagster took that same `0 */6 * * *` cron and derived a `FreshnessPolicy` — 'this asset should be materialized every 6 hours; allow 50% slack.' The state widget shows PASS / WARN / FAIL right in the UI."

> "This is critical for Prefect users: your `prefect.yaml` already has your schedule. You don't rewrite it as a Dagster freshness spec. Dagster reads what's there and computes the SLA."

> "And it's alertable — any freshness violation triggers a Dagster alert policy. Slack, email, webhook, PagerDuty. Your operational data quality story on Prefect's own configs."

---

## Section 7 — "dbt story — the crown jewel" (3 min)

**Assets → filter kind: dbt**. You'll see the jaffle_shop dbt models.

> "This is where Prefect users' eyes should light up. In Prefect, when you use `PrefectDbtRunner` to run a dbt project, it's ONE opaque task — one log stream, one status, no per-model view. If a single model fails, you re-run everything."

> "Watch what happens here."

**Click** `stg_customers`.

> "This is ONE Dagster asset per dbt model. The whole jaffle_shop dbt project is expanded into individual assets — because the Prefect code called `PrefectDbtRunner(project_dir='dbt/jaffle_shop')`, and Dagster reads the same manifest.json dbt would."

**Click the "Schema" or metadata tab**.

> "Column names, types — from `catalog.json`. Not manually curated on our side. dbt generated it, we read it."

**Click the "Checks" tab**.

> "Every dbt test on this model is a Dagster asset check. `not_null customer_id`, `unique customer_id` — those are dbt tests in the model's schema.yml file, and Dagster shows them as pass/fail checks with actual test result data. You get pipeline-level assertion status without writing any Dagster-specific check code."

**Go back to the lineage view — filter kind=dbt**.

> "The dbt DAG in Dagster — same shape as `dbt docs serve` would show, but embedded in the unified graph with Prefect assets. Upstream Prefect `@materialize` writing to a source? It shows up as an incoming edge to the source model. Downstream Prefect `@materialize` reading a mart? Outgoing edge. It all connects."

---

## Section 8 — "The unified graph" (1 min)

**Assets → Global asset lineage**. Zoom out fully.

> "This is the story in one screen: Prefect classic flows on the left with their internal ops. Prefect `@materialize` assets in the middle, with real URIs and cross-team lineage. dbt models on the right, expanded per model with test status. All connected. All in one catalog. All from a Prefect repo, unchanged."

**Filter → kind: prefect + kind: dbt at the same time**.

> "The reality: your Prefect team's assets and your dbt team's models are two halves of the same graph. Dagster is the place they meet."

---

## Closing — 1 minute

> "So, what changed from the Prefect user's perspective?"
> ""

> "**Nothing** in the code. The `@flow`s still run as Prefect flows, the `@task`s still have their retry semantics, the `PrefectDbtRunner` still calls dbt. Prefect stays the execution engine."

> ""

> "**Everything** in the observability layer. Now they have a first-class asset catalog with owners, kinds, descriptions, and freshness SLAs. Cross-team lineage across Prefect + dbt + Airflow in one graph. Per-model dbt visibility instead of one opaque `run dbt` step. Real Dagster alert policies on top of the same crons that were already in `prefect.yaml`."

> ""

> "**Migration path?** Optional and gradual. Some teams stop here — they wanted the catalog + observability. Others move Prefect flows one at a time into native Dagster `@asset`s over months. Both are valid."

> ""

> "That's the pitch. Questions?"

---

## Follow-up questions to be ready for

**Q: "Do we need to rewrite our Prefect blocks?"**
> Runtime: no — they still work in the running flow. Projection into Dagster resources: not today (roadmap).

**Q: "Does this replace our Prefect workers?"**
> No. Prefect workers still execute the flows. Dagster orchestrates ABOVE them — asset catalog, scheduling, alerting. Execution engine unchanged.

**Q: "What about our custom sensors / event triggers in Prefect?"**
> Dagster sensors + Prefect event-driven runs are two sides of the same idea. We can wire a Dagster sensor that triggers a Prefect deployment, or vice versa. Not automatic in the mapping today, but easy to hand-wire.

**Q: "Cost?"**
> Dagster+ orchestration cost is metered on materialization events and run-minutes. Prefect execution cost is unchanged. For a lot of teams the net is a wash — you drop some Prefect Cloud tier (if they had one for observability) and pick up Dagster+ for orchestration.

**Q: "What if we have thousands of flows?"**
> The parser handles them. The initial build_state pass can take a couple minutes at scale (git clone + AST walk), but that's a Docker-build-time cost, not runtime. Once deployed, code-location load is standard Dagster.

**Q: "Airflow users too?"**
> Same component, `airflow_enabled: true` in the defs.yaml. The mapping is symmetric — Airflow DAGs → Dagster assets, DAG dependencies → asset deps, Airflow variables + connections injected as expected. Not covered in this demo because we scoped Prefect.

---

## If the demo blows up mid-flight (short recovery)

- **Asset materialization hangs or fails**: skip Section 3 and describe it verbally. The static story (Sections 1, 2, 4-8) is enough.
- **UI is slow**: pre-load the global asset lineage on your first click and it caches for the session.
- **Freshness widget missing**: `auto_freshness_policies: true` must be set on the code location AND the asset needs a schedule to derive from. Verify both.
- **dbt kinds missing**: means jaffle_shop compile didn't produce a catalog.json. Check the build log for `dbt docs generate` errors — usually a missing `dbt seed` step or a bad profiles.yml.

---

## Assets I most want people to remember

| Asset | Why |
|---|---|
| `prefect_model_inference` ← `prefect_model_training` | The "we can add deps without touching the source repo" story |
| `snowflake:/prod/ANALYTICS/MARTS/CUSTOMER_METRICS` | The `[dbt, prefect]` kind combo — cross-tool assets |
| Any jaffle_shop model (e.g. `customers` or `orders`) | The "opaque prefect_dbt → per-model expansion" story |
| `s3:/models/scores/parquet` | Implicit dep chain without asset_deps= |
| Anything with a freshness widget | Auto-derived SLA from prefect.yaml cron |
