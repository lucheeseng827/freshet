# freshet-dbt — what Freshet would have pruned from your dbt run

Point Freshet at an existing dbt project and get a number about *your* project: of the
models your last run built, how many did this change actually need? It reads the
artifacts dbt already wrote — nothing is installed into the project, dbt is never run,
and no warehouse is touched. This is the adoption wedge from
the planner core's product roadmap (Phase 1), shipped as `freshet explain`.

```sh
freshet explain --against ./jaffle_shop                          # graph facts only
freshet explain --against ./jaffle_shop --state ./prod-artifacts # the pruning number
freshet explain --against ./jaffle_shop --state ./prod-artifacts --json
```

* `--against DIR` — the dbt project root. Reads `DIR/target/manifest.json` (from
  `dbt parse`, `compile`, `run` or `build`) and, if present, `DIR/target/run_results.json`.
  A `target` directory itself is accepted too.
* `--state DIR` — dbt's own `--state` convention: a directory holding the `manifest.json`
  to compare against, typically production's. A path to the file works too. Without it
  there is no change to plan, so the report gives graph facts and **no pruning number**.

## What it prints

On the fixture in `tests/fixtures/jaffle_shop` (production manifest vs. a development
build in which `stg_orders` changed `amount_cents / 100` to `amount_cents / 100.0`):

```text
Freshet would rebuild 7 model(s):
    stg_orders              directly changed
    customer_regions        directly changed
    orders_daily            downstream of stg_orders via [order_date, orders]  (model-level lineage)
    orders_enriched         downstream of stg_orders via [amount]
    weekly_orders           downstream of orders_daily via [order_week, orders]  (model-level lineage)
    mart_big_orders         downstream of orders_enriched via [big_orders, customer_name]
    mart_revenue            downstream of orders_enriched via [revenue]

  vs the last `dbt build` (9 model(s)): 2 fewer — 22.2% avoided
      it builds, Freshet does not: mart_customers, stg_customers
  vs state:modified+ (Freshet's approximation) (8 model(s)): 1 fewer — 12.5% avoided
      it builds, Freshet does not: mart_customers
```

`--json` prints the same report as a document (`report_version: 1`); a rebuild's
`reason` uses freshet-embed's wire encoding (`"directly_changed"` or
`{"downstream": {"because_of", "via_columns"}}`).

## How a manifest becomes a plan

1. **Edges from the manifest.** A model's upstream models are its `depends_on.nodes`,
   restricted to models. dbt resolved every `ref()` to write them, so they are right even
   for SQL Freshet cannot read — and the plan's correctness rests on them.
2. **Column lineage from the SQL**, per model in dependency order, through
   `freshet_sql::analyze`: `compiled_code` when the manifest has it, otherwise
   `raw_code` with `{{ ref(...) }}` / `{{ source(...) }}` rewritten (any other Jinja makes
   the model unreadable). Compiled SQL names relations by **alias**, so the catalog is
   keyed by relation name and every reference is mapped back to a model name.
3. **Row-deciding references count.** A filter, join key, grouping or `DISTINCT` decides
   which rows every column has, so it joins every column's upstream.
4. **Anything uncertain drops to model-level lineage** — the model rebuilds whenever
   anything it reads changes: unreadable SQL (Jinja, Python, a parse error), a relation
   name two relations share, a filter on a name computed in a CTE, a cross or natural join,
   a table function. A model always has at least one column (its documented ones, or `*`).
   Ephemeral models, which dbt inlines into their readers' compiled SQL, count in full
   against every column of each reader.

## What counts as changed

Both manifests are read the same way — compiled SQL for a model only when *both* have
it — and each model is fingerprinted with `freshet-fingerprint`'s `AstFingerprinter`
(SQL + column names + upstream names), after replacing every rendered relation name
with the node's `unique_id` so a development schema is not a change. Also a change, as in
dbt's own `state:modified`: the materialization, the relation it builds, a macro it calls
(transitively), and a non-model input (a seed's contents, a snapshot, a source's
location). New models are changed; removed ones are listed.

The planner core then marks every output column of a changed model as changed —
unless the only edit is inside individual projection expressions, in which case the
**column diff** narrows it to the columns those expressions reach (followed by name
through every CTE). Any structural edit, or a changed column that later feeds a filter,
join or grouping, means every column.

## Reading the numbers

* **Counts are models, not cost.** A view and a table each count as one rebuild.
  Ephemeral models are never built on their own and are left out of every count.
* **The last run** is `run_results.json` when it records a `run` / `build` / `retry`;
  otherwise (no file, or it came from `dbt compile`) every model counts — what a plain
  `dbt run` builds. It assumes that run is the one that built this change.
* **`state:modified+` is an approximation**: the models Freshet found changed plus every
  descendant. dbt decides "modified" from its own checksums, so its selection can differ.
* **The saving is net**: models the baseline built that Freshet skips, minus models
  Freshet builds that the baseline did not.

Every rule above leans towards rebuilding *more*, so the report can understate the
saving but is built not to overstate it. The notes it prints say which assumptions
applied to your project.

## Known limits

* A model compared on raw SQL (either manifest lacks compiled SQL for it) cannot see a
  change that only appears once Jinja renders — a `var`, a target-dependent branch.
  Compile both sides to close it; the report says how many models this applies to.
* Compiled SQL can differ between environments with the source untouched (an
  `is_incremental()` branch compiles only where the table exists). Such a model counts as
  changed and is labelled `compiled sql only (source unchanged)`.
* Changes do not propagate *through* snapshots (a snapshot's readers see new data only
  after `dbt snapshot` runs); a changed snapshot definition still marks its readers.
* Documented columns of sources and seeds are trusted as their schema.
* A custom `target-path` is not read from `dbt_project.yml`; pass the target directory.

## Tests

`cargo test -p freshet-dbt`: unit tests per module; `tests/fixture.rs` over the
hand-written artifacts in `tests/fixtures/jaffle_shop` (manifest v12 and run-results v6,
both valid against dbt's published JSON schemas); `tests/scenarios.rs`, one small
manifest per rule. `crates/freshet-cli/tests/explain.rs` covers the CLI.
