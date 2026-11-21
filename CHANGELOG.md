# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/) (pre-1.0: minors may change the API).

## [Unreleased]

## [0.2.0] — 2026-11-21

### Added
- `--with-next-state` on `freshet plan --json` and `freshet submit`: the plan carries
  `next_state`, the exact snapshot `plan --commit` writes once it has run (left out of
  a plan over its `--budget`). For a consumer that reviews plans before they run and
  holds the commit it later receives to the plan it approved. Opt-in: a plan without
  the flag is what it was.
- **A cold plan says so: wire contract `planner-embed/6`.** A code-change plan made
  with no committed state to diff against — no `prior_state`, a state file or hosted
  state with no commits yet, or a snapshot with no fingerprints — carries
  `"cold": true`. Every model is `directly_changed` in such a plan, and only the
  planner can tell that apart from a change that really touched every model, so it
  says it: dagron-state holds a cold plan for a reviewer by name. The key is written
  only when true, so a warm plan's JSON is what it was under v5; a restatement is
  never cold. Set by `freshet_embed::plan` (and every binding over it),
  `freshet plan`, `freshet submit` and `freshet-serve`; `freshet plan` without
  `--json` prints a line saying the plan is cold. The C ABI, the wheel, the Python
  SDK's `Plan.cold` and `freshet_plan`'s `Plan.cold` follow.
- `freshet move --from <dialect> --to <dialect>`: a move to another warehouse,
  partition by partition. Each unit's unload (through the source's own export, or
  DuckDB for Postgres), its load on the other side in one transaction, and a parity
  check of both sides, as a `freshet-move/1` document for dagron to run. Executes
  nothing.
- `freshet-translate` (`integrations/translate`): translates a project's SQL to
  another warehouse with sqlglot, writes it as a project for the new side, and has
  Freshet's planner name the models the translation changed in substance and the
  models that read them. Hand-written overrides for what sqlglot cannot translate.
- `--strict-refs` on every command, and `strict_refs` on the wire request: a table
  that is neither a model nor declared in `sources.json` is an error naming the
  models and sources one edit from it (`did you mean `stg`?`), never an external
  table that silently cost a misspelled model its edge. Wire code
  `undeclared_table`, only when asked. Meant for CI.
- `freshet-serve`: `FRESHET_SERVE_TOKEN` makes `POST /rpc` require
  `Authorization: Bearer <token>` (a 401 otherwise; `/healthz` stays open), and
  `--watch` reloads the project from disk when its files change (the `watch`
  feature, in the image). No gRPC transport, by decision.
- `freshet cost record`: what a model's runs cost, in the team's own unit, kept in
  `costs.json` beside the project and never sent anywhere. With it, `plan` and
  `restate` print a measured estimate beside the rate card (each model at its own
  runs' median, the rest extrapolated at the measured rate per CU, the measured share
  stated) and the rebuild work avoided in that unit. `--json` is unchanged.
- `freshet history --state https://…`: hosted state's commits, newest first, each
  with what it changed and removed and the ledgers it moved, and the committer's
  context; `--limit`, `--json`. A state file keeps no history, and says so.
- `freshet-state-server`: hosted state a team runs itself. One directory holds a state
  per project (`--state https://<host>/v1/<name>`), each commit lands by
  compare-and-swap, and every commit is kept with the server's diff of what it changed,
  the newest `--keep` (100) of them. `FRESHET_STATE_SERVER_TOKEN` may commit;
  `FRESHET_STATE_SERVER_READ_TOKEN` may only read. A crash loses at most the commit in
  flight. In the image.
- The Freshet console (`apps/freshet-console`): a web page for a state server's states.
  Every state, each one's commits as a ledger of what each moved, and any kept
  version's snapshot, filterable and downloadable as the exact `state.json` (u64
  fingerprints shown as hex, written back digit for digit). Read-only; it signs in
  with either server token and says when a read-only one would do. Light and dark,
  a focus mode, a ⌘K palette and `g` shortcuts. `?demo` runs it on sample data.
  No build step and no npm.
- `freshet-state-server --console <dir>` (`FRESHET_STATE_SERVER_CONSOLE`) serves it at
  `/console/` under a strict content security policy; on in the image. Two reads for
  it: `GET /v1/` lists every state and what the caller's token may do, and
  `GET /v1/<name>/versions/<v>` returns a kept commit with its snapshot. `--port 0`
  takes a free port and the startup line names it.
- `freshet history --before <version>`: the next page of history, as the previous one
  names it. Refused against a server that does not page.
- `freshet-dagster` (`integrations/dagster`): a Freshet state plan as a Dagster job,
  one op per model wired from the plan's own edges. Each op runs a command under
  `bash -c` with Freshet's placeholders expanded, or your `run(context, task)`, for
  example to run the plan's rendered SQL on a connection a resource holds. Ops log
  the planner's facts and its warnings. Anything else in `{{ }}` is refused, since
  Dagster renders nothing afterwards.
- `freshet-plan` (`integrations/plan`): the plan reader `freshet-airflow` had, as a
  package of its own (standard library only), and the explanations both adapters
  give (the headline, why each model runs, what it writes, the warnings), worded
  once in `freshet_plan.explain`.
- The browser planner page re-reads a chosen folder and plans it with one click, in
  browsers that keep a folder handle (Chromium). Elsewhere it says, beside the folder
  button, that a folder is read once per pick and nothing is written back.

### Changed
- Without `--strict-refs`, a table one edit from a model or source is warned about on
  stderr. It is still planned as external, as before.
- `[]` in `sources.json` declares a table without its columns. `*` over it is now
  refused, as over any table whose columns are unknown, instead of expanding to no
  columns at all.
- `freshet plan` no longer parses every model twice: the AST fingerprint hashes the
  tree the build already parsed. A one-model edit at 100,000 models plans in 8.6 s
  on a 2.1 GHz host, from 10.0 s. Fingerprints are unchanged, so committed state
  still matches.
- `freshet-serve` keeps each model's fingerprint half beside its SQL, so a replan
  parses only SQL it has not seen: 0.39 s at 100,000 models, from 3.5 s.
- The README installs from a checkout of the release tag. Nothing is published to
  crates.io, PyPI or Docker Hub yet, which 0.1.0's README wrongly implied.
- `freshet-airflow` and `freshet-dagster` require `freshet-plan` 0.2.
- `freshet-airflow` depends on `freshet-plan`, and `freshet_airflow.plan`
  re-exports it under the names it had. Nothing else changes for a DAG.

### Fixed
- The browser planner page parses the planner's response without rounding any
  integer: one past 2^53 arrives as an exact `BigInt` (`assets/json.js`), so a u64
  the wire carries, fingerprints today or a field added later, is never silently
  changed in the page. The downloaded `plan.json` keeps the exact digits. The wire
  format is unchanged.

## [0.1.0] — first public release

### Added
- `freshet` CLI: `plan`, `restate`, `fingerprint`, `graph`, `explain --against <dbt
  project>`, `submit`. State in a local `state.json`, or hosted state over HTTPS with
  compare-and-swap commits (exit code 3 when another commit landed first).
- Column-level lineage from SQL, AST-canonical fingerprints (cosmetic edits never
  rebuild), and partition scoping for incremental models: lookback windows,
  non-aligned grains, watermarks, and an interval ledger of owed partitions.
- Replace strategies (`insert_overwrite`, `delete_insert`, `merge`, `replace_where`,
  `full_refresh`) and `--sql <dialect>` rendering for postgres, snowflake, bigquery,
  databricks and redshift. A declaration that cannot be honoured widens to a full
  refresh and says why.
- `--budget N` caps a plan at N compute units and reports the headroom left.
- `freshet-serve`: a daemon that keeps the parsed project warm and reparses only
  what changed, with a revision-stamped plan → commit handoff.
- Bindings over one frozen JSON wire contract (`planner-embed/5`): C ABI
  (`freshet-ffi`), WebAssembly (`freshet-wasm`), Python (`freshet-planner`).
- `freshet-sdk` (stdlib-only Python client for the daemon) and `freshet-airflow`
  (a plan as an Airflow DAG, one task per model).
- A reproducible comparison against dbt-core, dbt v2 and SQLMesh on one generated
  topology (`scripts/compare.sh`, results in `results/`, write-up in
  `docs/BENCHMARKS.md`).
