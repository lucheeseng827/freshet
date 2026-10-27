# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/) (pre-1.0: minors may change the API).

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
