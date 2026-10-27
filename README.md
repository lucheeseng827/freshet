# Freshet

**What must rebuild after this change?** Freshet reads a directory of SQL models and
answers at model and partition level. It follows lineage to every model a change reaches,
names the columns that carry it, narrows incremental models to the partitions that moved,
and prices the plan. It never runs a query: your orchestrator does that.

Get started at **[freshet.mancube.net/docs](https://freshet.mancube.net/docs/)**.

- **Standalone.** No dbt, no warehouse connection. A CLI, a daemon, and bindings for C,
  Python and WebAssembly, all speaking one frozen JSON contract.
- **Cosmetic edits never rebuild.** Models are fingerprinted by their syntax tree, not
  their text.
- **Widens, never narrows.** When a partition-scoped write can't be honoured, the plan
  widens it to a full refresh and says why.
- **Fast.** On a generated project of 10,240 models, the one-model-edit plan took
  1.039 s, against 80.322 s for `dbt ls` (dbt-core), 12.865 s for dbt v2 and 486.657 s
  for SQLMesh, with every tool selecting the same 80 models. One machine's medians,
  commit and host recorded: [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

## Install

```sh
cargo install freshet-cli                 # the `freshet` binary
docker run --rm mancube/freshet --help    # linux/amd64 + linux/arm64
pip install freshet-planner               # Python bindings (abi3 wheels)
pip install freshet-sdk freshet-airflow   # daemon client, Airflow adapter
```

Prebuilt `freshet` and `freshet-serve` binaries for Linux, macOS and Windows are
attached to every [release](https://github.com/lucheeseng827/freshet/releases).

## Try it

```sh
mkdir -p models
echo 'select a, b from raw'   > models/stg.sql
echo 'select a as x from stg' > models/mart_a.sql
echo 'select b as y from stg' > models/mart_b.sql
echo '{"raw":["a","b"]}'                            > models/sources.json
echo '{"stg":"incremental","mart_a":"incremental"}' > models/materializations.json

freshet plan --project models --state state.json --commit
```

```text
backfill plan (3 model(s), topological order):
  stg                      full                         (directly changed)
  mart_a                   full                         (directly changed)
  mart_b                   full                         (directly changed)
rebuild work: 3.000 CU planned, 0.000 CU (0%) avoided — rebuilding everything downstream of the change whole would be 3.000 CU
committed state -> state.json
```

Plan again and nothing rebuilds (`nothing to backfill — state is up to date.`).
Restate one day of `stg`, and the plan follows lineage: `mart_a` reads `stg` through
`x` and is incremental, so it replaces the same partition; `mart_b` is a table, so it
widens to a full refresh.

```text
$ freshet restate --project models --model stg --partitions 2026-06-20
backfill plan (3 model(s), topological order):
  stg                      partitions[2026-06-20]       (directly changed)
  mart_a                   partitions[2026-06-20]       (downstream of stg via [x])
  mart_b                   full                         (downstream of stg via [y])
rebuild work: 1.200 CU planned, 1.800 CU (60%) avoided — rebuilding everything downstream of the change whole would be 3.000 CU
```

Add `--json` for the wire `PlanResponse` every binding returns, and `--sql postgres`
(or `snowflake`, `bigquery`, `databricks`, `redshift`) for the statements a runner
executes. One CU is one full rebuild of one table model; a partition is 0.1.

## What else is here

| | |
|---|---|
| `freshet explain --against <dbt project>` | Reads a dbt project's `target/` and reports what Freshet would have pruned from its last run. |
| `freshet submit` | Posts the plan to a consumer of the wire contract, such as dagron, which compiles it into a run. Never commits state. |
| `freshet plan --budget N` | Caps a plan at N compute units and reports the headroom left; over budget it exits non-zero and commits nothing. |
| `freshet-serve` | Keeps the parsed project warm over HTTP; a replan on 10,240 models is 30–37 ms. No authentication yet, so it binds to loopback. |
| `freshet-planner` | The planner in-process from Python (`import freshet_planner`). |
| `freshet-sdk` | A standard-library-only client for `freshet-serve` (`import freshet_sdk`). |
| `freshet-airflow` | Turns a plan into an Airflow DAG with one task per model, wired from the plan's own edges. |
| `apps/effortmap-web` | The planner compiled to WebAssembly behind a static page: what will this PR rebuild, with no server. |

## State a team can share

`--state state.json` is a file you commit, which is last-writer-wins when two CI jobs
plan at once. Point `--state` at a hosted state API instead (`--state https://…`,
token in `FRESHET_STATE_TOKEN`) and every commit is a compare-and-swap: if another
commit landed first, nothing is written and `freshet` exits `3` so you re-plan. The
protocol is in [docs/API.md](docs/API.md); dagron Cloud serves one.

## Layout

```
core/         the planner core: freshet-core, freshet-sql, freshet-state, freshet-api
crates/       freshet-incremental, -fingerprint, -render, -embed, -dbt, -cli, -serve,
              -ffi, -wasm, -py, -bench
sdks/python   freshet-sdk          integrations/airflow   freshet-airflow
apps/         effort map (Streamlit, and a WebAssembly page)
docs/         CONFIG, API, OPERATIONS, BENCHMARKS
```

Build from source:

```sh
(cd core && cargo test --workspace --locked)
cargo build --release --locked -p freshet-cli    # target/release/freshet
```

## Docs

- [docs/CONFIG.md](docs/CONFIG.md): every flag, input file and exit code
- [docs/API.md](docs/API.md): the wire contract, the daemon API, hosted state, bindings
- [docs/OPERATIONS.md](docs/OPERATIONS.md): state and commit discipline, troubleshooting
- [docs/BENCHMARKS.md](docs/BENCHMARKS.md): the measurements and how to reproduce them

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE). Contributions welcome: see
[CONTRIBUTING.md](CONTRIBUTING.md); report vulnerabilities privately per
[SECURITY.md](SECURITY.md).
