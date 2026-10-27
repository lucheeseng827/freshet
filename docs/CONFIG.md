# Freshet configuration reference

Every knob in one place. Freshet has **no config file**, and reads exactly **one
environment variable** — `FRESHET_STATE_TOKEN`, and only when `--state` names hosted
state (see [State](#state-a-file-or-hosted-state)). Everything else is the `freshet`
CLI's flags, the project-directory input conventions, and the Cargo feature gates on
the binding/daemon crates. Source of truth: the clap derive in
[`crates/freshet-cli/src/main.rs`](../crates/freshet-cli/src/main.rs) and the
`[features]` tables in each crate's `Cargo.toml`; regenerate this file when
they change.

## `freshet` CLI flags

One binary, seven subcommands (`Cmd` in `freshet-cli/src/main.rs`). Paths are
ordinary filesystem paths, except that `--state` also takes an `http(s)://` URL.
Two default-on features carry the HTTP client: **`submit`** (the `submit`
subcommand) and **`hosted`** (hosted state behind `--state https://…`). Built with
`--no-default-features` the binary has no HTTP client, no TLS stack, no `submit`
subcommand in `--help`, and refuses a `--state` URL by name.

| Command | Flag | Type | Default | What it does |
|---|---|---|---|---|
| all | `--project <dir>` | path | `.` | The project directory to load: every `*.sql` file is a model (model name = file stem), plus the two optional JSON files below. |
| `plan` | `--state <file\|URL>` | path or URL | `.planner/state.json` | Where committed state lives: a freshet-state `FileStateBackend` snapshot file (missing = empty prior state, a cold plan), or an `http(s)://` **hosted state** API such as dagron Cloud's `https://<edge>/state/v1`, authenticated with `FRESHET_STATE_TOKEN`. Against hosted state, the version planned against is printed on stderr. See [State](#state-a-file-or-hosted-state). |
| `plan` | `--commit` | switch | off | Persist the new snapshot after planning. **Do this only after the plan actually ran** — state must advance only on success (the planner core's discipline). Against hosted state this is a compare-and-swap: if another commit landed after this plan was computed, nothing is written and the exit status is **3**. |
| `plan` | `--expect-version <N\|none>` | version | none | Hosted state, with `--commit`: commit only if the state is still at version `N` (`none`: it had never committed) — the version the plan that ran was computed against, which the planning run printed. Otherwise exit **3** without writing. Carries the compare-and-swap across plan → run elsewhere → commit; without it the commit compares against the version it loads itself. A file `--state` refuses it (a file is last-writer-wins). |
| `plan` | `--json` | switch | off | Emit the wire `PlanResponse` (via `to_wire`) instead of the human table. |
| `plan`, `restate`, `submit` | `--sql <DIALECT>` | `postgres` \| `snowflake` \| `bigquery` \| `databricks` \| `redshift` | none | Render each planned model's write as SQL in that dialect (contract v4: `sql` on every model, with `statements` and an `atomic` flag). All or nothing: if any model cannot be rendered, only the reasons are printed and nothing is committed. See [SQL rendering](#sql-rendering---sql). |
| `plan`, `restate`, `submit` | `--schema <SCHEMA>` | name | none | Requires `--sql`. Qualify every target relation (`analytics` → `analytics.events`; `proj.analytics` for three-part names). Without it names resolve through the session's search path, as the models' own SQL does. |
| `plan`, `restate`, `submit` | `--recreate <MODEL,…>` | comma-list | none | Requires `--sql`. Recreate these models from their SQL even though their definition did not change — for a relation someone altered by hand. Models whose definition did change are recreated without being named. |
| `plan` | `--budget <CU>` | float (CU) | none = uncapped | Cap the plan at N compute units and report the **headroom** left. Fractional CU accepted; the wire and the gate work in milliCU, so this flag is the one place the conversion happens (rounded, not truncated). Negative or `NaN` is rejected by name rather than saturating to a silent zero cap. `--budget 0` is legal and admits only an empty plan. |
| `restate` | `--model <name>` | string | required | The model whose partitions are being restated. |
| `restate` | `--partitions <p1,p2,…>` | comma-list | required | Partition keys to restate. |
| `restate` | `--json` | switch | off | Wire `PlanResponse` output. Restatement never reads or advances *fingerprints*. |
| `restate` | `--state <file\|URL>` | path or URL | none | Record the restatement in that state's partition ledger as **owed** (written before the run, so a run that dies is re-planned by the next `plan`). A file or hosted state; against hosted state each ledger write is a compare-and-swap (exit **3** if the state moved). Omit to plan only. |
| `restate` | `--commit` | switch | off | Requires `--state`. The restatement ran successfully: **settle** its partitions in the ledger. |
| `restate` | `--expect-version <N\|none>` | version | none | Requires `--state` (hosted): write the ledger only if the state is still at `N`. After recording, `restate` prints the version to settle against. |
| `fingerprint` | — | | | Print each model's AST-canonical fingerprint and its scheme (`AstV2`, or `Text` for SQL that failed to parse — fallbacks are listed on stderr). State written under an older scheme is reconciled, not rebuilt: a model whose fingerprint differs only in spelling is re-keyed on the next commit. |
| `graph` | — | | | Print the models in topological order; exits non-zero naming the cycle if there is one. |
| `submit` | `--state <file\|URL>` | path or URL | `.planner/state.json` | As `plan`, read only. The plan is computed exactly as `plan` computes it — same helper, so the two can never disagree — and carries its own CU estimate (`plan.cost`, the effort map at the default rate card) for a consumer that meters it. Against hosted state, the "once it has run, commit with" hint names `--expect-version`. |
| `submit` | `--to <URL>` | string | **required** unless `--dry-run` | The consumer's submit endpoint, in full. No default: freshet does not know who consumes its plans or where they mounted the route. |
| `submit` | `--command <SHELL>` | string | none | Shorthand for `options.command_template = ["sh","-c",<SHELL>]`. The shell string is forwarded byte-for-byte — placeholders like `{{ model }}` are the consumer's templating, not freshet's. |
| `submit` | `--options-file <FILE>` | path | none | A JSON **object** merged into `options` and forwarded without being interpreted. `--command` overrides the file's `command_template`. A non-object file is rejected by name. |
| `submit` | `--no-graph` | switch | off | Omit the project graph from the request. Smaller body; the consumer then cannot report what the plan *skipped* (e.g. "2 of 4 models — 50% pruned"), because only the graph carries the denominator. |
| `submit` | `--dry-run` | switch | off | Print the request body and exit; opens no socket. Makes `--to` optional. |
| `submit` | `--header 'NAME: VALUE'` | repeatable | none | Extra request headers, e.g. `--header 'Authorization: Bearer …'`. The route creates runs, so a real consumer will want auth; freshet adds none of its own. |
| `move` | `--from <DIALECT>`, `--to <DIALECT>` | `postgres` \| `snowflake` \| `bigquery` \| `databricks` \| `redshift` | required | Render a move of the project from one warehouse to another, partition by partition: each unit's **unload** (the source's own export, or DuckDB on Postgres), **load** (replacing the unit's rows on the target) and what a **parity** check compares on each side. Executes nothing. |
| `move` | `--stage <URI>` | `s3://…/`, `gs://…/`, `file:///…/` | required | The move's own prefix; every unit is one directory directly under it (`<model>.<column>=<key>/`). A stage either side cannot reach is refused before anything is rendered. |
| `move` | `--partitions <KEY,…>` | comma-list | none | Move each incremental model that declares a `partition_column` one unit per key; `YYYY-MM-DD..YYYY-MM-DD` is every day between, both included. Without it, every model moves whole. Tables always move whole; views never move (the target gets them by being planned for). |
| `move` | `--model <MODEL,…>` | comma-list | every model | Move only these, and none of their downstream. A named model asked to move by partition that declares no `partition_column` is refused. |
| `move` | `--from-schema`, `--to-schema` | name | none | Qualify every relation on that side (`public`; `db.schema`, `project.dataset`). A Postgres side's schema is one name. |
| `move` | `--storage-integration <NAME>` | name | none | Required when either side is Snowflake: the storage integration that reaches the stage. |
| `move` | `--iam-role <ARN\|default>` | string | none | Required when either side is Redshift: the role that reaches the stage. |
| `move` | `--json` | switch | off | Print the `freshet-move/1` document (units of `unload`, `load`, `parity`), which dagron-state compiles at `POST /api/state/moves`. |

There is deliberately **no `submit --commit`**. `plan --commit` advances the
fingerprint snapshot, and state must advance only after a rebuild has *succeeded*;
a submit has created a run that may still fail. Committing there would record
models as up to date that were never rebuilt — the under-rebuild bug the whole
planner exists to prevent. `submit` says so on stderr and leaves the commit to a
later `plan --commit`.

An empty plan makes `submit` exit 0 without opening a socket. "Nothing to
rebuild" is the planner's *good* outcome, and a caller planning on every commit
hits it constantly.

**Exit status.** `0` success; `1` an error, or a plan refused by `--budget`; `2` a
usage error (clap); **`3` hosted state moved** — a commit that lost its
compare-and-swap, or an `--expect-version` the state is no longer at. `3` means
"re-plan and try again", never "something is broken", which is why it is its own
number.

### `--budget`: what a cap does, and what it refuses to do

The processing budget is `freshet_api::ProcessingBudget`, reachable from every
surface as `PlanRequest::budget_millicu` — not CLI-only. It is a **guardrail a team
sets for itself**, not a tier limit: `Unlimited` is the default everywhere, and
there is no capped edition of Freshet. The typical use is a CI gate that fails a
pull request whose migration would rewrite the warehouse.

```console
$ freshet plan --project ./models --budget 5
backfill plan (3 model(s), topological order):
  stg                      full                         (directly changed)
  mart_a                   full                         (directly changed)
  mart_b                   full                         (directly changed)

budget: 2.000 CU (2000 mCU) left of 5.000 CU (5000 mCU) — used 3.000 CU (3000 mCU) (60.0%)
        [##############----------]
```

Three behaviours are contractual, not incidental:

* **Over budget still prints the plan**, then exits **1**. You cannot scope down
  what you cannot see, so the models that pushed past the cap are shown. The
  message goes to stderr as a plain `error:` line with no stack backtrace — being
  over budget is an expected operational outcome, not a crash, and a quota message
  that reads like one trains people to ignore crashes.
* **An over-budget plan never commits, even with `--commit`.** `plan --budget N
  --commit` is a sensible thing to run daily: it commits on the days the plan fits
  and writes nothing on the days it does not. A commit there would record models as
  up to date that the operator was just told they may not rebuild — the same
  under-rebuild bug the `submit --commit` rule above exists to prevent, arriving
  through the quota gate instead.
* **`--json` grows a `headroom` object** (`cap_millicu` / `used_millicu` /
  `remaining_millicu` / `exceeded`) and, when the cap binds, **drops `next_state`** —
  a refused plan must not hand back a commit-ready snapshot. Without `--budget` the
  response has no `headroom` key at all and is byte-identical to a pre-budget one.

`remaining_millicu` saturates at zero rather than going negative; `exceeded` is the
authoritative over/under answer, and a plan landing exactly *on* the cap is admitted
(`used <= cap`, matching `gate_against_budget`).

## Project-directory inputs

Read by `load_project` (`freshet-cli/src/main.rs`):

| File | Required | Shape | What it does |
|---|---|---|---|
| `*.sql` | ≥ 1 | one model per file | The model's SQL; the file stem is the model name. |
| `sources.json` | no | `{"raw": ["a","b"]}` | Source-table schemas for wildcard expansion / unqualified-ref resolution (freshet-sql's `Catalog`). |
| `materializations.json` | no | `{"stg": "incremental"}` | Materialization hints per model: `table` \| `view` \| `incremental` (case-insensitive; anything else is an error). Without a hint everything defaults to `table` and **nothing is partition-scopable**. |

### `incremental.json` (optional) — partitions and replace declarations

Per-model incremental configuration, all of it applied (`freshet-incremental`):
**`grain`** says how a model's partitions are keyed, **`edges`** says how many of
an upstream's partitions each downstream partition reads, and **`replace`** says
what a rebuild does to the relation. A model with no entry behaves exactly as v1
did: same-keyed partitions, a window of one.

```json
{
  "models": {
    "events":       { "grain": "Day",
                      "replace": { "strategy": "insert_overwrite", "partition_column": "dt", "partition_type": "date" } },
    "rolling_7d":   { "grain": "Day", "edges": { "events": { "window": 7 } },
                      "replace": { "strategy": "delete_insert", "partition_column": "dt", "partition_type": "date" } },
    "monthly":      { "grain": "Month" },
    "daily_rollup": { "replace": { "strategy": "merge", "unique_key": ["dt"] } }
  }
}
```

| Key | Values | Notes |
|---|---|---|
| `grain` | `"Hour"` (`YYYY-MM-DDTHH`), `"Day"` (`YYYY-MM-DD`), `"Week"` (ISO, `YYYY-Www`), `"Month"` (`YYYY-MM`), `{"Integer": {"step": N}}` | How the model's partition keys are spelled and what span each covers. Across an edge between two grains, a changed upstream key maps to every downstream key whose span it intersects (a day → its month; a month → its days; a straddling week → both months). A key that cannot be placed, grains with nothing in common, or a refinement beyond 10,000 keys escalates that downstream to a **full rebuild** — never a guess. |
| `edges.<upstream>.window` | integer ≥ 1 | The downstream partition `d` reads the upstream data covering `d-(window-1) … d`, counted in the downstream's grain. So a change at upstream partition `p` makes downstream `p … p+(window-1)` stale — forward, not backward. |
| `replace.strategy` | `insert_overwrite`, `delete_insert`, `merge`, `replace_where`, `full_refresh` | Omit the whole `replace` block to declare nothing — there is no default, because warehouses disagree about what it should be. |
| `replace.partition_column` | column name | Required by every strategy except `merge` and `full_refresh`. |
| `replace.partition_type` | `string` (default), `date`, `int` | How `--sql` spells the partition keys: `'2026-06-20'`, `DATE '2026-06-20'`, or `20260620`. |
| `replace.unique_key` | list of columns | Required by `merge`; ignored by the rest. |

Every field is independently optional, so an entry may declare only `replace`, or
only a `grain`.

**The partition ledger.** `restate --state` records the restated partitions as
*owed* in the state's watermarks before the run and `restate --commit` settles them
after it, so a restatement that dies halfway is planned again by the next `plan`
(which rebuilds owed partitions and settles them on `--commit`). The ledger lives
in `StateSnapshot.watermarks`; a file or hosted state both carry it, and one that
cannot be read owes a full rebuild of that model rather than nothing.

A declaration that cannot be honoured — `merge` with no key, a partition-wise
strategy with no column, any strategy on a non-incremental model, or a plan that
rebuilds the model in full — **widens to a full refresh of the whole relation**
and says why, on stderr and on the wire. It never narrows and never fails.

## SQL rendering (`--sql`)

`--sql <dialect>` turns each planned model's write into the statements a warehouse
runs, and attaches them to the plan (`sql: {dialect, statements, atomic}` on every
model, contract v4). Freshet never executes them: `dagron-step-sql` (`SQL_MODE=script`,
for every dialect below, with the dialect's name as its `SQL_ENGINE`), Airflow's
`SQLExecuteQueryOperator` (`freshet-airflow`) or `psql` does.

| Dialect | Partition replace | Whole-relation refresh | `merge` | Recreate (definition changed) |
|---|---|---|---|---|
| `postgres` | one statement: a data-modifying CTE deletes, the `INSERT` refills | same, unscoped | `MERGE` (15+) | `BEGIN` · drop dependent views the plan rebuilds · `DROP TABLE` · `CREATE TABLE AS` · `COMMIT` |
| `redshift` | `BEGIN` · `DELETE` · `INSERT` · `COMMIT` (never `TRUNCATE`, which commits implicitly) | same, unscoped | keyed delete-then-insert in one transaction | as Postgres |
| `snowflake` | `BEGIN TRANSACTION` · `DELETE` · `INSERT` · `COMMIT` | `INSERT OVERWRITE` | `MERGE` | `CREATE OR REPLACE TABLE … COPY GRANTS` |
| `bigquery` | `MERGE … ON FALSE` with a scoped `NOT MATCHED BY SOURCE` delete (`delete_insert` literally, as a transaction sent as **one** script entry, since BigQuery runs a multi-statement transaction only inside a single script) | same `MERGE`, unscoped | `MERGE` | `CREATE OR REPLACE TABLE … PARTITION BY` |
| `databricks` | `INSERT INTO … REPLACE WHERE` (`delete_insert` as two statements, **not atomic**) | `CREATE OR REPLACE TABLE` | `MERGE` | `CREATE OR REPLACE TABLE … PARTITIONED BY` |

The rules every dialect keeps:

* **Never narrower than the plan.** A partition-scoped write names exactly the
  plan's keys on *both* sides — the rows it deletes and the rows it inserts.
* **Named, not positional, inserts** wherever the statement allows a column list.
* **Recreate exactly when the definition changed** (the fingerprint diff says which);
  everything else keeps the relation — its grants, partitioning and dependent views —
  and replaces rows. `--recreate` adds models to that set.
* **Refuse rather than guess**: a partition-scoped rebuild with no declared strategy,
  an unsafe identifier or partition key, or an unnamed output column is an error, not
  a silent full rewrite.
* `atomic: false` says a failure part-way can leave the relation half-written (today
  only a Databricks `delete_insert`); consumers surface it.

Postgres and Redshift statements are executed against a live PostgreSQL in the
test suite; Snowflake, BigQuery and Databricks are syntax-checked by `sqlparser`.

## State: a file, or hosted state

`--state` names one of two homes for the same document, a freshet-state
`StateSnapshot`: `{"fingerprints": {"<model>": <u64>}, "watermarks": {"<model>": "<ledger JSON>"}}`.

**A file** (`FileStateBackend`, the default `.planner/state.json`). Written by
`plan --commit` and by `restate --state` (the partition ledger). Delete it to force
a full cold plan. Last-writer-wins: two jobs that commit at once both succeed, and
the second silently replaces the first.

**Hosted state** — any `http://` or `https://` URL, e.g. dagron Cloud's
`https://<edge>/state/v1`. The token comes from **`FRESHET_STATE_TOKEN`** and
nowhere else: not a flag (shell history, `ps`), not the URL (a URL with
`user:pass@` is refused, without echoing it). Plain `http://` to a non-loopback host
works but warns that the token crosses the network unencrypted. The protocol is two
calls, both answered for the token's own workspace:

| Call | Answer |
|---|---|
| `GET <url>/current` | `200 {"version": int\|null, "snapshot": {...}}` — `null` before the first commit |
| `POST <url>/commit` `{"expected_version": int\|null, "snapshot": {...}, "summary": {...}}` | `200 {"version": N}` · `409 {"error", "expected", "found"}` |

Every run that reads hosted state prints the version it planned against on stderr
(`hosted state: version 7 at …`). Every commit — `plan --commit`, and both of
`restate`'s ledger writes — sends that version as `expected_version`, so it lands
only if nothing else committed in between; otherwise nothing is written and freshet
exits **3** with *"another commit landed after this plan was computed; re-plan"*.
The window that matters is usually plan → run elsewhere → commit, which spans
invocations: pass the printed version to the commit with `--expect-version`. A
commit that would change nothing is **not sent** (`state unchanged: … nothing to
commit`, exit 0): it would record nothing, and moving the version would make every
other writer pinned to the current one conflict and re-run its plan. The
`summary` freshet sends is small context — `{"client": "freshet/<version>",
"command": "plan --commit", "models_rebuilt": N}` — which dagron Cloud keeps beside
its own server-computed record of what the commit changed.

**The mapping is the identity, and it is lossless or refused.** A state file's
content *is* a `StateSnapshot`, and so is what hosted state stores:

* fingerprints are u64s and cross JSON exactly (both ends use serde_json; the
  Postgres store keeps them as `numeric`, bit-exact);
* each watermark is a model's partition ledger — a JSON document (`{"v":2,
  "grain":…, "built":…, "pending":…}`) kept as a *string* — opaque in transit and at
  rest;
* the fingerprint **scheme** (AST v1/v2, text) is not stored anywhere, in either
  home: `freshet_fingerprint::reconcile` recognises every scheme by recomputing, so
  there is nothing to carry.

A hosted reply carrying a field this freshet has no slot for is **refused** (exit 1,
"upgrade freshet") rather than planned against — planning without it and then
committing would drop it for good. dagron Cloud refuses the mirror image, a commit
whose snapshot it could not store whole. The v2 AST-canonical fingerprinter writes
the same `BTreeMap<String, u64>` shape v1 does, so a v1-written state still loads (a
changed hashing scheme is re-keyed on the next commit, not rebuilt).

## `freshet-serve`: the daemon

Built with the `http` feature (`cargo build --release -p freshet-serve --features
http`; the release binaries and the `mancube/freshet` image include it). No config
file and no environment variables; everything is a flag:

| Flag | Default | What it does |
|---|---|---|
| `--project <DIR>` | none | The `.sql` models to load at startup (file stem = model name). Without it the daemon starts empty and waits for a `reload`. |
| `--sources <FILE>` | none | Source-table schemas, as `sources.json` above. |
| `--materializations <FILE>` | none | Materialization hints, as `materializations.json` above. Only `incremental` models are partition-scopable. |
| `--incremental <FILE>` | none | Per-model incremental declarations; the daemon reads the `replace` half. |
| `--port <N>` | `8788` | TCP port. |
| `--host <ADDR>` | `127.0.0.1` | Bind address. There is **no authentication** in this version, so anything but loopback prints a warning; put it behind your own proxy or network policy. |

Two routes: `POST /rpc` takes one request object (`{"op":"plan"}`, `restate`,
`graph`, `commit`, `reload` — see [API.md](./API.md)) and `GET /healthz`. The daemon
does not watch the filesystem (`reload` is explicit, so a plan's commit token is
never invalidated under a client), does not render SQL (use the CLI's `--sql`), and
holds its committed state in memory.

## `freshet-cli` feature gates

| Feature (on by default) | Pulls | Enables |
|---|---|---|
| `submit` | `ureq` (rustls) | the `submit` subcommand |
| `hosted` | the same `ureq` | `--state http(s)://…` — hosted state |

`--no-default-features` builds a binary with no HTTP client and no TLS stack: it
plans and commits against state files only, and names the missing feature if given a
`--state` URL.

## Binding feature gates

Both bindings compile as plain re-exports of `freshet_embed::plan_from_json`
with their glue off; the real artifacts are built with the feature on:

| Crate | Feature (off by default) | Enables | Real build command |
|---|---|---|---|
| `freshet-wasm` | `wasm` | `wasm-bindgen` glue (`plan(requestJson): string`) | `wasm-pack build --features wasm` |
| `freshet-py` | `extension-module` | pyo3 module `freshet_planner` (`plan`, `plan_dict`), abi3-py38 | `maturin build --features extension-module` |
| `freshet-ffi` | — (none) | always builds the C ABI (`cdylib` + `staticlib` + `rlib`) | `cargo build -p freshet-ffi --release` |

`freshet-wasm`, `freshet-py`, and `freshet-bench` are workspace members but
excluded from `default-members`, so a bare `cargo build` never selects a
`wasm32` target, links libpython, or compiles criterion.
