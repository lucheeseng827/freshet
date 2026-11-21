# Freshet API reference

The public entry points, as built. There are **no HTTP endpoints anywhere in
Freshet today** — the daemon core is an in-process request loop, and the
bindings are function calls. Everything funnels through one pure-Rust entry
point (`freshet_embed::plan_from_json`), so every surface answers identically.
Source of truth: [`freshet-serve/src/lib.rs`](../crates/freshet-serve/src/lib.rs)
(`Request`/`Response`), [`freshet-embed/src/lib.rs`](../crates/freshet-embed/src/lib.rs)
(the wire contract), and the three binding crates. All JSON examples below are
captured from real runs.

## 1. `freshet-serve` — the daemon core (in-process)

```rust
use freshet_serve::{ProjectCache, Request, Response};

let cache = ProjectCache::new();
let resp: Response = cache.handle(Request::Plan);     // typed
let json: String  = cache.handle_json(r#"{"op":"plan"}"#); // JSON (the future transport lane)
```

`handle_json` is the transport-agnostic dispatch; the `freshet-serve` binary
(`http` feature) exposes exactly it as `POST /rpc`, one request object per call,
plus `GET /healthz` — flags in [CONFIG.md](./CONFIG.md#freshet-serve-the-daemon).
Requests are tagged with `"op"`, responses with `"status"` (both `snake_case`).

### Requests → responses

| `op` | Request fields | Success response |
|---|---|---|
| `reload` | `models: [[name, sql], …]`, `sources?: {table: [cols]}`, `materializations?: {model: "table"\|"view"\|"incremental"}`, `incremental?: {models: {model: {replace: {…}}}}` | `{"status":"reloaded","revision":<n>,"models":<count>,"reparsed":<k>,"full_rebuild":<bool>,"full_rebuild_reason"?:"…"}` — reparses only the edited models and their downstream closure (a full rebuild when that could differ from one, with the reason). The revision moves only when something that affects plans changed, so an identical reload keeps outstanding tokens committable. |
| `plan` | — | `{"status":"plan","revision":<n>,"token":<t>,"plan":{…}}` — plan vs the committed state; stages the post-run snapshot under `token`. |
| `restate` | `model`, `partitions: [keys]` | `{"status":"plan","revision":<n>,"token":null,"plan":{…}}` — **no token**: restatement never advances fingerprint state. |
| `graph` | — | `{"status":"graph","revision":<n>,"order":[models…]}` (topological). |
| `commit` | `token` | `{"status":"committed","revision":<n>}` — the staged snapshot becomes the committed state. |

Errors are `{"status":"error","message":"…"}` (bad JSON/op, unknown
materialization, dependency cycle, or the token failures below).

`incremental` is held with the project rather than resent on every `plan`:
replace declarations describe the models, so they belong to whatever `reload`
last established. Only the `replace` half is read — `grain` and `edges` drive
`plan_incremental`, which the daemon does not call. Omit it and every plan comes
back exactly as it did before contract v3.

### The revision-stamped plan → commit token contract

The planner core's discipline — *state advances only after a successful run* — carried
across a request boundary:

1. `plan` computes the plan **and stages the post-run snapshot** under a fresh
   `token`, stamped with the current project `revision`. Nothing is committed.
2. The embedder runs the plan (its orchestrator's job, outside the daemon).
3. `commit {token}` promotes that staged snapshot — **iff** the project
   revision has not moved since the plan.

Failure modes (exact strings):

- Reparsed in between (a `reload` that changed the project or the incremental
  config bumped the revision) →
  `{"status":"error","message":"stale plan: project changed since plan; re-plan"}`.
  The client must re-plan; this prevents a plan computed against an old graph
  committing over a newer one. A `reload` that changes neither — an editor
  re-saving a file it did not edit — keeps the revision and every outstanding token.
- Staged longer than the TTL (`DEFAULT_STAGED_PLAN_TTL`, 24 h; an embedder sets
  its own with `ProjectCache::with_staged_plan_ttl`) →
  `{"status":"error","message":"expired plan: token <t> waited longer than the 24h staged-plan TTL for its commit and was evicted; re-plan"}`.
- Pushed out by newer plans: at most **64** are staged (`MAX_STAGED_PLANS`),
  oldest evicted first →
  `{"status":"error","message":"evicted plan: token <t> was dropped to keep at most 64 uncommitted plans staged; re-plan"}`.
- Never staged, already committed, or evicted so long ago that its eviction
  record is gone (the daemon remembers the last 1,024 evictions) →
  `{"status":"error","message":"unknown or already-committed token <t>"}`.

Captured session:

```text
> {"op":"reload","models":[["stg","select a from raw"],["mart","select a from stg"]],"sources":{"raw":["a"]}}
{"status":"reloaded","revision":1,"models":2,"reparsed":2,"full_rebuild":true,"full_rebuild_reason":"first load"}
> {"op":"plan"}
{"status":"plan","revision":1,"token":0,"plan":{"models":[{"name":"stg","reason":"directly_changed","unit":"full_model","depends_on":[]},{"name":"mart","reason":"directly_changed","unit":"full_model","depends_on":["stg"]}]}}
> {"op":"commit","token":0}
{"status":"committed","revision":1}
> {"op":"plan"}
{"status":"plan","revision":1,"token":1,"plan":{"models":[]}}
```

## 2. `freshet-embed` — the JSON funnel (the frozen wire contract)

Rust callers: `plan(PlanRequest) -> anyhow::Result<PlanResponse>` (typed, no
JSON hop). Bindings: `plan_from_json(&str) -> String`, which **always** returns
a JSON `PlanResponse` — errors come back as an `error` field, never a panic.

**Request** (`PlanRequest`): `models: [{name, sql, materialization?}]`,
`sources?: {table: [cols]}`, `prior_state?: StateSnapshot` (omit for a cold
plan), `restate?: {model, partitions}` (plan a data restatement instead of a
code-change plan), `incremental?` (the `incremental.json` document: grains,
edge windows, replace declarations), `render?: {dialect, schema?, recreate?}`
(attach SQL — contract v4), `include_cost?` / `include_queue?` (the effort map and
the executable queue), `budget_millicu?` (a processing cap), `strict_refs?` (refuse a
table that is neither a model nor one of `sources`, instead of planning it as an
external table; `[]` in `sources` declares a table without its columns).

**Response** (`PlanResponse`): `models: [{name, reason, unit, depends_on}]` in
topological order; `next_state` (the snapshot to persist **after** a successful run —
stateless callers store it and pass it back as `prior_state` next time; absent
for restatements); `error?: {code, message}` with codes `bad_request`
(unparseable request), `plan_error` (planning failed, e.g. unknown
materialization or model), `render_error` (`render` asked for SQL the renderer
refused, or an unknown dialect), `undeclared_table` (`strict_refs` refused a table,
named with the models and sources one edit from it — ``model `mart` reads `stgg`, which
is neither a model nor a declared source: did you mean `stg`?``; only ever returned when
asked for), `serialize_error`, and — from the C ABI only — `ffi_error`. `reason` is `"directly_changed"` or
`{"downstream":{"because_of","via_columns"}}`; `unit` is `"full_model"` or
`{"partitions":[…]}`.

`depends_on` (contract **v2**) is the model's upstream dependencies **narrowed to
models this plan also rebuilds** — the execution edge set, ready to use as a task's
`depends_on` with no filtering and no access to the wider project. It is separate
from `reason` on purpose: `reason` explains *why* a model is here and names a single
cause, so a model downstream of both `a` and `b` is blamed on one of them. Ordering a
run from attributions is under-constrained; ordering it from `depends_on` is exact.

It is **always present, including as `[]`**. An absent field means a v1 producer and
the consumer must fall back; `[]` is an authoritative "waits for nothing". Because
the plan is topologically ordered and edges are plan-restricted, every entry appears
earlier in `models` than the model naming it — so a consumer can build the DAG in one
pass.

```text
in : {"sources":{"raw":["a"]},"models":[{"name":"stg","sql":"select a from raw","materialization":"incremental"}]}
out: {"models":[{"name":"stg","reason":"directly_changed","unit":"full_model","depends_on":[]}],"next_state":{"fingerprints":{"stg":16985606331583368279},"watermarks":{}}}
```

The wire DTOs live *here* (not in the planner core) because `freshet-core`'s types are
deliberately serde-free; the `to_wire` conversion is an exhaustive no-wildcard
match, so an upstream enum change is a compile error, never a silently dropped
field.

**What is frozen is `PlanResponse`, not anything wrapped around it.** `freshet
submit` posts this exact object under a `plan` key, alongside a `graph` and an
opaque `options` blob — but that envelope belongs to whoever consumes the plan, not
to Freshet, and the CLI forwards everything outside `plan` and `graph` without
interpreting it. Do not read the envelope's shape as part of this contract: a
second consumer may wrap the same `PlanResponse` differently, which is the point of
freezing the payload rather than the request. The `plan` `submit` posts carries its
effort map (`cost`, as `include_cost` would ask for): the plan's own CU estimate at
the default rate card, which a consumer that runs plans on compute it supplies may
meter. A consumer that does not mirror `cost` ignores it — `dagron-state` does.
Against hosted state (`--state https://…`, §2a) the envelope also carries
`expected_version`: the hosted version the plan was computed against (`null` before
the first commit), so a consumer that holds plans for review can refuse one planned
against state that has since moved. A state file has no versions, and the key is
left out.

`next_state` is left out of the CLI's plans unless asked for: `freshet plan --json
--with-next-state` and `freshet submit --with-next-state` carry it, the exact snapshot
`plan --commit` writes once the plan has run (never for a plan over its `--budget`).
It is for a consumer that reviews plans before they run and holds the commit it later
receives to the plan it approved: the same snapshot, against the same version.

### Contract v3 — `replace`

Each model may carry what its rebuild does to the target relation:

```jsonc
{
  "name": "events",
  "reason": "directly_changed",
  "unit": { "partitions": ["2026-06-20"] },
  "depends_on": [],
  "replace": {
    "strategy": "insert_overwrite",     // | delete_insert | merge | replace_where | full_refresh
    "partition_column": "dt",           // omitted when the strategy needs none
    "unique_key": ["id"],               // omitted when empty
    "target": { "partitions": ["2026-06-20"] },   // or "whole"
    "widened": {                        // omitted when the declaration was honoured
      "widened_because": "not_incremental",
      "materialization": "table"
    }
  }
}
```

Two things to read carefully:

* **Absence is not a version signal here**, unlike `depends_on`. A v2 producer
  omits `replace` because it cannot produce one; a v3 producer omits it because
  the model declared no strategy. Both mean *apply no replace-specific behaviour*,
  so there is nothing for a consumer to branch on and no `null`-versus-absent
  dance to perform. `depends_on` needed that distinction because "waits for
  nothing" and "cannot tell you" demand opposite handling; this does not.
* **`widened` is the field that matters operationally.** It means the planner
  could not honour the declaration and escalated to a full refresh of the whole
  relation — the difference between rewriting two partitions and rewriting the
  table. A consumer that shows a human anything should show this.

Declarations come in on the request half, so every binding can produce a v3 plan:

```jsonc
{
  "models": [ … ],
  "incremental": {
    "models": {
      "events": { "replace": { "strategy": "insert_overwrite", "partition_column": "dt" } }
    }
  }
}
```

All of `incremental` is applied: `replace` for the op above, and `grain` and
`edges` through `freshet-incremental` — a restatement's partitions map across grains
and widen forward through each edge's window, exactly as `freshet restate` does.

### Contract v4 — `sql`

With `render: {"dialect": "postgres"}` (or `snowflake`, `bigquery`, `databricks`,
`redshift`), every model carries the statements that perform its rebuild:

```jsonc
"sql": {
  "dialect": "postgres",
  "statements": ["WITH freshet_deleted AS (\n  DELETE FROM events WHERE dt IN (DATE '2026-06-20')\n)\nINSERT INTO events (dt, user_id, n)\n…"],
  "atomic": true
}
```

Statements run **in order, on one session**, and carry no trailing `;`. Rendering
is all-or-nothing: a plan with a model the renderer refuses comes back as a
`render_error` naming every such model, never as a plan with some SQL missing. The
dialect rules and per-dialect forms: [CONFIG.md](./CONFIG.md#sql-rendering---sql).

### Contract v5 — `cost.unpruned_millicu`

The effort map (`include_cost`) gains the baseline pruning is measured against:
what the same change would cost if every model downstream of the changed ones were
rebuilt whole. `unpruned_millicu - total_millicu`, floored at zero, is the rebuild
work the plan avoids — Freshet's north-star metric. `freshet plan` prints it as its
last line; the Python SDK exposes it as `CostEstimate.avoided_millicu`.

### Contract v6 — `cold`

`"cold": true` on a code-change plan made with no committed state to diff against:
no `prior_state`, a state file or hosted state with no commits yet, or a snapshot
with no fingerprints. Nothing says any model was built before, so every model is
`directly_changed` and the plan rebuilds the project rather than a change to it.
Only the planner knows this — from outside, a cold plan and a change that really
touched every model look the same — so it says so, and a consumer that gates
rebuilds can hold a cold plan for a person by name rather than by its size
(dagron-state's `auto_approve` does).

The key is written only when true, so a warm plan's JSON is byte-for-byte what it was
under v5. A restatement reads no state and is never cold. A fingerprint-scheme
upgrade is not cold either: the CLI re-keys the models it finds unchanged instead of
rebuilding them. `freshet plan`, `freshet submit`, `freshet-serve` and every binding
over `freshet_embed::plan` set it; `freshet plan` without `--json` prints a line
saying the plan is cold and why.

`freshet_embed::WIRE_CONTRACT_VERSION` names the revision (`"planner-embed/6"`); the
C ABI (`planner_contract_version()`), the wheel (`freshet_planner.WIRE_CONTRACT_VERSION`),
dagron-state and freshet-airflow all stamp the same string.

## 2a. Hosted state — what `freshet --state https://…` speaks

The CLI's second socket (after `submit`): state kept by a server instead of a file,
versioned, and committed by compare-and-swap. `freshet-state-server` serves it at
`https://<host>/v1/<name>`, one state per name; any server implementing these calls
works. `<url>` is what `--state` names, in full, and every call carries
`Authorization: Bearer $FRESHET_STATE_TOKEN`.

| Call | Request | Success | Other answers freshet reads |
|---|---|---|---|
| `GET <url>/current` | — | `200 {"version": int\|null, "snapshot": StateSnapshot}` | any non-`200`: an error carrying the server's `error` text |
| `POST <url>/commit` | `{"expected_version": int\|null, "snapshot": StateSnapshot, "summary": {...}}` | `200 {"version": N}` | `409 {"error", "expected", "found"}` → exit **3**; anything else → exit 1 |
| `GET <url>/history?limit=N[&before=V]` | — | `200 {"commits": [{"version", "committed_at", "summary"}], "limit": N, "next_before": V\|null}` | any non-`200`: an error carrying the server's `error` text |

`StateSnapshot` is the exact document a state file holds —
`{"fingerprints": {model: u64}, "watermarks": {model: string}}` — so nothing is
converted on the way: `version` is the server's commit counter (`null` until the
first commit), and `expected_version` is the version the plan being committed was
computed against. A reply whose snapshot carries a field this freshet cannot keep is
refused rather than planned against, and `freshet-state-server` refuses a commit
whose snapshot it could not store whole (422).

History is newest first. Each commit's `summary` is the server's own diff against
the commit before (`changed`: models whose fingerprint is new or different,
`removed`, `watermarks_changed`: models whose ledger moved) with the committer's
`summary` under `context`. `next_before` is the `before` for the next page, `null`
when the server keeps nothing older; a server that omits it does not page, and
`freshet history --before` refuses it. `freshet-state-server` caps a page at 100.

`freshet-state-server` answers two more reads, for its console (`apps/freshet-console`).
The CLI needs neither, so a server without them still serves `freshet`:

| Call | Success | Other answers |
|---|---|---|
| `GET /v1/` | `200 {"states": [{"name", "version", "committed_at", "models"}], "access": "open"\|"read"\|"commit"}`: every state with a commit, by name, and what the caller's token may do | `401` without a token the server accepts |
| `GET <url>/versions/{v}` | `200 {"version", "committed_at", "summary", "snapshot"}`: a kept commit and its snapshot | `404` for a version that never landed or that retention (`--keep`) dropped; `400` for a `v` that is not a positive integer |

Flags, exit codes and the lossless-mapping details:
[CONFIG.md](./CONFIG.md#state-a-file-or-hosted-state).

## 3. The language bindings (marshalling-only)

All three route through `plan_from_json`; none contains planning logic.

### C ABI — `freshet-ffi` (always built; `cargo build -p freshet-ffi --release`)

```c
#include "planner.h"                                 // crates/freshet-ffi/include, generated by cbindgen
uint32_t    planner_abi_version(void);               // 1; bumped on any breaking change
const char *planner_contract_version(void);          // "planner-embed/6"; static, never free it
char       *planner_plan(const char *request_json);  // JSON in, JSON out
void        planner_string_free(char *ptr);          // the ONLY way to free the result
```

Contract: Rust allocates, Rust frees (`planner_string_free`, never libc
`free`); every entry is `catch_unwind`-wrapped so a panic becomes
`{"models":[],"error":{"code":"ffi_error",…}}`, not an unwind into C; a null or
non-UTF-8 request yields the same structured error; `planner_string_free(NULL)`
is a no-op. Targets Go/C++/C#/JNI hosts. `tests/c/run.sh` compiles a C harness
against the header and runs the golden request plus NULL, non-UTF-8 and malformed
requests and 200 alloc/free round trips under AddressSanitizer + LeakSanitizer +
UBSan (and `--valgrind`); CI also checks the header against the source.

### WASM — `freshet-wasm` (glue behind `--features wasm`; build with `wasm-pack build --features wasm`)

```js
import { plan } from "freshet-wasm";
const resp = JSON.parse(plan(JSON.stringify({
  models: [{ name: "stg", sql: "select a from raw" }],
  sources: { raw: ["a"] },
})));
```

Stateless: pass `prior_state` in, store `next_state` yourself. With the feature
off the crate is a plain Rust re-export of the same `plan` function (used for
native tests).

`JSON.parse` is fine for reading a plan, but not for `next_state`. Its fingerprints
are u64s past 2^53, which `JSON.parse` rounds without an error, and a rounded state
makes every model look edited. Carry `next_state` as text, or parse with something
that keeps large integers exact. `apps/effortmap-web` does both: `planner.js` cuts
`next_state` out of the response as text, and `json.js` parses the rest with any
integer past 2^53 as a `BigInt`.

### Python — `freshet-py` (glue behind `--features extension-module`; build with `maturin build --features extension-module`, abi3-py38 wheel)

```python
import json, freshet_planner
resp = freshet_planner.plan_dict({"models": [{"name": "stg", "sql": "select a from raw"}],
                             "sources": {"raw": ["a"]}})          # dict in, dict out
# or the string core: freshet_planner.plan(json.dumps(request)) -> str
try:
    freshet_planner.plan_dict({"models": [{"name": "m", "sql": "selec 1"}]})
except freshet_planner.PlannerError as e:     # .code ("plan_error"), .message, .response
    ...
```

`plan` returns the wire response verbatim, errors included, so it answers exactly
as every other binding does; `plan_dict` raises `PlannerError` instead of returning
a dict with an `error` key. One `abi3` wheel (`maturin build`, from
`crates/freshet-py`) covers CPython 3.8+ and is tested on 3.8 and 3.13.

## 4. Not built (do not look for it)

No gRPC server, by decision (2026-10-27): HTTP and JSON are the funnel every
binding shares, and nothing that consumes the planner asked for another transport.
(`freshet-serve` ships the HTTP transport and binary behind its `http` feature, a
bearer token through `FRESHET_SERVE_TOKEN`, and `--watch` behind `watch` — see
[CONFIG.md](./CONFIG.md).) The daemon plans code changes and restatements but does
not render SQL; use the CLI or the funnel (`render`) for that.
