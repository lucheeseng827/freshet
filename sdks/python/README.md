# planner — Python SDK

Ask the backfill planner three questions from Python, without a Rust toolchain:

1. **What does this change rebuild?** — the minimal, column-pruned, topologically
   ordered set of models.
2. **What will it cost?** — the effort map: a predicted total in compute units, with
   a per-model breakdown and the rate-card basis for each.
3. **How do I run it?** — the queue: one work item per model, or per *partition* for a
   partition-scoped rebuild, with barriers and concurrency waves.

Standard library only (`json` + `urllib`). One module, no dependencies.

## Run the daemon

The planner is Rust and stays Rust; this talks to it over HTTP.

```sh
cargo build --release -p freshet-serve --features http     # once
freshet-serve --project ./models --sources sources.json --port 8788
```

`--project` is a directory of `.sql` files, one model per file (non-recursive).
`--sources` is a JSON map giving the columns of tables the project selects from but
does not define — without it those columns cannot be resolved and the planner falls
back to assuming they changed. `--materializations` marks models `incremental`, which
is what makes partition scoping reachable at all.

`freshet-serve` binds loopback by default. It has **no authentication**: anything else
prints a warning saying what that means.

## Use it

```python
from freshet_sdk import Client, load_project

api = Client("http://127.0.0.1:8788")
api.reload(load_project("./models"), sources={"raw_orders": ["id", "amount", "day"]})

plan = api.plan(include_cost=True, include_queue=True)
print(f"{len(plan.models)} models, {plan.total_cu:.1f} CU")

for m in plan.cost.heaviest(5):
    print(f"  {m.model:<20} {m.cu:>6.2f} CU   {m.basis_name}")

for i, wave in enumerate(plan.queue.wave_items()):
    print(f"wave {i}: " + ", ".join(f"{it.label} ({it.cu:.2f} CU)" for it in wave))

# Only after the plan has actually run.
api.commit(plan.token)
```

A data restatement instead of a code change:

```python
plan = api.restate("events", ["2026-09-01", "2026-09-02"], include_queue=True)
for item in plan.queue.items:
    print(item.seq, item.label, f"{item.cu:.2f} CU")
```

## What the numbers mean

**One CU is one full rebuild of one `table` model.** That is a definition, not a
measurement — the figure is a prediction read off a rate card
(`freshet_core::CostModel`), there is no benchmark behind it, and nothing here is
denominated in seconds. Print CU.

`millicu` is the exact integer the planner computed: compare, sum and store that.
`cu` is `millicu / 1000` for display and is the only lossy number in the SDK. The
integer scale exists so a `view` (50 mCU) does not round to zero.

The per-item shares of one model sum to exactly that model's total, so a per-partition
column and a per-model total shown side by side always add up.

## Two things the SDK will not let you do quietly

**`commit()` after a plan that did not run.** Committing advances the fingerprints
that record which code has been built. Do it without running the plan and the next
`plan()` sees nothing to do — the rebuild is skipped silently, which is the failure
mode this whole system exists to avoid. The daemon also rejects a token whose project
was reparsed in between, so a lost update is an error rather than a wrong answer.

**`commit()` on a restatement.** It returns no token, and asking for one raises. A
restatement rebuilds *data*; advancing *code* fingerprints from it would claim work
that never happened.

## Errors

`freshet-serve` answers HTTP 200 even for planner failures and reports them in the
document, so that every binding — HTTP, C-ABI, WASM, in-process — reads errors the
same way. The SDK raises `PlannerError` from the body. `PlannerError.status` is the
HTTP status for a genuine transport failure and `0` when the planner itself refused.

## The other route

`freshet-py` is a pyo3 wheel around the same JSON funnel: no server, no socket, in
process. Use that for a one-shot script; use this when the project is big enough that
keeping it parsed and warm between calls is worth a daemon.

## Tests

```sh
python -m unittest -v
```

31 tests, standard library only. The client tests run against a threaded
`http.server` fake that records each request, so they exercise real URL, method and
body construction rather than mocks. The canned bodies are the shapes `freshet-serve`
really emits — `crates/freshet-serve/tests/http.rs` asserts the same shapes off a real
socket from the Rust side.
