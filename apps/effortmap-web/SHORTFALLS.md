# Where the WebAssembly build falls short

Measured on 2026-09-08 against this tree, not reasoned about. Several things people
expect to be problems are not, and the ones that are real are mostly not about
WebAssembly at all.

## Not problems, measured

**Payload.** 1.1 MiB raw, 359 KiB gzipped. dagpane's `ROADMAP.md` §4 is right that
"Rust/WASM means a smaller payload" is false for anything shipping a SQL *engine* —
DuckDB-Wasm is 34.25 MB, shinylive's Pyodide base ~13 MB — and it does not apply here,
because the planner parses SQL for lineage and never executes it.

**Scale.** A synthetic project through the compiled artifact under node:

| Models | Plan | Response | |
|---|---|---|---|
| 51 | 24 ms | 11 KB | |
| 501 | 58 ms | 109 KB | |
| 1 001 | 83 ms | 218 KB | |
| 2 001 | 169 ms | 444 KB | 7 MB heap |

A dbt project larger than most plans in under a fifth of a second in a browser tab.
The wasm32 4 GB ceiling and the COOP/COEP threading problem are both irrelevant at
this size, and planning is single-threaded anyway.

**Determinism across bindings.** The native and wasm builds produce byte-identical
responses, and `test/wasm.test.mjs` plus `crates/freshet-wasm/tests/golden.rs` hold
them to it.

## Real, and caused by the browser

**1. Every u64 that crosses into JavaScript is a trap.** Fingerprints are `u64`;
JS numbers carry 53 bits. `JSON.parse` rounds them silently, and because the
fingerprints *are* the state, the incremental planner degrades into "rebuild
everything, every time" while looking merely conservative. Fixed for state by
handling it as raw text (`planner.js`), and the golden test compares raw bytes so it
cannot come back. **But the fix is per-field, not structural**: the next `i64` or `u64`
added anywhere in the wire will arrive with the same trap armed and no test watching
for it. The structural fix is serializing large integers as strings in the contract,
which is a change to `freshet-state` and would break existing `state.json` files.

**2. Directory access is uneven and cannot be evened out.** `showDirectoryPicker()`
keeps a re-readable handle but is Chromium-only. Firefox and Safari get a
`webkitdirectory` input: it works, but the folder must be re-picked every session and
nothing can be written back. Against `freshet-serve --project ./models` this is the
real regression, and it is a platform gap, not something to engineer around.

**3. State is per-browser.** `localStorage` is not shared, so two engineers looking at
the same repository get two different answers about what has been built. Exporting
`state.json` and committing it makes one answer — a snapshot is ~40 bytes per model, so
even a thousand-model project is ~40 KB and reviewable in a diff — but that is a
convention a team has to keep, not a property of the tool.

**4. A panic is a dead tab.** Rust panics abort across the wasm boundary with no
unwinding and no usable backtrace. The funnel returns errors as data rather than
panicking, which is why this has not bitten, but the day something does panic the page
will simply stop with nothing in the console worth reading. The native daemon prints a
backtrace and keeps serving.

**5. Toolchain friction for whoever builds it.** `wasm-bindgen`'s CLI and crate
versions must match exactly, and the mismatch fails at generation with a version
banner. `scripts/build.sh` reads the version out of `Cargo.lock` so this cannot drift
silently, but building still needs cargo, the wasm32 target and that CLI. Only
maintainers pay this; users get a static page.

## Real, and not about WebAssembly at all

These are planner behaviours. They matter more in the browser only because there is no
CLI beside it to cross-check against.

**6. A typo'd model reference is silent, and produces a wrong plan.** Given

```sql
-- mart.sql, meaning to read stg
select a from stgg
```

the planner reports no error. `stgg` is unknown, so it is assumed to be an external
table, `mart` gets **no edge to `stg`**, and both land in wave 0 — a runner would
rebuild them concurrently and `mart` would read stale data. This is precisely the
"a wrong edge is a wrong app" failure dagpane's `ROADMAP.md` §6 says the design exists
to avoid, and here it happens without a word.

The fix is a strictness setting: an unknown table is only external if it is declared in
`sources`, and otherwise an error. That cannot default to on without breaking projects
that legitimately read undeclared tables, so it wants to be a flag — and the flag wants
to be on in CI.

**7. Cost is a rate card, not a measurement.** One CU is *defined* as one full rebuild
of one `table` model. Two `table` models therefore always cost exactly 2.00 CU whether
one has a thousand rows and the other a billion. The number ranks a plan against
another plan of the same project; it does not predict a bill or a duration, and every
surface here says so. Making it predictive means feeding real execution telemetry back
into `CostModel`, which needs a runner reporting outcomes — nothing in this repository
does that yet.

**8. The planner has no data.** It reads SQL text and computes lineage. It never
connects to a warehouse, never sees a row, and cannot show anyone their data. Anything
of the form "let an analyst look at their data" is a different product, and
that product is dagpane, not this.

## What was fixed while writing this

- The error chain now reaches the wire. A bad statement used to report `parsing model
  \`m\`` and stop; it now reports ``parsing model `m`: SQL parse error: Expected: an SQL
  statement, found: selct at Line: 1, Column: 1``. One `{:#}` instead of
  `to_string()` — anyhow prints only the outermost context otherwise, and in a browser
  there is no CLI to re-run for the detail.
