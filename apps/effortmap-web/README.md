# Effort map — the browser build

The backfill planner, compiled to WebAssembly and running in a static page. Point it at
a folder of `.sql` models and it answers the same three questions the Streamlit app
does — what rebuilds, what it costs, how it would run — with **no server, no Python and
no upload**.

```sh
./scripts/build.sh                  # once; needs cargo + wasm-bindgen-cli
python3 -m http.server 8600         # or any static file server
open http://localhost:8600/
```

A static server is needed only because `fetch` refuses `file://`; nothing is served
dynamically. Click **Load the example project** to see it work with no files at all.

## Why this is small enough to be worth doing

A sibling project's roadmap makes the case that "Rust/WASM means a smaller payload" is
**false for anything that ships a SQL engine** — DuckDB-Wasm is 34.25 MB uncompressed,
shinylive's Pyodide base ~13 MB. That argument is correct, and it does not apply here,
for one reason: **the planner never had a SQL engine to ship.** It parses SQL to derive
lineage and never executes any of it.

| | Raw | Gzipped |
|---|---|---|
| `freshet_wasm_bg.wasm` | ~1.1 MiB | ~359 KiB |
| Pyodide base (shinylive) | ~13 MB | — |
| DuckDB-Wasm | 34.25 MB | — |

A cold plan of the example project takes ~15–20 ms in the browser, parsing included.

## Layout

```
index.html            the page
assets/
  app.js              wiring: files in, plan out, rows on the page — the DOM only
  effortmap.js        pure shaping; no DOM, no wasm, no fetch. Tested under node.
  planner.js          the seam to the WebAssembly module, and the state rules below
  charts.js           SVG bar charts, no library
  styles.css          the whole design system, light and dark
  favicon.svg
scripts/build.sh      cargo -> wasm-bindgen -> (wasm-opt) -> assets/
test/                 node --test, no dependencies
  effortmap.test.mjs  27 tests, a port of the Streamlit app's Python suite plus the
                      HTML-escaping rules the table renderer depends on
  wasm.test.mjs       12 tests over the real artifact, including the golden
  golden/             the request/response fixture both bindings are pinned to
```

There is no `package.json` and no bundler, and CI fails the build if one appears. The
page is served exactly as it sits in the tree.

## The one genuinely subtle thing: u64 fingerprints

`StateSnapshot.fingerprints` maps a model to a **u64**. JavaScript numbers are doubles
with 53 bits of integer precision, and a fingerprint routinely exceeds that:

```
wasm emits     16899773155021735464     exact
JSON.parse ->  16899773155021736000     rounded, silently
```

The fingerprints *are* the state — they are what a later plan compares against to
decide which models were edited. Parse them into JS numbers and every stored
fingerprint differs from the freshly computed one, so **every model looks changed** and
the incremental planner quietly degrades into "rebuild the world, every time". That
reads as conservatism, not corruption, and would not be noticed.

So state never becomes a JS number. It is pulled out of the response as raw **text**
and spliced back into the next request as raw text (`rawField` / `withRawField` in
`planner.js`), and stored as text in `localStorage`. This is the instinct
`dagron-state`'s `wire.rs` already records — *"the snapshot is carried opaquely as raw
JSON; dagron has no business interpreting fingerprints"* — except that in JavaScript
"opaque" has to mean text, because parsing is itself the lossy step.

This was found by the cross-binding golden test on its first run, not by reasoning.

## The cross-binding golden test

`freshet-embed`'s docstring promises every binding "answers identically (a cross-binding
golden test)". That test did not exist. It does now, in two halves pinned to one
fixture rather than to each other:

- `test/wasm.test.mjs` runs the compiled `wasm32` artifact and compares **raw text** —
  a parsed comparison rounds both sides identically and passes while the state is being
  corrupted, which is exactly what happened the first time.
- `../../crates/freshet-wasm/tests/golden.rs` runs the **native** build against the same
  files.

Regenerate the fixture only when the contract changes on purpose, and check both halves
in the same commit.

## What this build does *not* do

- **Directory access is uneven.** `showDirectoryPicker()` keeps a handle and can re-read
  a folder, but is Chromium-only today. Elsewhere the page falls back to a
  `webkitdirectory` input, which works but must be re-picked each session. This is the
  real regression against `freshet-serve --project ./models`.
- **State is per-browser.** `localStorage` is not shared, so two engineers get two
  answers. Export `state.json` and commit it to have one — a snapshot is roughly 40
  bytes per model, so a thousand-model project is ~40 KB, small enough to review in a
  diff. Sharing it live needs a server, which is what `freshet-serve` is for.
- **Nested project folders are ignored**, matching `freshet-serve --project`: flattening
  dbt-shaped subdirectories would silently collide two models both named `orders.sql`.
- **Nothing here executes SQL.** The planner decides what to rebuild; a warehouse
  rebuilds it. "Mark as built" only advances fingerprints, and the page says so.

## The unit

**One CU is one full rebuild of one `table` model** — a definition read off
`freshet_core::CostModel`, not a measurement. There is no benchmark in this repository
and nothing here is denominated in seconds. `mCU` is the exact integer; `CU` is that
divided by 1000, for display only.
