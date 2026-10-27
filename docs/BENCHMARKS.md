# Freshet benchmarks — measured, with the machine attached

> Two kinds of numbers, never mixed: **criterion** rows time
> library calls in-process (`crates/freshet-bench/benches`), and **comparison** rows
> time whole processes — Freshet's CLI beside dbt-core, dbt v2 and SQLMesh — on one
> exported topology (`scripts/compare.sh`). Nothing is extrapolated. Every comparison
> number below is printed from a committed file in `results/`, whose medians a test
> re-derives from the raw repetitions.

## Reproduce

```sh
cargo bench -p freshet-bench --bench freshet_bench -- \
  --warm-up-time 1 --measurement-time 3 --sample-size 20

# just the end-to-end warm replan
cargo bench -p freshet-bench --bench freshet_bench -- plan_warm \
  --warm-up-time 1 --measurement-time 3 --sample-size 20
```

`freshet-bench` sits outside `default-members`, so `criterion` never enters the
default build graph — a bare `cargo build` does not compile any of this.

## Against dbt and SQLMesh, on the same project

`freshet-bench-export` writes one generated topology three ways — a dbt-duckdb
project, a SQLMesh project, a Freshet directory — by rewriting only the relation
names in each model (a test undoes the rewrite and gets the generated SQL back byte
for byte). `scripts/compare.sh` then times each tool's own command as a whole
process: 1 warm-up and 5 timed repetitions each, median and MAD, CPU time and the
rest of the machine's CPU recorded per repetition. Each tool's answer is checked:
for the edit (`m_0_0` gains a `WHERE`, which changes its rows but not its columns
or dependencies) **every tool selected the same set — 16 models at 1,024, 80 at
10,240** — and a mismatch would have been written into the row.

```sh
# in a venv with dbt-core, dbt-duckdb and sqlmesh (dbt-core >= 1.12 also installs dbt v2)
scripts/compare.sh --out results/compare-$(date +%F).json   # ~2.5 h on 4 cores
scripts/compare.sh --only freshet                           # minutes
target/release/freshet-bench-manifest --table results/<file>.json   # full tables
```

### Run A — all four tools, one machine, one commit

`results/compare-2026-09-22-all-tools.json`, commit `65ce159`. **10,240 models**
(80 layers × 128, 8 columns):

| question | Freshet 0.1.0 | dbt-core 1.12.5 | dbt v2 2.0.5 (beta) | SQLMesh 0.236.2 |
|---|--:|--:|--:|--:|
| LOAD — parse every model, build the DAG | 0.810 s `graph` | 73.577 s `parse` | 10.201 s `parse` | 101.317 s `info` |
| WARM — the same, tool's cache warm | 0.333 s daemon replan¹ | 12.430 s partial parse | 1.213 s `--partial-parse` | 75.460 s `.cache/` |
| DIFF — nothing changed (all answer 0) | 1.113 s `plan` | 77.805 s `ls state:modified+` | 13.023 s `ls state:modified+` | 181.049 s `plan --explain` |
| DIFF — `m_0_0` edited (all answer 80) | **1.039 s** `plan` | 80.322 s | 12.865 s | 486.657 s |
| PLAN0 — nothing built yet (all N) | 1.354 s `plan` | — (no such step) | — | 521.904 s `plan --explain` |

At **1,024 models** (16 × 64) the edited-model DIFF row reads 0.108 s / 11.527 s /
1.488 s / 23.697 s, and LOAD 0.088 s / 10.520 s / 1.150 s / 11.841 s.

¹ A `curl` against a running `freshet-serve` that parsed the project once at start-up;
a long-running daemon is a different architecture from the others' per-run caches,
and the row says so. Freshet's CLI keeps no parse cache, so its DIFF rows are cold.

**How to read it.** The rows answer the same question, not the same work: Freshet's
`plan` also derives column lineage, fingerprints every model's AST and prices the
plan; `dbt ls` compares file checksums and selects descendants (model-level, no
lineage); SQLMesh's `plan --explain` renders and optimizes every query, reads its
state from DuckDB and computes intervals. What they share is the answer a user
waits for — *what must rebuild* — and on that Freshet is **77× dbt-core, 12× dbt
v2 and 468× SQLMesh** for the one-model edit at 10,240 models, single-threaded.
At 10,240 models every MAD is within 6% of its median (the widest, SQLMesh's cold
load); at 1,024 the sub-100 ms Freshet rows are noisier, up to 11% (`graph`).

### Run B — the merged planner, with a control

Run A timed the planner *before* the lineage soundness fixes (row-deciding refs,
import-CTE edges, uncertainty propagation) and contract v5. Run B re-times Freshet
at commit `7e68f20`, which has them (`results/compare-2026-09-23-freshet.json`),
and dbt v2 again as a control (`…-dbt-oss-control.json`), because the container
had been replaced in between.

| 10,240 models | run A | run B | B / A |
|---|--:|--:|--:|
| Freshet `graph` | 0.810 s | 0.560 s | 0.69 |
| Freshet `plan`, `m_0_0` edited | 1.039 s | 0.823 s | 0.79 |
| Freshet daemon replan | 0.333 s | 0.295 s | 0.89 |
| **control:** dbt v2 `parse` | 10.201 s | 7.345 s | 0.72 |
| **control:** dbt v2 `ls`, edited | 12.865 s | 10.535 s | 0.82 |

The control moved as much as Freshet did: run B's machine is simply faster, and
the soundness fixes cost no measurable time. Read run B for the current code and
run A for the cross-tool ratios; do not divide a run-B Freshet row by a run-A row.

## Results — criterion

Criterion median, one full `GenSpec` ladder, at commit `7e68f20` (the merged planner,
lineage soundness fixes included) on run B's machine. Fixtures are generated once
outside the timed region and `black_box`'d; the generator is seeded (`0x5151_5151`)
so a rerun reproduces the same topology. The previous revision of this table (the
planner before the soundness fixes, on an earlier host) read 604.57 ms for
`build_project` at 10,240. The two are different hosts, so the difference is not a
code speed-up: in run A vs run B, the same dbt v2 binary moved by as much as Freshet did.

| Models | `build_project`<br/>(parse + lineage) | `fingerprint_project` | `changed_models`<br/>(state diff) | `plan_backfill_1col`<br/>(1-column blast radius) |
|------:|------:|------:|------:|------:|
| 128 | 3.89 ms | 97.6 µs | 5.8 µs | 48.4 µs |
| 512 | 18.60 ms | 441.4 µs | 35.6 µs | 271.6 µs |
| 2,048 | 78.83 ms | 2.35 ms | 195.6 µs | 1.46 ms |
| 5,120 | 225.35 ms | 9.42 ms | 723.0 µs | 6.37 ms |
| 10,240 | 466.01 ms | 22.72 ms | 1.63 ms | 12.88 ms |

### The 10k target: met on this machine, which is not a benchmark host

§M5 sets "**10k models < 600 ms parse**". Measured: **466.01 ms at 10,240 models**.
The previous revision measured 604.57 ms on an earlier host, and hosts in this
environment differ by ~30% (the dbt v2 control above), so the margin belongs as much
to the host as to the code. The
claim this page makes is the measured one: under 600 ms at 10,240 models on a 4-core
containerised Xeon, not a rate card.

### Scaling is super-linear, mildly

`build_project` µs/model across the ladder:

| Models | 128 | 512 | 2,048 | 5,120 | 10,240 |
|---|---|---|---|---|---|
| µs/model | 30.4 | 36.3 | 38.5 | 44.0 | 45.5 |

An 80× growth in model count costs 50% more per model, flattening at the top (44.0 →
45.5 over the last 2×). Not the blow-up the problem statement attributes to dbt-core,
and — at 10,240 in run A — 90× faster than dbt-core's own parse of the same files,
but not flat, and this is the constant a nightly regression gate would watch.

### What the numbers actually say about the product

**Parsing dominates.** At 10,240 models `build_project` is 466.0 ms — **21×**
fingerprinting (22.7 ms), **36×** planning (12.9 ms) and **290×** the state diff
(1.6 ms). The four stages sum to 503.2 ms, so parse + lineage is **93%** of a cold
run.

That ratio is the case for the daemon, and the warm path is **measured end to end**:

| Models | `plan_warm_nochange`<br/>(snapshot matches — the every-commit case) | `plan_warm_1model`<br/>(one model edited) |
|------:|------:|------:|
| 128 | 146.9 µs | 183.4 µs |
| 512 | 721.7 µs | 927.3 µs |
| 2,048 | 3.95 ms | 4.69 ms |
| 5,120 | 13.81 ms | 12.50 ms |
| 10,240 | **29.98 ms** | **32.91 ms** |

Two reruns of just the 10,240 rows read 33.6 / 36.1 ms (no change) and 37.3 / 36.0 ms
(one edit), so on this host the warm replan is **30–37 ms** on 10,000 models, one
core; the earlier revision's 25.6 ms came from a different host and the planner
before the soundness fixes. None of the code on the no-change path (fingerprint,
state load, diff) changed in between, so the spread is read as host variance, not as
a regression — and a nightly baseline on a fixed host is what would settle it.
`BackfillPlanner::plan` re-fingerprints on every call, so the figure covers
fingerprint + state load + diff + plan + cost estimate, timed as one call.

> **This corrected a real error, not just a label.** An earlier revision of this page
> reported the warm replan as **≈39 ms**, arrived at by *adding* the separately-timed
> `fingerprint_project`, `changed_models` and `plan_backfill_1col` rows. That sum was
> never a measurement of anything — and it was **32% too high**. Adding stage medians
> assumes the stages compose as benched, and they do not: `plan_backfill_1col` times a
> single-column blast radius against a hand-built change set, which is not the work
> `plan()` does. `bench_warm_replan` now times the real call, which is why the number
> moved. A page whose first line claims nothing is extrapolated had no business
> carrying a sum.

Note the two warm rows are not comparable with `plan_backfill_1col` above. `plan()`
diffs whole-model fingerprints, so it cannot express "only column `c0` changed" — it
marks the model changed and uses all its output columns. The `plan_backfill_1col` row
is the lower-level optimistic bound; `plan_warm_1model` is what an edit actually costs.

The parse is also what the browser build pays: `apps/effortmap-web` answers the same
question with no server at all.

A plan on a large project therefore costs milliseconds of one core. What that is worth
in money is not this module's question.

## Provenance

| | |
|---|---|
| CPU | Intel Xeon @ 2.80 GHz, 4 cores (containerised; not an isolated benchmark host) |
| rustc | 1.94.1 (e408947bf 2026-03-25); criterion under the `bench` profile, the comparison CLIs under `release` (`opt-level = "z"`, LTO) |
| Hosts | run A: Linux 6.18.44-fc-v40; run B and the criterion tables: Linux 6.18.44-fc-v37 — same CPU model and core count, ~30% apart (see run B) |
| Compared tools | dbt-core 1.12.5 + dbt-duckdb 1.11.0, dbt v2 2.0.5 (`dbt-core-experimental-parser`), SQLMesh 0.236.2 + sqlglot 30.8.0, DuckDB 1.5.5, Python 3.11.15 |
| criterion | 0.5.1 |
| sqlparser | 0.52.0 (pinned — a bump is an explicit fingerprint-scheme bump) |
| Criterion settings | `--warm-up-time 1 --measurement-time 3 --sample-size 20` |
| Generator | `GenSpec { cols: 8, seed: 0x5151_5151 }`, layers × width per rung |

**Read these as one machine's medians, not as a published rate card.** The sample size
and measurement time are below criterion's defaults (chosen so the suite finishes in
minutes), so the confidence intervals are wider than a release-quality run's. Nothing
here is denominated in warehouse seconds — Freshet parses SQL and never executes it,
so no figure on this page includes a query.

## Still open from §M5

These remain roadmap; this page does not claim them.

* The criterion tables are still transcribed from criterion's output (by script, not
  by hand, but not from a committed file). The comparison rows are not: they come
  from `results/compare-*.json`, and `committed_comparison_results_are_self_consistent`
  fails the build if a median stops matching its samples.
* A full four-tool run on the merged planner (run A predates it; run B re-timed
  Freshet and one control tool). `scripts/compare.sh` does it in ~2.5 hours.
* **Blast-radius ratio** across lineage-density settings: the generator has every
  model read all of its parent's columns, so column pruning cannot shrink any plan
  here — the comparison measures speed at equal answers, not pruning.
* Models/sec and bytes/sec per rung; a CI smoke lane and a nightly
  `--save-baseline main` gate on a fixed host.
* A 1e5 rung — ~5 s per `build_project` iteration at 45 µs/model, so a nightly job.
