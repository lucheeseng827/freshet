# Freshet benchmarks — measured, with the machine attached

> Two kinds of timings, never mixed: **criterion** rows time
> library calls in-process (`crates/freshet-bench/benches`), and **comparison** rows
> time whole processes — Freshet's CLI beside dbt-core, dbt v2 and SQLMesh — on one
> exported topology (`scripts/compare.sh`). And one kind of answer that is not timed
> at all: **plan quality**, which models a plan rebuilds against the models that
> really change, across lineage densities. Nothing is extrapolated. Every comparison
> and quality number below is printed from a committed file in `results/`, which a
> test holds to its raw repetitions or to the code.

## Reproduce

```sh
cargo bench -p freshet-bench --bench freshet_bench -- \
  --warm-up-time 1 --measurement-time 3 --sample-size 20

# just the end-to-end warm replan
cargo bench -p freshet-bench --bench freshet_bench -- plan_warm \
  --warm-up-time 1 --measurement-time 3 --sample-size 20

# plan quality across lineage densities (exact, and also a test)
cargo run -p freshet-bench --bin freshet-bench-quality -- --table results/quality-sweep.json

# the weekly gate, locally (~20 min on 4 cores)
scripts/compare.sh --only freshet --sizes 80x128,625x160 --out weekly.json
target/release/freshet-bench-manifest --gate weekly.json
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

### Run C — 100,000 models

`results/compare-2026-10-27-freshet-100k.json`, commit `b1acf5e`, Freshet only, on a
**slower host** than runs A and B (Xeon @ 2.10 GHz, 4 cores). Two rungs, back to back
in one run: 10,240 models (80 × 128) and 100,000 (625 × 160). The edit to `m_0_0`
rebuilds 80 and 3,114 models, and every plan reported exactly that.

| operation | 10,240 models | 100,000 models | µs per model, 10,240 → 100,000 |
|---|--:|--:|--:|
| LOAD: `graph` | 0.658 s | 8.617 s | 64.3 → 86.2 (1.34×) |
| PLAN0: `plan`, nothing built yet | 1.032 s | 11.659 s | 100.8 → 116.6 (1.16×) |
| DIFF: `plan`, nothing changed | 0.872 s | 9.732 s | 85.2 → 97.3 (1.14×) |
| DIFF: `plan`, `m_0_0` edited | 0.883 s | **9.976 s** | 86.2 → 99.8 (1.16×) |
| WARM: daemon replan | 0.337 s | 3.331 s | 32.9 → 33.3 (1.01×) |

Medians of 5; the widest MAD at 100,000 is 0.73 s (`plan`, nothing changed), and the
edited plan's is 0.24 s.

**The budget is met, by less than the noise.** The budget is the CLI planning a
one-model edit at 100,000 models within 10 s — "100k < 10s" from the roadmap's
targets to beat. The median is 9.976 s, 24 ms under, and that row's own MAD is
240 ms: on this host the budget holds at the median and would not hold on a slower
minute. That is why it is a weekly gate on a runner rather than a sentence on this
page.

**Scaling is mildly super-linear, and inside the rule.** Per model, 100,000 models
cost 1.14–1.34× what 10,240 do, the load the most, against the gate's limit of 1.5×.
The daemon's warm replan stays flat at 33 µs per model: 3.3 s for 100,000, which is
the argument for the daemon at this size.

**Where the 10 s goes.** One run of each command on the same 100,000-model project,
not medians: `freshet graph` took 7.0 s of wall time but 5.1 s of CPU, so about 2 s
is reading 100,001 files; `freshet fingerprint` took 10.6 s, so fingerprinting adds
about 3.5 s — the AST fingerprint parses every model's SQL again, after
`build_project` has already parsed it. Run D takes that margin.

### Run D — the second parse removed

`results/compare-2026-10-27-freshet-100k-run-d.json`, commit `eb6bc36`, on run C's
host and in the same harness. The AST fingerprint no longer parses: the build hands
each model's parsed tree over the moment it has analyzed it, the fingerprint hashes
the tree there and keeps only the hasher's state, and the tree is freed — every
fingerprint byte for byte what it was (all 100,000 compared, and a state committed by
the old binary plans as up to date).

| operation | 100,000, run C | 100,000, run D | run D / run C | 10,240, run D |
|---|--:|--:|--:|--:|
| LOAD: `graph` | 8.617 s | 6.565 s | 0.76 | 0.617 s |
| PLAN0: `plan`, nothing built yet | 11.659 s | 10.924 s | 0.94 | 0.861 s |
| DIFF: `plan`, nothing changed | 9.732 s | 7.749 s | 0.80 | 0.708 s |
| DIFF: `plan`, `m_0_0` edited | 9.976 s | **8.588 s** | 0.86 | 0.750 s |
| WARM: daemon replan (unchanged code) | 3.331 s | 3.548 s | 1.07 | 0.362 s |

The daemon's code did not change and came out 7% slower, which is how far the host
drifted between the two runs; read the other ratios with that in mind. Alternating
the two binaries on one project, which cancels the drift, gave `graph` 10.74 → 7.38 s
and the edited plan 12.60 → 9.38 s, peak memory 1,455 → 1,167 MB. **The budget now
holds by 1.4 s**, with a MAD of 0.13 s, and per-model growth is 1.09–1.30×.

**`graph` gained most, and it never fingerprints.** The trees are now freed as the
build goes, each right after its model is analyzed, not all at its end, so what is
allocated afterwards reuses warm memory instead of faulting in fresh pages. That is
also why the first attempt failed: it kept every tree alive past the build for the
fingerprint to reuse, which saved the parse's CPU (about 1.9 s) and lost more in
memory — 112 MB more at peak, 30,000 more page faults — and planned the edit in
11.837 s, failing the budget and three scaling rows
(`results/compare-2026-10-27-freshet-100k-kept-trees.json`, commit `af4b34e`, kept
because it is the reason the design is what it is).

### Run E — the daemon's fingerprint cache

`results/compare-2026-10-27-freshet-100k-run-e.json`, commit `2bd597b`, the same host
and harness. The daemon used to fingerprint every model from scratch on every plan,
parsing all of them under its write lock. It now keeps each model's *SQL half* of
the fingerprint beside the exact SQL text it was hashed from, and finishes it with
the model's column names and dependencies as they are at that plan: a cached half is
only ever used beside its own text, so a model whose SQL stayed while its columns or
dependencies moved is still fingerprinted exactly. A full reload seeds the cache from
the trees it parses anyway; a plan parses only SQL the cache has not seen.

| daemon replan, nothing changed | run D | run E | |
|---|--:|--:|--:|
| 10,240 models | 0.362 s | **0.046 s** | 7.9× faster |
| 100,000 models | 3.548 s | **0.394 s** | 9.0× faster |

That is 3.9 µs per model at 100,000, and the 0.39 s includes the curl process and
the loopback HTTP round trip. The row's `note` in this run's file still describes the
daemon re-parsing every model: it was recorded by `compare.sh` before the script's
wording caught up with the cache, and a measured record is not edited after the fact.

**This run's CLI rows are not a measurement of the CLI's code.** At 100,000 models
they came out 23–46% slower than run D's — the edited plan in 10.69 s, which misses
the 10 s budget, and three operations at 1.53–1.55× per model, past the 1.5× rule —
while at 10,240 they matched run D within 5%. The CLI's work did not change between
the two commits (one argument became an `Option`, and each model's hasher state is
cloned once). Alternating run D's binary and run E's on the same 100,000-model
project, five times each, settles it: `graph` 7.97 s and 7.03 s, the edited plan
9.29 s and 8.59 s, the same 1,167 MB peak and the same 298,913 page faults. The host
was slower during run E's 100,000-model rung. The budget stands on run D, and the
gate's verdict on run E is published as it came out.

## The weekly gate

Every Sunday (and on demand) a GitHub-hosted runner runs `scripts/compare.sh --only freshet --sizes
80x128,625x160` and `freshet-bench-manifest --gate` over what it wrote. The job fails
on any of three rules (`freshet_bench::gate::Rules::PUBLISHED`):

| rule | holds when |
|---|---|
| **budget** | the CLI plans the one-model edit at 100,000 models within 10 s (median of 5) |
| **scaling** | every operation's time per model at 100,000 is at most 1.5× its time per model at 10,240 |
| **answers** | every operation planned the number of models it should |

Both rungs run in the same job on the same runner, so a slow runner slows both and
cancels out of the scaling ratio: that rule catches a super-linear regression on any
machine. The budget does not cancel out — it is a 4-vCPU GitHub runner's; on run D's
2.1 GHz host it holds by 1.4 s, on run C's by 24 ms. The run's manifest is kept with
the job. Tests hold the committed runs C and D, and run B's two rungs, to the same
rules, and the kept-trees run and run E's CLI rows to failing them. Plan *quality* is not timed, so it is not in the weekly job: the quality
sweep above is a test on every change.

## Plan quality across lineage densities

Speed at equal answers says nothing about the answer. This section measures it:
for an edit to one column of one model, how many of the models downstream of it
the plan rebuilds, against how many really change. It is computed, not timed, so it
is exact and the same on every machine.

The generator takes two density knobs: how many parents each model joins (on
`c0`), and how many of each parent's columns it selects. Each row edits one column
of each of eight layer-0 models, one at a time, and sums three counts: the
**closure** (the edited model and everything downstream: what a planner without
column lineage rebuilds, dbt's `state:modified+`), the models whose output
**really changes**, and the models Freshet **planned**. The truth comes from the
generator's own record of which column each output selects — a changed column
reaches the columns that select it, and a changed join key changes every column of
its join — never from the SQL the planner reads.

1024 models (16 layers × 64, 8 columns, seed 0x51515151); each row sums 8 edits, one layer-0 model at a time.

| parents | columns read per parent | edited column | closure | changed (truth) | planned | planned / closure | truth / planned | missed |
|--:|--:|---|--:|--:|--:|--:|--:|--:|
| 1 | 1 | `c0` (key) | 48 | 38 | 38 | 0.79 | 1.00 | 0 |
| 1 | 1 | last (value) | 48 | 8 | 8 | 0.17 | 1.00 | 0 |
| 1 | 2 | `c0` (key) | 128 | 23 | 23 | 0.18 | 1.00 | 0 |
| 1 | 2 | last (value) | 128 | 23 | 23 | 0.18 | 1.00 | 0 |
| 1 | 4 | `c0` (key) | 128 | 22 | 22 | 0.17 | 1.00 | 0 |
| 1 | 4 | last (value) | 128 | 30 | 30 | 0.23 | 1.00 | 0 |
| 1 | 8 | `c0` (key) | 128 | 128 | 128 | 1.00 | 1.00 | 0 |
| 1 | 8 | last (value) | 128 | 128 | 128 | 1.00 | 1.00 | 0 |
| 2 | 1 | `c0` (key) | 3916 | 3916 | 3916 | 1.00 | 1.00 | 0 |
| 2 | 1 | last (value) | 3916 | 8 | 8 | 0.00 | 1.00 | 0 |
| 2 | 2 | `c0` (key) | 3266 | 3266 | 3266 | 1.00 | 1.00 | 0 |
| 2 | 2 | last (value) | 3266 | 1196 | 1196 | 0.37 | 1.00 | 0 |
| 2 | 4 | `c0` (key) | 3321 | 3321 | 3321 | 1.00 | 1.00 | 0 |
| 2 | 4 | last (value) | 3321 | 763 | 763 | 0.23 | 1.00 | 0 |
| 4 | 1 | `c0` (key) | 6494 | 6494 | 6494 | 1.00 | 1.00 | 0 |
| 4 | 1 | last (value) | 6494 | 844 | 844 | 0.13 | 1.00 | 0 |
| 4 | 2 | `c0` (key) | 5593 | 5593 | 5593 | 1.00 | 1.00 | 0 |
| 4 | 2 | last (value) | 5593 | 1578 | 1578 | 0.28 | 1.00 | 0 |

`results/quality-sweep.json`, regenerated by `freshet-bench-quality >
results/quality-sweep.json`; `freshet-bench-quality --table` printed the table.

**How to read it.** `missed` is the column that matters: a model that changes and is
not planned is left stale, and the test that pins this file fails as **unsound** the
moment any row is above zero. `truth / planned` is precision — 1.00 means every model
rebuilt needed it — and is 1.00 at every point: on this family of projects Freshet's
lineage is exact. `planned / closure` is what column lineage saves over model-level
selection: nothing where every model selects every column of its parent (the
`8`-column row, the topology the timings above use, and why their edits rebuild the
whole closure), and up to all but the edited models where a value nobody downstream
selects changes (two parents, one column each: 8 of 3,916). A change to a join key
reaches every join below it, so its rows sit at 1.00 wherever there are joins.

The test fails as a **regression** if any row's plan grows, and asks for the file
to be regenerated if one shrinks, so the published numbers cannot overstate or
understate the planner. It was checked against two deliberate breakages of the
planner: dropping join keys from row lineage fails it as unsound (2,429 models
missed), and making every column depend on all of its source's columns fails it as
a regression. What the sweep does not exercise is filters: the generator writes no
`WHERE`, `GROUP BY` or `QUALIFY`, and those row-deciding clauses are covered by the
live-PostgreSQL oracle in CI instead, which executes random projects and checks
every model whose rows changed was planned.

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
but not flat, and this is the constant the weekly gate's scaling rule watches.

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
a regression — and a baseline on a fixed host is what would settle it.
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
| CPU | runs A, B and criterion: Intel Xeon @ 2.80 GHz; runs C, D and E: Intel Xeon @ 2.10 GHz; 4 cores each (containerised; not isolated benchmark hosts) |
| rustc | 1.94.1 (e408947bf 2026-03-25) for runs A, B and criterion; 1.97.0 (2d8144b78 2026-07-07) for runs C, D and E. Criterion under the `bench` profile, the comparison CLIs under `release` (`opt-level = "z"`, LTO) |
| Hosts | run A: Linux 6.18.44-fc-v40; run B and the criterion tables: Linux 6.18.44-fc-v37 — same CPU model and core count, ~30% apart (see run B); runs C, D and E: Linux 6.18.44-fc-v165 |
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
* Bytes/sec per rung, and criterion's own `--save-baseline` diffs. The weekly gate
  judges whole-process runs instead (see [The weekly gate](#the-weekly-gate)),
  and its budget is a GitHub-hosted runner's, not a fixed host's.
* The sweep's topology has no filters, so its precision says nothing about
  `WHERE` / `GROUP BY` / `QUALIFY` lineage; the live-PostgreSQL oracle covers their
  soundness, and nothing yet measures their precision.
