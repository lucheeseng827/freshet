# Effort map

A Streamlit front end for the backfill planner. It answers three questions about a
change to the data layer, on one page:

- **What rebuilds?** The minimal, column-pruned set — and for each model, whether it
  is your change or its blast radius, with the columns that pulled it in.
- **What does it cost?** The effort map: a predicted total in compute units, with a
  per-model breakdown and the rate-card basis behind every number.
- **How would it run?** The queue: one work item per model — or per *partition* where
  the rebuild is partition-scoped — with barriers, concurrency waves, and an NDJSON
  export a runner can drain.

## Run it

Start the daemon once:

```sh
cargo build --release -p freshet-serve --features http
# cargo puts the binary in the module's own target/, two levels up from this app.
../../target/release/freshet-serve --project ./models --sources sources.json --materializations mat.json --port 8788
```

Then pick a renderer. **The choice is made in Python**, and three of the four cost no
install at all:

```sh
python render.py --to text                     # a terminal, a notebook, a CI log
python render.py --to html --out map.html      # one self-contained file, no server
python render.py --to json                     # for a machine
python render.py --to ndjson                   # the work queue, for a runner

pip install -r requirements.txt                # only for the next line
python render.py --to streamlit                # the interactive app
```

`render.py --help` lists every renderer with what it costs. `--project ./models` loads
a directory into the daemon first, so one command goes from SQL on disk to a rendered
answer; `--fail-over 5` exits 2 when a plan is heavier than that many CU, which makes a
CI gate a one-liner.

Everything talks to the daemon over HTTP through the `planner` SDK next door
(`../../sdks/python`). No Rust toolchain is needed to *use* any of it, only to build the
daemon once.

## What the numbers mean

**One CU is one full rebuild of one `table` model.** That is a definition, not a
measurement: the figure is read off a rate card (`freshet_core::CostModel`), there is
no benchmark behind it, and nothing on the page is denominated in seconds. The page
says so in its own footer, because a cost estimate that quietly reads as minutes is
worse than no estimate.

`mCU` is the exact integer the planner computed — compare and sum that. `CU` is
`mCU / 1000`, for display only.

The per-partition shares of a model sum to exactly that model's total, so the
per-item column and the per-model total always add up.

## Layout

```
effortmap.py       pure shaping: a Plan in, rows out. No Streamlit, no pandas,
                   no Altair, so the arithmetic is testable alone.
render.py          the renderer seam + the CLI. text / html / json / ndjson are
                   standard library; `streamlit` is one registered option among
                   them, declared as needing extras.
app.py             the Streamlit shell — layout only
test_effortmap.py  29 tests, standard library
test_render.py     18 tests, standard library, and none of them import streamlit
```

That split is the same one the Rust side uses, where `freshet-core` is pure and the
transports are thin. Adding a renderer is registering a function:

```python
@renderer("md", "GitHub-flavoured Markdown, for a PR comment")
def _markdown(plan, out, **opts): ...
```

`RENDERERS` is what `--to` reads, so a new entry is listed in `--help` and selectable
without touching the argument parser.

## Two notes on the charts

**Colour carries one thing: changed vs. downstream.** Two hues, categorical slots 1
and 2 of the reference palette, validated in both light and dark (worst CVD ΔE 24.7
light / 26.8 dark against an ΔE≥8 target). Identity is never colour-alone — the same
split is a text column in the table under every chart, and the cost table is the
accessible view of the same numbers.

**The legend sits on the right, not on top.** Streamlit sizes an Altair chart with
Vega-Lite's fit-autosize, and under that a top- or bottom-oriented legend takes its
space out of the same height budget as the plot: the plot collapses to zero and the
axis labels stack on one line. This was verified in a browser, not reasoned about;
the comment at the encoding says the same thing so nobody re-introduces it.

## Where it stops

- **The daemon derives what changed from fingerprints.** There is no endpoint that
  takes a caller-supplied change set, so the UI cannot ask hypothetical "what if I
  changed column X" questions. The two modes it does offer are the two the planner
  has: a code-change plan, and a data restatement.
- **No authentication, anywhere.** `freshet-serve` binds loopback and warns on
  anything else; this app inherits that and adds nothing.
- **Committing is a manual button and says why.** It advances the fingerprints that
  record which code has been built, so pressing it without having run the plan makes
  the next plan see nothing to do — a silently skipped rebuild.
