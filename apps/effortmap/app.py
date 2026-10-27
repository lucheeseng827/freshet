"""Effort map — what rebuilding the data layer costs, and the queue to do it.

A Streamlit front end for the backfill planner. Point it at a `freshet-serve`
daemon; it answers three questions on one page:

  * **What rebuilds?** The minimal, column-pruned set, and for each model whether it
    is your change or its blast radius.
  * **What does it cost?** The effort map: a predicted total in compute units with a
    per-model breakdown and the rate-card basis behind each number.
  * **How would it run?** The queue: one work item per model — or per *partition* for
    a partition-scoped rebuild — with barriers, concurrency waves, and an NDJSON
    export a runner can drain.

Run it:

    pip install streamlit
    freshet-serve --project ./models --sources sources.json --port 8788   # in another shell
    streamlit run app.py

All arithmetic lives in `effortmap.py`, which imports neither Streamlit nor pandas.
This file is layout.

## On the unit

**One CU is one full rebuild of one `table` model.** That is a definition, not a
measurement: the figure comes off a rate card (`freshet_core::CostModel`), there is
no benchmark behind it, and no number on this page is denominated in seconds. The
page says CU everywhere, and says so out loud in the footer, because a cost estimate
that quietly reads as minutes is worse than none.
"""

from __future__ import annotations

import json
import os
import sys

import altair as alt
import pandas as pd
import streamlit as st

# The SDK and the shaping module sit beside this file in the repository rather than
# on the path. Prefer an installed `planner` if the user has one.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "sdks", "python"))

import effortmap  # noqa: E402
from freshet_sdk import Client, PlannerError, load_project  # noqa: E402

st.set_page_config(page_title="Effort map", page_icon="◔", layout="wide")

# Every `width="stretch"` below is the current sizing API and needs Streamlit >= 1.50,
# which is what requirements.txt pins and why. Its predecessor `use_container_width=True`
# reaches back to 1.40 but is deprecated and documented as due for removal, so this file
# stays on `width` and the floor carries the constraint. Swapping one back is a TypeError
# at render time on an older install — the page half-draws and then throws.


def is_dark() -> bool:
    """Which theme the page is being drawn in, tolerating older Streamlit builds."""
    try:
        return str(st.get_option("theme.base") or "light").lower() == "dark"
    except Exception:
        return False


COLORS = effortmap.series_colors(is_dark())
ORIGIN_DOMAIN = list(effortmap.origin_domain())
ORIGIN_RANGE = [COLORS["direct"], COLORS["downstream"]]

# Recessive grid and axes: the marks carry the data, the frame should not compete.
AXIS = alt.Axis(grid=False, domainOpacity=0.3, tickOpacity=0.3, labelFontSize=12)


# ── connection ────────────────────────────────────────────────────────────────

st.sidebar.title("planner")
url = st.sidebar.text_input("Daemon", value="http://127.0.0.1:8788", help="Where freshet-serve is listening.")
client = Client(url)

try:
    health = client.health()
    st.sidebar.success(f"connected · revision {health['revision']}")
except PlannerError as e:
    st.sidebar.error(str(e))
    st.title("Effort map")
    st.info(
        "No planner daemon reachable. Start one and reload:\n\n"
        "```sh\n"
        "cargo build --release -p freshet-serve --features http\n"
        "freshet-serve --project ./models --sources sources.json --port 8788\n"
        "```"
    )
    st.stop()


# ── loading a project ─────────────────────────────────────────────────────────

with st.sidebar.expander("Load a project", expanded=False):
    st.caption(
        "Replaces the daemon's project. A reload bumps the revision and invalidates "
        "any outstanding commit token."
    )
    directory = st.text_input("Directory of .sql models", value="", placeholder="./models")
    sources_text = st.text_area(
        "Sources (JSON)",
        value="",
        placeholder='{"raw_orders": ["id", "amount", "day"]}',
        help=(
            "Columns of tables the project selects from but does not define. Without "
            "them those columns cannot be resolved and the planner conservatively "
            "assumes they changed — which over-selects rather than under-selects."
        ),
    )
    materializations_text = st.text_area(
        "Materializations (JSON)",
        value="",
        placeholder='{"events": "incremental"}',
        help=(
            "Anything unnamed is a table. Only `incremental` models are partition-"
            "scopable, so a project with no hints plans every rebuild as a whole model."
        ),
    )
    if st.button("Load", width="stretch"):
        try:
            revision = client.reload(
                load_project(directory),
                sources=effortmap.sources_from_json(sources_text),
                materializations=effortmap.materializations_from_json(materializations_text) if materializations_text.strip() else None,
            )
            st.session_state.pop("plan", None)
            st.success(f"loaded · revision {revision}")
        except (PlannerError, ValueError, OSError, json.JSONDecodeError) as e:
            st.error(str(e))


# ── asking for a plan ─────────────────────────────────────────────────────────

st.sidebar.divider()
mode = st.sidebar.radio(
    "Plan",
    ["Code changes", "Data restatement"],
    help=(
        "Code changes: what the edits since the last commit force to rebuild. "
        "Restatement: rebuild named partitions of one model, regardless of code."
    ),
)

if mode == "Data restatement":
    try:
        known = client.graph()
    except PlannerError as e:
        st.sidebar.error(str(e))
        known = []
    restate_model = st.sidebar.selectbox("Model", known) if known else None
    partitions_text = st.sidebar.text_area(
        "Partitions (one per line)", value="", placeholder="2026-09-01\n2026-09-02"
    )

if st.sidebar.button("Plan", type="primary", width="stretch"):
    try:
        if mode == "Data restatement":
            partitions = [p.strip() for p in partitions_text.splitlines() if p.strip()]
            if not restate_model or not partitions:
                raise ValueError("pick a model and at least one partition")
            st.session_state["plan"] = client.restate(
                restate_model, partitions, include_cost=True, include_queue=True
            )
        else:
            st.session_state["plan"] = client.plan(include_cost=True, include_queue=True)
    except (PlannerError, ValueError) as e:
        st.sidebar.error(str(e))

plan = st.session_state.get("plan")

st.title("Effort map")
st.caption("What rebuilding the data layer costs, and the queue that would do it.")

if plan is None:
    st.info("Press **Plan** to ask the planner what a rebuild would take.")
    st.stop()

if not plan:
    st.success(
        "**Nothing to rebuild.** Every model's fingerprint matches the last commit, "
        "so no code has changed since then."
    )
    st.stop()


# ── the headline figures ──────────────────────────────────────────────────────

s = effortmap.summary(plan)
cols = st.columns(5)
cols[0].metric("Models rebuilding", s["models"], help="Out of the whole project.")
cols[1].metric(
    "Predicted cost",
    f"{s['total_cu']:.2f} CU" if s["total_cu"] is not None else "—",
    help="One CU is one full rebuild of one table model. A rate-card prediction, not a measurement.",
)
cols[2].metric("Work items", s["work_items"] if s["work_items"] is not None else "—",
               help="One per model, or one per partition where the rebuild is partition-scoped.")
cols[3].metric("Critical path", s["depth"] if s["depth"] is not None else "—",
               help="How many waves deep the queue is: the longest chain of dependencies.")
cols[4].metric("Widest wave", s["width"] if s["width"] is not None else "—",
               help="The most work that can run at once.")

if s["downstream"]:
    # A restatement is the one plan with no commit token, and "changed" is the wrong
    # word for it: nothing changed, the data was asked for again.
    verb = "restated" if plan.token is None else "changed directly"
    st.caption(f"**{s['directly_changed']}** {verb} · **{s['downstream']}** pulled in downstream.")

rows = effortmap.effort_rows(plan)
tab_effort, tab_breakdown, tab_queue = st.tabs(["Effort map", "Breakdown", "Queue"])


# ── the effort map ────────────────────────────────────────────────────────────

with tab_effort:
    frame = pd.DataFrame(rows)
    if frame["cu"].notna().any():
        # Horizontal bars: model names are text and read better along the y axis,
        # and sorting by length is the whole point of the chart.
        chart = (
            alt.Chart(frame)
            .mark_bar(cornerRadiusEnd=4, height=alt.RelativeBandSize(0.72))
            .encode(
                x=alt.X("cu:Q", title="compute units (CU)", axis=AXIS),
                y=alt.Y("model:N", title=None, sort="-x", axis=AXIS),
                color=alt.Color(
                    "origin:N",
                    title="Why",
                    # A fixed domain, so filtering one category never repaints the other.
                    scale=alt.Scale(domain=ORIGIN_DOMAIN, range=ORIGIN_RANGE),
                    # Right-hand legend, not `orient="top"`. Streamlit sizes an
                    # Altair chart with Vega-Lite's fit-autosize, and under that a
                    # top- or bottom-oriented legend takes its space out of the same
                    # height budget as the plot — which collapses the plot to zero
                    # and leaves the axis labels stacked on one line. Verified in a
                    # browser: identical charts render correctly with the legend on
                    # the right, in a tab or out of one.
                    legend=alt.Legend(orient="right", titleFontSize=12),
                ),
                tooltip=[
                    alt.Tooltip("model:N", title="Model"),
                    alt.Tooltip("cu:Q", title="CU", format=".3f"),
                    alt.Tooltip("millicu:Q", title="mCU (exact)"),
                    alt.Tooltip("basis_label:N", title="Charged as"),
                    alt.Tooltip("unit:N", title="Rebuilds"),
                    alt.Tooltip("because_of:N", title="Because of"),
                    alt.Tooltip("via_columns:N", title="Via columns"),
                ],
            )
            .properties(height=max(140, 28 * len(frame)))
        )
        st.altair_chart(chart, width="stretch")
    else:
        st.info("This plan carries no cost estimate.")

    # The table is not a fallback, it is the accessible view of the same numbers —
    # and it is where `basis` and the attribution are readable rather than hovered.
    st.dataframe(
        frame[["model", "origin", "cu", "millicu", "basis_label", "unit", "because_of", "via_columns"]],
        width="stretch",
        hide_index=True,
        column_config={
            "model": "Model",
            "origin": "Why",
            "cu": st.column_config.NumberColumn("CU", format="%.3f"),
            "millicu": st.column_config.NumberColumn("mCU (exact)"),
            "basis_label": "Charged as",
            "unit": "Rebuilds",
            "because_of": "Because of",
            "via_columns": "Via columns",
        },
    )


# ── how the data breaks down ──────────────────────────────────────────────────

with tab_breakdown:
    breakdown = effortmap.breakdown_rows(plan)
    st.subheader("Partition scoping")
    if breakdown:
        st.caption(
            "Models the planner scoped to specific partitions. Each partition becomes "
            "its own work item, so this is where a rebuild stops being all-or-nothing."
        )
        st.dataframe(
            pd.DataFrame([{k: v for k, v in r.items() if k != "keys"} for r in breakdown]),
            width="stretch",
            hide_index=True,
            column_config={
                "model": "Model",
                "partitions": st.column_config.NumberColumn("Partitions"),
                "first": "First",
                "last": "Last",
            },
        )
        for row in breakdown:
            with st.expander(f"{row['model']} — {row['partitions']} partitions"):
                st.code("\n".join(row["keys"]), language=None)
    else:
        st.info(
            "Nothing in this plan is partition-scoped: every selected model rebuilds "
            "whole. Only `incremental` models can be scoped — mark them in the "
            "materializations map, and restate specific partitions to see the split."
        )

    st.subheader("Concurrency waves")
    waves = effortmap.wave_rows(plan)
    if waves:
        st.caption(
            "Every item in a wave can run at the same time; a wave starts once the one "
            "before it finishes. This is the shape of the rebuild, not its duration — "
            "nothing here is measured in time."
        )
        wave_frame = pd.DataFrame([{k: v for k, v in w.items() if k != "models"} for w in waves])
        wave_chart = (
            alt.Chart(wave_frame)
            .mark_bar(cornerRadiusEnd=4, height=alt.RelativeBandSize(0.72), color=COLORS["direct"])
            .encode(
                x=alt.X("cu:Q", title="compute units (CU)", axis=AXIS),
                y=alt.Y("wave:O", title="wave", axis=AXIS),
                tooltip=[
                    alt.Tooltip("wave:O", title="Wave"),
                    alt.Tooltip("items:Q", title="Items"),
                    alt.Tooltip("cu:Q", title="CU", format=".3f"),
                ],
            )
            .properties(height=max(120, 34 * len(wave_frame)))
        )
        st.altair_chart(wave_chart, width="stretch")
        for w in waves:
            st.caption(f"**Wave {w['wave']}** · {w['items']} item(s) · {w['cu']:.2f} CU · {', '.join(w['models'])}")
    else:
        st.info("No queue in this plan.")


# ── the queue ─────────────────────────────────────────────────────────────────

with tab_queue:
    queue = effortmap.queue_rows(plan)
    if not queue:
        st.info("No queue in this plan.")
    else:
        st.caption(
            "The work items in execution order. Draining them in `seq` order is always "
            "dependency-safe; running a whole wave at a time is the parallel form. "
            "`depends_on` is model-level: an item waits on every in-plan upstream of "
            "its model, which stays correct under lookback windows where partition-to-"
            "partition edges would not."
        )
        st.dataframe(
            pd.DataFrame(queue)[["seq", "target", "wave", "depends_on", "cu", "millicu"]],
            width="stretch",
            hide_index=True,
            column_config={
                "seq": st.column_config.NumberColumn("#"),
                "target": "Work item",
                "wave": st.column_config.NumberColumn("Wave"),
                "depends_on": "Waits on",
                "cu": st.column_config.NumberColumn("CU", format="%.3f"),
                "millicu": st.column_config.NumberColumn("mCU (exact)"),
            },
        )
        st.download_button(
            "Download queue (NDJSON)",
            data=effortmap.queue_ndjson(plan),
            file_name="queue.ndjson",
            mime="application/x-ndjson",
            help="One work item per line, in dependency-safe order — pipe it straight into a runner.",
        )
        st.caption(
            "To turn this plan into a dagron run graph instead, hand the planner's JSON "
            "response to dagron's `dagron-state`, which compiles it into tasks with "
            "the dependency edges already drawn."
        )

    if plan.token is not None:
        st.divider()
        st.subheader("After the run")
        st.caption(
            "Committing advances the fingerprints that record which code has been "
            "built. Do it **only once this plan has actually run** — commit without "
            "running and the next plan sees nothing to do, so the rebuild is skipped "
            "silently. The daemon rejects the token if the project was reparsed in the "
            "meantime."
        )
        if st.button(f"Mark plan {plan.token} as built"):
            try:
                st.success(f"committed · revision {client.commit(plan.token)}")
                st.session_state.pop("plan", None)
            except PlannerError as e:
                st.error(str(e))
    else:
        st.caption(
            "A restatement produces no commit token: it rebuilds data, and advancing "
            "code fingerprints from it would claim work that never happened."
        )

st.divider()
st.caption(
    "Cost is a prediction read off a rate card, in compute units — **1 CU is one full "
    "rebuild of one `table` model**. There is no benchmark behind it and no figure on "
    "this page is measured in seconds. `mCU` is the exact integer the planner computed; "
    "`CU` is that divided by 1000 for display."
)
