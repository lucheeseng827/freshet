"""Shaping a planner result into the rows and frames the effort-map UI draws.

Deliberately free of Streamlit, pandas and Altair: everything here is a pure
function over the SDK's dataclasses, returning lists of plain dicts. That keeps the
part with the arithmetic in it testable with `python -m unittest` and no display
stack, and keeps `app.py` to layout — the same split the Rust side uses, where
`freshet-core` is pure and the transports are thin.

The vocabulary is the planner's, unchanged: cell counts are counts, cost is in
compute units, and one CU is one full rebuild of one `table` model. Nothing here
converts to time, because nothing upstream measures time.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from freshet_sdk import MILLICU_PER_CU, Plan

#: Categorical slots 1 and 2 of the reference palette, light and dark. Two hues is
#: the whole categorical need here — "did I change this, or is it blast radius?" —
#: and the pair validates on every check in both modes (worst CVD ΔE 24.7 light /
#: 26.8 dark against an ΔE>=8 target). Identity is never carried by colour alone:
#: the same split is a text column in the table beside every chart.
SERIES = {
    "light": {"direct": "#2a78d6", "downstream": "#eb6834"},
    "dark": {"direct": "#3987e5", "downstream": "#d95926"},
}

#: What each rate-card basis means, in the words the planner would use. Shown in the
#: table so the cost column is never a bare number the reader has to trust.
BASIS_LABEL = {
    "table_full": "full table rebuild",
    "view_full": "view redefinition",
    "incremental_full": "full incremental refresh",
    "incremental_partitions": "partition-scoped",
    "unknown_model": "not in the project",
}


def cu(millicu: int) -> float:
    """Whole compute units, for display only. `millicu` is the exact figure."""
    return millicu / MILLICU_PER_CU


def effort_rows(plan: Plan) -> List[Dict[str, Any]]:
    """One row per model in the plan: what rebuilds, why, and what it costs.

    Sorted by cost descending so the models that dominate the rebuild are at the top,
    which is the question this table exists to answer. Ties break by name, so the
    order is stable across reruns and a reader's eye does not have to re-find a row.

    A plan without cost still produces rows — the cost columns are simply absent, so
    the dependency answer is readable on its own.
    """
    by_model = {m.model: m for m in (plan.cost.per_model if plan.cost else [])}
    rows: List[Dict[str, Any]] = []
    for model in plan.models:
        cost = by_model.get(model.name)
        rows.append(
            {
                "model": model.name,
                "origin": "changed" if model.directly_changed else "downstream",
                "because_of": model.because_of or "",
                "via_columns": ", ".join(model.via_columns),
                "unit": "whole model" if model.partitions is None else f"{len(model.partitions)} partitions",
                "partitions": model.partitions or [],
                "millicu": cost.millicu if cost else None,
                "cu": cu(cost.millicu) if cost else None,
                "basis": cost.basis_name if cost else "",
                "basis_label": BASIS_LABEL.get(cost.basis_name, cost.basis_name) if cost else "",
            }
        )
    rows.sort(key=lambda r: (-(r["millicu"] or 0), r["model"]))
    return rows


def wave_rows(plan: Plan) -> List[Dict[str, Any]]:
    """One row per concurrency wave: how much work it holds and what it costs.

    This is the shape of the rebuild over time — not its duration, which nothing here
    measures. A plan that is one item deep and forty wide is a very different
    operational proposition from one that is forty deep and one wide, and the wave
    table is where that shows.
    """
    if plan.queue is None:
        return []
    by_seq = {i.seq: i for i in plan.queue.items}
    rows = []
    for index, wave in enumerate(plan.queue.waves):
        items = [by_seq[s] for s in wave]
        millicu = sum(i.millicu for i in items)
        rows.append(
            {
                "wave": index,
                "items": len(items),
                "models": sorted({i.model for i in items}),
                "millicu": millicu,
                "cu": cu(millicu),
            }
        )
    return rows


def queue_rows(plan: Plan) -> List[Dict[str, Any]]:
    """One row per work item, in execution order — the queue a runner would drain."""
    if plan.queue is None:
        return []
    return [
        {
            "seq": i.seq,
            "model": i.model,
            "partition": i.partition or "",
            "target": i.label,
            "wave": i.wave,
            "depends_on": ", ".join(i.depends_on),
            "millicu": i.millicu,
            "cu": cu(i.millicu),
        }
        for i in plan.queue.items
    ]


def breakdown_rows(plan: Plan) -> List[Dict[str, Any]]:
    """One row per partition-scoped model: how its data was broken down.

    Only models the planner actually scoped appear. A model rebuilding whole is not a
    breakdown of one, it is the absence of a breakdown, and listing it here would
    imply partition scoping was considered and produced a single chunk.
    """
    rows = []
    for model in plan.models:
        partitions = model.partitions
        if not partitions:
            continue
        rows.append(
            {
                "model": model.name,
                "partitions": len(partitions),
                "first": partitions[0],
                "last": partitions[-1],
                "keys": list(partitions),
            }
        )
    return rows


def summary(plan: Plan) -> Dict[str, Any]:
    """The headline figures, as a stat row rather than a chart.

    Five numbers, each one a count or a sum of counts. `total_cu` is `None` when cost
    was not requested rather than 0.0, so a UI can say "not requested" instead of
    printing a zero that reads as "free".
    """
    return {
        "models": len(plan.models),
        "directly_changed": sum(1 for m in plan.models if m.directly_changed),
        "downstream": sum(1 for m in plan.models if not m.directly_changed),
        "total_millicu": plan.cost.total_millicu if plan.cost else None,
        "total_cu": plan.cost.total_cu if plan.cost else None,
        "work_items": len(plan.queue.items) if plan.queue else None,
        "depth": plan.queue.depth if plan.queue else None,
        "width": plan.queue.width if plan.queue else None,
    }


def queue_ndjson(plan: Plan) -> str:
    """The queue as newline-delimited JSON — one work item per line.

    NDJSON rather than a JSON array because this is what gets piped into a runner or
    a queue: a consumer can read it a line at a time without holding the whole plan in
    memory, and appending is concatenation. Order is `seq` order, which is
    dependency-safe to drain serially.
    """
    import json

    return "\n".join(json.dumps(row, sort_keys=True) for row in queue_rows(plan))


def sources_from_json(text: str) -> Dict[str, List[str]]:
    """Parse the `--sources` document, tolerating an empty box.

    The values are checked, not just the envelope, because `list(v)` will iterate
    anything: `{"raw": "id"}` parses to `["i", "d"]` and hands the planner two columns
    nobody has — a wrong answer with no error attached to it — and `{"raw": 3}` raises
    TypeError, which the caller does not catch and which therefore lands on the page as
    a traceback. Both are ordinary typos in a text box, so both are answered here as a
    ValueError naming the offending table, which is what the UI already handles.
    """
    import json

    if not text.strip():
        return {}
    parsed = json.loads(text)
    if not isinstance(parsed, dict):
        raise ValueError("sources must be a JSON object of {table: [columns]}")
    for table, columns in parsed.items():
        if not isinstance(columns, list) or not all(isinstance(c, str) for c in columns):
            raise ValueError(f"sources[{table!r}] must be a list of column names")
    return {k: list(v) for k, v in parsed.items()}


def materializations_from_json(text: str) -> Dict[str, str]:
    """Parse the `--materializations` document, tolerating an empty box.

    Same shape of document, different values: the planner takes {model: materialization},
    one name per model, not a list. It gets its own parser rather than sharing the
    sources one because the two disagree about what a value is — run `{"events":
    "incremental"}` through the sources parser and the model is marked as the eleven
    materializations "i", "n", "c", … — and a validated parser cannot be right for both.
    """
    import json

    if not text.strip():
        return {}
    parsed = json.loads(text)
    if not isinstance(parsed, dict):
        raise ValueError("materializations must be a JSON object of {model: materialization}")
    for model, kind in parsed.items():
        if not isinstance(kind, str):
            raise ValueError(f"materializations[{model!r}] must be a materialization name, e.g. \"incremental\"")
    return dict(parsed)


def series_colors(dark: bool = False) -> Dict[str, str]:
    """The two categorical hues for the current mode."""
    return SERIES["dark" if dark else "light"]


def origin_domain() -> Sequence[str]:
    """Fixed category order, so a filter that drops one never repaints the other."""
    return ("changed", "downstream")
