"""A plan in words: the same sentences in every orchestrator that runs one.

dagron-state's ``explain`` says what a plan rebuilds, why each model is in it, what
each one writes, and what must not be missed. An adapter that worded these itself
would drift from the others the first time someone fixed a sentence in one of them.
So the wording lives here, once. ``freshet-airflow`` puts it in a DAG's and each
task's ``doc_md``; ``freshet-dagster`` puts it in a job's and each op's description;
both log the warnings when the task runs.

Plain text with backticks around names, which every one of those surfaces renders.
"""

from __future__ import annotations

import math
from typing import List, Mapping, Optional, Sequence, Tuple

from . import PlanModel, Replace, Task

__all__ = ["summary", "why", "unit_words", "writes_words", "warnings", "ticked", "count"]


def summary(tasks: Sequence[Task], graph: Optional[Mapping[str, Sequence[str]]] = None) -> str:
    """dagron-state's headline: a pruning percentage only when the project size is known.

    A plan sees the models it selected, never the ones it skipped, so without a
    ``graph`` the saving is not measurable, and saying so beats implying one.
    """
    selected = len(tasks)
    plural = "model" if selected == 1 else "models"
    total = None
    if graph is not None:
        names = {t.model.name for t in tasks}
        for model, upstreams in graph.items():
            names.add(model)
            names.update(upstreams)
        total = len(names)
    if total is not None and total >= selected and total > 0:
        pruned = total - selected
        # Rust's f64::round, which rounds half away from zero; Python's round()
        # rounds half to even, and the two headlines should not disagree on 12.5%.
        exact = pruned / total * 100.0
        floor = math.floor(exact)
        pct = floor + (1 if exact - floor >= 0.5 else 0)
        return f"{selected} of {total} {plural} rebuild — {pct}% pruned"
    return f"{selected} {plural} rebuild (project size unknown — pass `graph` to report pruning)"


def why(model: PlanModel) -> str:
    """Why the model is in the plan."""
    if model.because_of is None:
        return "directly changed"
    if not model.via_columns:
        return f"downstream of `{model.because_of}`"
    return f"downstream of `{model.because_of}` via {ticked(model.via_columns)}"


def unit_words(model: PlanModel) -> str:
    """How much of the model rebuilds."""
    if model.partitions is None:
        return "full model"
    return count(len(model.partitions), "partition")


def writes_words(replace: Replace) -> str:
    """One write, in words: the declaration, never a statement (dagron-state's ``writes_words``)."""
    if replace.target_partitions is None:
        scope = "the whole relation"
    else:
        scope = count(len(replace.target_partitions), "partition")
        if replace.partition_column is not None:
            scope += f" on `{replace.partition_column}`"
    key = f" on key {ticked(replace.unique_key)}" if replace.unique_key else ""
    return f"`{replace.strategy}` over {scope}{key}"


def warnings(model: PlanModel, runs_sql: bool) -> Tuple[str, ...]:
    """The notes that must not be missed, one sentence each.

    A widened replace op is always worth saying, whatever runs the model: it is the
    difference between rewriting two partitions and rewriting a table. A non-atomic
    write matters only when the task actually runs the rendered statements, which is
    what ``runs_sql`` says.
    """
    out: List[str] = []
    if model.replace is not None and model.replace.widened is not None:
        out.append(
            f"`{model.name}` was widened to a full refresh of the whole relation: "
            f"{model.replace.widened.explain()}."
        )
    if runs_sql and model.sql is not None and not model.sql.atomic:
        out.append(
            f"`{model.name}` is a non-atomic write: the planner does not promise that its "
            f"{count(len(model.sql.statements), 'statement')} apply all or nothing, so a "
            "failure part-way can leave the relation partially written. A retry runs the "
            "task again from its first statement; check the relation before clearing it."
        )
    return tuple(out)


def ticked(names: Sequence[str]) -> str:
    """Names in backticks, comma-separated."""
    return ", ".join(f"`{n}`" for n in names)


def count(n: int, noun: str) -> str:
    """``1 model``, ``2 models``."""
    return f"{n} {noun}" if n == 1 else f"{n} {noun}s"
