"""Build an Airflow DAG from a Freshet plan: one task per model, wired from the plan's edges.

This is the Airflow half, and it is thin on purpose. Every decision about the plan
(which task waits for which, what each is called, what its command says) is made in
:mod:`freshet_airflow.plan`, which cannot import Airflow. What is left here is
choosing an operator per task and refusing inputs that would make that choice
ambiguous or unsafe.

That work is split in two. :func:`plan_operators` makes every decision and returns
plain :class:`OperatorSpec` records; :func:`build_dag` only turns those into Airflow
objects. Airflow is imported inside :func:`build_dag`, never at module import, so
``import freshet_airflow`` works, and every refusal below is testable, on a machine
without Airflow.

Two ways to run a model
-----------------------
* ``conn_id``: run the SQL Freshet rendered for the model (contract v4) with
  ``SQLExecuteQueryOperator``, the one generic SQL operator Airflow itself
  consolidated its per-warehouse operators into. The statement list goes to the
  operator whole, so the provider's ``DbApiHook.run`` executes it on one connection,
  in order, and commits once after the last statement.
* ``command_template``: run a shell command per model with ``BashOperator``, the
  same idea as dagron-state's ``command_template``, with the same placeholders.

Exactly one of the two. A plan with no SQL cannot be run through ``conn_id``, and
this module refuses rather than running the models that have SQL and skipping the
ones that do not.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from .plan import (
    NoCommand,
    PlanInput,
    PlanModel,
    RefusedRequest,
    Replace,
    Sql,
    Task,
    compile_plan,
    misspelled_placeholders,
    protect_from_jinja,
)

__all__ = ["OperatorSpec", "plan_operators", "build_dag", "DEFAULT_TAGS"]

#: Tags for the generated DAG unless ``dag_kwargs`` sets its own. ``state-plan`` is
#: dagron-state's default tag; both readers label the runs a plan produces the same way.
DEFAULT_TAGS = ("freshet", "state-plan")

# `jinja_environment_kwargs` keys that change Jinja's syntax. `protect_from_jinja`
# assumes the default delimiters, so a DAG that changes them is refused rather than
# handed values that are no longer protected.
_JINJA_SYNTAX_KEYS = frozenset(
    {
        "block_start_string",
        "block_end_string",
        "variable_start_string",
        "variable_end_string",
        "comment_start_string",
        "comment_end_string",
        "line_statement_prefix",
        "line_comment_prefix",
    }
)

_SQL_PLACEHOLDER_RE = re.compile(r"\{\{ sql \}\}|\{\{sql\}\}")


@dataclass(frozen=True)
class OperatorSpec:
    """Everything needed to construct one Airflow task, decided without Airflow."""

    task_id: str
    #: Task ids this task waits on.
    upstream: Tuple[str, ...]
    #: ``"sql"`` for ``SQLExecuteQueryOperator``, ``"bash"`` for ``BashOperator``.
    kind: str
    #: The planner's facts (:func:`freshet_airflow.plan.task_facts`).
    facts: Mapping[str, Any]
    #: Loud notes for this task: a widened replace op, a non-atomic write. Each is
    #: in the task's doc and logged at WARNING level every time the task runs.
    warnings: Tuple[str, ...]
    doc_md: str
    #: ``kind == "bash"``: the command with this model's placeholders expanded and
    #: protected from Airflow's Jinja pass, which still renders ``{{ ds }}`` and the rest.
    bash_command: Optional[str] = None
    #: ``kind == "sql"``: the connection, and the statements to run on it, in order.
    conn_id: Optional[str] = None
    statements: Tuple[str, ...] = ()


def plan_operators(
    plan: PlanInput,
    *,
    command_template: Optional[str] = None,
    conn_id: Optional[str] = None,
    graph: Optional[Mapping[str, Sequence[str]]] = None,
    ordering: str = "derived",
    jinja_environment_kwargs: Optional[Mapping[str, Any]] = None,
) -> Tuple[OperatorSpec, ...]:
    """Decide every task of the DAG :func:`build_dag` would build, without Airflow.

    Raises what :func:`freshet_airflow.plan.compile_plan` raises, then
    :class:`~freshet_airflow.plan.NoCommand` when nothing would run and
    :class:`~freshet_airflow.plan.RefusedRequest` for an ambiguous or unsafe request.
    """
    return _prepare(
        plan,
        command_template=command_template,
        conn_id=conn_id,
        graph=graph,
        ordering=ordering,
        jinja_environment_kwargs=jinja_environment_kwargs,
    )[1]


def build_dag(
    plan: PlanInput,
    *,
    dag_id: str,
    command_template: Optional[str] = None,
    conn_id: Optional[str] = None,
    graph: Optional[Mapping[str, Sequence[str]]] = None,
    ordering: str = "derived",
    dag_kwargs: Optional[Mapping[str, Any]] = None,
) -> Any:
    """Build an Airflow DAG with one task per plan model, wired with ``>>`` from its edges.

    ``plan`` is the planner's JSON (``freshet plan --json``) as text, bytes, a decoded
    mapping, or a parsed :class:`~freshet_airflow.plan.Plan`.

    Pass exactly one of:

    * ``conn_id``: every model must carry contract-v4 ``sql``; each becomes a
      ``SQLExecuteQueryOperator`` running its statements on that connection.
    * ``command_template``: one shell string, run per model by ``BashOperator``, with
      the Freshet placeholders expanded (see :func:`freshet_airflow.plan.render_template`).
      Airflow's own Jinja, ``{{ ds }}`` and the rest, is left for Airflow.

    ``graph`` (model to upstream models) supplies edges for a contract-v1 plan and,
    for any plan, lets the DAG's doc report how much of the project was pruned.
    ``ordering="sequential"`` chains the plan instead of deriving edges.

    ``dag_kwargs`` go to the DAG constructor and override these defaults: no
    schedule (a state plan has no cron; it is triggered, per ADR-0001), no catchup,
    tags ``freshet`` and ``state-plan``, and a ``doc_md`` explaining the plan. Use its
    ``default_args`` for retries, pools and the like. They cannot override the
    SQL operator's ``autocommit``, ``parameters`` or ``split_statements``: those are
    passed explicitly, and explicit arguments win over ``default_args``.
    """
    dag_kwargs = dict(dag_kwargs or {})
    tasks, specs = _prepare(
        plan,
        command_template=command_template,
        conn_id=conn_id,
        graph=graph,
        ordering=ordering,
        jinja_environment_kwargs=dag_kwargs.get("jinja_environment_kwargs"),
    )
    summary = _summary(tasks, graph)
    defaults: Dict[str, Any] = {
        "schedule": None,
        "catchup": False,
        "tags": list(DEFAULT_TAGS),
        "description": f"Freshet state plan: {summary}",
        "doc_md": _dag_doc(summary, tasks, specs),
    }
    dag = _dag_class()(dag_id=dag_id, **{**defaults, **dag_kwargs})

    operators: Dict[str, Any] = {}
    for spec in specs:
        operators[spec.task_id] = _operator(spec, dag)
    for spec in specs:
        for upstream in spec.upstream:
            operators[upstream] >> operators[spec.task_id]
    return dag


# ── deciding ───────────────────────────────────────────────────────────────────


def _prepare(
    plan: PlanInput,
    *,
    command_template: Optional[str],
    conn_id: Optional[str],
    graph: Optional[Mapping[str, Sequence[str]]],
    ordering: str,
    jinja_environment_kwargs: Optional[Mapping[str, Any]],
) -> Tuple[Tuple[Task, ...], Tuple[OperatorSpec, ...]]:
    # The plan's own refusals come first, in dagron-state's order: a failed plan,
    # then an empty one, and only then "you gave me nothing to run it with".
    tasks = compile_plan(plan, graph=graph, ordering=ordering)
    kind = _execution_kind(tasks, command_template, conn_id, jinja_environment_kwargs)
    runs_sql = kind == "sql" or bool(_SQL_PLACEHOLDER_RE.search(command_template or ""))

    specs: List[OperatorSpec] = []
    for task in tasks:
        warnings = _warnings(task.model, runs_sql)
        common: Dict[str, Any] = {
            "task_id": task.task_id,
            "upstream": task.upstream,
            "kind": kind,
            "facts": task.facts,
            "warnings": warnings,
            "doc_md": _task_doc(task, kind, warnings, runs_sql),
        }
        if kind == "sql":
            # _execution_kind has already refused a model without sql.
            specs.append(OperatorSpec(**common, conn_id=conn_id, statements=task.model.sql.statements))
        else:
            command = task.render(command_template or "", escape=protect_from_jinja)
            specs.append(OperatorSpec(**common, bash_command=command))
    return tasks, tuple(specs)


def _execution_kind(
    tasks: Sequence[Task],
    command_template: Optional[str],
    conn_id: Optional[str],
    jinja_environment_kwargs: Optional[Mapping[str, Any]],
) -> str:
    if command_template is not None and not isinstance(command_template, str):
        raise TypeError(
            "command_template is one shell string for BashOperator, not "
            f"{type(command_template).__name__}; to reuse a dagron-state argv, pass "
            "shlex.join(argv)"
        )
    has_command = bool(command_template and command_template.strip())
    if not has_command and not conn_id:
        raise NoCommand()
    if has_command and conn_id:
        raise RefusedRequest(
            "pass command_template or conn_id, not both: conn_id runs the SQL Freshet "
            "rendered and command_template runs your own command, and which of them "
            "writes the table is not something to guess"
        )
    if conn_id:
        _check_sql(tasks)
        return "sql"
    assert command_template is not None
    _check_command(command_template, jinja_environment_kwargs)
    return "bash"


def _check_sql(tasks: Sequence[Task]) -> None:
    missing = [t.model.name for t in tasks if t.model.sql is None]
    if len(missing) == len(tasks):
        raise RefusedRequest(
            "conn_id runs the SQL Freshet rendered for each model, and this plan carries "
            "none: it comes from a producer older than contract v4, or the request did "
            "not ask Freshet to render SQL. Re-plan with SQL rendering on, or pass "
            "command_template to run your own command."
        )
    if missing:
        raise RefusedRequest(
            f"conn_id runs the SQL Freshet rendered for each model, but {_names(missing)} "
            f"{_carry(missing)} none. Running the rest would rebuild part of the plan and "
            "silently skip those; re-plan with SQL rendering on, or pass command_template."
        )
    empty = [t.model.name for t in tasks if t.model.sql is not None and not t.model.sql.statements]
    if empty:
        raise RefusedRequest(
            f"{_names(empty)} {_carry(empty)} an empty SQL statement list. A task that "
            "succeeds without writing anything is how a rebuild gets skipped silently."
        )
    dialects = sorted({t.model.sql.dialect for t in tasks if t.model.sql is not None})
    if len(dialects) > 1:
        raise RefusedRequest(
            f"this plan's SQL is rendered for {len(dialects)} dialects ({', '.join(dialects)}), "
            "and one conn_id speaks one of them"
        )


def _check_command(template: str, jinja_environment_kwargs: Optional[Mapping[str, Any]]) -> None:
    wrong = misspelled_placeholders(template)
    if wrong:
        shown = wrong[0] if len(wrong[0]) <= 60 else wrong[0][:57] + "..."
        raise RefusedRequest(
            f"command_template spells a Freshet placeholder as {shown!r}. Only the "
            "'{{ name }}' and '{{name}}' spellings are expanded (the two dagron-state "
            "reads); any other reaches Airflow's Jinja, where the name is undefined, "
            "and fails when the task runs, after upstream tasks have already written."
        )
    custom = sorted(set(jinja_environment_kwargs or ()) & _JINJA_SYNTAX_KEYS)
    if custom:
        raise RefusedRequest(
            f"dag_kwargs['jinja_environment_kwargs'] changes Jinja's syntax ({', '.join(custom)}). "
            "Values substituted into command_template are protected from Airflow's "
            "Jinja pass assuming the default delimiters, and under a custom syntax "
            "they would not be."
        )


def _warnings(model: PlanModel, runs_sql: bool) -> Tuple[str, ...]:
    """The notes that must not be missed, one sentence each.

    A widened replace op is always worth saying, whatever runs the model: it is the
    difference between rewriting two partitions and rewriting a table. A non-atomic
    write matters only when this task actually runs the rendered statements.
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
            f"{_count(len(model.sql.statements), 'statement')} apply all or nothing, so a "
            "failure part-way can leave the relation partially written. A retry runs the "
            "task again from its first statement; check the relation before clearing it."
        )
    return tuple(out)


# ── explaining ─────────────────────────────────────────────────────────────────


def _task_doc(task: Task, kind: str, warnings: Sequence[str], runs_sql: bool) -> str:
    """A task's ``doc_md``: the warnings first, then why it runs, then the record."""
    model = task.model
    lines = [f"#### Freshet plan: `{model.name}`", ""]
    for warning in warnings:
        lines += [f"> **WARNING:** {warning}", ""]
    lines += [
        f"- **Why:** {_why(model)}",
        f"- **Unit:** {_unit_words(model)}",
        f"- **Waits on:** {_ticked(task.upstream) or '—'}",
    ]
    if model.replace is not None:
        lines.append(f"- **Writes:** {_writes_words(model.replace)}")
    if model.sql is not None:
        lines.append(f"- **SQL:** {_sql_words(model.sql, kind, runs_sql)}")
    if kind == "sql" and model.sql is not None:
        lines += ["", "```sql", ";\n".join(model.sql.statements), "```"]
    lines += [
        "",
        "Planner facts. The task id is sanitized, so `model` here is the key back to the plan:",
        "",
        "```json",
        json.dumps(task.facts, indent=2, sort_keys=True),
        "```",
    ]
    return "\n".join(lines) + "\n"


def _dag_doc(summary: str, tasks: Sequence[Task], specs: Sequence[OperatorSpec]) -> str:
    """The DAG's ``doc_md``: dagron-state's ``explain``, trimmed to what fits a DAG page."""
    lines = ["### Freshet state plan", "", f"**{summary}**", ""]
    warnings = [w for spec in specs for w in spec.warnings]
    if warnings:
        lines.append("> **WARNING: read before triggering.**")
        lines += [f"> - {w}" for w in warnings]
        lines.append("")
    lines += [
        "| Model | Task | Why | Unit | Waits on | Writes |",
        "|---|---|---|---|---|---|",
    ]
    for task in tasks:
        model = task.model
        writes = _writes_words(model.replace) if model.replace is not None else "—"
        if model.replace is not None and model.replace.widened is not None:
            writes = f"**widened:** {writes}"
        lines.append(
            f"| `{model.name}` | `{task.task_id}` | {_why(model)} | {_unit_words(model)} "
            f"| {_ticked(task.upstream) or '—'} | {writes} |"
        )
    return "\n".join(lines) + "\n"


def _summary(tasks: Sequence[Task], graph: Optional[Mapping[str, Sequence[str]]]) -> str:
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


def _why(model: PlanModel) -> str:
    if model.because_of is None:
        return "directly changed"
    if not model.via_columns:
        return f"downstream of `{model.because_of}`"
    return f"downstream of `{model.because_of}` via {_ticked(model.via_columns)}"


def _unit_words(model: PlanModel) -> str:
    if model.partitions is None:
        return "full model"
    return _count(len(model.partitions), "partition")


def _writes_words(replace: Replace) -> str:
    """One write, in words: the declaration, never a statement (dagron-state's ``writes_words``)."""
    if replace.target_partitions is None:
        scope = "the whole relation"
    else:
        scope = _count(len(replace.target_partitions), "partition")
        if replace.partition_column is not None:
            scope += f" on `{replace.partition_column}`"
    key = f" on key {_ticked(replace.unique_key)}" if replace.unique_key else ""
    return f"`{replace.strategy}` over {scope}{key}"


def _sql_words(sql: Sql, kind: str, runs_sql: bool) -> str:
    what = f"{_count(len(sql.statements), 'statement')} in `{sql.dialect}`"
    if kind == "sql":
        how = "run in order on one connection with autocommit off, committed after the last"
        atomic = "the planner marks them atomic" if sql.atomic else "the planner marks them **not atomic**"
        return f"{what}, {how}; {atomic}"
    if runs_sql:
        return f"{what}, rendered by Freshet and passed to this task's command"
    return f"{what}, rendered by Freshet; this task's command does not use them"


def _ticked(names: Sequence[str]) -> str:
    return ", ".join(f"`{n}`" for n in names)


def _names(names: Sequence[str]) -> str:
    return f"{_count(len(names), 'model')} ({_ticked(names)})"


def _carry(names: Sequence[str]) -> str:
    return "carries" if len(names) == 1 else "carry"


def _count(n: int, noun: str) -> str:
    return f"{n} {noun}" if n == 1 else f"{n} {noun}s"


# ── constructing ───────────────────────────────────────────────────────────────


def _dag_class() -> Any:
    try:
        from airflow.sdk import DAG  # Airflow 3
    except ImportError:
        try:
            from airflow import DAG  # Airflow 2
        except ImportError as e:
            raise ImportError(
                "build_dag needs Apache Airflow where the DAG is parsed (this package's "
                "`airflow` extra installs it). The rest of freshet_airflow works without it."
            ) from e
    return DAG


def _operator(spec: OperatorSpec, dag: Any) -> Any:
    common = {
        "task_id": spec.task_id,
        "doc_md": spec.doc_md,
        "freshet_facts": spec.facts,
        "freshet_warnings": spec.warnings,
        "dag": dag,
    }
    if spec.kind == "sql":
        from .operators.sql import FreshetSQLExecuteQueryOperator

        return FreshetSQLExecuteQueryOperator(
            conn_id=spec.conn_id,
            # A list, not one joined string: DbApiHook.run executes a list statement
            # by statement on one connection and one cursor, and never re-splits it.
            sql=list(spec.statements),
            # One transaction around the whole list, committed after the last
            # statement. Explicit, so that default_args cannot turn it on.
            autocommit=False,
            # None, not {}: with no parameters the driver executes each statement
            # verbatim, so a `%` in the SQL is never read as a placeholder.
            parameters=None,
            split_statements=False,
            **common,
        )
    from .operators.bash import FreshetBashOperator

    return FreshetBashOperator(bash_command=spec.bash_command, **common)
