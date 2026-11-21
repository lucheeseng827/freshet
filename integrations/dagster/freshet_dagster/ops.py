"""Everything about a Dagster job for a Freshet plan that can be decided without Dagster.

:func:`plan_ops` makes every decision: which op waits on which, what each is called,
what its command says, what its description warns about. It returns plain
:class:`OpSpec` records and imports nothing from Dagster, so every refusal and every
name is tested on a machine without it. :mod:`freshet_dagster.job` only turns the
records into Dagster objects.

The rules come from ``freshet-plan``, the reader ``freshet-airflow`` uses too: the
same edges, the same placeholders, the same facts and the same words. What is
Dagster's own is small, and all of it is here:

* **Op names** are ``[A-Za-z0-9_]`` only, and Dagster reserves Python's keywords and
  a few words of its own (``input``, ``type``, ``config``, …). A model called
  ``type`` gets an op called ``type_2``, by the same rule that settles a collision,
  and a name Dagster accepts is never changed; the real name is in the op's tags
  and facts.
* **No second template pass.** Airflow renders a command with Jinja after Freshet
  has, so ``{{ ds }}`` means something there. Dagster renders nothing, so anything
  in ``{{ }}`` that is not one of Freshet's placeholders would reach the shell as
  written, and is refused when the job is built.
"""

from __future__ import annotations

import json
import re
import string
import subprocess
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Deque, Dict, List, Mapping, Optional, Sequence, Tuple

from freshet_plan import (
    PLACEHOLDERS,
    NoCommand,
    PlanInput,
    RefusedRequest,
    Sql,
    Task,
    compile_plan,
    misspelled_placeholders,
    unknown_placeholders,
)
from freshet_plan.explain import count, summary, ticked, unit_words, warnings, why, writes_words

__all__ = ["DEFAULT_TAGS", "SHELL", "OpSpec", "JobSpec", "CommandFailed", "plan_ops", "op_names", "run_command"]

#: Tags for the generated job unless ``job_kwargs`` sets its own. ``state-plan`` is
#: what dagron-state and freshet-airflow label a plan's runs with, too.
DEFAULT_TAGS: Mapping[str, str] = {"freshet": "state-plan"}

#: What runs a rendered command: ``bash -c``, as Airflow's ``BashOperator`` does, so a
#: command written for one runs unchanged on the other.
SHELL: Tuple[str, ...] = ("bash", "-c")

_SQL_PLACEHOLDER_RE = re.compile(r"\{\{ sql \}\}|\{\{sql\}\}")
_OP_NAME_CHARS = frozenset(string.ascii_letters + string.digits + "_")


@dataclass(frozen=True)
class OpSpec:
    """Everything one op needs, decided."""

    #: The op's name: the task id, made a Dagster name.
    name: str
    task: Task
    #: Op names this op waits on, sorted.
    upstream: Tuple[str, ...]
    description: str
    tags: Mapping[str, str]
    #: Logged at WARNING level whenever the op runs, and first in its description.
    warnings: Tuple[str, ...]
    #: The command with this model's placeholders expanded; ``None`` when ``run`` runs it.
    command: Optional[str] = None

    @property
    def facts(self) -> Dict[str, Any]:
        """The planner's facts for this op's model; see ``freshet_plan.task_facts``."""
        return self.task.facts


@dataclass(frozen=True)
class JobSpec:
    """The job: its ops in plan order, and what it says about itself."""

    ops: Tuple[OpSpec, ...]
    summary: str
    description: str
    tags: Mapping[str, str] = field(default_factory=lambda: dict(DEFAULT_TAGS))


class CommandFailed(RuntimeError):
    """A model's command exited non-zero."""

    def __init__(self, model: str, code: int, tail: Sequence[str]) -> None:
        super().__init__(f"the command for `{model}` exited {code}")
        self.model = model
        self.code = code
        #: The last lines it printed, stdout and stderr together.
        self.tail = tuple(tail)


def plan_ops(
    plan: PlanInput,
    *,
    command_template: Optional[str] = None,
    run: Optional[Callable[..., Any]] = None,
    graph: Optional[Mapping[str, Sequence[str]]] = None,
    ordering: str = "derived",
    valid_name: Callable[[str], bool] = lambda name: True,
) -> JobSpec:
    """Decide the job for ``plan``: one op per model, wired from the plan's own edges.

    Exactly one of ``command_template`` (a shell command per model, with Freshet's
    placeholders) or ``run`` (your function, called as ``run(context, task)``).
    ``graph`` and ``ordering`` are ``freshet_plan.compile_plan``'s. ``valid_name`` is
    Dagster's own test for a name, which :mod:`freshet_dagster.job` passes in.

    Refuses, in dagron-state's order: a failed plan, an empty one, then nothing to run
    it with; after those, both ways at once, and a template whose ``{{ }}`` Freshet
    would not expand.
    """
    tasks = compile_plan(plan, graph=graph, ordering=ordering)
    runs_sql = _check_way(command_template, run)

    names = op_names(tasks, valid_name)
    ops: List[OpSpec] = []
    for task in tasks:
        upstream = tuple(sorted(names[t] for t in task.upstream))
        notes = warnings(task.model, runs_sql)
        ops.append(
            OpSpec(
                name=names[task.task_id],
                task=task,
                upstream=upstream,
                description=_op_description(task, upstream, notes, run is not None, runs_sql),
                tags=_op_tags(task),
                warnings=notes,
                # From the way _check_way chose: with `run`, a blank template beside
                # it is no command, and rendering it would run `bash -c ""` instead.
                command=None if run is not None else task.render(command_template),
            )
        )
    headline = summary(tasks, graph)
    return JobSpec(ops=tuple(ops), summary=headline, description=_job_description(headline, ops))


def op_names(tasks: Sequence[Task], valid_name: Callable[[str], bool] = lambda name: True) -> Dict[str, str]:
    """Task id -> op name, unique within the job and valid in Dagster.

    A task id Dagster accepts as it is stays the op's name. Every other one has each
    character outside ``[A-Za-z0-9_]`` turned into ``_`` (the task id allows ``-``,
    which Dagster does not) and, if that is taken or ``valid_name`` refuses it, a
    suffix ``_2``, ``_3``, … until free: the task id's own collision rule, applied
    once more. The names that need no change are settled first, so a model already
    called ``a_b`` keeps its name when a model ``a-b`` needs one.
    """
    out: Dict[str, str] = {}
    for task in tasks:
        if set(task.task_id) <= _OP_NAME_CHARS and valid_name(task.task_id):
            out[task.task_id] = task.task_id
    taken = set(out.values())
    for task in tasks:
        if task.task_id in out:
            continue
        base = "".join(c if c in _OP_NAME_CHARS else "_" for c in task.task_id)
        candidate, n = base, 2
        while candidate in taken or not valid_name(candidate):
            candidate = f"{base}_{n}"
            n += 1
        taken.add(candidate)
        out[task.task_id] = candidate
    return out


def run_command(
    command: str,
    model: str,
    log: Callable[[str], None],
    *,
    shell: Sequence[str] = SHELL,
    keep: int = 20,
) -> None:
    """Run one model's command, logging each line it prints as it prints it.

    stdout and stderr arrive as one stream, in the order the command wrote them.
    Raises :class:`CommandFailed`, holding the last ``keep`` lines, on a non-zero exit.
    """
    tail: Deque[str] = deque(maxlen=keep)
    with subprocess.Popen(
        [*shell, command],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        errors="replace",
    ) as proc:
        assert proc.stdout is not None
        for line in proc.stdout:
            line = line.rstrip("\n")
            tail.append(line)
            log(line)
    if proc.returncode:
        raise CommandFailed(model, proc.returncode, tail)


# ── deciding ───────────────────────────────────────────────────────────────────


def _check_way(command_template: Optional[str], run: Optional[Callable[..., Any]]) -> bool:
    """Refuse what cannot be run, and say whether the op runs the rendered SQL."""
    if command_template is not None and not isinstance(command_template, str):
        raise TypeError(
            f"command_template is one shell string, run by `bash -c`, not {type(command_template).__name__}; "
            "to reuse a dagron-state argv, pass shlex.join(argv)"
        )
    if run is not None and not callable(run):
        raise TypeError(f"run is called as run(context, task), so it must be callable, not {type(run).__name__}")
    has_command = bool(command_template and command_template.strip())
    if not has_command and run is None:
        raise NoCommand(
            "command_template (a shell command per model, run by `bash -c`) or run "
            "(your function, called as run(context, task), e.g. to run the SQL Freshet rendered)"
        )
    if has_command and run is not None:
        raise RefusedRequest(
            "pass command_template or run, not both: each one is how a model is rebuilt, "
            "and which of them writes the table is not something to guess"
        )
    if run is not None:
        # What `run` does with the SQL cannot be known here, so a non-atomic write is
        # warned about whenever the plan carries one.
        return True
    assert command_template is not None
    _check_command(command_template)
    return bool(_SQL_PLACEHOLDER_RE.search(command_template))


def _check_command(template: str) -> None:
    wrong = misspelled_placeholders(template)
    if wrong:
        raise RefusedRequest(
            f"command_template spells a Freshet placeholder as {_short(wrong[0])!r}. Only the "
            "'{{ name }}' and '{{name}}' spellings are expanded (the two dagron-state reads); "
            "Dagster renders nothing after Freshet does, so any other reaches the shell as written."
        )
    unknown = unknown_placeholders(template)
    if unknown:
        known = ", ".join("{{ %s }}" % name for name in PLACEHOLDERS)
        raise RefusedRequest(
            f"command_template contains {_short(unknown[0])!r}, which is not one of Freshet's "
            f"placeholders ({known}). Dagster renders nothing after Freshet does, so it would "
            "reach the shell as written. An Airflow macro such as {{ ds }} has no meaning here: "
            "read a run's values from its environment or its config instead."
        )


def _short(text: str) -> str:
    return text if len(text) <= 60 else text[:57] + "..."


def _op_tags(task: Task) -> Dict[str, str]:
    model = task.model
    tags = {
        "freshet/model": model.name,
        "freshet/reason": "directly_changed" if model.because_of is None else "downstream",
        "freshet/unit": model.unit,
    }
    if model.replace is not None:
        tags["freshet/replace"] = model.replace.strategy
    return tags


# ── explaining ─────────────────────────────────────────────────────────────────


def _op_description(task: Task, upstream: Sequence[str], notes: Sequence[str], by_run: bool, runs_sql: bool) -> str:
    """An op's description: the warnings first, then why it runs, then the record."""
    model = task.model
    lines = [f"**Freshet plan: `{model.name}`**", ""]
    for note in notes:
        lines += [f"> **WARNING:** {note}", ""]
    lines += [
        f"- **Why:** {why(model)}",
        f"- **Unit:** {unit_words(model)}",
        f"- **Waits on:** {ticked(upstream) or '—'}",
    ]
    if model.replace is not None:
        lines.append(f"- **Writes:** {writes_words(model.replace)}")
    if model.sql is not None:
        lines.append(f"- **SQL:** {_sql_words(model.sql, by_run, runs_sql)}")
    lines += [
        "",
        "Planner facts. The op name is sanitized, so `model` here is the key back to the plan:",
        "",
        "```json",
        json.dumps(task.facts, indent=2, sort_keys=True),
        "```",
    ]
    return "\n".join(lines) + "\n"


def _sql_words(sql: Sql, by_run: bool, runs_sql: bool) -> str:
    what = f"{count(len(sql.statements), 'statement')} in `{sql.dialect}`"
    if by_run:
        return f"{what}, rendered by Freshet and handed to this job's `run` with the task"
    if runs_sql:
        return f"{what}, rendered by Freshet and passed to this op's command"
    return f"{what}, rendered by Freshet; this op's command does not use them"


def _job_description(headline: str, ops: Sequence[OpSpec]) -> str:
    """The job's description: dagron-state's ``explain``, trimmed to what fits a job page."""
    lines = ["**Freshet state plan**", "", f"**{headline}**", ""]
    notes = [w for op in ops for w in op.warnings]
    if notes:
        lines.append("> **WARNING: read before launching.**")
        lines += [f"> - {w}" for w in notes]
        lines.append("")
    lines += [
        "| Model | Op | Why | Unit | Waits on | Writes |",
        "|---|---|---|---|---|---|",
    ]
    for op in ops:
        model = op.task.model
        writes = writes_words(model.replace) if model.replace is not None else "—"
        if model.replace is not None and model.replace.widened is not None:
            writes = f"**widened:** {writes}"
        lines.append(
            f"| `{model.name}` | `{op.name}` | {why(model)} | {unit_words(model)} "
            f"| {ticked(op.upstream) or '—'} | {writes} |"
        )
    return "\n".join(lines) + "\n"
