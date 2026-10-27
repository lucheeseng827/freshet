"""Freshet's frozen plan contract, read the way dagron reads it, with no Airflow.

Freshet answers one question: *given this SQL change, what is the minimal set of
models to rebuild?* The answer crosses every boundary as the same JSON, the
``PlanResponse`` that ``freshet-embed`` serializes and every binding
funnels through. dagron reads it in its ``dagron-state`` crate. This module is
the second reader, and the only interesting thing about it is that it is not the
first: the planner core's design notes call the planner orchestrator-agnostic because
consumers couple to the wire rather than to the linker, and that claim is only as
good as a second consumer that reads the same bytes by the same rules.

So the rules here are dagron-state's, ported rather than reinvented:

* :func:`parse_plan` mirrors ``src/wire.rs``: same fields, same optionality, and the
  same refusal of an enum variant it does not know.
* :func:`compile_plan` mirrors ``compile::compile``: same refusals, same edge
  precedence, same acyclicity filter, same task-name sanitization.
* :func:`render_template` expands the placeholders ``render_command`` expands, plus
  the two that contract v4 adds.
* :func:`task_facts` is ``task_input``: the planner's own facts, kept with the task.

The tests are a line-for-line port of dagron-state's, run against the same fixtures.
Where this module departs from dagron-state, the docstring says so and says why.

Why no Airflow import
---------------------
Everything above has an exact answer that does not depend on Airflow: which task
waits for which, what each one is called, what its command says. Keeping it in a
module that cannot import Airflow means those answers are tested on a machine with
no Airflow installed, and leaves the Airflow half (:mod:`freshet_airflow.dag`) with
nothing to decide, only objects to construct.

Versions
--------
Each revision is additive, and a payload from any older producer still parses.

* **v1**: ``name``, ``reason``, ``unit``. Execution edges had to be guessed from the
  single-cause ``reason``.
* **v2**: ``depends_on``, the planner's own edge set, narrowed to the plan.
* **v3**: ``replace``, what each rebuild does to the relation, and whether the
  planner had to widen it to a full refresh.
* **v4**: ``sql``, the statements Freshet rendered for the rebuild, present only
  when the request asked it to render them.
"""

from __future__ import annotations

import json
import re
import string
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

__all__ = [
    "WIRE_CONTRACT_VERSION",
    "REPLACE_STRATEGIES",
    "PLACEHOLDERS",
    "ORDERINGS",
    "ContractViolation",
    "CompileError",
    "PlannerFailed",
    "EmptyPlan",
    "NoCommand",
    "RefusedRequest",
    "Plan",
    "PlanModel",
    "PlanFailure",
    "Replace",
    "Widening",
    "Sql",
    "Task",
    "parse_plan",
    "compile_plan",
    "task_facts",
    "placeholder_values",
    "render_template",
    "misspelled_placeholders",
    "protect_from_jinja",
]

#: The contract revision this package reads — the planner's own
#: ``freshet_embed::WIRE_CONTRACT_VERSION``, which dagron-state stamps too. Bump it
#: with the planner's, in the same commit as the fixture that proves it. (v5 added
#: ``cost.unpruned_millicu``, which this package does not read.)
WIRE_CONTRACT_VERSION = "planner-embed/5"

#: The replace strategies, spelled as the wire (and the project's config) spells them.
REPLACE_STRATEGIES = ("insert_overwrite", "delete_insert", "merge", "replace_where", "full_refresh")

#: Every placeholder :func:`render_template` expands. The first six are
#: dagron-state's; ``sql`` and ``dialect`` arrive with contract v4.
PLACEHOLDERS = (
    "model",
    "unit",
    "partitions",
    "replace",
    "partition_column",
    "unique_key",
    "sql",
    "dialect",
)

#: How edges are derived: dagron-state's ``Ordering``, spelled as its serde spells it.
ORDERINGS = ("derived", "sequential")

PlanInput = Union["Plan", str, bytes, bytearray, Mapping[str, Any]]


# ── errors ─────────────────────────────────────────────────────────────────────


class ContractViolation(ValueError):
    """The payload is not a plan this package can read.

    dagron-state's equivalent is a serde error, and the discipline is the same: a
    reason, unit, strategy or widening this package does not know is the contract
    moving. Guessing past it would mean running something the planner did not say,
    so the DAG fails to import instead, loudly and with the path to the bad value.

    Unknown *fields* are not violations. Every contract revision so far has been
    additive, and ignoring a field is exactly how a v3 reader keeps working against
    a v4 producer.
    """


class CompileError(ValueError):
    """Why a plan could not become a DAG. Mirrors dagron-state's ``CompileError``."""


class PlannerFailed(CompileError):
    """The planner itself failed, so there is nothing to compile."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"planner failed [{code}]: {message}")
        self.code = code
        self.message = message


class EmptyPlan(CompileError):
    """A plan with no models.

    Not an error upstream: it is the planner's good outcome, *nothing to rebuild*.
    It still has no run in it, and a caller planning on every commit must be able to
    tell it apart from a failure, so it is its own class.
    """

    def __init__(self) -> None:
        super().__init__("plan is empty: nothing to rebuild")


class NoCommand(CompileError):
    """Nothing would run: neither a command nor a connection to run SQL on."""

    def __init__(self) -> None:
        super().__init__(
            "nothing would run: pass command_template (a shell command per model, run by "
            "BashOperator) or conn_id (runs the SQL Freshet rendered for each model, "
            "contract v4)"
        )


class RefusedRequest(CompileError):
    """The inputs are ambiguous, or unsafe to turn into a DAG.

    Raised instead of picking a reading, because every case here is one where the
    wrong pick would fail late: after upstream tasks have already written, or
    partway through a table that a retry cannot put back.
    """


# ── the contract ───────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Widening:
    """Why the planner widened a replace declaration it could not honour.

    On the wire this is internally tagged by ``widened_because``; ``materialization``
    is set only for ``not_incremental`` and ``declared`` only for
    ``no_partition_column``.
    """

    because: str
    materialization: Optional[str] = None
    declared: Optional[str] = None

    def explain(self) -> str:
        """One sentence an operator can act on, worded exactly as dagron-state's."""
        if self.because == "not_incremental":
            return (
                f"the model is a {self.materialization}, so it has no partitions to "
                "replace in place"
            )
        if self.because == "whole_model_in_plan":
            return "the plan rebuilds this model in full, so there is nothing to scope to"
        if self.because == "no_partition_column":
            return f"`{self.declared}` needs a partition_column and the project declares none"
        if self.because == "no_unique_key":
            return "`merge` needs a unique_key and the project declares none"
        raise ValueError(f"unknown widening {self.because!r}")


@dataclass(frozen=True)
class Replace:
    """What a rebuild does to the target relation (contract v3).

    Semantics, not SQL. This package never renders one; contract v4's ``sql`` is
    Freshet's own rendering, and the placeholders expand to the planner's *word*
    (``insert_overwrite``), never to a statement.
    """

    strategy: str
    #: The partitions the write replaces; ``None`` when it covers the whole
    #: relation (wire: ``"whole"``).
    target_partitions: Optional[Tuple[str, ...]]
    partition_column: Optional[str] = None
    unique_key: Tuple[str, ...] = ()
    #: Present when the declared strategy could not be honoured and the planner
    #: widened to a full refresh. It is the difference between rewriting two
    #: partitions and rewriting a table, so the DAG says it loudly.
    widened: Optional[Widening] = None


@dataclass(frozen=True)
class Sql:
    """The statements Freshet rendered for one model's rebuild (contract v4).

    ``statements`` must run in order on one session. ``atomic`` is the planner's
    promise about them: when it is ``False``, a failure part-way can leave the
    relation partially written, and the DAG says so on the task.
    """

    dialect: str
    statements: Tuple[str, ...]
    atomic: bool

    @property
    def script(self) -> str:
        """The statements as one string, joined by ``";\\n"``: what ``{{ sql }}`` expands to."""
        return ";\n".join(self.statements)


@dataclass(frozen=True)
class PlanModel:
    """One model in the plan: what to rebuild, why, how much, and what it waits for."""

    name: str
    #: The upstream the planner attributed this rebuild to; ``None`` when the model
    #: itself changed. An explanation, not an edge list: see :func:`compile_plan`.
    because_of: Optional[str] = None
    via_columns: Tuple[str, ...] = ()
    #: The partitions to rebuild; ``None`` for a whole-model rebuild. An empty tuple
    #: is a partition-scoped rebuild of no partitions, which is not the same thing.
    partitions: Optional[Tuple[str, ...]] = None
    #: The planner's own edges, narrowed to plan members (contract v2).
    #:
    #: ``None`` and ``()`` are different answers and are never collapsed. ``None``
    #: means the producer predates the field, so edges must come from somewhere else;
    #: ``()`` is a v2 producer saying, authoritatively, that this model waits for
    #: nothing. Reading one as the other would give a root model an edge the planner
    #: denied.
    depends_on: Optional[Tuple[str, ...]] = None
    replace: Optional[Replace] = None
    sql: Optional[Sql] = None

    @property
    def reason(self) -> str:
        """``"directly_changed"`` or ``"downstream"``."""
        return "directly_changed" if self.because_of is None else "downstream"

    @property
    def unit(self) -> str:
        """``"full_model"`` or ``"partitions"``."""
        return "full_model" if self.partitions is None else "partitions"


@dataclass(frozen=True)
class PlanFailure:
    """The planner's structured failure. When present, ``models`` is empty."""

    code: str
    message: str


@dataclass(frozen=True)
class Plan:
    """A planner result: the minimal, topologically ordered set of models to rebuild.

    ``next_state`` is carried opaquely, as dagron-state carries it: this package has
    no business interpreting fingerprints or watermarks. It belongs to whoever owns
    the state backend, and must be committed only after the plan has run.
    """

    models: Tuple[PlanModel, ...] = ()
    next_state: Any = None
    error: Optional[PlanFailure] = None


# ── parsing ────────────────────────────────────────────────────────────────────


def parse_plan(payload: PlanInput) -> Plan:
    """Read a plan from JSON text, bytes, an already-decoded mapping, or a :class:`Plan`.

    Strict about what the contract defines and blind to what it does not: a missing
    required field or an unknown variant raises :class:`ContractViolation`, an
    unknown field is ignored. That is serde's default behaviour, and matching it is
    what keeps this reader and dagron-state's accepting the same payloads.
    """
    if isinstance(payload, Plan):
        return payload
    if isinstance(payload, (str, bytes, bytearray)):
        try:
            payload = json.loads(payload)
        except ValueError as e:
            raise ContractViolation(f"plan is not valid JSON: {e}") from e
    obj = _object(payload, "plan")
    models = obj.get("models", [])
    if not isinstance(models, list):
        raise ContractViolation(f"plan.models: expected an array, got {_kind(models)}")
    return Plan(
        models=tuple(_parse_model(m, f"models[{i}]") for i, m in enumerate(models)),
        next_state=obj.get("next_state"),
        error=_parse_error(obj.get("error")),
    )


def _parse_model(raw: Any, where: str) -> PlanModel:
    obj = _object(raw, where)
    name = _string(_required(obj, "name", where), f"{where}.name")
    because_of, via_columns = _parse_reason(_required(obj, "reason", where), f"{where}.reason")
    partitions = _parse_unit(_required(obj, "unit", where), f"{where}.unit")
    raw_deps = obj.get("depends_on")
    depends_on = None if raw_deps is None else _strings(raw_deps, f"{where}.depends_on")
    return PlanModel(
        name=name,
        because_of=because_of,
        via_columns=via_columns,
        partitions=partitions,
        depends_on=depends_on,
        replace=_parse_replace(obj.get("replace"), f"{where}.replace"),
        sql=_parse_sql(obj.get("sql"), f"{where}.sql"),
    )


def _parse_reason(value: Any, where: str) -> Tuple[Optional[str], Tuple[str, ...]]:
    if value == "directly_changed":
        return None, ()
    if isinstance(value, Mapping) and list(value) == ["downstream"]:
        where = f"{where}.downstream"
        body = _object(value["downstream"], where)
        because_of = _string(_required(body, "because_of", where), f"{where}.because_of")
        return because_of, _strings(body.get("via_columns", []), f"{where}.via_columns")
    raise _unknown(where, value, '"directly_changed" or {"downstream": {...}}')


def _parse_unit(value: Any, where: str) -> Optional[Tuple[str, ...]]:
    if value == "full_model":
        return None
    if isinstance(value, Mapping) and list(value) == ["partitions"]:
        return _strings(value["partitions"], f"{where}.partitions")
    raise _unknown(where, value, '"full_model" or {"partitions": [...]}')


def _parse_replace(value: Any, where: str) -> Optional[Replace]:
    if value is None:
        return None
    obj = _object(value, where)
    target = _required(obj, "target", where)
    if target == "whole":
        target_partitions = None
    elif isinstance(target, Mapping) and list(target) == ["partitions"]:
        target_partitions = _strings(target["partitions"], f"{where}.target.partitions")
    else:
        raise _unknown(f"{where}.target", target, '"whole" or {"partitions": [...]}')
    column = obj.get("partition_column")
    return Replace(
        strategy=_strategy(_required(obj, "strategy", where), f"{where}.strategy"),
        target_partitions=target_partitions,
        partition_column=None if column is None else _string(column, f"{where}.partition_column"),
        unique_key=_strings(obj.get("unique_key", []), f"{where}.unique_key"),
        widened=_parse_widening(obj.get("widened"), f"{where}.widened"),
    )


def _parse_widening(value: Any, where: str) -> Optional[Widening]:
    if value is None:
        return None
    obj = _object(value, where)
    because = _string(_required(obj, "widened_because", where), f"{where}.widened_because")
    if because == "not_incremental":
        materialization = _string(_required(obj, "materialization", where), f"{where}.materialization")
        return Widening(because, materialization=materialization)
    if because == "no_partition_column":
        return Widening(because, declared=_strategy(_required(obj, "declared", where), f"{where}.declared"))
    if because in ("whole_model_in_plan", "no_unique_key"):
        return Widening(because)
    raise _unknown(
        f"{where}.widened_because",
        because,
        "not_incremental | whole_model_in_plan | no_partition_column | no_unique_key",
    )


def _parse_sql(value: Any, where: str) -> Optional[Sql]:
    if value is None:
        return None
    obj = _object(value, where)
    atomic = _required(obj, "atomic", where)
    if not isinstance(atomic, bool):
        raise ContractViolation(f"{where}.atomic: expected a boolean, got {_kind(atomic)}")
    return Sql(
        dialect=_string(_required(obj, "dialect", where), f"{where}.dialect"),
        statements=_strings(_required(obj, "statements", where), f"{where}.statements"),
        atomic=atomic,
    )


def _parse_error(value: Any) -> Optional[PlanFailure]:
    if value is None:
        return None
    obj = _object(value, "error")
    return PlanFailure(
        code=_string(_required(obj, "code", "error"), "error.code"),
        message=_string(_required(obj, "message", "error"), "error.message"),
    )


def _strategy(value: Any, where: str) -> str:
    if value in REPLACE_STRATEGIES:
        return value
    raise _unknown(where, value, " | ".join(REPLACE_STRATEGIES))


def _required(obj: Mapping[str, Any], key: str, where: str) -> Any:
    if key not in obj:
        raise ContractViolation(f"{where}: missing required field `{key}`")
    return obj[key]


def _object(value: Any, where: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ContractViolation(f"{where}: expected an object, got {_kind(value)}")
    return value


def _string(value: Any, where: str) -> str:
    if not isinstance(value, str):
        raise ContractViolation(f"{where}: expected a string, got {_kind(value)}")
    return value


def _strings(value: Any, where: str) -> Tuple[str, ...]:
    if not isinstance(value, list):
        raise ContractViolation(f"{where}: expected an array of strings, got {_kind(value)}")
    return tuple(_string(v, f"{where}[{i}]") for i, v in enumerate(value))


def _unknown(where: str, value: Any, expected: str) -> ContractViolation:
    return ContractViolation(
        f"{where}: expected {expected}, got {json.dumps(value, default=repr)}. This package reads "
        f"{WIRE_CONTRACT_VERSION}; a variant it does not know means the contract moved, "
        "and both consumers have to move with it"
    )


def _kind(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "a boolean"
    if isinstance(value, (int, float)):
        return "a number"
    if isinstance(value, str):
        return "a string"
    if isinstance(value, list):
        return "an array"
    if isinstance(value, Mapping):
        return "an object"
    return type(value).__name__


# ── compiling ──────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Task:
    """One plan model as one Airflow task: its id, its edges, and the model itself."""

    task_id: str
    model: PlanModel
    #: Task ids this task waits on, sorted and deduplicated.
    upstream: Tuple[str, ...]

    @property
    def facts(self) -> Dict[str, Any]:
        """The planner's facts for this task; see :func:`task_facts`."""
        return task_facts(self.model)

    def render(self, template: str, *, escape: Optional[Callable[[str], str]] = None) -> str:
        """Expand this model's placeholders in ``template``; see :func:`render_template`."""
        return render_template(template, self.model, escape=escape)


def compile_plan(
    plan: PlanInput,
    *,
    graph: Optional[Mapping[str, Sequence[str]]] = None,
    ordering: str = "derived",
) -> Tuple[Task, ...]:
    """Turn a plan into tasks with ids and edges: dagron-state's ``compile``, minus the spec.

    Refuses, in dagron-state's order, a plan the planner failed to produce
    (:class:`PlannerFailed`) and a plan with nothing in it (:class:`EmptyPlan`).

    ``graph`` maps a model to its upstream models and matters only for a plan from a
    contract-v1 producer; see :func:`_derived_edges`. ``ordering="sequential"``
    ignores edges entirely and chains the plan in order: always correct, never
    parallel, for a caller who would rather not reason about edges at all.

    One refusal is this package's own. A model named twice raises
    :class:`ContractViolation`: a plan is a set, and with two ``a`` tasks there is no
    right answer to which one ``mart`` waits for. dagron-state looks task names up by
    model name, so it gives both copies the second one's name instead.
    """
    parsed = parse_plan(plan)
    if parsed.error is not None:
        raise PlannerFailed(parsed.error.code, parsed.error.message)
    if not parsed.models:
        raise EmptyPlan()
    if ordering not in ORDERINGS:
        raise ValueError(f"ordering must be one of {ORDERINGS}, got {ordering!r}")
    _check_graph(graph)
    seen: Set[str] = set()
    for model in parsed.models:
        if model.name in seen:
            raise ContractViolation(
                f"model `{model.name}` appears twice in the plan; a plan names each model "
                "once, so there is no right answer to which copy a dependent waits for"
            )
        seen.add(model.name)

    # Model name -> task id, and the plan position that makes edge filtering
    # acyclic. Built in one pass so that later models can resolve earlier ones.
    taken: Set[str] = set()
    task_ids: Dict[str, str] = {}
    position: Dict[str, int] = {}
    for i, model in enumerate(parsed.models):
        task_ids[model.name] = _unique_task_id(model.name, taken)
        position[model.name] = i

    tasks: List[Task] = []
    for i, model in enumerate(parsed.models):
        if ordering == "sequential":
            upstream: Tuple[str, ...] = (task_ids[parsed.models[i - 1].name],) if i > 0 else ()
        else:
            upstream = _derived_edges(model, i, graph, task_ids, position)
        tasks.append(Task(task_id=task_ids[model.name], model=model, upstream=upstream))
    return tuple(tasks)


def _derived_edges(
    model: PlanModel,
    index: int,
    graph: Optional[Mapping[str, Sequence[str]]],
    task_ids: Mapping[str, str],
    position: Mapping[str, int],
) -> Tuple[str, ...]:
    """One model's upstream task ids: dagron-state's ``derived_edges``, rule for rule.

    Sources, in descending order of trust, and only the first one present is used:

    1. **The plan's own ``depends_on``** (contract v2). The planner computed the plan
       and knows the project graph, so this is exact and needs nothing from the
       caller. An empty list is an answer, and stops here.
    2. **The caller's ``graph``**, looked up per model, for a v1 producer that cannot
       carry edges. Exact if the caller's graph is.
    3. **The ``because_of`` attribution**, when there is nothing else. It names one
       cause, not the edge set, so a model downstream of two changed parents waits
       for only one of them. Kept so a v1 plan with no graph still gets *some*
       ordering rather than a flat fan-out.

    Whatever the source, an edge survives only if it names a model that is in the
    plan *and* earlier in it. That is what keeps the DAG acyclic when a caller's graph
    is stale: an edge to a model that runs later, or never, is dropped rather than
    handed to Airflow as a cycle or a dangling reference. For source 1 the filter is
    a no-op, since the planner already emits plan members in topological order.
    """
    if model.depends_on is not None:
        upstreams: Iterable[str] = model.depends_on
    elif graph is not None and model.name in graph:
        upstreams = graph[model.name]
    else:
        upstreams = () if model.because_of is None else (model.because_of,)
    return tuple(
        sorted({task_ids[up] for up in upstreams if up in position and position[up] < index})
    )


def _check_graph(graph: Optional[Mapping[str, Sequence[str]]]) -> None:
    if graph is None:
        return
    if not isinstance(graph, Mapping):
        raise TypeError("graph must map each model name to a list of its upstream model names")
    for model, upstreams in graph.items():
        # A bare string iterates one character at a time, which would quietly turn
        # {"mart": "stg"} into edges to models named "s", "t" and "g".
        if isinstance(upstreams, (str, bytes)) or not isinstance(upstreams, Iterable):
            raise TypeError(
                f"graph[{model!r}] must be a list of model names, not {type(upstreams).__name__}"
            )


_TASK_ID_CHARS = frozenset(string.ascii_letters + string.digits + "_-")


def _unique_task_id(model: str, taken: Set[str]) -> str:
    """Sanitize a model name into a task id, unique within the DAG.

    The same rule as dagron-state's ``unique_task_name``: anything outside
    ``[A-Za-z0-9_-]`` becomes ``_``, and a collision is suffixed ``_2``, ``_3``, and so
    on until free. The reason is Airflow's own here, not dagron's: Airflow uses ``.``
    to nest a task id inside its TaskGroup, so ``analytics.stg_orders`` would read as
    task ``stg_orders`` in a group called ``analytics``. Because the id is sanitized
    and possibly suffixed, it is not a reliable key back to the model; the model
    name is kept in :func:`task_facts` for that.
    """
    base = "".join(c if c in _TASK_ID_CHARS else "_" for c in model) or "model"
    candidate, n = base, 2
    while candidate in taken:
        candidate = f"{base}_{n}"
        n += 1
    taken.add(candidate)
    return candidate


def task_facts(model: PlanModel) -> Dict[str, Any]:
    """The planner's facts about one model, for the task's record.

    The same keys as dagron-state's ``task_input``, plus ``sql`` from contract v4.
    Recorded on the task, not only substituted into its command, so that what a run
    meant to do to the table is on the record even when the command never
    mentioned it, above all that the planner widened it.
    """
    facts: Dict[str, Any] = {"model": model.name}
    if model.because_of is None:
        facts["reason"] = "directly_changed"
    else:
        facts["reason"] = "downstream"
        facts["because_of"] = model.because_of
        facts["via_columns"] = list(model.via_columns)
    if model.partitions is None:
        facts["unit"] = "full_model"
    else:
        facts["unit"] = "partitions"
        facts["partitions"] = list(model.partitions)
    replace = model.replace
    if replace is not None:
        facts["replace"] = replace.strategy
        if replace.partition_column is not None:
            facts["partition_column"] = replace.partition_column
        if replace.unique_key:
            facts["unique_key"] = list(replace.unique_key)
        if replace.widened is not None:
            facts["replace_widened"] = replace.widened.explain()
    if model.sql is not None:
        facts["sql"] = {
            "dialect": model.sql.dialect,
            "statements": list(model.sql.statements),
            "atomic": model.sql.atomic,
        }
    return facts


# ── templating ─────────────────────────────────────────────────────────────────


def placeholder_values(model: PlanModel) -> Dict[str, str]:
    """What each placeholder expands to for ``model``.

    An absent fact expands to the empty string rather than leaving the braces in
    place, as in dagron-state: a literal ``{{ replace }}`` reaching a shell is a
    worse failure than an empty argument, and in Airflow it would not even reach the
    shell, because Jinja would fail on the undefined name first.
    """
    replace, sql = model.replace, model.sql
    return {
        "model": model.name,
        "unit": model.unit,
        "partitions": ",".join(model.partitions or ()),
        "replace": replace.strategy if replace is not None else "",
        "partition_column": (replace.partition_column or "") if replace is not None else "",
        "unique_key": ",".join(replace.unique_key) if replace is not None else "",
        "sql": sql.script if sql is not None else "",
        "dialect": sql.dialect if sql is not None else "",
    }


_NAMES = "|".join(PLACEHOLDERS)
# Exactly the two spellings dagron-state's `render_command` replaces: `{{ name }}` and
# `{{name}}`. Nothing more liberal, so a template means the same thing to both readers.
_PLACEHOLDER_RE = re.compile(r"\{\{ (%s) \}\}|\{\{(%s)\}\}" % (_NAMES, _NAMES))
# Anything that looks like it *meant* to be one of ours: other spacing, whitespace
# control, a filter. See `misspelled_placeholders`.
_LOOSE_PLACEHOLDER_RE = re.compile(r"\{\{-?\s*(?:%s)\b.*?\}\}" % _NAMES, re.S)
_EXACT_SPELLINGS = frozenset(
    spelling for name in PLACEHOLDERS for spelling in ("{{ %s }}" % name, "{{%s}}" % name)
)


def render_template(
    template: str,
    model: PlanModel,
    *,
    escape: Optional[Callable[[str], str]] = None,
) -> str:
    """Expand the Freshet placeholders in ``template`` for ``model``, and nothing else.

    ``{{ model }}``, ``{{ unit }}``, ``{{ partitions }}`` (comma-joined, empty for a
    whole-model rebuild), ``{{ replace }}``, ``{{ partition_column }}``,
    ``{{ unique_key }}`` (comma-joined) as in dagron-state; and from contract v4
    ``{{ sql }}`` (the statements joined by ``";\\n"``) and ``{{ dialect }}``. Each
    in both the spaced and the unspaced spelling, empty when the plan has no such fact.

    Any other ``{{ ... }}`` is left exactly as written. That is the point of doing
    this in Python before Airflow sees the string: the plan's facts are fixed when
    the DAG is built, while ``{{ ds }}`` and friends belong to the run and to
    Airflow's own Jinja pass, which happens later.

    ``escape`` is applied to each substituted *value*. The Airflow glue passes
    :func:`protect_from_jinja` so that a value containing braces (SQL does) comes out
    of that later pass as it went in.

    One deliberate difference from dagron-state: the expansion is a single pass, so
    a substituted value is never scanned again. dagron-state chains ``str::replace``
    calls, so a model name containing the text ``{{ unit }}`` would have it expanded
    by the next call. No real model name does; rendered SQL, which v4 now
    substitutes, can contain anything, including the text ``{{ model }}`` in a string
    literal.
    """
    values = placeholder_values(model)

    def substitute(match: "re.Match[str]") -> str:
        value = values[match.group(1) or match.group(2)]
        return escape(value) if escape is not None else value

    return _PLACEHOLDER_RE.sub(substitute, template)


def misspelled_placeholders(template: str) -> List[str]:
    """Placeholder-shaped text in ``template`` that :func:`render_template` will not expand.

    ``{{model }}``, ``{{- model -}}``, ``{{ model | upper }}``: dagron-state leaves these
    in the argv untouched. In Airflow they would reach Jinja instead, where ``model``
    is undefined, and fail at run time, after upstream tasks have already written.
    The glue refuses them when the DAG is built.
    """
    return [
        m.group(0)
        for m in _LOOSE_PLACEHOLDER_RE.finditer(template)
        if m.group(0) not in _EXACT_SPELLINGS
    ]


def protect_from_jinja(value: str) -> str:
    """Rewrite ``value`` so that a Jinja render reproduces it exactly.

    A substituted value lands in a string that Airflow renders with Jinja later. If
    it contains ``{{``, ``{%`` or ``{#`` (SQL can: a JSON literal, a regex, a comment)
    Jinja would read it as template syntax: fail on it, or worse, silently eat a
    ``{# ... #}`` span. Every Jinja construct opens with ``{``, so replacing each
    ``{`` in the value with the expression ``{{ '{' }}``, which renders to a single
    brace, leaves nothing in the value for Jinja to open. Nothing else needs
    escaping: a lone ``}`` or ``%`` outside a construct is plain text.

    This assumes Jinja's default delimiters, which is why the glue refuses a DAG
    whose ``jinja_environment_kwargs`` change them.
    """
    return value.replace("{", "{{ '{' }}")
