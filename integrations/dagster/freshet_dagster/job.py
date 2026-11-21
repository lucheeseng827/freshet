"""Build a Dagster job from a Freshet plan: one op per model, wired from the plan's edges.

The Dagster half, thin on purpose: :func:`freshet_dagster.ops.plan_ops` has decided
everything, and this module constructs. Dagster is imported inside
:func:`build_job`, never at module import, so ``import freshet_dagster`` works on a
machine without it.

Each op takes one ``Nothing`` input per op it waits on and gives one ``Nothing``
output, so the edges order the ops and carry no data: what a model's rebuild
produces is in the warehouse, not in Dagster's IO managers.
"""

from __future__ import annotations

import json
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence

from freshet_plan import PlanInput, Task

from .ops import CommandFailed, OpSpec, plan_ops, run_command

__all__ = ["build_job"]


def build_job(
    plan: PlanInput,
    *,
    name: str,
    command_template: Optional[str] = None,
    run: Optional[Callable[[Any, Task], Any]] = None,
    required_resource_keys: Iterable[str] = (),
    graph: Optional[Mapping[str, Sequence[str]]] = None,
    ordering: str = "derived",
    job_kwargs: Optional[Mapping[str, Any]] = None,
) -> Any:
    """A Dagster ``JobDefinition`` that rebuilds what ``plan`` says, in the order it says.

    ``command_template`` is run by ``bash -c`` per model, after Freshet's placeholders
    (``{{ model }}``, ``{{ partitions }}``, ``{{ sql }}``, …) are expanded for it.
    ``run`` instead is your function, called as ``run(context, task)`` with the op's
    context and the ``freshet_plan.Task`` (its model, its SQL, its facts): the way to
    run the SQL Freshet rendered on a connection a resource of yours holds. Name those
    resources in ``required_resource_keys`` and supply them in ``job_kwargs``.

    ``job_kwargs`` goes to ``GraphDefinition.to_job`` and overrides the description and
    tags given here; put ``resource_defs``, ``executor_def``, ``op_retry_policy`` or
    ``config`` in it.
    """
    import dagster as dg

    spec = plan_ops(
        plan,
        command_template=command_template,
        run=run,
        graph=graph,
        ordering=ordering,
        valid_name=_valid_name(),
    )
    keys = frozenset(required_resource_keys)
    ops = [_op(dg, op, run, keys) for op in spec.ops]
    dependencies = {
        op.name: {upstream: dg.DependencyDefinition(upstream) for upstream in op.upstream} for op in spec.ops
    }
    graph_def = dg.GraphDefinition(name=name, node_defs=ops, dependencies=dependencies)
    return graph_def.to_job(**{"description": spec.description, "tags": dict(spec.tags), **dict(job_kwargs or {})})


def _op(dg: Any, spec: OpSpec, run: Optional[Callable[[Any, Task], Any]], keys: frozenset) -> Any:
    facts = json.dumps(spec.facts, indent=2, sort_keys=True)

    # Unannotated: Dagster refuses any annotation on `context` but its own classes.
    def body(context) -> None:  # type: ignore[no-untyped-def]
        model = spec.task.model.name
        context.log.info(f"Freshet plan facts for `{model}`:\n{facts}")
        for warning in spec.warnings:
            context.log.warning(warning)
        if spec.command is None:
            assert run is not None
            run(context, spec.task)
            return
        try:
            run_command(spec.command, model, context.log.info)
        except CommandFailed as failed:
            raise dg.Failure(
                description=str(failed),
                metadata={"exit code": failed.code, "last output": "\n".join(failed.tail)},
            ) from None

    return dg.op(
        name=spec.name,
        description=spec.description,
        ins={upstream: dg.In(dg.Nothing) for upstream in spec.upstream},
        out=dg.Out(dg.Nothing),
        tags=dict(spec.tags),
        required_resource_keys=set(keys),
    )(body)


def _valid_name() -> Callable[[str], bool]:
    """Dagster's own test for a name: its characters, and the words it reserves.

    Read from Dagster rather than copied, because the reserved words are Dagster's to
    change. The import is of a private module, so a Dagster that moves it falls back
    to the characters and Python's keywords, the bulk of the list.
    """
    try:
        from dagster._core.definitions.utils import is_valid_name

        return is_valid_name
    except ImportError:  # pragma: no cover - depends on the Dagster version
        import keyword
        import re

        pattern = re.compile(r"^[A-Za-z0-9_]+$")
        return lambda name: bool(pattern.match(name)) and not keyword.iskeyword(name)
