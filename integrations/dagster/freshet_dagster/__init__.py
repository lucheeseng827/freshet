"""freshet-dagster: run a Freshet state plan as a Dagster job.

Freshet decides the minimal set of models a SQL change requires rebuilding and says
so as JSON on a frozen wire contract. dagron reads it in ``dagron-state``, Airflow
through ``freshet-airflow``, and Dagster through this package, all three by the
rules ``freshet-plan`` holds::

    freshet plan --project ./models --state .planner/state.json --json > plan.json

    import dagster as dg
    from freshet_dagster import build_job

    job = build_job(open("plan.json").read(), name="freshet_state_plan",
                    command_template="dbt run --select {{ model }}")
    defs = dg.Definitions(jobs=[job])

Two halves, as in freshet-airflow. :mod:`freshet_dagster.ops` decides everything
and imports no Dagster; :mod:`freshet_dagster.job` constructs, and imports Dagster
only when :func:`build_job` is called.
"""

from freshet_plan import (
    CompileError,
    ContractViolation,
    EmptyPlan,
    NoCommand,
    PlannerFailed,
    RefusedRequest,
    Task,
    parse_plan,
)

from .job import build_job
from .ops import DEFAULT_TAGS, SHELL, CommandFailed, JobSpec, OpSpec, op_names, plan_ops, run_command

__version__ = "0.2.0"

__all__ = [
    "__version__",
    "DEFAULT_TAGS",
    "SHELL",
    "CompileError",
    "ContractViolation",
    "PlannerFailed",
    "EmptyPlan",
    "NoCommand",
    "RefusedRequest",
    "CommandFailed",
    "Task",
    "OpSpec",
    "JobSpec",
    "parse_plan",
    "plan_ops",
    "op_names",
    "run_command",
    "build_job",
]
