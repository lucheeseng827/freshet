"""freshet-airflow: run a Freshet state plan as an Airflow DAG.

Freshet decides the minimal set of models a SQL change requires rebuilding and says
so as JSON on a frozen wire contract. dagron was the first orchestrator to read that
contract (its ``dagron-state`` crate); this package is the second, and it
reads the same JSON by the same rules::

    freshet plan --project ./models --state .planner/state.json --json > plan.json

    from freshet_airflow import build_dag
    dag = build_dag(open("plan.json").read(), dag_id="freshet_state_plan",
                    command_template="dbt run --select {{ model }}")

The package is two halves. :mod:`freshet_airflow.plan` reads, validates and compiles a
plan with the standard library only, and imports nothing from Airflow.
:mod:`freshet_airflow.dag` turns the result into a DAG and imports Airflow only when
it is asked to build one.
"""

from .dag import DEFAULT_TAGS, OperatorSpec, build_dag, plan_operators
from .plan import (
    ORDERINGS,
    PLACEHOLDERS,
    REPLACE_STRATEGIES,
    WIRE_CONTRACT_VERSION,
    CompileError,
    ContractViolation,
    EmptyPlan,
    NoCommand,
    Plan,
    PlanFailure,
    PlanModel,
    PlannerFailed,
    RefusedRequest,
    Replace,
    Sql,
    Task,
    Widening,
    compile_plan,
    misspelled_placeholders,
    parse_plan,
    placeholder_values,
    protect_from_jinja,
    render_template,
    task_facts,
)

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "WIRE_CONTRACT_VERSION",
    "REPLACE_STRATEGIES",
    "PLACEHOLDERS",
    "ORDERINGS",
    "DEFAULT_TAGS",
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
    "OperatorSpec",
    "parse_plan",
    "compile_plan",
    "task_facts",
    "placeholder_values",
    "render_template",
    "misspelled_placeholders",
    "protect_from_jinja",
    "plan_operators",
    "build_dag",
]
