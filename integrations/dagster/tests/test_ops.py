"""The Dagster job's decisions, made without Dagster.

The edges, placeholders and facts are freshet-plan's and tested there; what is
pinned here is what this package adds: op names Dagster accepts, the refusals that
follow from Dagster rendering nothing after Freshet does, the warnings, and the
command each op runs.
"""

import json
import pathlib
import sys

import pytest

from freshet_dagster import (
    CommandFailed,
    EmptyPlan,
    NoCommand,
    PlannerFailed,
    RefusedRequest,
    op_names,
    plan_ops,
    run_command,
)
from freshet_plan import compile_plan
from freshet_plan.explain import summary, why

# The contract fixtures belong to freshet-plan, the reader this package builds on.
FIXTURES = pathlib.Path(__file__).resolve().parents[2] / "plan" / "tests" / "fixtures"
ECHO = "echo {{ model }}"


def fixture(name):
    return (FIXTURES / name).read_text()


def plan_of(*models):
    return {"models": [{"reason": "directly_changed", "unit": "full_model", "depends_on": [], **m} for m in models]}


@pytest.mark.parametrize("name", sorted(p.name for p in FIXTURES.glob("*.json")))
def test_every_op_waits_on_exactly_the_tasks_freshet_plan_compiles(name):
    spec = plan_ops(fixture(name), command_template=ECHO)
    tasks = compile_plan(fixture(name))
    assert [op.name for op in spec.ops] == [t.task_id for t in tasks], "these names need no change"
    assert [op.upstream for op in spec.ops] == [t.upstream for t in tasks]


def test_a_fan_in_waits_on_both_parents():
    ops = {op.name: op for op in plan_ops(fixture("captured_v2_fan_in.json"), command_template=ECHO).ops}
    assert ops["mart_revenue"].upstream == ("dim_customer", "stg_orders")


def test_op_names_keep_only_what_dagster_accepts_and_stay_unique():
    plan = plan_of(
        {"name": "analytics.stg"},
        {"name": "a-b"},
        {"name": "a_b", "depends_on": ["a-b"]},
        {"name": "type", "depends_on": ["a_b"]},
    )
    reserved = {"type", "input"}
    spec = plan_ops(plan, command_template=ECHO, valid_name=lambda n: n not in reserved)
    assert [op.name for op in spec.ops] == ["analytics_stg", "a_b_2", "a_b", "type_2"], "a_b keeps its name"
    assert [op.upstream for op in spec.ops] == [(), (), ("a_b_2",), ("a_b",)], "edges follow the renames"
    assert [op.tags["freshet/model"] for op in spec.ops] == ["analytics.stg", "a-b", "a_b", "type"]
    assert [op.facts["model"] for op in spec.ops] == ["analytics.stg", "a-b", "a_b", "type"]


def test_a_reserved_name_is_suffixed_like_a_collision():
    tasks = compile_plan(plan_of({"name": "input"}, {"name": "input_2"}))
    assert op_names(tasks, lambda n: n != "input") == {"input": "input_3", "input_2": "input_2"}
    assert op_names(tasks) == {"input": "input", "input_2": "input_2"}


def test_the_plans_own_refusals_come_first_in_dagron_states_order():
    with pytest.raises(PlannerFailed):
        plan_ops({"models": [], "error": {"code": "bad_request", "message": "no"}})
    with pytest.raises(EmptyPlan):
        plan_ops({"models": []})
    with pytest.raises(NoCommand, match="nothing would run.*run\\(context, task\\)"):
        plan_ops(fixture("golden.json"))
    with pytest.raises(NoCommand):
        plan_ops(fixture("golden.json"), command_template="   ")


def test_a_blank_template_beside_run_leaves_run_to_rebuild_the_model():
    # command_template=os.environ.get("CMD", "") plus run= must not become `bash -c ""`,
    # which exits 0 and rebuilds nothing.
    for blank in ("", "   "):
        spec = plan_ops(fixture("golden.json"), command_template=blank, run=lambda context, task: None)
        assert [op.command for op in spec.ops] == [None, None], blank


def test_both_ways_at_once_is_refused():
    with pytest.raises(RefusedRequest, match="not both"):
        plan_ops(fixture("golden.json"), command_template=ECHO, run=lambda context, task: None)


def test_a_wrong_type_is_a_type_error():
    with pytest.raises(TypeError, match="shlex.join"):
        plan_ops(fixture("golden.json"), command_template=["dbt", "run"])
    with pytest.raises(TypeError, match="callable"):
        plan_ops(fixture("golden.json"), run="dbt run")


@pytest.mark.parametrize("template", ["dbt run -s {{model }}", "dbt run -s {{ model | upper }}"])
def test_a_misspelled_placeholder_is_refused_when_the_job_is_built(template):
    with pytest.raises(RefusedRequest, match="spells a Freshet placeholder"):
        plan_ops(fixture("golden.json"), command_template=template)


@pytest.mark.parametrize("template", ["dbt run -s {{ model }} --vars '{{ ds }}'", "run {{ params.x }}"])
def test_anything_else_in_braces_is_refused_because_nothing_renders_it_later(template):
    with pytest.raises(RefusedRequest, match="not one of Freshet's placeholders"):
        plan_ops(fixture("golden.json"), command_template=template)


def test_the_command_is_the_template_with_this_models_facts():
    spec = plan_ops(
        fixture("captured_v3_with_a_widening.json"),
        command_template="run {{ model }} [{{ partitions }}] {{replace}} key={{ unique_key }}",
    )
    assert [op.command for op in spec.ops] == [
        "run events [2026-06-20] insert_overwrite key=",
        "run daily_rollup [2026-06-20] merge key=dt",
        "run user_dim [] full_refresh key=",
    ]


def test_substituted_sql_is_neither_expanded_again_nor_refused():
    plan = plan_of({"name": "m", "sql": {"dialect": "postgres", "statements": ["SELECT '{{ model }}', '{}'::jsonb"], "atomic": True}})
    (op,) = plan_ops(plan, command_template="psql -c \"{{ sql }}\"").ops
    assert op.command == "psql -c \"SELECT '{{ model }}', '{}'::jsonb\""


def test_a_widening_is_warned_about_whatever_runs_the_model():
    spec = plan_ops(fixture("captured_v3_with_a_widening.json"), command_template=ECHO)
    ops = {op.name: op for op in spec.ops}
    assert len(ops["user_dim"].warnings) == 1
    assert "widened to a full refresh" in ops["user_dim"].warnings[0]
    assert ops["user_dim"].description.startswith("**Freshet plan: `user_dim`**\n\n> **WARNING:**")
    assert "read before launching" in spec.description
    assert not ops["events"].warnings


def test_a_non_atomic_write_is_warned_about_only_when_the_op_runs_the_sql():
    v4 = fixture("handwritten_v4_sql.json")
    quiet = {op.name: op for op in plan_ops(v4, command_template=ECHO).ops}
    loud = {op.name: op for op in plan_ops(v4, command_template="psql -c '{{ sql }}'").ops}
    by_run = {op.name: op for op in plan_ops(v4, run=lambda context, task: None).ops}
    assert not any("non-atomic" in w for w in quiet["user_dim"].warnings)
    assert any("non-atomic" in w for w in loud["user_dim"].warnings)
    assert any("non-atomic" in w for w in by_run["user_dim"].warnings), "what run does is not knowable"
    assert by_run["user_dim"].command is None


def test_the_job_says_what_freshet_plan_says():
    plan = fixture("captured_v2_fan_in.json")
    graph = {"dim_customer": [], "stg_orders": [], "mart_revenue": ["dim_customer", "stg_orders"], "other": []}
    spec = plan_ops(plan, command_template=ECHO, graph=graph)
    tasks = compile_plan(plan, graph=graph)
    assert spec.summary == summary(tasks, graph) == "3 of 4 models rebuild — 25% pruned"
    for task in tasks:
        assert why(task.model) in spec.description
    assert spec.tags == {"freshet": "state-plan"}


def test_a_command_streams_its_output_and_fails_with_its_tail():
    lines = []
    run_command("echo one; echo two >&2; echo three", "m", lines.append)
    assert lines == ["one", "two", "three"], "stdout and stderr, in the order written"

    lines.clear()
    with pytest.raises(CommandFailed) as failed:
        run_command("for i in 1 2 3 4; do echo line $i; done; exit 3", "m", lines.append, keep=2)
    assert failed.value.code == 3
    assert failed.value.tail == ("line 3", "line 4")
    assert str(failed.value) == "the command for `m` exited 3"


def test_a_command_runs_under_the_shell_it_is_given():
    lines = []
    run_command("print('from python')", "m", lines.append, shell=(sys.executable, "-c"))
    assert lines == ["from python"]


def test_a_plan_reaches_the_job_untouched_by_json():
    text = fixture("golden.json")
    assert [op.name for op in plan_ops(text, command_template=ECHO).ops] == [
        op.name for op in plan_ops(json.loads(text), command_template=ECHO).ops
    ]
