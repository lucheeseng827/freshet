"""What the Airflow glue decides, tested without Airflow.

:func:`freshet_airflow.plan_operators` makes every decision :func:`build_dag` acts on:
which operator, what command or statements, which edges, what the task's doc warns
about, and which requests to refuse. None of that needs Airflow, so none of it
waits for an Airflow install to be tested. ``test_airflow.py`` then checks that the
real operators are built from these records faithfully.
"""

import json
import pathlib
import re

import pytest

from freshet_airflow import (
    EmptyPlan,
    NoCommand,
    PlannerFailed,
    RefusedRequest,
    plan_operators,
)

FIXTURES = pathlib.Path(__file__).with_name("fixtures")


def fixture(name):
    return (FIXTURES / name).read_text()


def v4():
    return json.loads(fixture("handwritten_v4_sql.json"))


def by_id(specs):
    return {s.task_id: s for s in specs}


# ── choosing how a model runs ──────────────────────────────────────────────────


def test_conn_id_runs_each_models_rendered_statements_in_order():
    specs = by_id(plan_operators(fixture("handwritten_v4_sql.json"), conn_id="warehouse"))
    events = specs["events"]
    assert events.kind == "sql"
    assert events.conn_id == "warehouse"
    assert events.statements == tuple(v4()["models"][0]["sql"]["statements"])
    assert events.bash_command is None
    assert specs["daily_rollup"].upstream == ("events",)
    assert specs["user_dim"].upstream == ("events",)


def test_the_statements_reach_the_sql_operator_untouched_by_any_escaping():
    # The SQL operator does not template its SQL, so nothing is escaped for it:
    # the braces in '{}'::jsonb must arrive exactly as the planner rendered them.
    user_dim = by_id(plan_operators(fixture("handwritten_v4_sql.json"), conn_id="wh"))["user_dim"]
    assert any("'{}'::jsonb" in s for s in user_dim.statements)


def test_command_template_runs_a_shell_command_per_model_with_airflow_jinja_intact():
    specs = by_id(
        plan_operators(
            fixture("captured_v2_fan_in.json"),
            command_template="dbt run --select {{ model }} --vars 'ds: {{ ds }}'",
        )
    )
    mart = specs["mart_revenue"]
    assert mart.kind == "bash"
    assert mart.bash_command == "dbt run --select mart_revenue --vars 'ds: {{ ds }}'"
    assert mart.upstream == ("dim_customer", "stg_orders"), "the fan-in keeps both parents"
    assert mart.statements == () and mart.conn_id is None


def test_a_substituted_value_is_protected_from_airflows_later_jinja_pass():
    spec = by_id(plan_operators(fixture("handwritten_v4_sql.json"), command_template="psql -c \"{{ sql }}\""))[
        "user_dim"
    ]
    assert "'{{ '{' }}}'::jsonb" in spec.bash_command, "the brace is rewritten so Jinja renders it back"


# ── refusals ───────────────────────────────────────────────────────────────────


def test_nothing_to_run_it_with_is_refused():
    with pytest.raises(NoCommand, match="nothing would run"):
        plan_operators(fixture("captured_v2_fan_in.json"))


def test_the_plans_own_refusals_come_before_the_missing_command():
    # dagron-state's order: a failed plan, then an empty one, then "no command".
    with pytest.raises(PlannerFailed):
        plan_operators({"models": [], "error": {"code": "bad_request", "message": "x"}})
    with pytest.raises(EmptyPlan):
        plan_operators({"models": []})


def test_both_a_command_and_a_connection_is_refused_as_ambiguous():
    with pytest.raises(RefusedRequest, match="not both"):
        plan_operators(fixture("handwritten_v4_sql.json"), conn_id="wh", command_template="dbt run -s {{ model }}")


def test_conn_id_on_a_plan_with_no_sql_is_refused_with_the_way_out():
    with pytest.raises(RefusedRequest, match="carries none") as caught:
        plan_operators(fixture("captured_v3_with_a_widening.json"), conn_id="wh")
    assert "command_template" in str(caught.value)


def test_conn_id_on_a_plan_where_some_models_lack_sql_names_them_instead_of_skipping_them():
    payload = v4()
    del payload["models"][1]["sql"]
    with pytest.raises(RefusedRequest, match=re.escape("1 model (`daily_rollup`) carries none")):
        plan_operators(payload, conn_id="wh")


def test_an_empty_statement_list_is_refused_because_it_would_succeed_without_writing():
    payload = v4()
    payload["models"][2]["sql"]["statements"] = []
    with pytest.raises(RefusedRequest, match="empty SQL statement list"):
        plan_operators(payload, conn_id="wh")


def test_sql_rendered_for_two_dialects_cannot_share_one_connection():
    payload = v4()
    payload["models"][1]["sql"]["dialect"] = "snowflake"
    with pytest.raises(RefusedRequest, match="postgres, snowflake"):
        plan_operators(payload, conn_id="wh")


def test_a_misspelled_placeholder_is_refused_when_the_dag_is_built_not_when_it_runs():
    with pytest.raises(RefusedRequest, match=re.escape("'{{model }}'")):
        plan_operators(fixture("golden.json"), command_template="dbt run -s {{model }}")


def test_a_dag_with_custom_jinja_delimiters_is_refused_for_command_templates():
    with pytest.raises(RefusedRequest, match="variable_start_string"):
        plan_operators(
            fixture("golden.json"),
            command_template="dbt run -s {{ model }}",
            jinja_environment_kwargs={"variable_start_string": "[[", "variable_end_string": "]]"},
        )
    # Harmless environment options are fine.
    plan_operators(
        fixture("golden.json"),
        command_template="dbt run -s {{ model }}",
        jinja_environment_kwargs={"trim_blocks": True},
    )


def test_an_argv_is_refused_with_the_one_line_that_converts_it():
    with pytest.raises(TypeError, match="shlex.join"):
        plan_operators(fixture("golden.json"), command_template=["sh", "-c", "dbt run -s {{ model }}"])


# ── the loud notes ─────────────────────────────────────────────────────────────


def test_a_widened_replace_op_is_a_warning_whatever_runs_the_model():
    for kwargs in ({"conn_id": "wh"}, {"command_template": "dbt run -s {{ model }}"}):
        specs = by_id(plan_operators(fixture("handwritten_v4_sql.json"), **kwargs))
        [widened] = [w for w in specs["user_dim"].warnings if "widened" in w]
        assert "full refresh of the whole relation" in widened
        assert "no partitions to replace in place" in widened
        assert specs["events"].warnings == ()


def test_a_non_atomic_write_is_a_warning_when_the_task_runs_the_sql():
    specs = by_id(plan_operators(fixture("handwritten_v4_sql.json"), conn_id="wh"))
    [non_atomic] = [w for w in specs["user_dim"].warnings if "non-atomic" in w]
    assert "4 statements" in non_atomic
    assert "partially written" in non_atomic
    assert not any("non-atomic" in w for w in specs["events"].warnings)


def test_a_non_atomic_write_is_a_warning_for_a_command_only_if_the_command_runs_the_sql():
    runs_it = by_id(plan_operators(fixture("handwritten_v4_sql.json"), command_template="psql -c \"{{sql}}\""))
    assert any("non-atomic" in w for w in runs_it["user_dim"].warnings)

    ignores_it = by_id(plan_operators(fixture("handwritten_v4_sql.json"), command_template="dbt run -s {{ model }}"))
    assert not any("non-atomic" in w for w in ignores_it["user_dim"].warnings), (
        "dbt never executes the rendered statements, so their atomicity is not this task's risk"
    )


def test_the_task_doc_leads_with_its_warnings_and_ends_with_the_planner_facts():
    user_dim = by_id(plan_operators(fixture("handwritten_v4_sql.json"), conn_id="wh"))["user_dim"]
    doc = user_dim.doc_md
    first_warning = doc.index("> **WARNING:**")
    assert first_warning < doc.index("- **Why:**"), "warnings before anything else"
    assert doc.count("> **WARNING:**") == 2
    assert "downstream of `events` via `last_seen`, `user_id`" in doc
    assert "the planner marks them **not atomic**" in doc
    assert "```sql\nDROP TABLE IF EXISTS user_dim__freshet_new;\nCREATE TABLE" in doc
    facts_json = doc.split("```json\n", 1)[1].split("\n```", 1)[0]
    assert json.loads(facts_json) == user_dim.facts, "the doc carries the exact record"


def test_the_facts_are_the_same_record_whichever_operator_runs_the_model():
    sql = by_id(plan_operators(fixture("handwritten_v4_sql.json"), conn_id="wh"))
    bash = by_id(plan_operators(fixture("handwritten_v4_sql.json"), command_template="run {{ model }}"))
    for task_id in sql:
        assert sql[task_id].facts == bash[task_id].facts
