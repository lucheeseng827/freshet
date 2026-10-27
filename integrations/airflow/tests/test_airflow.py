"""The DAG, built by real Airflow.

Skipped as a module when Airflow is not importable. Everything these tests check was
decided without Airflow (``test_dag_decisions.py``); what is left to prove is that
the real operators are built faithfully from those decisions, that Airflow's own
machinery (edges, templating, serialization) treats them as intended, and that the
DAG survives the serialization the scheduler actually reads.
"""

import json
import pathlib

import pytest

pytest.importorskip("airflow", reason="apache-airflow is not installed")
pytest.importorskip(
    "airflow.providers.common.sql.operators.sql",
    reason="apache-airflow-providers-common-sql is not installed",
)

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator  # noqa: E402

try:  # noqa: E402
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:  # Airflow 2 without the standard provider
    from airflow.operators.bash import BashOperator

from freshet_airflow import build_dag  # noqa: E402
from freshet_airflow.operators.bash import FreshetBashOperator  # noqa: E402
from freshet_airflow.operators.sql import FreshetSQLExecuteQueryOperator  # noqa: E402

FIXTURES = pathlib.Path(__file__).with_name("fixtures")


def fixture(name):
    return (FIXTURES / name).read_text()


def v4_statements(model_index):
    return json.loads(fixture("handwritten_v4_sql.json"))["models"][model_index]["sql"]["statements"]


def render(dag, task, context):
    """Render a task's template fields the way the task runner does before ``execute``."""
    task.render_template_fields(context, jinja_env=dag.get_template_env())
    return task


def roundtrip(dag):
    """Serialize and deserialize the DAG as the DAG processor and scheduler do."""
    try:
        from airflow.serialization.serialized_objects import DagSerialization as Serializer
    except ImportError:  # Airflow 2 and early 3
        from airflow.serialization.serialized_objects import SerializedDAG as Serializer
    return Serializer.from_dict(json.loads(json.dumps(Serializer.to_dict(dag))))


# ── edges ──────────────────────────────────────────────────────────────────────


def test_a_v2_fan_in_becomes_a_dag_where_the_mart_waits_for_both_parents():
    dag = build_dag(
        fixture("captured_v2_fan_in.json"),
        dag_id="fan_in",
        command_template="dbt run --select {{ model }}",
    )
    assert set(dag.task_dict) == {"dim_customer", "stg_orders", "mart_revenue"}
    assert dag.get_task("mart_revenue").upstream_task_ids == {"dim_customer", "stg_orders"}
    assert dag.get_task("dim_customer").upstream_task_ids == set()
    assert dag.get_task("stg_orders").downstream_task_ids == {"mart_revenue"}


def test_a_v1_plan_falls_back_to_the_graph_then_to_the_attribution():
    with_graph = build_dag(
        fixture("golden.json"),
        dag_id="v1_graph",
        command_template="run {{ model }}",
        graph={"mart_revenue": ["stg_orders", "raw_orders"]},  # raw_orders is not in the plan
    )
    assert with_graph.get_task("mart_revenue").upstream_task_ids == {"stg_orders"}

    without = build_dag(fixture("captured_from_planner_cli.json"), dag_id="v1", command_template="run {{ model }}")
    assert without.get_task("mart_revenue").upstream_task_ids == {"stg_orders"}


def test_sequential_ordering_chains_the_dag():
    dag = build_dag(
        fixture("captured_v2_fan_in.json"),
        dag_id="chain",
        command_template="run {{ model }}",
        ordering="sequential",
    )
    assert dag.get_task("stg_orders").upstream_task_ids == {"dim_customer"}
    assert dag.get_task("mart_revenue").upstream_task_ids == {"stg_orders"}


def test_dotted_model_names_become_task_ids_airflow_does_not_read_as_task_groups():
    plan = {
        "models": [
            {"name": "analytics.stg_orders", "reason": "directly_changed", "unit": "full_model", "depends_on": []},
            {
                "name": "analytics.mart",
                "reason": {"downstream": {"because_of": "analytics.stg_orders", "via_columns": ["id"]}},
                "unit": "full_model",
                "depends_on": ["analytics.stg_orders"],
            },
        ]
    }
    dag = build_dag(plan, dag_id="dotted", command_template="run {{ model }}")
    mart = dag.get_task("analytics_mart")
    assert mart.upstream_task_ids == {"analytics_stg_orders"}
    assert mart.freshet_facts["model"] == "analytics.mart", "the facts keep the real name"
    assert mart.bash_command == "run analytics.mart", "and the command gets the real name too"


# ── the SQL path ───────────────────────────────────────────────────────────────


def test_conn_id_builds_one_sql_operator_per_model_with_its_statements_in_order():
    dag = build_dag(fixture("handwritten_v4_sql.json"), dag_id="sql", conn_id="warehouse")
    for index, task_id in enumerate(["events", "daily_rollup", "user_dim"]):
        task = dag.get_task(task_id)
        assert isinstance(task, SQLExecuteQueryOperator)
        assert isinstance(task, FreshetSQLExecuteQueryOperator)
        assert task.conn_id == "warehouse"
        assert task.sql == v4_statements(index), "the list, in order, as rendered"
        assert task.autocommit is False, "one transaction, committed after the last statement"
        assert task.parameters is None, "so a % in the SQL is never read as a placeholder"
        assert task.split_statements is False
    assert dag.get_task("daily_rollup").upstream_task_ids == {"events"}
    assert dag.get_task("user_dim").upstream_task_ids == {"events"}


def test_the_sql_is_not_templated_so_it_executes_byte_for_byte_as_rendered():
    plan = {
        "models": [
            {
                "name": "tricky",
                "reason": "directly_changed",
                "unit": "full_model",
                "sql": {
                    "dialect": "postgres",
                    # Jinja would eat the comment, choke on the braces, and try to
                    # load the last statement as a template file.
                    "statements": [
                        "SELECT '{# not a comment #}', '{{ ds }}', '{%'",
                        "SELECT 1 -- rendered from models/tricky.sql",
                    ],
                    "atomic": True,
                },
            }
        ]
    }
    dag = build_dag(plan, dag_id="tricky", conn_id="wh")
    task = dag.get_task("tricky")
    assert "sql" not in task.template_fields
    assert task.template_ext == ()
    render(dag, task, {"ds": "2026-09-22"})
    assert task.sql == plan["models"][0]["sql"]["statements"]


def test_default_args_cannot_turn_on_autocommit_or_inject_parameters():
    dag = build_dag(
        fixture("handwritten_v4_sql.json"),
        dag_id="defaults",
        conn_id="wh",
        dag_kwargs={
            "default_args": {"autocommit": True, "parameters": {"x": 1}, "split_statements": True, "retries": 3}
        },
    )
    task = dag.get_task("events")
    assert task.autocommit is False
    assert task.parameters is None
    assert task.split_statements is False
    assert task.retries == 3, "while everything else in default_args still applies"


# ── the command path ───────────────────────────────────────────────────────────


def test_command_template_builds_bash_operators_that_airflow_still_templates():
    dag = build_dag(
        fixture("handwritten_v4_sql.json"),
        dag_id="bash",
        command_template="psql -c \"{{ sql }}\" --set ds={{ ds }} # {{ model }} {{ partitions }}",
    )
    task = dag.get_task("user_dim")
    assert isinstance(task, BashOperator)
    assert isinstance(task, FreshetBashOperator)
    render(dag, task, {"ds": "2026-09-22"})
    assert task.bash_command == (
        'psql -c "' + ";\n".join(v4_statements(2)) + '" --set ds=2026-09-22 # user_dim '
    ), "Airflow rendered ds, and the braces in '{}'::jsonb came through untouched"


def test_task_docs_and_the_dag_doc_warn_about_widened_and_non_atomic_writes():
    dag = build_dag(fixture("handwritten_v4_sql.json"), dag_id="docs", conn_id="wh")
    user_dim = dag.get_task("user_dim")
    assert user_dim.doc_md.count("> **WARNING:**") == 2
    assert "widened to a full refresh" in user_dim.doc_md
    assert "non-atomic write" in user_dim.doc_md
    assert "WARNING" not in dag.get_task("events").doc_md

    assert "read before triggering" in dag.doc_md
    assert "`user_dim` was widened" in dag.doc_md
    assert "`user_dim` is a non-atomic write" in dag.doc_md
    assert "3 models rebuild" in dag.doc_md


def test_the_dag_is_unscheduled_and_tagged_unless_told_otherwise():
    dag = build_dag(fixture("golden.json"), dag_id="defaults", command_template="run {{ model }}")
    assert type(dag.timetable).__name__ == "NullTimetable", "a state plan has no cron; it is triggered"
    assert dag.catchup is False
    assert set(dag.tags) == {"freshet", "state-plan"}
    assert dag.description.startswith("Freshet state plan: 2 models rebuild")

    custom = build_dag(
        fixture("golden.json"),
        dag_id="custom",
        command_template="run {{ model }}",
        dag_kwargs={"tags": ["mine"], "doc_md": "my doc", "max_active_runs": 1},
    )
    assert set(custom.tags) == {"mine"}
    assert custom.doc_md == "my doc"
    assert custom.max_active_runs == 1


def test_a_graph_lets_the_dag_doc_report_what_was_pruned():
    dag = build_dag(
        fixture("captured_v2_fan_in.json"),
        dag_id="pruned",
        command_template="run {{ model }}",
        graph={"mart_revenue": ["dim_customer", "stg_orders"], "unchanged_a": [], "unchanged_b": ["stg_orders"]},
    )
    assert "3 of 5 models rebuild — 40% pruned" in dag.doc_md


# ── what the scheduler and the task runner see ─────────────────────────────────


def test_the_dag_survives_the_serialization_the_scheduler_reads():
    dag = build_dag(fixture("handwritten_v4_sql.json"), dag_id="serialized", conn_id="wh")
    back = roundtrip(dag)
    assert set(back.task_dict) == {"events", "daily_rollup", "user_dim"}
    assert back.get_task("user_dim").upstream_task_ids == {"events"}
    assert back.get_task("user_dim").task_type == "FreshetSQLExecuteQueryOperator"
    assert "non-atomic write" in back.get_task("user_dim").doc_md


def test_running_a_task_logs_the_planner_facts_and_warnings_before_it_executes(monkeypatch):
    dag = build_dag(fixture("handwritten_v4_sql.json"), dag_id="announce", conn_id="wh")
    task = dag.get_task("user_dim")

    events = []

    class Recorder:
        def info(self, message, *args):
            events.append(("info", message))

        def warning(self, message, *args):
            events.append(("warning", message))

        def __getattr__(self, name):
            return lambda *a, **k: None

    monkeypatch.setattr(FreshetSQLExecuteQueryOperator, "log", property(lambda self: Recorder()))
    monkeypatch.setattr(SQLExecuteQueryOperator, "execute", lambda self, context: events.append(("run", None)))

    task.execute({})

    # Airflow may log its own lines through the same logger (outside the task runner
    # it warns that execute was called directly), so look only at ours.
    ours = [
        (level, message)
        for level, message in events
        if level == "run" or message.startswith(("Freshet plan facts: ", "FRESHET WARNING: "))
    ]
    assert [level for level, _ in ours] == ["info", "warning", "warning", "run"], (
        "facts, then both warnings, and only then the statements"
    )
    facts = json.loads(ours[0][1][len("Freshet plan facts: "):])
    assert facts["model"] == "user_dim" and facts["sql"]["atomic"] is False
    assert "widened" in ours[1][1] and "non-atomic" in ours[2][1]
