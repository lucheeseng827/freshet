"""Contract v4: the SQL Freshet rendered, carried per model.

``handwritten_v4_sql.json`` is hand-written to the v4 shape as specified while the
producer side is being built: the v3 capture's three models, each with a ``sql``
block, and ``user_dim`` deliberately non-atomic. Swap it for a payload captured from
the real planner once one exists; these tests should pass against it unchanged,
except where they pin this fixture's own statements.
"""

import pathlib
import re

import pytest

from freshet_airflow import ContractViolation, PlannerFailed, compile_plan, parse_plan

FIXTURES = pathlib.Path(__file__).with_name("fixtures")


def fixture(name):
    return (FIXTURES / name).read_text()


def tasks_by_id():
    return {t.task_id: t for t in compile_plan(fixture("handwritten_v4_sql.json"))}


def test_a_v4_plan_carries_the_statements_for_each_rebuild():
    plan = parse_plan(fixture("handwritten_v4_sql.json"))
    events = plan.models[0]
    assert events.sql is not None
    assert events.sql.dialect == "postgres"
    assert events.sql.atomic is True
    assert events.sql.statements == (
        "DELETE FROM events WHERE dt IN ('2026-06-20')",
        "INSERT INTO events SELECT * FROM (SELECT id, user_id, dt, seen_at AS last_seen "
        "FROM raw_events) AS freshet_src WHERE dt IN ('2026-06-20')",
    )
    assert plan.models[2].sql.atomic is False, "user_dim is the non-atomic one"


def test_v4_is_additive_so_the_edges_are_exactly_the_v3_edges():
    v3 = compile_plan(fixture("captured_v3_with_a_widening.json"))
    v4 = compile_plan(fixture("handwritten_v4_sql.json"))
    assert [(t.task_id, t.upstream) for t in v4] == [(t.task_id, t.upstream) for t in v3]


def test_the_sql_placeholder_joins_the_statements_in_order():
    events = tasks_by_id()["events"]
    assert events.render("{{ sql }}") == (
        "DELETE FROM events WHERE dt IN ('2026-06-20');\n"
        "INSERT INTO events SELECT * FROM (SELECT id, user_id, dt, seen_at AS last_seen "
        "FROM raw_events) AS freshet_src WHERE dt IN ('2026-06-20')"
    )
    assert events.render("{{sql}}") == events.render("{{ sql }}"), "both spellings"


def test_the_dialect_placeholder_names_the_dialect_in_both_spellings():
    events = tasks_by_id()["events"]
    assert events.render("psql-for-{{ dialect }}/{{dialect}}") == "psql-for-postgres/postgres"


def test_a_plan_without_sql_expands_the_v4_placeholders_to_nothing():
    for task in compile_plan(fixture("captured_v3_with_a_widening.json")):
        assert task.render("[{{ sql }}][{{ dialect }}]") == "[][]"


def test_the_facts_record_the_sql_so_the_run_says_what_it_executed():
    facts = tasks_by_id()["user_dim"].facts
    assert facts["sql"] == {
        "dialect": "postgres",
        "statements": [
            "DROP TABLE IF EXISTS user_dim__freshet_new",
            "CREATE TABLE user_dim__freshet_new AS SELECT user_id, max(last_seen) AS last_seen, "
            "'{}'::jsonb AS traits FROM events GROUP BY user_id",
            "DROP TABLE IF EXISTS user_dim",
            "ALTER TABLE user_dim__freshet_new RENAME TO user_dim",
        ],
        "atomic": False,
    }
    # ...alongside everything v3 already recorded.
    assert facts["replace"] == "full_refresh"
    assert "no partitions to replace in place" in facts["replace_widened"]


def test_a_plan_without_sql_records_no_sql_key_at_all():
    for task in compile_plan(fixture("captured_v3_with_a_widening.json")):
        assert "sql" not in task.facts, "absent, not null"


def test_a_substituted_statement_is_never_scanned_for_placeholders_again():
    # Rendered SQL can contain anything in a string literal, including the text of a
    # placeholder. Expansion is one pass, so the literal survives as written.
    payload = {
        "models": [
            {
                "name": "notes",
                "reason": "directly_changed",
                "unit": "full_model",
                "sql": {
                    "dialect": "postgres",
                    "statements": ["INSERT INTO notes VALUES ('use {{ model }} here')"],
                    "atomic": True,
                },
            }
        ]
    }
    task = compile_plan(payload)[0]
    assert task.render("{{ sql }}") == "INSERT INTO notes VALUES ('use {{ model }} here')"


@pytest.mark.parametrize(
    "sql, where",
    [
        ({"statements": ["select 1"], "atomic": True}, "missing required field `dialect`"),
        ({"dialect": "postgres", "atomic": True}, "missing required field `statements`"),
        ({"dialect": "postgres", "statements": ["select 1"]}, "missing required field `atomic`"),
        ({"dialect": "postgres", "statements": "select 1", "atomic": True}, "sql.statements"),
        ({"dialect": "postgres", "statements": ["select 1", 2], "atomic": True}, "sql.statements[1]"),
        ({"dialect": "postgres", "statements": ["select 1"], "atomic": "yes"}, "sql.atomic"),
        ({"dialect": "postgres", "statements": ["select 1"], "atomic": 1}, "sql.atomic"),
    ],
)
def test_a_malformed_sql_block_is_refused_with_the_path_to_it(sql, where):
    payload = {"models": [{"name": "m", "reason": "directly_changed", "unit": "full_model", "sql": sql}]}
    with pytest.raises(ContractViolation, match=re.escape(where)):
        parse_plan(payload)


def test_a_v4_error_plan_is_a_planner_failure_with_its_code_and_message():
    failed = (
        '{"models": [], "error": {"code": "plan_error", '
        '"message": "parsing model `m`: sql parser error: Expected SELECT"}}'
    )
    with pytest.raises(PlannerFailed) as caught:
        compile_plan(failed)
    assert caught.value.code == "plan_error"
    assert "Expected SELECT" in caught.value.message
    assert "planner failed [plan_error]" in str(caught.value)


def test_next_state_rides_along_opaquely_and_is_never_interpreted():
    opaque = {"fingerprints": {"events": 1}, "watermarks": {"events": "2026-06-20"}, "v": [1, None]}
    payload = {
        "models": [{"name": "events", "reason": "directly_changed", "unit": "full_model"}],
        "next_state": opaque,
    }
    assert parse_plan(payload).next_state == opaque
