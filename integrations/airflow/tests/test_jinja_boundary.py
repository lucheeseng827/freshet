"""The boundary between Freshet's placeholders and Airflow's Jinja.

Placeholders are expanded in Python when the DAG is built; Airflow renders what is
left with Jinja when the task runs. These tests hold both halves of that line: our
expansion leaves Airflow's templating alone, and Airflow's templating leaves our
substituted values alone. The Jinja round trips use ``jinja2`` directly when it is
importable (it is wherever Airflow is), configured the way Airflow configures it.
"""

import pytest

from freshet_airflow import compile_plan, misspelled_placeholders, protect_from_jinja


def one_task(**model):
    base = {"name": "events", "reason": "directly_changed", "unit": "full_model"}
    return compile_plan({"models": [{**base, **model}]})[0]


def airflow_like_render(template, **context):
    jinja2 = pytest.importorskip("jinja2")
    from jinja2.sandbox import SandboxedEnvironment

    # What Airflow's DAG.get_template_env builds by default: sandboxed, strict about
    # undefined names, no autoescape, default delimiters.
    env = SandboxedEnvironment(undefined=jinja2.StrictUndefined, extensions=["jinja2.ext.do"])
    return env.from_string(template).render(**context)


def test_airflow_jinja_is_left_exactly_as_written():
    task = one_task()
    template = (
        "dbt run -s {{ model }} --vars 'ds: {{ ds }}' "
        "{% if params.full %}--full-refresh{% endif %} {# a comment #} {{ macros.ds_add(ds, 1) }}"
    )
    assert task.render(template) == (
        "dbt run -s events --vars 'ds: {{ ds }}' "
        "{% if params.full %}--full-refresh{% endif %} {# a comment #} {{ macros.ds_add(ds, 1) }}"
    )


def test_only_the_two_spellings_dagron_state_reads_are_expanded():
    task = one_task()
    assert task.render("{{ model }}|{{model}}") == "events|events"
    assert misspelled_placeholders("{{ model }} {{model}} {{ ds }} {{ models }} {{ params.model }}") == []


@pytest.mark.parametrize(
    "spelling",
    ["{{model }}", "{{  model }}", "{{- model -}}", "{{ model|upper }}", "{{ sql}}", "{{\tunit\t}}"],
)
def test_a_near_miss_spelling_is_reported_rather_than_left_for_jinja(spelling):
    assert misspelled_placeholders(f"run {spelling} now") == [spelling]


def test_protected_values_survive_airflows_jinja_pass_unchanged():
    nasty = [
        "plain",
        "'{}'::jsonb",
        "SELECT '{{ ds }}'",
        "regexp_replace(x, '{#', '')",
        "{% if %}",
        "a{",
        "{{{",
        "}}",
        "{ '{' }",
    ]
    for value in nasty:
        assert airflow_like_render(protect_from_jinja(value)) == value, value


def test_a_value_ending_in_a_brace_cannot_join_the_template_text_after_it():
    # The placeholder's value ends in `{` and the template continues with `%`:
    # unprotected, the two would open a Jinja block.
    protected = "echo " + protect_from_jinja("a{") + "% done {{ ds }}"
    assert airflow_like_render(protected, ds="2026-09-22") == "echo a{% done 2026-09-22"


def test_rendered_sql_with_braces_reaches_the_shell_verbatim_while_ds_still_renders():
    task = one_task(
        sql={
            "dialect": "postgres",
            "statements": ["INSERT INTO t SELECT '{}'::jsonb, '{# not a comment #}'"],
            "atomic": True,
        }
    )
    built = task.render("psql -c \"{{ sql }}\" # {{ ds }}", escape=protect_from_jinja)
    assert airflow_like_render(built, ds="2026-09-22") == (
        "psql -c \"INSERT INTO t SELECT '{}'::jsonb, '{# not a comment #}'\" # 2026-09-22"
    )


def test_unprotected_the_same_sql_would_have_been_eaten_by_jinja():
    # Why protect_from_jinja exists: the same command without it loses SQL silently.
    task = one_task(
        sql={"dialect": "postgres", "statements": ["SELECT '{# x #}' AS c"], "atomic": True}
    )
    assert airflow_like_render(task.render("{{ sql }}")) == "SELECT '' AS c"
