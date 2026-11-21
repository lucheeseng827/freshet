"""Translation and its refusals, with no freshet CLI.

Nothing here reaches the classification: each case is decided before Freshet is
asked, or answered by a stand-in that refuses.
"""

import os
import stat
import sys

import pytest
from sqlglot.errors import SqlglotError

from freshet_translate import TranslateError, translate_project, translate_sql
from freshet_translate.cli import main


def test_a_respelling_is_the_same_on_both_sides():
    t = translate_sql("select dt::date as dt, sum(amount) as total from events group by 1", "postgres", "snowflake")
    assert t.baseline == t.compared, "nothing in it is Postgres's own, so nothing was rewritten"


def test_function_name_case_is_not_compared():
    t = translate_sql("select count(*) as n, coalesce(note, '') as note from events", "bigquery", "postgres")
    assert "count(*)" in t.baseline.lower() and "COUNT(*)" in t.sql
    assert t.baseline == t.compared, "BigQuery and Postgres case the same calls differently"


def test_a_rewrite_is_what_differs_from_the_baseline():
    t = translate_sql("select dt from events where note ilike '%x%'", "postgres", "bigquery")
    assert "ILIKE" in t.baseline and t.baseline != t.compared
    assert "ILIKE" not in t.sql and "LOWER(note) LIKE" in t.sql, t.sql


def test_what_the_target_cannot_write_is_an_error_not_a_guess():
    with pytest.raises(SqlglotError):
        translate_sql("select generate_series(1, 3) as n", "postgres", "snowflake")


def test_a_model_is_one_statement():
    translate_sql("select 1 as a;", "postgres", "snowflake")
    with pytest.raises(ValueError, match="one statement, and this is 2"):
        translate_sql("select 1 as a; select 2 as b", "postgres", "snowflake")


def test_functions_the_translator_does_not_know_are_named():
    t = translate_sql("select my_udf(amount) as y from events", "postgres", "snowflake")
    assert t.unknown == ["my_udf"]
    assert "MY_UDF(amount)" in t.sql, "passed through, not dropped; Snowflake folds the name anyway"
    bq = translate_sql("select my_udf(amount) as y from events", "postgres", "bigquery")
    assert "my_udf(amount)" in bq.sql, "a BigQuery UDF's name is case-sensitive, so it is kept as written"


def test_a_dialect_freshet_does_not_render_is_refused(project, tmp_path):
    with pytest.raises(TranslateError, match="--from 'tsql' is not a dialect Freshet renders"):
        translate_project(project(), tmp_path / "out", "tsql", "snowflake")


def test_a_model_that_fails_fails_the_run_and_nothing_is_written(project, tmp_path):
    report = translate_project(
        project(numbers="select generate_series(1, 3) as n"), tmp_path / "out", "postgres", "snowflake"
    )
    assert not report.ok
    assert not (tmp_path / "out").exists(), "a partial project on the new side would plan without the model"
    by_name = {m.name: m for m in report.models}
    assert by_name["numbers"].status == "failed"
    assert "GenerateSeries" in by_name["numbers"].error
    assert {by_name[n].status for n in ("events", "daily", "recent")} == {"translated"}


def test_out_is_never_the_project_or_something_this_tool_did_not_write(project, tmp_path):
    models = project()
    with pytest.raises(TranslateError, match="is the project itself"):
        translate_project(models, models, "postgres", "snowflake")
    out = tmp_path / "out"
    out.mkdir()
    (out / "README.md").write_text("mine")
    with pytest.raises(TranslateError, match="holds README.md"):
        translate_project(models, out, "postgres", "snowflake")
    assert (out / "README.md").read_text() == "mine"


def test_a_project_of_sql_files_this_tool_did_not_write_is_not_replaced(project, tmp_path):
    # Only .sql files, so it looks exactly like a translation; without the marker it
    # is somebody's project, and a run aimed at it by mistake must not delete it.
    theirs = tmp_path / "models.snowflake"
    theirs.mkdir()
    (theirs / "events.sql").write_text("select 1 as hand_written")
    with pytest.raises(TranslateError, match="was not written by freshet-translate"):
        translate_project(project(), theirs, "postgres", "snowflake")
    assert (theirs / "events.sql").read_text() == "select 1 as hand_written"


def test_an_override_for_no_model_is_refused(project, tmp_path):
    hand = tmp_path / "hand"
    hand.mkdir()
    (hand / "dialy.sql").write_text("select 1 as dt")
    with pytest.raises(TranslateError, match="names no model of the project: dialy.sql"):
        translate_project(project(), tmp_path / "out", "postgres", "snowflake", overrides=hand)


@pytest.mark.skipif(sys.platform == "win32", reason="the stand-in is a shell script")
def test_a_project_freshet_cannot_plan_fails_the_run(project, tmp_path):
    fake = tmp_path / "freshet"
    fake.write_text("#!/bin/sh\necho 'error: building project from SQL' >&2\nexit 1\n")
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
    with pytest.raises(TranslateError, match="freshet could not plan the source:\nerror: building project"):
        translate_project(project(), tmp_path / "out", "postgres", "snowflake", freshet=str(fake))
    assert not (tmp_path / "out").exists()


def test_the_cli_refuses_an_unknown_dialect_as_a_usage_error(project, tmp_path, capsys):
    with pytest.raises(SystemExit) as e:
        main(["--project", str(project()), "--from", "oracle", "--to", "snowflake", "--out", str(tmp_path / "o")])
    assert e.value.code == 2
    assert "invalid choice: 'oracle'" in capsys.readouterr().err


def test_the_cli_says_which_models_failed_and_what_to_do(project, tmp_path, capsys):
    code = main([
        "--project", str(project(numbers="select generate_series(1, 3) as n")),
        "--from", "postgres", "--to", "snowflake", "--out", str(tmp_path / "out"),
    ])
    out = capsys.readouterr().out
    assert code == 1
    assert "1 of 4 model(s) could not be translated, so nothing was written" in out
    assert "numbers" in out and "--overrides" in out
    assert os.listdir(tmp_path) == ["models"]
