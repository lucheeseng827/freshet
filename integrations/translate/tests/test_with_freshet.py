"""The classification, with the real freshet CLI: which models a translation changed.

Each test translates the sample project and checks what Freshet planned for it.
They skip without ``$FRESHET`` or a ``freshet`` on PATH.
"""

import json
import subprocess

import pytest

from freshet_translate import DIALECTS, translate_project
from freshet_translate.cli import main

#: SQL every warehouse spells alike: no rewrite, in any direction, except for `recent`,
#: whose ORDER BY is the one place the warehouses disagree. An ascending sort puts
#: NULLs last in these, and first in BigQuery and Databricks.
PORTABLE = {
    "daily": "select dt, count(*) as n, sum(amount) as total, avg(amount) as mean, max(note) as last_note "
    "from events where amount > 0 and note is not null group by dt",
    "joined": "select e.dt, e.user_id, d.total, case when e.amount > 10 then 'big' else 'small' end as size, "
    "coalesce(e.note, '') as note from events as e join daily as d on e.dt = d.dt",
    "recent": "select dt, total from daily where total between 1 and 100 order by dt",
    "uniq": "select distinct user_id from events where user_id in (1, 2, 3)",
}
NULLS_LAST = {"postgres", "snowflake", "redshift"}


def statuses(report):
    return {m.name: (m.status, m.because_of) for m in report.models}


@pytest.mark.parametrize("read,write", [(r, w) for r in DIALECTS for w in DIALECTS if r != w])
def test_portable_sql_is_unchanged_between_every_pair_but_where_null_order_differs(project, tmp_path, freshet, read, write):
    report = translate_project(project(**PORTABLE), tmp_path / "out", read, write, freshet=freshet)
    assert report.ok
    flagged = {m.name for m in report.models if m.status != "unchanged"}
    assert flagged == ({"recent"} if (read in NULLS_LAST) != (write in NULLS_LAST) else set()), [
        m.diff(read, write) for m in report.models if m.name in flagged
    ]


def test_a_rewrite_is_planned_and_so_is_what_reads_it(project, tmp_path, freshet):
    models = project(daily="select dt, sum(amount) as total from events where note ilike '%x%' group by dt")
    out = tmp_path / "models.bigquery"
    report = translate_project(models, out, "postgres", "bigquery", freshet=freshet)
    assert report.ok
    assert statuses(report) == {
        "events": ("unchanged", None),
        "daily": ("rewritten", None),
        "recent": ("downstream", "daily"),
    }
    written = sorted(p.name for p in out.iterdir())
    assert written == [
        ".freshet-translate",
        "daily.sql",
        "events.sql",
        "incremental.json",
        "materializations.json",
        "recent.sql",
        "sources.json",
    ]
    assert "LOWER(note) LIKE" in (out / "daily.sql").read_text()
    assert (out / "incremental.json").read_text() == (models / "incremental.json").read_text()


def test_respelling_alone_changes_nothing(project, tmp_path, freshet):
    models = project(daily="select dt::date as dt, Sum(amount) as total from events group by dt")
    report = translate_project(models, tmp_path / "out", "postgres", "snowflake", freshet=freshet)
    assert {m.status for m in report.models} == {"unchanged"}


def test_a_function_name_keeps_its_case_so_it_is_not_a_rewrite(project, tmp_path, freshet):
    """sqlglot writes an unknown function in upper case for Postgres and as written for
    BigQuery, and the fingerprint keeps function-name case. Without keeping names as
    written on both sides, every model calling a UDF would read as rewritten."""
    models = project(daily="select dt, sum(amount) as total, my_udf(note) as tag from events group by dt, tag")
    out = tmp_path / "out"
    report = translate_project(models, out, "postgres", "bigquery", freshet=freshet)
    daily = next(m for m in report.models if m.name == "daily")
    assert (daily.status, daily.untranslated_functions) == ("unchanged", ["my_udf"])
    assert "my_udf(note)" in (out / "daily.sql").read_text(), "the UDF's name as its author wrote it"


def test_against_the_original_every_respelled_model_would_read_as_changed(project, tmp_path, freshet):
    """Why the baseline exists. The translator respells `sum` as `SUM`, and the
    fingerprint keeps function-name case because a UDF's name can be case-sensitive.
    Planned against the source as written, the respelled `daily` is a change."""
    models = project()
    out = tmp_path / "out"
    assert {m.status for m in translate_project(models, out, "postgres", "snowflake", freshet=freshet).models} == {
        "unchanged"
    }
    state = tmp_path / "state.json"
    subprocess.run([freshet, "plan", "--project", str(models), "--state", str(state), "--commit"], check=True, capture_output=True)
    plan = json.loads(
        subprocess.run([freshet, "plan", "--project", str(out), "--state", str(state), "--json"], check=True, capture_output=True, text=True).stdout
    )
    assert "daily" in {m["name"] for m in plan["models"]}


def test_an_approximate_function_is_a_rewrite(project, tmp_path, freshet):
    models = project(daily="select dt, percentile_cont(0.5) within group (order by amount) as total from events group by dt")
    out = tmp_path / "out"
    report = translate_project(models, out, "postgres", "databricks", freshet=freshet)
    assert statuses(report)["daily"] == ("rewritten", None)
    assert "PERCENTILE_APPROX" in (out / "daily.sql").read_text(), "exact on one side, approximate on the other"


def test_an_override_stands_in_for_what_the_translator_cannot_write(project, tmp_path, freshet):
    models = project(numbers="select generate_series(1, 3) as n")
    hand = tmp_path / "hand"
    hand.mkdir()
    by_hand = "select n from (select 1 as n union all select 2 union all select 3) as t\n"
    (hand / "numbers.sql").write_text(by_hand)
    out = tmp_path / "out"
    report = translate_project(models, out, "postgres", "snowflake", overrides=hand, freshet=freshet)
    assert report.ok
    numbers = next(m for m in report.models if m.name == "numbers")
    assert (numbers.status, numbers.override) == ("rewritten", True)
    assert (out / "numbers.sql").read_text() == by_hand, "used as written"


def test_a_rerun_replaces_the_earlier_translation_whole(project, tmp_path, freshet):
    out = tmp_path / "out"
    translate_project(project(), out, "postgres", "snowflake", freshet=freshet)
    assert (out / "recent.sql").exists()
    report = translate_project(project(recent=None), out, "postgres", "snowflake", freshet=freshet)
    assert report.ok
    assert not (out / "recent.sql").exists(), "a model gone from the source is gone from the new side"
    assert oct(out.stat().st_mode & 0o777) == oct(0o777 & ~_umask()), "readable as a plain mkdir would make it"
    assert (out / ".freshet-translate").is_file(), "the record that this tool wrote it"


def test_a_promotion_that_fails_keeps_the_earlier_translation(project, tmp_path, freshet, monkeypatch):
    import freshet_translate.translate as t

    out = tmp_path / "out"
    translate_project(project(), out, "postgres", "snowflake", freshet=freshet)
    before = sorted(p.name for p in out.iterdir())
    real = t.os.replace
    calls = []

    def replace(src, dst):
        calls.append((str(src), str(dst)))
        if len(calls) == 2:  # the staged translation into place
            raise OSError("disk full")
        return real(src, dst)

    monkeypatch.setattr(t.os, "replace", replace)
    with pytest.raises(OSError, match="disk full"):
        translate_project(project(recent=None), out, "postgres", "snowflake", freshet=freshet)
    monkeypatch.undo()
    assert sorted(p.name for p in out.iterdir()) == before, "put back as it was"
    assert (out / "recent.sql").exists()
    assert [p.name for p in tmp_path.iterdir() if p.name.startswith(".out.")] == [], "nothing left beside it"


def test_the_json_report(project, tmp_path, freshet, capsys):
    models = project(daily="select dt, sum(amount) as total, my_udf(note) as tag from events where note ilike '%x%' group by dt, tag")
    code = main(["--project", str(models), "--from", "postgres", "--to", "bigquery", "--out", str(tmp_path / "out"), "--freshet", freshet, "--json"])
    assert code == 0
    doc = json.loads(capsys.readouterr().out)
    assert (doc["contract"], doc["from"], doc["to"], doc["ok"]) == ("freshet-translate/1", "postgres", "bigquery", True)
    assert doc["translator"].startswith("sqlglot ")
    daily = next(m for m in doc["models"] if m["name"] == "daily")
    assert daily == {
        "name": "daily",
        "status": "rewritten",
        "override": False,
        "because_of": None,
        "untranslated_functions": ["my_udf"],
        "error": None,
    }


def test_the_text_report_and_its_diff(project, tmp_path, freshet, capsys):
    models = project(daily="select dt, sum(amount) as total from events where note ilike '%x%' group by dt")
    code = main(["--project", str(models), "--from", "postgres", "--to", "bigquery", "--out", str(tmp_path / "out"), "--freshet", freshet, "--diff"])
    text = capsys.readouterr().out
    assert code == 0
    assert "3 model(s) into" in text and "1 rewritten, 1 downstream, 1 unchanged" in text
    lines = text.splitlines()
    assert lines[1].split() == ["rewritten", "daily", "rebuild", "it", "on", "bigquery", "and", "check", "it"]
    assert lines[2].split()[:2] == ["downstream", "recent"] and "it reads daily" in lines[2]
    assert "-  note ILIKE '%x%'" in text and "+  LOWER(note) LIKE LOWER('%x%')" in text


def _umask():
    import os

    mask = os.umask(0)
    os.umask(mask)
    return mask
